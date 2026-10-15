package modules

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/deckhouse/deckhouse/pkg/log"
	"github.com/gofrs/uuid/v5"
	"github.com/kennygrant/sanitize"
	"github.com/werf/nelm/pkg/action"
	"go.opentelemetry.io/otel"
	"helm.sh/helm/v3/pkg/storage/driver"

	"github.com/flant/addon-operator/pkg"
	"github.com/flant/addon-operator/pkg/helm"
	"github.com/flant/addon-operator/pkg/helm/client"
	"github.com/flant/addon-operator/pkg/helm/helm3lib"
	"github.com/flant/addon-operator/pkg/metrics"
	"github.com/flant/addon-operator/pkg/utils"
	"github.com/flant/kube-client/manifest"
	"github.com/flant/shell-operator/pkg/utils/measure"
)

const (
	LabelMaintenanceNoResourceReconciliation = "maintenance.deckhouse.io/no-resource-reconciliation"
	helmModuleServiceName                    = "helm-module"
)

// HelmModule representation of the module, which has Helm Chart and could be installed with the helm lib
type HelmModule struct {
	// Name of the module
	name string
	// default namespace for module
	defaultNamespace string
	// Path of the module on the fs
	path string

	values utils.Values

	tmpDir string

	dependencies *HelmModuleDependencies
	validator    HelmValuesValidator

	logger *log.Logger

	additionalLabels map[string]string
}

type HelmValuesValidator interface {
	ValidateModuleHelmValues(string, utils.Values) error
}

type HelmResourceManager interface {
	GetAbsentResources(manifests []manifest.Manifest, defaultNamespace string) ([]manifest.Manifest, error)
}

type MetricsStorage interface {
	HistogramObserve(metric string, value float64, labels map[string]string, buckets []float64)
}

type HelmModuleDependencies struct {
	HelmClientFactory   *helm.ClientFactory
	HelmResourceManager HelmResourceManager
	MetricsStorage      MetricsStorage
	HelmValuesValidator HelmValuesValidator
}

var ErrModuleIsNotHelm = errors.New("module is not a helm")

// NewHelmModule build HelmModule from the Module templates and values + global values
func NewHelmModule(bm *BasicModule, namespace string, tmpDir string, deps *HelmModuleDependencies, validator HelmValuesValidator, opts ...ModuleOption) (*HelmModule, error) {
	moduleValues := bm.GetValues(false)

	chartValues := map[string]interface{}{
		"global": bm.dc.GlobalValuesGetter.GetValues(false),
		utils.ModuleNameToValuesKey(bm.GetName()): moduleValues,
	}

	additionalLabels := make(map[string]string)
	// LabelMaintenanceNoResourceReconciliation is now only stored in Helm release metadata,and in resources annotations
	if bm.GetMaintenanceState() != Managed {
		additionalLabels[LabelMaintenanceNoResourceReconciliation] = ""
	}

	hm := &HelmModule{
		name:             bm.GetName(),
		defaultNamespace: namespace,
		path:             bm.Path,
		values:           chartValues,
		tmpDir:           tmpDir,
		dependencies:     deps,
		validator:        validator,
		additionalLabels: additionalLabels,
	}

	for _, opt := range opts {
		opt.Apply(hm)
	}

	if hm.logger == nil {
		hm.logger = log.NewLogger().Named("helm-module")
	}

	hm.logger = hm.logger.With(slog.String(pkg.LogKeyModule, hm.name))

	isHelm, err := hm.isHelmChart()
	if err != nil {
		return nil, fmt.Errorf("create HelmModule failed: %w", err)
	}

	if !isHelm {
		hm.logger.Info("module has neither Chart.yaml nor templates/ dir, is't not a helm chart",
			slog.String(pkg.LogKeyName, bm.GetName()))

		return nil, ErrModuleIsNotHelm
	}

	return hm, nil
}

func (hm *HelmModule) WithLogger(logger *log.Logger) {
	hm.logger = logger.With(slog.String(pkg.LogKeyModule, hm.name))
}

// isHelmChart check, could it be considered as helm chart or not
func (hm *HelmModule) isHelmChart() (bool, error) {
	chartPath := filepath.Join(hm.path, "Chart.yaml")

	_, err := os.Stat(chartPath)
	if err == nil {
		// Chart.yaml exists, consider this module as helm chart
		return true, nil
	}

	if os.IsNotExist(err) {
		// Chart.yaml does not exist
		// check that templates/ dir exists
		_, err = os.Stat(filepath.Join(hm.path, "templates"))
		if err == nil {
			return true, nil
		}

		if os.IsNotExist(err) {
			// if templates not exists - it's not a helm module
			return false, nil
		}
	}

	return false, err
}

// checkHelmValues returns error if there is a wrong patch or values are not satisfied
// a Helm values contract defined by schemas in 'openapi' directory.
func (hm *HelmModule) checkHelmValues() error {
	// TODO: key
	return hm.validator.ValidateModuleHelmValues(utils.ModuleNameToValuesKey(hm.name), hm.values)
}

var ErrReleaseIsUnmanaged = errors.New("release is unmanaged")

// HelmRunResult describes the outcome of a successful Helm run. It carries
// everything the subsequent monitor-publishing recovery phase needs, so a
// failure to publish the monitor is retried without ever re-running Helm.
type HelmRunResult struct {
	// Revision of the release that is current after the run.
	Revision string
	// ManifestChecksum is the moduleChecksum of the rendered manifests.
	ManifestChecksum string
	// RenderedManifests is the raw rendered YAML of the release manifests.
	// The monitor publisher re-parses it on every attempt, so a manifest
	// parsing failure after a successful upgrade only retries publishing.
	RenderedManifests string
	// DefaultNamespace of the module.
	DefaultNamespace string
	// RanUpgrade reports whether this run produced a new release revision.
	RanUpgrade bool
}

// RunHelmInstall installs or upgrades a Helm release for the module.
// The `state` parameter determines the maintenance state of the release:
// - If `state` is `Unmanaged`, a release label check is triggered, and the Helm upgrade is skipped.
//
// Monitor publishing is intentionally not part of the Helm run: it is a
// separate, independently retryable recovery phase driven by the returned
// HelmRunResult.
func (hm *HelmModule) RunHelmInstall(ctx context.Context, logLabels map[string]string, state MaintenanceState) (*HelmRunResult, error) {
	_, span := otel.Tracer(helmModuleServiceName).Start(ctx, "RunHelmInstall")
	defer span.End()

	metricLabels := map[string]string{
		pkg.MetricKeyModule:     hm.name,
		pkg.MetricKeyActivation: logLabels[pkg.LogKeyEventType],
	}

	defer measure.Duration(func(d time.Duration) {
		hm.dependencies.MetricsStorage.HistogramObserve(metrics.ModuleHelmSeconds, d.Seconds(), metricLabels, nil)
	})()

	logEntry := utils.EnrichLoggerWithLabels(hm.logger, logLabels)

	err := hm.checkHelmValues()
	if err != nil {
		return nil, fmt.Errorf("check helm values: %v", err)
	}

	// TODO Now it returns just a module name. Should it be cleaned from special symbols?
	helmReleaseName := hm.name

	valuesPath, err := hm.PrepareValuesYamlFile()
	if err != nil {
		return nil, err
	}
	defer os.Remove(valuesPath)

	helmClientOptions := []helm.ClientOption{
		helm.WithExtraLabels(hm.additionalLabels),
		helm.WithLogLabels(logLabels),
	}

	helmClient := hm.dependencies.HelmClientFactory.NewClient(hm.logger.Named("helm-client"), helmClientOptions...)

	if state == Unmanaged {
		isUnmanaged, err := helmClient.GetReleaseLabels(helmReleaseName, LabelMaintenanceNoResourceReconciliation)
		if err != nil && !errors.Is(err, helm3lib.ErrLabelIsNotFound) && !errors.Is(err, driver.ErrReleaseNotFound) {
			// Also check for nelm ReleaseNotFoundError
			var releaseNotFoundErr *action.ReleaseNotFoundError
			if !errors.As(err, &releaseNotFoundErr) {
				logEntry.Warn("get release label failed", log.Err(err), slog.String(pkg.LogKeyRelease, helmReleaseName))
				return nil, fmt.Errorf("get release label failed: %w", err)
			}

			logEntry.Debug("release not found when checking unmanaged state", slog.String(pkg.LogKeyRelease, helmReleaseName))
		}

		if isUnmanaged == "true" {
			logEntry.Info("helm release is Unmanaged, skip helm upgrade", slog.String(pkg.LogKeyRelease, helmReleaseName))

			return nil, ErrReleaseIsUnmanaged
		}
	}

	span.AddEvent("ModuleRun-HelmPhase-helm-render")

	// Prepare release labels
	releaseLabels := map[string]string{
		LabelMaintenanceNoResourceReconciliation: "false",
	}

	if state == Unmanaged {
		releaseLabels[LabelMaintenanceNoResourceReconciliation] = "true"
	}

	// Render templates to prevent excess helm runs.
	var renderedManifests string

	func() {
		metricLabels := map[string]string{
			pkg.MetricKeyModule:     hm.name,
			pkg.MetricKeyActivation: logLabels[pkg.LogKeyEventType],
			pkg.MetricKeyOperation:  "template",
		}

		defer measure.Duration(func(d time.Duration) {
			hm.dependencies.MetricsStorage.HistogramObserve(metrics.HelmOperationSeconds, d.Seconds(), metricLabels, nil)
		})()

		renderedManifests, err = helmClient.Render(
			helmReleaseName,
			hm.path,
			[]string{valuesPath},
			[]string{},
			releaseLabels,
			hm.defaultNamespace,
			false,
		)
	}()

	if err != nil {
		return nil, err
	}

	checksum := utils.CalculateStringsChecksum(renderedManifests)

	// Add checksum to release labels
	releaseLabels["moduleChecksum"] = checksum

	manifests, err := manifest.ListFromYamlDocs(renderedManifests)
	if err != nil {
		return nil, err
	}

	logEntry.Debug("chart has resources", slog.Int(pkg.LogKeyCount, len(manifests)))

	span.AddEvent("ModuleRun-HelmPhase-helm-check-upgrade")
	// Skip upgrades if nothing is changed
	var runUpgradeRelease bool

	func() {
		metricLabels := map[string]string{
			pkg.MetricKeyModule:     hm.name,
			pkg.MetricKeyActivation: logLabels[pkg.LogKeyEventType],
			pkg.MetricKeyOperation:  "check-upgrade",
		}

		defer measure.Duration(func(d time.Duration) {
			hm.dependencies.MetricsStorage.HistogramObserve(metrics.HelmOperationSeconds, d.Seconds(), metricLabels, nil)
		})()

		runUpgradeRelease, err = hm.shouldRunHelmUpgrade(helmClient, helmReleaseName, checksum, manifests, logLabels)
	}()

	if err != nil {
		return nil, err
	}

	if !runUpgradeRelease {
		// Release is unchanged: no Helm side effects happened. The monitor
		// publisher reconciles the monitor against the current release
		// (starts it when missing, otherwise just confirms the revision).
		return hm.helmRunResult(helmClient, helmReleaseName, checksum, renderedManifests, false)
	}

	span.AddEvent("ModuleRun-HelmPhase-helm-upgrade")
	// Run helm upgrade. Trace and measure its time.
	func() {
		metricLabels := map[string]string{
			pkg.MetricKeyModule:     hm.name,
			pkg.MetricKeyActivation: logLabels[pkg.LogKeyEventType],
			pkg.MetricKeyOperation:  "upgrade",
		}

		defer measure.Duration(func(d time.Duration) {
			hm.dependencies.MetricsStorage.HistogramObserve(metrics.HelmOperationSeconds, d.Seconds(), metricLabels, nil)
		})()

		err = helmClient.UpgradeRelease(
			helmReleaseName,
			hm.path,
			[]string{valuesPath},
			[]string{},
			releaseLabels,
			hm.defaultNamespace,
		)
	}()

	if err != nil {
		return nil, err
	}

	// The release was upgraded successfully. The resources monitor is
	// published by the separate monitor-publish recovery phase, so a
	// manifest parse or monitor start failure only retries that phase
	// instead of re-running Helm.
	return hm.helmRunResult(helmClient, helmReleaseName, checksum, renderedManifests, true)
}

// helmRunResult builds the HelmRunResult of a finished run, anchoring the
// monitor-publish phase to the release revision that is current in the cluster.
func (hm *HelmModule) helmRunResult(helmClient client.HelmClient, releaseName, checksum, renderedManifests string, ranUpgrade bool) (*HelmRunResult, error) {
	revision, _, err := helmClient.LastReleaseStatus(releaseName)
	if err != nil {
		return nil, fmt.Errorf("get revision of release %q after helm run: %w", releaseName, err)
	}

	return &HelmRunResult{
		Revision:          revision,
		ManifestChecksum:  checksum,
		RenderedManifests: renderedManifests,
		DefaultNamespace:  hm.defaultNamespace,
		RanUpgrade:        ranUpgrade,
	}, nil
}

// RenderCurrentRelease renders the module chart and reads the live release
// revision without performing any Helm mutation. The monitor-publish recovery
// phase uses it to (re-)fetch manifests when its registered transaction is
// missing or stale, so recovery never re-runs an already confirmed Helm task.
func (hm *HelmModule) RenderCurrentRelease(state MaintenanceState) (*HelmRunResult, error) {
	if err := hm.checkHelmValues(); err != nil {
		return nil, fmt.Errorf("check helm values: %v", err)
	}

	valuesPath, err := hm.PrepareValuesYamlFile()
	if err != nil {
		return nil, err
	}
	defer os.Remove(valuesPath)

	helmClient := hm.dependencies.HelmClientFactory.NewClient(
		hm.logger.Named("helm-client"),
		helm.WithExtraLabels(hm.additionalLabels),
	)

	releaseLabels := map[string]string{
		LabelMaintenanceNoResourceReconciliation: strconv.FormatBool(state == Unmanaged),
	}

	rendered, err := helmClient.Render(
		hm.name,
		hm.path,
		[]string{valuesPath},
		[]string{},
		releaseLabels,
		hm.defaultNamespace,
		false,
	)
	if err != nil {
		return nil, err
	}

	checksum := utils.CalculateStringsChecksum(rendered)

	revision, _, err := helmClient.LastReleaseStatus(hm.name)
	if err != nil {
		return nil, fmt.Errorf("get revision of release %q: %w", hm.name, err)
	}

	return &HelmRunResult{
		Revision:          revision,
		ManifestChecksum:  checksum,
		RenderedManifests: rendered,
		DefaultNamespace:  hm.defaultNamespace,
	}, nil
}

// If all these conditions aren't met, helm upgrade can be skipped.
func (hm *HelmModule) shouldRunHelmUpgrade(helmClient client.HelmClient, releaseName string, checksum string, manifests []manifest.Manifest, logLabels map[string]string) (bool, error) {
	logEntry := utils.EnrichLoggerWithLabels(hm.logger, logLabels)

	revision, status, err := helmClient.LastReleaseStatus(releaseName)

	if revision == "0" {
		logEntry.Debug("helm release not exists: should run upgrade", slog.String(pkg.LogKeyRelease, releaseName))
		return true, nil
	}

	if err != nil {
		return false, err
	}

	// Run helm upgrade if last release isn't `deployed`
	if strings.ToLower(status) != "deployed" {
		logEntry.Debug("helm release: should run upgrade",
			slog.String(pkg.LogKeyRelease, releaseName),
			slog.String(pkg.LogKeyStatus, strings.ToLower(status)))

		return true, nil
	}

	// Get values for a non-failed release.
	recordedChecksum, err := helmClient.GetReleaseChecksum(releaseName)
	if err != nil {
		logEntry.Debug("helm release get values error, no upgrade",
			slog.String(pkg.LogKeyRelease, releaseName),
			log.Err(err))

		return false, err
	}

	// Calculate a checksum of current values and compare to a stored checksum.
	// Run helm upgrade if checksum is changed.
	if recordedChecksum != checksum {
		logEntry.Debug("helm release checksum is changed: should run upgrade",
			slog.String(pkg.LogKeyRelease, releaseName),
			slog.String(pkg.LogKeyChecksum, recordedChecksum),
			slog.String(pkg.LogKeyNewChecksum, checksum))

		return true, nil
	}

	// Check if there are absent resources
	absent, err := hm.dependencies.HelmResourceManager.GetAbsentResources(manifests, hm.defaultNamespace)
	if err != nil {
		return false, err
	}

	// Run helm upgrade if there are absent resources
	if len(absent) > 0 {
		logEntry.Debug("helm release has absent resources: should run upgrade",
			slog.String(pkg.LogKeyRelease, releaseName),
			slog.Int(pkg.LogKeyCount, len(absent)),
		)

		return true, nil
	}

	logEntry.Debug("helm release is unchanged: skip release upgrade",
		slog.String(pkg.LogKeyRelease, releaseName))

	return false, nil
}

func (hm *HelmModule) PrepareValuesYamlFile() (string, error) {
	data, err := hm.values.YamlBytes()
	if err != nil {
		return "", err
	}

	path := filepath.Join(hm.tmpDir, fmt.Sprintf("%s.module-values.yaml-%s", hm.safeName(), uuid.Must(uuid.NewV4()).String()))

	err = utils.DumpData(path, data)
	if err != nil {
		return "", err
	}

	hm.logger.Debug("Prepared module helm values info",
		slog.String(pkg.LogKeyModuleName, hm.name),
		slog.String(pkg.LogKeyValues, hm.values.DebugString()))

	return path, nil
}

func (hm *HelmModule) safeName() string {
	return sanitize.BaseName(hm.name)
}

func (hm *HelmModule) Render(namespace string, debug bool, state MaintenanceState) (string, error) {
	return hm.renderChart(hm.path, namespace, debug, state)
}

// renderChart renders the chart located at chartPath with the module values.
func (hm *HelmModule) renderChart(chartPath, namespace string, debug bool, state MaintenanceState) (string, error) {
	if namespace == "" {
		namespace = "default"
	}

	valuesPath, err := hm.PrepareValuesYamlFile()
	if err != nil {
		return "", err
	}
	defer os.Remove(valuesPath)

	helmClientOptions := []helm.ClientOption{
		helm.WithExtraLabels(hm.additionalLabels),
	}

	releaseLabels := map[string]string{
		LabelMaintenanceNoResourceReconciliation: strconv.FormatBool(state == Unmanaged),
	}

	return hm.dependencies.HelmClientFactory.NewClient(hm.logger.Named("helm-client"), helmClientOptions...).Render(hm.name, chartPath, []string{valuesPath}, nil, releaseLabels, namespace, debug)
}

// ConversionWebhookKind is the kind of the rendered resource that carries a CRD
// conversion webhook configuration. Such resources must be applied to the cluster
// before the main helm release, so the release can adopt them afterwards.
const ConversionWebhookKind = "ConversionWebhook"

// RenderConversionWebhooks renders the given templates and returns only the
// manifests of kind ConversionWebhook. They are extracted from the chart so the
// caller can apply them ahead of the main helm install, letting the subsequent
// release adopt the already-present resources. If the reduced chart fails to
// render (e.g. a template depends on another non-helper template), it falls back
// to the full chart.
func (hm *HelmModule) RenderConversionWebhooks(namespace string, state MaintenanceState, templates []string) ([]manifest.Manifest, error) {
	rendered, err := hm.renderConversionWebhookChart(namespace, state, templates)
	if err != nil {
		hm.logger.Warn("render conversion webhooks chart, fallback to full render", log.Err(err))

		rendered, err = hm.Render(namespace, false, state)
		if err != nil {
			return nil, fmt.Errorf("render templates: %w", err)
		}
	}

	all, err := manifest.ListFromYamlDocs(rendered)
	if err != nil {
		return nil, fmt.Errorf("split rendered manifests: %w", err)
	}

	webhooks := make([]manifest.Manifest, 0)

	for _, m := range all {
		if m.Kind() == ConversionWebhookKind {
			webhooks = append(webhooks, m)
		}
	}

	return webhooks, nil
}

// renderConversionWebhookChart builds a reduced chart with the given templates in
// a temporary directory and renders it.
func (hm *HelmModule) renderConversionWebhookChart(namespace string, state MaintenanceState, templates []string) (string, error) {
	chartDir, err := os.MkdirTemp(hm.tmpDir, hm.safeName()+"-conversion-webhooks-")
	if err != nil {
		return "", fmt.Errorf("create chart dir: %w", err)
	}
	defer os.RemoveAll(chartDir)

	if err = buildConversionWebhookChart(hm.path, chartDir, templates); err != nil {
		return "", fmt.Errorf("build chart: %w", err)
	}

	return hm.renderChart(chartDir, namespace, false, state)
}

// conversionWebhookChartDirs are optional module dirs mirrored into the reduced chart:
// charts/ holds library charts (helm_lib), webhooks/ holds handlers read via .Files.Get.
var conversionWebhookChartDirs = []string{"charts", "webhooks"}

// buildConversionWebhookChart mirrors into dst the parts of the module chart
// needed to render the given templates: the conversionWebhookChartDirs, the
// template helpers ("_"-prefixed partials) and the templates themselves.
// Directories are created for real and files are symlinked, so loaders that do
// not follow directory symlinks see the whole tree.
func buildConversionWebhookChart(modulePath, dst string, templates []string) error {
	for _, dir := range conversionWebhookChartDirs {
		if err := mirrorTree(filepath.Join(modulePath, dir), filepath.Join(dst, dir)); err != nil && !errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("mirror %s: %w", dir, err)
		}
	}

	helpers, err := getTemplateHelpers(filepath.Join(modulePath, "templates"))
	if err != nil {
		return fmt.Errorf("get template helpers: %w", err)
	}

	linked := make(map[string]struct{}, len(templates)+len(helpers))

	for _, file := range append(helpers, templates...) {
		if _, ok := linked[file]; ok {
			continue
		}

		linked[file] = struct{}{}

		if err = linkFile(filepath.Join(modulePath, "templates", file), filepath.Join(dst, "templates", file)); err != nil {
			return fmt.Errorf("link template %q: %w", file, err)
		}
	}

	return nil
}

// getTemplateHelpers returns paths, relative to templatesDir, of helm partials
// (files prefixed with "_") that define helpers used by the templates.
func getTemplateHelpers(templatesDir string) ([]string, error) {
	var helpers []string

	err := filepath.WalkDir(templatesDir, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}

		if d.IsDir() || !strings.HasPrefix(d.Name(), "_") {
			return nil
		}

		rel, err := filepath.Rel(templatesDir, path)
		if err != nil {
			return err
		}

		helpers = append(helpers, rel)

		return nil
	})
	if err != nil {
		return nil, err
	}

	return helpers, nil
}

// mirrorTree recreates the src directory tree in dst following symlinks, and
// links every regular file to its resolved source.
func mirrorTree(src, dst string) error {
	info, err := os.Stat(src)
	if err != nil {
		return err
	}

	if !info.IsDir() {
		return linkFile(src, dst)
	}

	if err = os.MkdirAll(dst, 0o755); err != nil {
		return err
	}

	entries, err := os.ReadDir(src)
	if err != nil {
		return err
	}

	for _, entry := range entries {
		if err = mirrorTree(filepath.Join(src, entry.Name()), filepath.Join(dst, entry.Name())); err != nil {
			return err
		}
	}

	return nil
}

// linkFile symlinks dst to the resolved src, creating parent directories of dst.
func linkFile(src, dst string) error {
	resolved, err := filepath.EvalSymlinks(src)
	if err != nil {
		return err
	}

	resolved, err = filepath.Abs(resolved)
	if err != nil {
		return err
	}

	if err = os.MkdirAll(filepath.Dir(dst), 0o755); err != nil {
		return err
	}

	return os.Symlink(resolved, dst)
}
