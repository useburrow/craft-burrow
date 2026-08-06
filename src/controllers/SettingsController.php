<?php
namespace burrow\Burrow\controllers;

use Craft;
use craft\web\Controller;
use yii\web\Response;

use burrow\Burrow\jobs\CleanupOutboxRetentionJob;
use burrow\Burrow\Plugin;

class SettingsController extends Controller
{
    protected array|bool|int $allowAnonymous = false;

    public function actionIndex(): Response
    {
        $this->requirePermission('accessPlugin-burrow');

        $state = Plugin::getInstance()->getState()->getState();
        if (!empty($state['onboardingCompleted'])) {
            return $this->redirect('burrow/settings?section=overview');
        }

        return $this->redirect('burrow/setup');
    }

    public function actionSetup(): Response
    {
        $this->requirePermission('accessPlugin-burrow');

        $plugin = Plugin::getInstance();
        $runtimeState = $plugin->getState()->getState();
        $relink = $this->isRelinkRequest();
        if (!empty($runtimeState['onboardingCompleted']) && !$relink) {
            return $this->redirect('burrow/settings?section=overview');
        }

        return $this->renderTemplate('burrow/settings/index', $this->buildWizardViewData($relink));
    }

    public function actionConfigure(): Response
    {
        $this->requirePermission('accessPlugin-burrow');

        $plugin = Plugin::getInstance();
        $runtimeState = $plugin->getState()->getState();
        if (empty($runtimeState['onboardingCompleted'])) {
            return $this->redirect('burrow/setup');
        }

        $viewData = $this->buildConfigureViewData();
        $sections = $viewData['settingsSections'];
        $section = (string)Craft::$app->getRequest()->getQueryParam('section', 'overview');
        if (!array_key_exists($section, $sections)) {
            $section = 'overview';
        }
        $viewData['section'] = $section;

        return $this->renderTemplate('burrow/settings/configure', $viewData);
    }

    /**
     * @return array<string,mixed>
     */
    private function buildWizardViewData(bool $relinkMode = false): array
    {
        $plugin = Plugin::getInstance();
        $integrationsService = $plugin->getIntegrations();
        $settings = $plugin->getConnectionSettingsForDisplay();
        $runtimeState = $plugin->getState()->getState();
        $availableIntegrations = $integrationsService->getAvailableIntegrations();
        $selectedIntegrations = array_values(array_filter(array_map('strval', (array)($runtimeState['selectedIntegrations'] ?? []))));
        $wizardSteps = $integrationsService->buildWizardSteps($selectedIntegrations);
        if ($relinkMode) {
            $wizardSteps = array_intersect_key($wizardSteps, array_flip(['connection', 'project']));
        }
        $requestedStep = (string)Craft::$app->getRequest()->getQueryParam('step', '');
        $currentStep = array_key_exists((string)($runtimeState['onboardingStep'] ?? ''), $wizardSteps)
            ? (string)$runtimeState['onboardingStep']
            : 'connection';
        if ($relinkMode && !array_key_exists($currentStep, $wizardSteps)) {
            $currentStep = 'connection';
        }
        $wizardStepOrder = array_keys($wizardSteps);
        $currentStepIndex = array_search($currentStep, $wizardStepOrder, true);
        if ($currentStepIndex === false) {
            $currentStepIndex = 0;
        }
        $step = $currentStep;
        if ($requestedStep !== '' && array_key_exists($requestedStep, $wizardSteps)) {
            if ($relinkMode) {
                $step = $requestedStep;
            } else {
                $requestedIndex = array_search($requestedStep, $wizardStepOrder, true);
                if ($requestedIndex !== false && $requestedIndex <= $currentStepIndex) {
                    $step = $requestedStep;
                }
            }
        }

        $shared = $this->buildSharedIntegrationViewData($runtimeState, $availableIntegrations, $selectedIntegrations);
        $integrationSettings = is_array($runtimeState['integrationSettings'] ?? null) ? $runtimeState['integrationSettings'] : [];
        $backfillState = is_array($integrationSettings['backfill'] ?? null) ? $integrationSettings['backfill'] : [];
        $availableBackfillSources = $plugin->getBackfill()->availableSources($runtimeState);
        $backfillSources = array_values(array_filter(array_map('strval', (array)($backfillState['sources'] ?? $availableBackfillSources))));

        $craftSites = $plugin->getState()->listCraftSites();
        $craftSiteId = (int)Craft::$app->getRequest()->getQueryParam('craftSiteId', 0);
        if ($craftSiteId <= 0) {
            $craftSiteId = $this->resolveDefaultOnboardingSiteId($runtimeState, $craftSites, $relinkMode);
        }
        $projectsSessionKey = $craftSiteId > 0
            ? 'burrow.discoveredProjects.' . $craftSiteId
            : 'burrow.discoveredProjects';
        $projects = (array)Craft::$app->getSession()->get($projectsSessionKey, []);
        if ($projects === []) {
            $projects = (array)Craft::$app->getSession()->get('burrow.discoveredProjects', []);
        }
        $projects = $this->filterProjectsForSite($projects, $craftSiteId);

        $siteStates = is_array($runtimeState['siteStates'] ?? null) ? $runtimeState['siteStates'] : [];
        $activeSiteState = $craftSiteId > 0
            ? $plugin->getState()->getSiteState($craftSiteId)
            : $runtimeState;
        $pendingSiteConfirm = (bool)Craft::$app->getSession()->get('burrow.pendingSiteConfirm.' . $craftSiteId, false);

        return array_merge($shared, [
            'settings' => $settings,
            'step' => $step,
            'currentStep' => $currentStep,
            'currentStepIndex' => $currentStepIndex,
            'wizardSteps' => $wizardSteps,
            'nextStep' => $integrationsService->nextWizardStep($step, $selectedIntegrations),
            'projects' => $projects,
            'craftSites' => $craftSites,
            'craftSiteId' => $craftSiteId,
            'siteStates' => $siteStates,
            'activeSiteState' => $activeSiteState,
            'pendingSiteConfirm' => $pendingSiteConfirm,
            'isMultiSite' => count($craftSites) > 1,
            'selectedSubnavItem' => $relinkMode ? 'settings' : 'setup',
            'settingsMode' => false,
            'relinkMode' => $relinkMode,
            'backfillState' => $backfillState,
            'backfillSources' => $backfillSources,
            'availableBackfillSources' => $availableBackfillSources,
            'backfillPresets' => $plugin->getBackfill()->presetOptions(),
        ]);
    }

    /**
     * @return array<string,mixed>
     */
    private function buildConfigureViewData(): array
    {
        $plugin = Plugin::getInstance();
        $integrationsService = $plugin->getIntegrations();
        $settings = $plugin->getConnectionSettingsForDisplay();
        $runtimeState = $plugin->getState()->getState();
        $availableIntegrations = $integrationsService->getAvailableIntegrations();
        $selectedIntegrations = array_values(array_filter(array_map('strval', (array)($runtimeState['selectedIntegrations'] ?? []))));
        $settingsSections = $integrationsService->buildSettingsSections($selectedIntegrations);
        $shared = $this->buildSharedIntegrationViewData($runtimeState, $availableIntegrations, $selectedIntegrations);

        return array_merge($shared, [
            'settings' => $settings,
            'settingsSections' => $settingsSections,
            'selectedSubnavItem' => 'settings',
            'settingsMode' => true,
            'projects' => (array)Craft::$app->getSession()->get('burrow.discoveredProjects', []),
        ]);
    }

    /**
     * @param array<string,mixed> $runtimeState
     * @param array<string,array<string,mixed>> $availableIntegrations
     * @param string[] $selectedIntegrations
     * @return array<string,mixed>
     */
    private function buildSharedIntegrationViewData(array $runtimeState, array $availableIntegrations, array $selectedIntegrations): array
    {
        $plugin = Plugin::getInstance();
        $integrationsService = $plugin->getIntegrations();
        $formAdapterViewData = $integrationsService->buildFormAdapterViewData($runtimeState);
        $formsContracts = $integrationsService->buildFormsContracts($runtimeState);
        $integrationReadinessRows = $integrationsService->buildIntegrationReadinessRows($runtimeState);
        $craftSites = $plugin->getState()->listCraftSites();
        $formSiteOptions = [];
        $siteNameById = [];
        foreach ($craftSites as $site) {
            $siteId = (int)($site['id'] ?? 0);
            if ($siteId <= 0) {
                continue;
            }
            $siteNameById[$siteId] = (string)($site['name'] ?? $site['handle'] ?? $siteId);
            $siteState = $plugin->getState()->getSiteState($siteId);
            if (!empty($siteState['enabled']) && trim((string)($siteState['projectId'] ?? '')) !== '') {
                $formSiteOptions[] = $site;
            }
        }
        // During onboarding before every site is linked, still offer enabled sites (or all sites).
        if ($formSiteOptions === []) {
            foreach ($craftSites as $site) {
                $siteId = (int)($site['id'] ?? 0);
                if ($siteId <= 0) {
                    continue;
                }
                $siteState = is_array(($runtimeState['siteStates'][(string)$siteId] ?? null))
                    ? $runtimeState['siteStates'][(string)$siteId]
                    : [];
                if (!empty($siteState['enabled']) || count($craftSites) === 1) {
                    $formSiteOptions[] = $site;
                }
            }
        }
        if ($formSiteOptions === []) {
            $formSiteOptions = $craftSites;
        }

        $contractRows = [];
        $integrationLabels = $integrationsService->integrationLabels();
        foreach ($formsContracts as $contract) {
            $mode = trim((string)($contract['mode'] ?? 'count_only'));
            $modeLabel = $mode === 'custom_fields'
                ? 'Custom fields'
                : ($mode === 'off' ? 'Off' : 'Count-only');
            $providerKey = trim((string)($contract['provider'] ?? ''));
            $mappedSiteId = (int)($contract['craftSiteId'] ?? 0);
            $contractRows[] = [
                'provider' => (string)($integrationLabels[$providerKey] ?? $providerKey),
                'providerKey' => $providerKey,
                'formName' => trim((string)($contract['formName'] ?? '')),
                'externalFormId' => trim((string)($contract['externalFormId'] ?? '')),
                'mode' => $modeLabel,
                'craftSiteId' => $mappedSiteId > 0 ? $mappedSiteId : null,
                'siteName' => $mappedSiteId > 0 ? (string)($siteNameById[$mappedSiteId] ?? $mappedSiteId) : '',
                'mappingCount' => is_array($contract['fieldMappings'] ?? null) ? count((array)$contract['fieldMappings']) : 0,
            ];
        }
        $syncMeta = is_array($runtimeState['integrationSettings']['contractSync'] ?? null)
            ? $runtimeState['integrationSettings']['contractSync']
            : [];
        $integrationSettings = is_array($runtimeState['integrationSettings'] ?? null) ? $runtimeState['integrationSettings'] : [];
        $contractsByProvider = [];
        foreach ($formsContracts as $contract) {
            $providerKey = trim((string)($contract['provider'] ?? ''));
            if ($providerKey === '' || empty($contract['enabled'])) {
                continue;
            }
            $contractsByProvider[$providerKey] = (int)($contractsByProvider[$providerKey] ?? 0) + 1;
        }
        $integrationSummaryRows = [];
        foreach ($selectedIntegrations as $integrationKey) {
            $detail = '';
            if ($integrationsService->isFormIntegration($integrationKey)) {
                $count = (int)($contractsByProvider[$integrationKey] ?? 0);
                $detail = $count . ' form' . ($count === 1 ? '' : 's');
            } elseif ($integrationKey === 'commerce') {
                $commerce = is_array($integrationSettings['commerce'] ?? null) ? $integrationSettings['commerce'] : [];
                $mode = (string)($commerce['mode'] ?? 'track');
                $funnel = !empty($commerce['ecommerceFunnel']);
                $statusMap = is_array($commerce['orderStatusMap'] ?? null) ? $commerce['orderStatusMap'] : [];
                $mappedCount = 0;
                foreach ($statusMap as $handles) {
                    if (is_array($handles)) {
                        $mappedCount += count($handles);
                    }
                }
                if ($mode !== 'track') {
                    $detail = 'Off';
                } elseif ($funnel && $mappedCount > 0) {
                    $detail = 'Orders + Funnel + ' . $mappedCount . ' status mapping' . ($mappedCount === 1 ? '' : 's');
                } elseif ($funnel) {
                    $detail = 'Orders and line items + Funnel';
                } elseif ($mappedCount > 0) {
                    $detail = 'Orders + ' . $mappedCount . ' status mapping' . ($mappedCount === 1 ? '' : 's');
                } else {
                    $detail = 'Orders and line items';
                }
            } elseif ($integrationKey === 'shopify') {
                $shopify = is_array($integrationSettings['shopify'] ?? null) ? $integrationSettings['shopify'] : [];
                $shopDomain = trim((string)($shopify['shopDomain'] ?? ''));
                if ((string)($shopify['mode'] ?? 'track') !== 'track') {
                    $detail = 'Off';
                } elseif (!empty($shopify['ecommerceFunnel'])) {
                    $detail = 'Cart funnel' . ($shopDomain !== '' ? ' · ' . $shopDomain : '');
                } else {
                    $detail = 'Funnel capture off';
                }
            }
            $integrationSummaryRows[] = [
                'key' => $integrationKey,
                'label' => (string)($integrationLabels[$integrationKey] ?? $integrationKey),
                'detail' => $detail,
                'iconDataUri' => (string)($availableIntegrations[$integrationKey]['iconDataUri'] ?? ''),
            ];
        }
        $projectUrl = $this->resolveProjectUrl($runtimeState, $plugin->getBurrowBaseUrl());
        $defaultFormSiteId = (int)($formSiteOptions[0]['id'] ?? Craft::$app->getSites()->getPrimarySite()?->id ?? 0);

        return [
            'state' => $runtimeState,
            'availableIntegrations' => $availableIntegrations,
            'integrationStepData' => [
                'formAdapterViewData' => $formAdapterViewData,
                'settings' => $integrationSettings,
                'commerceOrderStatuses' => $integrationsService->getCommerceOrderStatuses(),
                'shopify' => [
                    'detectedShopDomain' => $plugin->getShopifyTracking()->detectShopDomainFromShopifyPlugin(),
                    'suggested' => $plugin->getShopifyTracking()->isHeadlessShopifySuggested(),
                ],
            ],
            'formIntegrationIds' => array_keys($formAdapterViewData),
            'integrationReadinessRows' => $integrationReadinessRows,
            'contractRows' => $contractRows,
            'contractSyncMeta' => $syncMeta,
            'integrationSummaryRows' => $integrationSummaryRows,
            'projectUrl' => $projectUrl,
            'craftSites' => $craftSites,
            'formSiteOptions' => $formSiteOptions,
            'defaultFormSiteId' => $defaultFormSiteId,
            'requireFormSiteMapping' => count($formSiteOptions) > 1,
            'sdkAvailable' => $plugin->getBurrowApi()->isSdkAvailable(),
            'queueStats' => $plugin->getQueue()->stats(),
            'logs' => $plugin->getLogs()->latest(25),
        ];
    }

    /**
     * @param array<string,mixed> $runtimeState
     */
    private function resolveProjectUrl(array $runtimeState, string $baseUrl): string
    {
        $projectUrl = trim((string)($runtimeState['burrowProject']['url'] ?? ''));
        if ($projectUrl !== '') {
            return $projectUrl;
        }

        $path = trim((string)($runtimeState['burrowProject']['path'] ?? ''));
        if ($path === '' || $baseUrl === '') {
            return '';
        }

        $parts = parse_url($baseUrl);
        if (!is_array($parts) || empty($parts['scheme']) || empty($parts['host'])) {
            return '';
        }

        $host = (string)$parts['host'];
        if (str_starts_with($host, 'api.')) {
            $host = 'app.' . substr($host, 4);
        }

        return (string)$parts['scheme'] . '://' . $host . '/' . ltrim($path, '/');
    }

    private function configureSectionUrl(string $section): string
    {
        return 'burrow/settings?section=' . urlencode($section);
    }

    private function isRelinkRequest(): bool
    {
        $request = Craft::$app->getRequest();

        return $request->getQueryParam('relink') === '1'
            || $request->getBodyParam('relink') === '1';
    }

    private function setupStepUrl(string $step, bool $relink = false, int $craftSiteId = 0): string
    {
        $url = 'burrow/setup?step=' . urlencode($step);
        if ($relink) {
            $url .= '&relink=1';
        }
        if ($craftSiteId > 0) {
            $url .= '&craftSiteId=' . $craftSiteId;
        }

        return $url;
    }

    /**
     * @param array<string,mixed> $runtimeState
     * @param array<int,array<string,mixed>> $craftSites
     */
    private function resolveDefaultOnboardingSiteId(array $runtimeState, array $craftSites, bool $relinkMode): int
    {
        if ($craftSites === []) {
            return 0;
        }

        $siteStates = is_array($runtimeState['siteStates'] ?? null) ? $runtimeState['siteStates'] : [];
        if (!$relinkMode) {
            foreach ($craftSites as $site) {
                $siteId = (int)($site['id'] ?? 0);
                if ($siteId <= 0) {
                    continue;
                }
                $state = is_array($siteStates[(string)$siteId] ?? null) ? $siteStates[(string)$siteId] : [];
                if (!empty($state['enabled']) && trim((string)($state['projectId'] ?? '')) === '') {
                    return $siteId;
                }
            }
        }

        foreach ($craftSites as $site) {
            if (!empty($site['primary'])) {
                return (int)$site['id'];
            }
        }

        return (int)($craftSites[0]['id'] ?? 0);
    }

    /**
     * Hides projects already linked by another Craft site in this install.
     *
     * @param array<int,mixed> $projects
     * @return array<int,array<string,mixed>>
     */
    private function filterProjectsForSite(array $projects, int $craftSiteId): array
    {
        $plugin = Plugin::getInstance();
        $filtered = [];
        foreach ($projects as $project) {
            if (!is_array($project)) {
                continue;
            }
            $projectId = trim((string)($project['projectId'] ?? ''));
            if ($projectId === '') {
                continue;
            }
            if ($craftSiteId > 0 && $plugin->getState()->isProjectLinkedToOtherSite($projectId, $craftSiteId)) {
                continue;
            }
            $filtered[] = $project;
        }

        return $filtered;
    }

    /**
     * @return int the next enabled site that still needs linking, or 0 when done
     */
    private function nextUnlinkedEnabledSiteId(array $runtimeState, int $exceptSiteId = 0): int
    {
        $siteStates = is_array($runtimeState['siteStates'] ?? null) ? $runtimeState['siteStates'] : [];
        foreach (Plugin::getInstance()->getState()->listCraftSites() as $site) {
            $siteId = (int)($site['id'] ?? 0);
            if ($siteId <= 0 || $siteId === $exceptSiteId) {
                continue;
            }
            $state = is_array($siteStates[(string)$siteId] ?? null) ? $siteStates[(string)$siteId] : [];
            if (!empty($state['enabled']) && trim((string)($state['projectId'] ?? '')) === '') {
                return $siteId;
            }
        }

        return 0;
    }

    /**
     * @param array{ok:bool,error:string,notice:string} $syncResult
     */
    private function applySyncFlashMessages(array $syncResult): void
    {
        if (!$syncResult['ok']) {
            Craft::$app->getSession()->setError($syncResult['error'] !== ''
                ? $syncResult['error']
                : Craft::t('burrow', 'Sync to Burrow failed.'));
            return;
        }

        Craft::$app->getSession()->setNotice($syncResult['notice'] !== ''
            ? $syncResult['notice']
            : Craft::t('burrow', 'Settings saved and synced to Burrow.'));
    }

    /**
     * @param array<string,mixed> $runtimeState
     * @return array<string,mixed>
     */
    private function applyIntegrationSettingsFromRequest(array $runtimeState, string $integration): array
    {
        $plugin = Plugin::getInstance();
        $selected = array_values(array_filter(array_map('strval', (array)($runtimeState['selectedIntegrations'] ?? []))));

        if (!$plugin->getIntegrations()->isIntegrationStep($integration) || !in_array($integration, $selected, true)) {
            throw new \InvalidArgumentException(Craft::t('burrow', 'Invalid integration step.'));
        }

        $integrationSettings = is_array($runtimeState['integrationSettings'] ?? null) ? $runtimeState['integrationSettings'] : [];
        $formAdapter = $plugin->getIntegrations()->getFormIntegration($integration);
        if ($formAdapter !== null) {
            $prefixParam = $integration . 'Prefix';
            $integrationSettings[$integration] = $formAdapter->normalizeSettingsFromRequest(
                (array)Craft::$app->getRequest()->getBodyParams(),
                (string)Craft::$app->getRequest()->getBodyParam($prefixParam, $formAdapter->getDefaultPrefix())
            );
        } elseif ($integration === 'commerce') {
            $mode = trim((string)Craft::$app->getRequest()->getBodyParam('commerceMode', 'track'));
            if (!in_array($mode, ['track', 'off'], true)) {
                $mode = 'track';
            }
            $allowedLifecycleStates = ['fulfilled', 'refunded', 'cancelled'];
            $orderStatusMap = [];
            foreach ($allowedLifecycleStates as $lifecycleState) {
                $raw = Craft::$app->getRequest()->getBodyParam('orderStatusMap_' . $lifecycleState, []);
                $handles = [];
                if (is_array($raw)) {
                    foreach ($raw as $handle) {
                        $h = trim((string)$handle);
                        if ($h !== '') {
                            $handles[] = $h;
                        }
                    }
                }
                $orderStatusMap[$lifecycleState] = $handles;
            }
            $integrationSettings['commerce'] = [
                'mode' => $mode,
                'ecommerceFunnel' => (bool)Craft::$app->getRequest()->getBodyParam('ecommerceFunnel', false),
                'orderStatusMap' => $orderStatusMap,
            ];
        } elseif ($integration === 'shopify') {
            $mode = trim((string)Craft::$app->getRequest()->getBodyParam('shopifyMode', 'track'));
            if (!in_array($mode, ['track', 'off'], true)) {
                $mode = 'track';
            }
            $shopDomain = $plugin->getShopifyTracking()->normalizeShopDomain(
                (string)Craft::$app->getRequest()->getBodyParam('shopDomain', '')
            );
            if ($shopDomain === '') {
                $shopDomain = $plugin->getShopifyTracking()->detectShopDomainFromShopifyPlugin();
            }
            $integrationSettings['shopify'] = [
                'mode' => $mode,
                'ecommerceFunnel' => (bool)Craft::$app->getRequest()->getBodyParam('ecommerceFunnel', false),
                'shopDomain' => $shopDomain,
            ];
        }

        $runtimeState['integrationSettings'] = $integrationSettings;
        if (in_array($integration, ['commerce', 'shopify'], true)) {
            $capabilities = is_array($runtimeState['capabilities'] ?? null) ? $runtimeState['capabilities'] : [];
            $capabilities['ecommerce_funnel'] = $plugin->getIntegrations()->resolveFunnelCapability($integrationSettings, $selected);
            $runtimeState['capabilities'] = $capabilities;
        }

        return $runtimeState;
    }

    public function actionDashboard(): Response
    {
        $this->requirePermission('accessPlugin-burrow');

        $plugin = Plugin::getInstance();
        $state = $plugin->getState()->getState();
        $queueStats = $plugin->getQueue()->stats();
        $formsContracts = $plugin->getIntegrations()->buildFormsContracts($state);
        $integrationSettings = is_array($state['integrationSettings'] ?? null) ? $state['integrationSettings'] : [];
        $backfillState = is_array($integrationSettings['backfill'] ?? null) ? $integrationSettings['backfill'] : [];
        $operationsSettings = is_array($integrationSettings['operations'] ?? null) ? $integrationSettings['operations'] : [];
        $outboxRetentionDays = max(1, min(365, (int)($operationsSettings['outboxRetentionDays'] ?? 30)));
        $availableSources = $plugin->getBackfill()->availableSources($state);
        $backfillSources = array_values(array_filter(array_map('strval', (array)($backfillState['sources'] ?? $availableSources))));
        $backfillPresets = $plugin->getBackfill()->presetOptions();

        $integrationLabels = $plugin->getIntegrations()->integrationLabels();
        $integrationNames = array_values(array_map(
            static fn(string $key): string => (string)($integrationLabels[$key] ?? $key),
            array_values(array_filter(array_map('strval', (array)($state['selectedIntegrations'] ?? []))))
        ));

        $projectUrl = trim((string)($state['burrowProject']['url'] ?? ''));
        if ($projectUrl === '') {
            $path = trim((string)($state['burrowProject']['path'] ?? ''));
            $base = $plugin->getBurrowBaseUrl();
            if ($path !== '' && $base !== '') {
                $parts = parse_url($base);
                if (is_array($parts) && !empty($parts['scheme']) && !empty($parts['host'])) {
                    $host = (string)$parts['host'];
                    if (str_starts_with($host, 'api.')) {
                        $host = 'app.' . substr($host, 4);
                    }
                    $projectUrl = (string)$parts['scheme'] . '://' . $host . '/' . ltrim($path, '/');
                }
            }
        }

        return $this->renderTemplate('burrow/dashboard/index', [
            'state' => $state,
            'canDispatchToBurrow' => $plugin->canDispatchToBurrow($state),
            'queueStats' => $queueStats,
            'contractRows' => array_values(array_map(static function (array $contract) use ($integrationLabels): array {
                $providerKey = trim((string)($contract['provider'] ?? ''));
                $mode = trim((string)($contract['mode'] ?? 'count_only'));
                $modeLabel = $mode === 'custom_fields'
                    ? 'Custom fields'
                    : ($mode === 'off' ? 'Off' : 'Count-only');
                return [
                    'provider' => (string)($integrationLabels[$providerKey] ?? $providerKey),
                    'providerKey' => $providerKey,
                    'formName' => trim((string)($contract['formName'] ?? '')),
                    'externalFormId' => trim((string)($contract['externalFormId'] ?? '')),
                    'mode' => $modeLabel,
                    'mappingCount' => is_array($contract['fieldMappings'] ?? null) ? count((array)$contract['fieldMappings']) : 0,
                ];
            }, $formsContracts)),
            'contractSyncMeta' => is_array($integrationSettings['contractSync'] ?? null)
                ? $integrationSettings['contractSync']
                : [],
            'backfillState' => $backfillState,
            'backfillSources' => $backfillSources,
            'availableBackfillSources' => $availableSources,
            'backfillPresets' => $backfillPresets,
            'outboxRetentionDays' => $outboxRetentionDays,
            'integrationNames' => $integrationNames,
            'formIntegrationIds' => $plugin->getIntegrations()->getFormIntegrations()->ids(),
            'projectUrl' => $projectUrl,
            'selectedSubnavItem' => 'dashboard',
        ]);
    }

    public function actionBackfillProbe(): Response
    {
        $this->requirePermission('accessPlugin-burrow');

        $plugin = Plugin::getInstance();
        $runtimeState = $plugin->getState()->getState();
        $windowPreset = trim((string)Craft::$app->getRequest()->getQueryParam('windowPreset', 'last_90_days'));
        $presetOptions = $plugin->getBackfill()->presetOptions();
        if (!isset($presetOptions[$windowPreset])) {
            $windowPreset = 'last_90_days';
        }
        $probe = $plugin->getBackfill()->debugProbe($runtimeState, $windowPreset);

        return $this->renderTemplate('burrow/debug/backfill-probe', [
            'probe' => $probe,
            'windowPreset' => $windowPreset,
            'presetOptions' => $presetOptions,
            'formAdapterLabels' => array_map(
                static fn(array $row): string => (string)($row['label'] ?? ''),
                $plugin->getIntegrations()->buildFormAdapterViewData($runtimeState)
            ),
            'selectedSubnavItem' => 'dashboard',
        ]);
    }

    public function actionOutbox(): Response
    {
        $this->requirePermission('accessPlugin-burrow');

        $plugin = Plugin::getInstance();

        return $this->renderTemplate('burrow/outbox/index', [
            'selectedSubnavItem' => 'outbox',
            'canDispatchToBurrow' => $plugin->canDispatchToBurrow(),
            'outboxFailedCount' => $plugin->getQueue()->stats()['failed'] ?? 0,
        ]);
    }

    public function actionRetryOutbox(): ?Response
    {
        $this->requirePostRequest();
        $this->requirePermission('accessPlugin-burrow');

        $status = trim((string)Craft::$app->getRequest()->getBodyParam('status', 'all'));
        $q = trim((string)Craft::$app->getRequest()->getBodyParam('q', ''));
        $page = max(1, (int)Craft::$app->getRequest()->getBodyParam('page', 1));
        $query = ['status' => $status !== '' ? $status : 'all', 'page' => $page];
        if ($q !== '') {
            $query['q'] = $q;
        }

        $id = (string)Craft::$app->getRequest()->getBodyParam('id', '');
        if ($id === '') {
            Craft::$app->getSession()->setError(Craft::t('burrow', 'Invalid outbox id.'));
            return $this->redirect('burrow/outbox?' . http_build_query($query));
        }

        $ok = Plugin::getInstance()->getQueue()->retryNow($id);
        Craft::$app->getSession()->setNotice($ok ? Craft::t('burrow', 'Outbox record queued for retry.') : Craft::t('burrow', 'Unable to retry outbox record.'));

        $return = trim((string)Craft::$app->getRequest()->getBodyParam('return', ''));
        if ($return !== '' && preg_match('#^burrow/outbox/\d+$#', $return)) {
            return $this->redirect($return);
        }

        return $this->redirect('burrow/outbox?' . http_build_query($query));
    }

    public function actionRetryFailedOutbox(): ?Response
    {
        $this->requirePostRequest();
        $this->requirePermission('accessPlugin-burrow');

        $return = trim((string)Craft::$app->getRequest()->getBodyParam('return', 'burrow/outbox'));
        if ($return !== 'burrow' && $return !== 'burrow/outbox') {
            $return = 'burrow/outbox';
        }

        if (!Plugin::getInstance()->canDispatchToBurrow()) {
            Craft::$app->getSession()->setError(Craft::t('burrow', 'Configure the Burrow connection and ingestion key in Settings to retry delivery.'));
            return $this->redirect($return !== '' ? $return : 'burrow/outbox');
        }

        $count = Plugin::getInstance()->getQueue()->retryAllFailed();
        if ($count > 0) {
            $message = $count === 1
                ? Craft::t('burrow', 'Queued 1 failed record for retry.')
                : Craft::t('burrow', 'Queued {count} failed records for retry.', ['count' => $count]);
            Craft::$app->getSession()->setNotice($message);
        } else {
            Craft::$app->getSession()->setNotice(Craft::t('burrow', 'No failed outbox records to retry.'));
        }

        return $this->redirect($return !== '' ? $return : 'burrow/outbox');
    }

    public function actionDeleteOutbox(): ?Response
    {
        $this->requirePostRequest();
        $this->requirePermission('accessPlugin-burrow');

        $status = trim((string)Craft::$app->getRequest()->getBodyParam('status', 'all'));
        $q = trim((string)Craft::$app->getRequest()->getBodyParam('q', ''));
        $page = max(1, (int)Craft::$app->getRequest()->getBodyParam('page', 1));
        $query = ['status' => $status !== '' ? $status : 'all', 'page' => $page];
        if ($q !== '') {
            $query['q'] = $q;
        }

        $id = (string)Craft::$app->getRequest()->getBodyParam('id', '');
        if ($id === '') {
            Craft::$app->getSession()->setError(Craft::t('burrow', 'Invalid outbox id.'));
            return $this->redirect('burrow/outbox?' . http_build_query($query));
        }

        $ok = Plugin::getInstance()->getQueue()->deleteRecord($id);
        Craft::$app->getSession()->setNotice($ok ? Craft::t('burrow', 'Outbox record deleted.') : Craft::t('burrow', 'Unable to delete outbox record.'));

        return $this->redirect('burrow/outbox?' . http_build_query($query));
    }

    public function actionSaveOperationsSettings(): ?Response
    {
        $this->requirePostRequest();
        $this->requirePermission('accessPlugin-burrow');

        $plugin = Plugin::getInstance();
        $runtimeState = $plugin->getState()->getState();
        $integrationSettings = is_array($runtimeState['integrationSettings'] ?? null) ? $runtimeState['integrationSettings'] : [];
        $operations = is_array($integrationSettings['operations'] ?? null) ? $integrationSettings['operations'] : [];
        $storedRetention = max(1, min(365, (int)($operations['outboxRetentionDays'] ?? 30)));
        $requested = (int)Craft::$app->getRequest()->getBodyParam('outboxRetentionDays', $storedRetention);
        $requested = max(0, min(365, $requested));

        if ($requested === 0) {
            $operations['outboxRetentionDays'] = $storedRetention;
            $integrationSettings['operations'] = $operations;
            $runtimeState['integrationSettings'] = $integrationSettings;
            $plugin->getState()->saveState($runtimeState);

            Craft::$app->getQueue()->push(new CleanupOutboxRetentionJob([
                'forcePurge' => true,
            ]));

            Craft::$app->getSession()->setNotice(Craft::t('burrow', 'Outbox cleanup has been queued. All sent/failed rows and the send dedupe index will be cleared; retention remains {days} days. Large tables may take several minutes—check the queue if this does not finish.', [
                'days' => (string)$storedRetention,
            ]));
            return $this->redirect('burrow/dashboard#data-backfill');
        }

        $retention = $requested;
        $operations['outboxRetentionDays'] = $retention;
        $integrationSettings['operations'] = $operations;
        $runtimeState['integrationSettings'] = $integrationSettings;
        $plugin->getState()->saveState($runtimeState);

        Craft::$app->getQueue()->push(new CleanupOutboxRetentionJob([
            'retentionDays' => $retention,
            'forcePurge' => false,
        ]));

        Craft::$app->getSession()->setNotice(Craft::t('burrow', 'Outbox retention saved as {days} days. Old sent/failed rows are being removed in the queue; large cleanups may take several minutes.', [
            'days' => (string)$retention,
        ]));
        return $this->redirect('burrow/dashboard#data-backfill');
    }

    public function actionSaveConnection(): ?Response
    {
        $this->requirePostRequest();
        $this->requirePermission('accessPlugin-burrow');

        $request = Craft::$app->getRequest();
        $plugin = Plugin::getInstance();
        $baseUrl = trim((string)$request->getBodyParam('baseUrl', $plugin->getBurrowBaseUrl()));
        $apiKey = trim((string)$request->getBodyParam('apiKey', $plugin->getBurrowApiKey()));

        $relink = $this->isRelinkRequest() || $plugin->isOnboardingCompleted();
        $craftSiteId = (int)$request->getBodyParam('craftSiteId', 0);

        if ($baseUrl === '' || $apiKey === '') {
            Craft::$app->getSession()->setError(Craft::t('burrow', 'Base URL and API key are required.'));
            return $this->redirect($this->setupStepUrl('connection', $relink, $craftSiteId));
        }

        $runtimeState = $plugin->getState()->getState();
        $runtimeState['connectionBaseUrl'] = $baseUrl;
        $runtimeState['connectionApiKey'] = $apiKey;
        if (!$plugin->getState()->saveState($runtimeState)) {
            Craft::$app->getSession()->setError(Craft::t('burrow', 'Could not save connection settings.'));
            return $this->redirect($this->setupStepUrl('connection', $relink, $craftSiteId));
        }

        $general = Craft::$app->getConfig()->getGeneral();
        if ($general->allowAdminChanges) {
            $settings = $plugin->getSettings();
            $settings->baseUrl = $baseUrl;
            $settings->apiKey = $apiKey;
            if (!Craft::$app->getPlugins()->savePluginSettings($plugin, $settings->toArray())) {
                $errors = $settings->getFirstErrors();
                $message = Craft::t('burrow', 'Could not sync connection to project config.');
                if (!empty($errors)) {
                    $message .= ' ' . implode(' ', array_values($errors));
                }
                Craft::error('Burrow project config sync failed: ' . json_encode($errors), __METHOD__);
                Craft::$app->getSession()->setError($message);
                return $this->redirect($this->setupStepUrl('connection', $relink, $craftSiteId));
            }
        }

        $craftSites = $plugin->getState()->listCraftSites();
        $isMultiSite = count($craftSites) > 1;
        $discoverSiteUrl = null;
        if ($craftSiteId > 0) {
            $siteState = $plugin->getState()->getSiteState($craftSiteId);
            $discoverSiteUrl = trim((string)($siteState['siteUrl'] ?? '')) ?: null;
        } elseif (!$isMultiSite && isset($craftSites[0])) {
            $discoverSiteUrl = (string)($craftSites[0]['baseUrl'] ?? '');
            $craftSiteId = (int)($craftSites[0]['id'] ?? 0);
            $plugin->getState()->saveSiteState($craftSiteId, [
                'enabled' => true,
                'siteUrl' => $discoverSiteUrl,
            ], [
                'connectionBaseUrl' => $baseUrl,
                'connectionApiKey' => $apiKey,
            ]);
        }

        $discover = $plugin->getBurrowApi()->discover(
            $baseUrl,
            $apiKey,
            (array)($runtimeState['capabilities'] ?? []),
            $discoverSiteUrl
        );
        if (!$discover['ok']) {
            $plugin->getLogs()->log('error', 'Connection discover failed', 'onboarding', 'system', null, ['error' => $discover['error']]);
            Craft::$app->getSession()->setError(Craft::t('burrow', 'Connection failed: {error}', ['error' => $discover['error']]));
            return $this->redirect($this->setupStepUrl('connection', $relink, $craftSiteId));
        }

        Craft::$app->getSession()->set('burrow.discoveredProjects', $discover['projects']);
        if ($craftSiteId > 0) {
            Craft::$app->getSession()->set('burrow.discoveredProjects.' . $craftSiteId, $discover['projects']);
        }

        if (!$relink) {
            if ($isMultiSite) {
                $runtimeState = $plugin->getState()->getState();
                $runtimeState['onboardingStep'] = 'sites';
                $plugin->getState()->saveState($runtimeState);
                $plugin->getLogs()->log('info', 'Connection established; choose sites to link', 'onboarding', 'system', null, [
                    'projectsCount' => count($discover['projects']),
                ]);
                Craft::$app->getSession()->setNotice(Craft::t('burrow', 'Connection established.'));

                return $this->redirect($this->setupStepUrl('sites', false));
            }

            $runtimeState = $plugin->getState()->getState();
            $runtimeState['onboardingStep'] = 'project';
            $plugin->getState()->saveState($runtimeState);
        }

        $plugin->getLogs()->log('info', 'Connection established and projects discovered', 'onboarding', 'system', null, [
            'projectsCount' => count($discover['projects']),
            'siteId' => $craftSiteId,
        ]);

        Craft::$app->getSession()->setNotice(Craft::t('burrow', 'Connection established.'));

        return $this->redirect($this->setupStepUrl('project', $relink, $craftSiteId));
    }

    public function actionSaveSites(): ?Response
    {
        $this->requirePostRequest();
        $this->requirePermission('accessPlugin-burrow');

        $plugin = Plugin::getInstance();
        $enabledIds = array_values(array_filter(array_map('intval', (array)Craft::$app->getRequest()->getBodyParam('enabledSites', []))));
        $craftSites = $plugin->getState()->listCraftSites();
        if ($enabledIds === []) {
            Craft::$app->getSession()->setError(Craft::t('burrow', 'Enable at least one Craft site to continue.'));
            return $this->redirect($this->setupStepUrl('sites'));
        }

        $allowedIds = [];
        foreach ($craftSites as $site) {
            $allowedIds[(int)$site['id']] = $site;
        }

        $firstEnabledId = 0;
        foreach ($craftSites as $site) {
            $siteId = (int)$site['id'];
            $enabled = in_array($siteId, $enabledIds, true) && isset($allowedIds[$siteId]);
            $plugin->getState()->saveSiteState($siteId, [
                'enabled' => $enabled,
                'siteUrl' => (string)$site['baseUrl'],
                'siteUid' => (string)$site['uid'],
                'siteHandle' => (string)$site['handle'],
            ]);
            if ($enabled && $firstEnabledId === 0) {
                $firstEnabledId = $siteId;
            }
        }

        $runtimeState = $plugin->getState()->getState();
        $runtimeState['onboardingStep'] = 'project';
        $plugin->getState()->saveState($runtimeState);

        // Discover projects for the first enabled site using that site's URL.
        $siteState = $plugin->getState()->getSiteState($firstEnabledId);
        $discover = $plugin->getBurrowApi()->discover(
            $plugin->getBurrowBaseUrl(),
            $plugin->getBurrowApiKey(),
            (array)($runtimeState['capabilities'] ?? []),
            trim((string)($siteState['siteUrl'] ?? '')) ?: null
        );
        if ($discover['ok']) {
            Craft::$app->getSession()->set('burrow.discoveredProjects.' . $firstEnabledId, $discover['projects']);
            Craft::$app->getSession()->set('burrow.discoveredProjects', $discover['projects']);
        }

        $plugin->getLogs()->log('info', 'Sites selected for Burrow linking', 'onboarding', 'system', null, [
            'enabledSiteIds' => $enabledIds,
        ]);
        Craft::$app->getSession()->setNotice(Craft::t('burrow', 'Sites saved. Link a Burrow project for each enabled site.'));

        return $this->redirect($this->setupStepUrl('project', false, $firstEnabledId));
    }

    public function actionSelectProject(): ?Response
    {
        $this->requirePostRequest();
        $this->requirePermission('accessPlugin-burrow');

        $request = Craft::$app->getRequest();
        $plugin = Plugin::getInstance();
        $runtimeState = $plugin->getState()->getState();
        $craftSiteId = (int)$request->getBodyParam('craftSiteId', 0);
        if ($craftSiteId <= 0) {
            $craftSiteId = $this->resolveDefaultOnboardingSiteId(
                $runtimeState,
                $plugin->getState()->listCraftSites(),
                $this->isRelinkRequest() || $plugin->isOnboardingCompleted()
            );
        }
        if ($craftSiteId <= 0) {
            Craft::$app->getSession()->setError(Craft::t('burrow', 'Choose a Craft site before linking a project.'));
            return $this->redirect($this->setupStepUrl('project'));
        }

        // customSelect coerces JSON option values to objects and posts "[object Object]".
        // Accept a plain projectId (preferred) or legacy JSON, then hydrate from session.
        $rawSelection = trim((string)$request->getBodyParam('projectSelection', ''));
        $decoded = json_decode($rawSelection, true);
        if (is_array($decoded)) {
            $selection = [
                'organizationId' => trim((string)($decoded['organizationId'] ?? '')),
                'clientId' => trim((string)($decoded['clientId'] ?? '')),
                'projectId' => trim((string)($decoded['projectId'] ?? '')),
            ];
        } else {
            $selection = [
                'organizationId' => '',
                'clientId' => '',
                'projectId' => $rawSelection,
            ];
        }
        $discoveredProjects = (array)Craft::$app->getSession()->get('burrow.discoveredProjects.' . $craftSiteId, []);
        if ($discoveredProjects === []) {
            $discoveredProjects = (array)Craft::$app->getSession()->get('burrow.discoveredProjects', []);
        }
        if ($selection['projectId'] !== '') {
            foreach ($discoveredProjects as $project) {
                if (!is_array($project)) {
                    continue;
                }
                if ((string)($project['projectId'] ?? '') !== $selection['projectId']) {
                    continue;
                }
                if ($selection['organizationId'] === '') {
                    $selection['organizationId'] = trim((string)($project['organizationId'] ?? ''));
                }
                if ($selection['clientId'] === '') {
                    $selection['clientId'] = trim((string)($project['clientId'] ?? ''));
                }
                break;
            }
        }

        $relink = $this->isRelinkRequest() || $plugin->isOnboardingCompleted();
        $confirmSiteChange = $request->getBodyParam('confirmSiteChange') === '1';

        if ($selection['projectId'] === '') {
            Craft::$app->getSession()->setError(Craft::t('burrow', 'Please choose a project.'));
            return $this->redirect($this->setupStepUrl('project', $relink, $craftSiteId));
        }

        if ($plugin->getState()->isProjectLinkedToOtherSite($selection['projectId'], $craftSiteId)) {
            Craft::$app->getSession()->setError(Craft::t('burrow', 'That Burrow project is already linked to another Craft site in this install.'));
            return $this->redirect($this->setupStepUrl('project', $relink, $craftSiteId));
        }

        $siteRuntime = $plugin->getState()->getSiteState($craftSiteId);
        $siteRuntime['capabilities'] = $plugin->getIntegrations()->buildCapabilities(
            (array)($runtimeState['selectedIntegrations'] ?? [])
        );
        $siteUrl = trim((string)($siteRuntime['siteUrl'] ?? ''));

        $link = $plugin->getBurrowApi()->link(
            $plugin->getBurrowBaseUrl(),
            $plugin->getBurrowApiKey(),
            $selection,
            (array)$siteRuntime['capabilities'],
            $siteRuntime,
            $siteUrl !== '' ? $siteUrl : null,
            $confirmSiteChange
        );
        if (!$link['ok']) {
            $code = (string)($link['code'] ?? '');
            if ($code === 'project_site_mismatch' && !$confirmSiteChange) {
                Craft::$app->getSession()->set('burrow.pendingSiteConfirm.' . $craftSiteId, true);
                Craft::$app->getSession()->setError(Craft::t(
                    'burrow',
                    'This Burrow project is registered to a different site URL. Confirm the site URL change to continue, or choose another project.'
                ));
                return $this->redirect($this->setupStepUrl('project', $relink, $craftSiteId));
            }
            $plugin->getLogs()->log('error', 'Project link failed', 'onboarding', 'system', null, [
                'error' => $link['error'],
                'code' => $code,
                'siteId' => $craftSiteId,
            ]);
            Craft::$app->getSession()->setError(Craft::t('burrow', 'Project linking failed: {error}', ['error' => $link['error']]));
            return $this->redirect($this->setupStepUrl('project', $relink, $craftSiteId));
        }

        Craft::$app->getSession()->remove('burrow.pendingSiteConfirm.' . $craftSiteId);
        $siteRuntime = $plugin->getBurrowApi()->applyLinkResult($siteRuntime, $link);

        if (!$plugin->runtimeStateHasIngestionKey($siteRuntime)) {
            $plugin->getLogs()->log('error', 'Project link succeeded but no ingestion key was returned', 'onboarding', 'system', null, $selection + ['siteId' => $craftSiteId]);
            Craft::$app->getSession()->setError(Craft::t('burrow', 'Project linking succeeded but Burrow did not return an ingestion key. Try again or re-enter your account API key.'));
            return $this->redirect($this->setupStepUrl('project', $relink, $craftSiteId));
        }

        // Retain the organization API key so additional Craft sites can be linked.
        $plugin->getState()->saveSiteState($craftSiteId, [
            'enabled' => true,
            'linked' => true,
            'projectId' => (string)($siteRuntime['projectId'] ?? ''),
            'clientId' => (string)($siteRuntime['clientId'] ?? ''),
            'organizationId' => (string)($siteRuntime['organizationId'] ?? ''),
            'projectSourceId' => (string)($siteRuntime['projectSourceId'] ?? ''),
            'sourceIds' => (array)($siteRuntime['sourceIds'] ?? []),
            'sdkState' => (array)($siteRuntime['sdkState'] ?? []),
            'ingestionKey' => (array)($siteRuntime['ingestionKey'] ?? []),
            'burrowProject' => (array)($siteRuntime['burrowProject'] ?? []),
            'siteUrl' => $siteUrl,
        ], [
            'capabilities' => (array)($siteRuntime['capabilities'] ?? $runtimeState['capabilities'] ?? []),
            'connectionBaseUrl' => (string)($runtimeState['connectionBaseUrl'] ?? ''),
            'connectionApiKey' => (string)($runtimeState['connectionApiKey'] ?? ''),
        ]);

        if ($relink) {
            $runtimeState = $plugin->getState()->getState();
            $syncResult = $plugin->getIntegrations()->syncConfiguration($runtimeState, false);
            $runtimeState = $syncResult['runtimeState'];
            $plugin->getState()->saveState($runtimeState);
            $plugin->getLogs()->log('info', 'Project re-linked', 'settings', 'system', null, $selection + ['siteId' => $craftSiteId]);

            if ($syncResult['ok']) {
                Craft::$app->getSession()->setNotice(Craft::t('burrow', 'Project re-linked. A new ingestion key is active.'));
            } else {
                Craft::$app->getSession()->setError($syncResult['error'] !== ''
                    ? Craft::t('burrow', 'Project re-linked, but sync failed: {error}', ['error' => $syncResult['error']])
                    : Craft::t('burrow', 'Project re-linked, but sync to Burrow failed.'));
            }

            return $this->redirect($this->configureSectionUrl('connection'));
        }

        $runtimeState = $plugin->getState()->getState();
        $nextSiteId = $this->nextUnlinkedEnabledSiteId($runtimeState, $craftSiteId);
        if ($nextSiteId > 0) {
            $nextSite = $plugin->getState()->getSiteState($nextSiteId);
            $discover = $plugin->getBurrowApi()->discover(
                $plugin->getBurrowBaseUrl(),
                $plugin->getBurrowApiKey(),
                (array)($runtimeState['capabilities'] ?? []),
                trim((string)($nextSite['siteUrl'] ?? '')) ?: null
            );
            if ($discover['ok']) {
                Craft::$app->getSession()->set('burrow.discoveredProjects.' . $nextSiteId, $discover['projects']);
            }
            $runtimeState['onboardingStep'] = 'project';
            $plugin->getState()->saveState($runtimeState);
            $plugin->getLogs()->log('info', 'Project linked; continue with next site', 'onboarding', 'system', null, $selection + ['siteId' => $craftSiteId, 'nextSiteId' => $nextSiteId]);
            Craft::$app->getSession()->setNotice(Craft::t('burrow', 'Project linked. Continue with the next enabled site.'));

            return $this->redirect($this->setupStepUrl('project', false, $nextSiteId));
        }

        $runtimeState['onboardingStep'] = 'integrations';
        $plugin->getState()->saveState($runtimeState);
        $plugin->getLogs()->log('info', 'Project linked', 'onboarding', 'system', null, $selection + ['siteId' => $craftSiteId]);

        Craft::$app->getSession()->setNotice(Craft::t('burrow', 'Project selected and linked.'));

        return $this->redirect('burrow/setup?step=integrations');
    }

    public function actionSaveIntegrations(): ?Response
    {
        $this->requirePostRequest();
        $this->requirePermission('accessPlugin-burrow');

        $plugin = Plugin::getInstance();
        $runtimeState = $plugin->getState()->getState();
        $selected = (array)Craft::$app->getRequest()->getBodyParam('integrations', []);
        $selected = array_values(array_filter(array_map('strval', $selected)));
        $allowed = array_keys($plugin->getIntegrations()->getAvailableIntegrations());
        $selected = array_values(array_intersect($plugin->getIntegrations()->integrationOrder(), array_intersect($selected, $allowed)));

        if (empty($selected)) {
            Craft::$app->getSession()->setError(Craft::t('burrow', 'Select at least one integration.'));
            return $this->redirect('burrow/setup?step=integrations');
        }

        $runtimeState['selectedIntegrations'] = $selected;
        $runtimeState['capabilities'] = $plugin->getIntegrations()->buildCapabilities($selected);
        $existingSettings = is_array($runtimeState['integrationSettings'] ?? null) ? $runtimeState['integrationSettings'] : [];
        $runtimeState['integrationSettings'] = array_intersect_key($existingSettings, array_flip($selected));
        $nextStep = $plugin->getIntegrations()->nextWizardStep('integrations', $selected);
        $runtimeState['onboardingStep'] = $nextStep;
        $plugin->getState()->saveState($runtimeState);
        $plugin->getLogs()->log('info', 'Integrations updated', 'onboarding', 'system', null, ['selected' => $selected]);

        Craft::$app->getSession()->setNotice(Craft::t('burrow', 'Integrations saved.'));

        return $this->redirect('burrow/setup?step=' . urlencode($nextStep));
    }

    public function actionSaveIntegrationStep(): ?Response
    {
        $this->requirePostRequest();
        $this->requirePermission('accessPlugin-burrow');

        $plugin = Plugin::getInstance();
        $runtimeState = $plugin->getState()->getState();
        $integration = trim((string)Craft::$app->getRequest()->getBodyParam('integration', ''));

        try {
            $runtimeState = $this->applyIntegrationSettingsFromRequest($runtimeState, $integration);
        } catch (\InvalidArgumentException $e) {
            Craft::$app->getSession()->setError($e->getMessage());
            return $this->redirect('burrow/setup?step=integrations');
        }

        $selected = array_values(array_filter(array_map('strval', (array)($runtimeState['selectedIntegrations'] ?? []))));
        $nextStep = $plugin->getIntegrations()->nextWizardStep($integration, $selected);
        $runtimeState['onboardingStep'] = $nextStep;
        $plugin->getState()->saveState($runtimeState);
        $plugin->getLogs()->log('info', 'Integration setup step completed', 'onboarding', 'system', $integration);

        return $this->redirect('burrow/setup?step=' . urlencode($nextStep));
    }

    public function actionSaveIntegrationsSettings(): ?Response
    {
        $this->requirePostRequest();
        $this->requirePermission('accessPlugin-burrow');

        $plugin = Plugin::getInstance();
        $runtimeState = $plugin->getState()->getState();
        if (empty($runtimeState['onboardingCompleted'])) {
            return $this->actionSaveIntegrations();
        }

        $selected = (array)Craft::$app->getRequest()->getBodyParam('integrations', []);
        $selected = array_values(array_filter(array_map('strval', $selected)));
        $allowed = array_keys($plugin->getIntegrations()->getAvailableIntegrations());
        $selected = array_values(array_intersect($plugin->getIntegrations()->integrationOrder(), array_intersect($selected, $allowed)));

        if (empty($selected)) {
            Craft::$app->getSession()->setError(Craft::t('burrow', 'Select at least one integration.'));
            return $this->redirect($this->configureSectionUrl('integrations'));
        }

        $previousCapabilities = (array)($runtimeState['capabilities'] ?? []);
        $runtimeState['selectedIntegrations'] = $selected;
        $runtimeState['capabilities'] = $plugin->getIntegrations()->buildCapabilities($selected);
        $existingSettings = is_array($runtimeState['integrationSettings'] ?? null) ? $runtimeState['integrationSettings'] : [];
        $runtimeState['integrationSettings'] = array_intersect_key($existingSettings, array_flip($selected));
        $forceRelink = $plugin->getIntegrations()->capabilitiesFingerprint($previousCapabilities)
            !== $plugin->getIntegrations()->capabilitiesFingerprint((array)$runtimeState['capabilities']);

        $syncResult = $plugin->getIntegrations()->syncConfiguration($runtimeState, $forceRelink);
        $runtimeState = $syncResult['runtimeState'];
        $plugin->getState()->saveState($runtimeState);
        $this->applySyncFlashMessages($syncResult);

        $plugin->getLogs()->log('info', 'Integrations settings saved', 'settings', 'system', null, [
            'selected' => $selected,
            'synced' => $syncResult['ok'],
        ]);

        return $this->redirect($this->configureSectionUrl('integrations'));
    }

    public function actionSaveIntegrationSettings(): ?Response
    {
        $this->requirePostRequest();
        $this->requirePermission('accessPlugin-burrow');

        $plugin = Plugin::getInstance();
        $runtimeState = $plugin->getState()->getState();
        $integration = trim((string)Craft::$app->getRequest()->getBodyParam('integration', ''));

        if (empty($runtimeState['onboardingCompleted'])) {
            return $this->actionSaveIntegrationStep();
        }

        try {
            $runtimeState = $this->applyIntegrationSettingsFromRequest($runtimeState, $integration);
        } catch (\InvalidArgumentException $e) {
            Craft::$app->getSession()->setError($e->getMessage());
            return $this->redirect($this->configureSectionUrl('integrations'));
        }

        $forceRelink = $integration === 'commerce';
        $syncResult = $plugin->getIntegrations()->syncConfiguration($runtimeState, $forceRelink);
        $runtimeState = $syncResult['runtimeState'];
        $plugin->getState()->saveState($runtimeState);
        $this->applySyncFlashMessages($syncResult);

        $plugin->getLogs()->log('info', 'Integration settings saved', 'settings', 'system', $integration, [
            'synced' => $syncResult['ok'],
        ]);

        return $this->redirect($this->configureSectionUrl($integration));
    }

    public function actionSyncToBurrow(): ?Response
    {
        $this->requirePostRequest();
        $this->requirePermission('accessPlugin-burrow');

        $plugin = Plugin::getInstance();
        $runtimeState = $plugin->getState()->getState();
        $section = trim((string)Craft::$app->getRequest()->getBodyParam('returnSection', 'overview'));
        if ($section === '') {
            $section = 'overview';
        }

        $syncResult = $plugin->getIntegrations()->syncConfiguration($runtimeState, true);
        $runtimeState = $syncResult['runtimeState'];
        $plugin->getState()->saveState($runtimeState);
        $this->applySyncFlashMessages($syncResult);

        return $this->redirect($this->configureSectionUrl($section));
    }

    public function actionRefreshSnapshot(): ?Response
    {
        $this->requirePostRequest();
        $this->requirePermission('accessPlugin-burrow');

        $returnTo = trim((string)Craft::$app->getRequest()->getBodyParam('returnTo', ''));
        $plugin = Plugin::getInstance();
        $runtimeState = $plugin->getState()->getState();
        $runtimeState['lastSnapshot'] = $plugin->getSnapshot()->collectSnapshot();
        $plugin->getState()->saveState($runtimeState);
        $plugin->getLogs()->log('info', 'System snapshot refreshed', 'snapshot', 'system');

        Craft::$app->getSession()->setNotice(Craft::t('burrow', 'System snapshot refreshed.'));
        if ($returnTo === 'dashboard') {
            return $this->redirect('burrow/dashboard');
        }
        return $this->redirect('burrow/setup?step=review');
    }

    public function actionSyncContracts(): ?Response
    {
        $this->requirePostRequest();
        $this->requirePermission('accessPlugin-burrow');

        $plugin = Plugin::getInstance();
        $runtimeState = $plugin->getState()->getState();
        if (trim((string)($runtimeState['projectId'] ?? '')) === '') {
            Craft::$app->getSession()->setError(Craft::t('burrow', 'Project is not linked yet. Complete Step 2 before syncing contracts.'));
            return $this->redirect('burrow/setup?step=review');
        }

        $contracts = $plugin->getIntegrations()->buildFormsContracts($runtimeState);
        $forceRelink = empty($contracts);
        $syncResult = $plugin->getIntegrations()->syncConfiguration($runtimeState, $forceRelink, true);
        if (!$syncResult['ok']) {
            $plugin->getLogs()->log('error', 'Onboarding sync failed', 'onboarding', 'system', null, ['error' => $syncResult['error']]);
            Craft::$app->getSession()->setError($syncResult['error'] !== ''
                ? $syncResult['error']
                : Craft::t('burrow', 'Sync to Burrow failed.'));
            return $this->redirect('burrow/setup?step=review');
        }

        $runtimeState = $syncResult['runtimeState'];
        // Keep setup open so the Finish step can offer backfill before onboarding completes.
        $runtimeState['onboardingCompleted'] = false;
        $runtimeState['onboardingStep'] = 'finish';
        $plugin->getState()->saveState($runtimeState);
        $plugin->getLogs()->log('info', 'Onboarding sync completed', 'onboarding', 'system', null, [
            'contractsCount' => $syncResult['contractsCount'],
            'contractsSynced' => $syncResult['contractsSynced'],
            'snapshotSynced' => $syncResult['snapshotSynced'],
        ]);
        $this->applySyncFlashMessages($syncResult);

        return $this->redirect('burrow/setup?step=finish');
    }

    public function actionFinish(): ?Response
    {
        $this->requirePostRequest();
        $this->requirePermission('accessPlugin-burrow');

        $plugin = Plugin::getInstance();
        $runtimeState = $plugin->getState()->getState();
        $runtimeState['lastSnapshot'] = $plugin->getSnapshot()->collectSnapshot();
        $linkedSites = $plugin->getState()->getLinkedSiteStates();
        if ($linkedSites === []) {
            $syncResult = $plugin->getBurrowApi()->publishSystemSnapshot(
                $plugin->getBurrowBaseUrl(),
                $plugin->getBurrowApiKey(),
                $runtimeState,
                $runtimeState['lastSnapshot']
            );
        } else {
            $syncResult = ['ok' => true, 'error' => ''];
            foreach ($linkedSites as $siteKey => $_meta) {
                $siteRuntime = $plugin->getState()->getSiteState((int)$siteKey);
                $siteRuntime['lastSnapshot'] = $runtimeState['lastSnapshot'];
                $siteResult = $plugin->getBurrowApi()->publishSystemSnapshot(
                    $plugin->getBurrowBaseUrl(),
                    $plugin->getBurrowApiKey(),
                    $siteRuntime,
                    $siteRuntime['lastSnapshot']
                );
                $plugin->getState()->saveSiteState((int)$siteKey, [
                    'lastSnapshot' => $siteRuntime['lastSnapshot'],
                ]);
                if (empty($siteResult['ok'])) {
                    $syncResult = $siteResult;
                }
            }
        }
        if ($syncResult['ok']) {
            $integrationSettings = is_array($runtimeState['integrationSettings'] ?? null) ? $runtimeState['integrationSettings'] : [];
            $systemJobs = is_array($integrationSettings['systemJobs'] ?? null) ? $integrationSettings['systemJobs'] : [];
            $systemJobs['snapshotLastRunAt'] = gmdate('c');
            $systemJobs['snapshotQueuedAt'] = '';
            $systemJobs['snapshotLastError'] = '';
            $integrationSettings['systemJobs'] = $systemJobs;
            $runtimeState['integrationSettings'] = $integrationSettings;
        }
        $runtimeState['onboardingCompleted'] = true;
        $runtimeState['onboardingStep'] = 'finish';
        $plugin->getState()->saveState($runtimeState);

        if ($syncResult['ok']) {
            $plugin->getLogs()->log('info', 'Onboarding finished and snapshot synced', 'onboarding', 'system');
            Craft::$app->getSession()->setNotice(Craft::t('burrow', 'Burrow setup is complete and snapshot synced.'));
        } else {
            $plugin->getLogs()->log('warning', 'Onboarding finished with snapshot sync warning', 'onboarding', 'system', null, [
                'error' => $syncResult['error'],
            ]);
            Craft::$app->getSession()->setNotice(Craft::t('burrow', 'Burrow setup is complete. Snapshot sync pending: {error}', [
                'error' => $syncResult['error'],
            ]));
        }

        return $this->redirect('burrow/dashboard');
    }

    /**
     * Clears a persisted `queued` / `running` backfill when the Craft queue is no longer processing it
     * (worker stopped, timeout, deploy, etc.) so a new run can be started.
     */
    public function actionResetBackfill(): ?Response
    {
        $this->requirePostRequest();
        $this->requirePermission('accessPlugin-burrow');

        $plugin = Plugin::getInstance();
        $runtimeState = $plugin->getState()->getState();
        $integrationSettings = is_array($runtimeState['integrationSettings'] ?? null) ? $runtimeState['integrationSettings'] : [];
        $backfill = is_array($integrationSettings['backfill'] ?? null) ? $integrationSettings['backfill'] : [];
        $status = (string)($backfill['status'] ?? '');

        if ($status !== 'queued' && $status !== 'running') {
            Craft::$app->getSession()->setNotice(Craft::t('burrow', 'No active backfill to reset.'));
            return $this->redirect('burrow/dashboard#data-backfill');
        }

        $backfill['status'] = 'failed';
        $backfill['error'] = Craft::t('burrow', 'Backfill was reset from the dashboard because the run was no longer active in Craft’s queue (worker stopped, timeout, or deployment).');
        $backfill['completedAt'] = gmdate('c');
        unset($backfill['checkpoint']);

        $integrationSettings['backfill'] = $backfill;
        $runtimeState['integrationSettings'] = $integrationSettings;
        $plugin->getState()->saveState($runtimeState);

        $plugin->getLogs()->log('warning', 'Backfill reset from CP (stuck queued/running)', 'backfill', 'system', null, [
            'previousStatus' => $status,
            'accepted' => (int)($backfill['accepted'] ?? 0),
            'requested' => (int)($backfill['requested'] ?? 0),
        ]);

        Craft::$app->getSession()->setNotice(Craft::t('burrow', 'Backfill state was reset. You can start a new run when ready.'));
        return $this->redirect('burrow/dashboard#data-backfill');
    }

    public function actionStartBackfill(): ?Response
    {
        $this->requirePostRequest();
        $this->requirePermission('accessPlugin-burrow');

        $plugin = Plugin::getInstance();
        $runtimeState = $plugin->getState()->getState();
        $request = Craft::$app->getRequest();
        $fromSetup = $request->getBodyParam('fromSetup') === '1';
        $errorRedirect = $fromSetup ? 'burrow/setup?step=finish' : 'burrow/dashboard#data-backfill';

        if (trim((string)($runtimeState['projectId'] ?? '')) === '') {
            Craft::$app->getSession()->setError(Craft::t('burrow', 'Project is not linked yet.'));
            return $this->redirect($fromSetup ? 'burrow/setup?step=finish' : 'burrow/dashboard');
        }

        $windowPreset = trim((string)$request->getBodyParam('backfillWindowPreset', 'last_730_days'));
        $sources = (array)$request->getBodyParam('backfillSources', []);

        $integrationSettings = is_array($runtimeState['integrationSettings'] ?? null) ? $runtimeState['integrationSettings'] : [];
        $existingBackfill = is_array($integrationSettings['backfill'] ?? null) ? $integrationSettings['backfill'] : [];
        $existingStatus = (string)($existingBackfill['status'] ?? '');
        if ($existingStatus === 'queued' || $existingStatus === 'running') {
            Craft::$app->getSession()->setError(Craft::t('burrow', 'A backfill is already running. Wait for it to finish or check the queue before starting another.'));
            return $this->redirect($errorRedirect);
        }

        $checkpoint = $plugin->getBackfill()->createInitialCheckpoint($runtimeState, $windowPreset, $sources);
        if ($checkpoint === null) {
            $normalized = array_values(array_filter(array_map('strval', $sources)));
            $normalized = array_values(array_intersect($normalized, ['forms', 'ecommerce']));
            if ($normalized === []) {
                Craft::$app->getSession()->setError(Craft::t('burrow', 'Choose at least one source for backfill.'));
            } else {
                Craft::$app->getSession()->setError(Craft::t('burrow', 'No backfill source is available for the selected integrations.'));
            }
            return $this->redirect($errorRedirect);
        }

        $integrationSettings['backfill'] = [
            'status' => 'queued',
            'windowPreset' => $windowPreset,
            'windowStart' => (string)$checkpoint['windowStart'],
            'windowEnd' => (string)$checkpoint['windowEnd'],
            'sources' => (array)$checkpoint['sources'],
            'requested' => 0,
            'accepted' => 0,
            'rejected' => 0,
            'validationRejected' => 0,
            'latestCursor' => '',
            'breakdown' => ['forms' => 0, 'ecommerce' => 0],
            'startedAt' => gmdate('c'),
            'completedAt' => '',
            'error' => '',
            'checkpoint' => $checkpoint,
        ];
        $runtimeState['integrationSettings'] = $integrationSettings;
        if ($fromSetup) {
            $runtimeState['onboardingCompleted'] = true;
            $runtimeState['onboardingStep'] = 'finish';
        }
        $plugin->getState()->saveState($runtimeState);

        \craft\helpers\Queue::push(new \burrow\Burrow\jobs\BackfillChunkJob(), null, 0, \burrow\Burrow\jobs\BackfillChunkJob::TTR);

        if ($fromSetup) {
            $plugin->getLogs()->log('info', 'Onboarding finished with backfill queued', 'onboarding', 'system');
            Craft::$app->getSession()->setNotice(Craft::t('burrow', 'Setup complete. Backfill queued — keep your queue worker running; progress appears on the dashboard.'));
        } else {
            Craft::$app->getSession()->setNotice(Craft::t('burrow', 'Backfill queued. It will run via Craft’s queue in the background; keep your queue worker running until it finishes. Progress appears on this dashboard.'));
        }

        return $this->redirect('burrow/dashboard#data-backfill');
    }
}
