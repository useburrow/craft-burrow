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

        return array_merge($shared, [
            'settings' => $settings,
            'step' => $step,
            'currentStep' => $currentStep,
            'currentStepIndex' => $currentStepIndex,
            'wizardSteps' => $wizardSteps,
            'nextStep' => $integrationsService->nextWizardStep($step, $selectedIntegrations),
            'projects' => (array)Craft::$app->getSession()->get('burrow.discoveredProjects', []),
            'selectedSubnavItem' => $relinkMode ? 'settings' : 'setup',
            'settingsMode' => false,
            'relinkMode' => $relinkMode,
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
        $contractRows = [];
        $integrationLabels = $integrationsService->integrationLabels();
        foreach ($formsContracts as $contract) {
            $mode = trim((string)($contract['mode'] ?? 'count_only'));
            $modeLabel = $mode === 'custom_fields'
                ? 'Custom fields'
                : ($mode === 'off' ? 'Off' : 'Count-only');
            $providerKey = trim((string)($contract['provider'] ?? ''));
            $contractRows[] = [
                'provider' => (string)($integrationLabels[$providerKey] ?? $providerKey),
                'providerKey' => $providerKey,
                'formName' => trim((string)($contract['formName'] ?? '')),
                'externalFormId' => trim((string)($contract['externalFormId'] ?? '')),
                'mode' => $modeLabel,
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
            }
            $integrationSummaryRows[] = [
                'key' => $integrationKey,
                'label' => (string)($integrationLabels[$integrationKey] ?? $integrationKey),
                'detail' => $detail,
                'iconDataUri' => (string)($availableIntegrations[$integrationKey]['iconDataUri'] ?? ''),
            ];
        }
        $projectUrl = $this->resolveProjectUrl($runtimeState, $plugin->getBurrowBaseUrl());

        return [
            'state' => $runtimeState,
            'availableIntegrations' => $availableIntegrations,
            'integrationStepData' => [
                'formAdapterViewData' => $formAdapterViewData,
                'settings' => $integrationSettings,
                'commerceOrderStatuses' => $integrationsService->getCommerceOrderStatuses(),
            ],
            'formIntegrationIds' => array_keys($formAdapterViewData),
            'integrationReadinessRows' => $integrationReadinessRows,
            'contractRows' => $contractRows,
            'contractSyncMeta' => $syncMeta,
            'integrationSummaryRows' => $integrationSummaryRows,
            'projectUrl' => $projectUrl,
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

    private function setupStepUrl(string $step, bool $relink = false): string
    {
        $url = 'burrow/setup?step=' . urlencode($step);
        if ($relink) {
            $url .= '&relink=1';
        }

        return $url;
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
            $runtimeState['capabilities']['ecommerce_funnel'] = $mode === 'track' && !empty($integrationSettings['commerce']['ecommerceFunnel']);
        }

        $runtimeState['integrationSettings'] = $integrationSettings;

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

        if ($baseUrl === '' || $apiKey === '') {
            Craft::$app->getSession()->setError(Craft::t('burrow', 'Base URL and API key are required.'));
            return $this->redirect($this->setupStepUrl('connection', $relink));
        }

        $runtimeState = $plugin->getState()->getState();
        $runtimeState['connectionBaseUrl'] = $baseUrl;
        $runtimeState['connectionApiKey'] = $apiKey;
        if (!$plugin->getState()->saveState($runtimeState)) {
            Craft::$app->getSession()->setError(Craft::t('burrow', 'Could not save connection settings.'));
            return $this->redirect($this->setupStepUrl('connection', $relink));
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
                return $this->redirect($this->setupStepUrl('connection', $relink));
            }
        }

        $discover = $plugin->getBurrowApi()->discover($baseUrl, $apiKey, (array)($runtimeState['capabilities'] ?? []));
        if (!$discover['ok']) {
            $plugin->getLogs()->log('error', 'Connection discover failed', 'onboarding', 'system', null, ['error' => $discover['error']]);
            Craft::$app->getSession()->setError(Craft::t('burrow', 'Connection failed: {error}', ['error' => $discover['error']]));
            return $this->redirect($this->setupStepUrl('connection', $relink));
        }

        Craft::$app->getSession()->set('burrow.discoveredProjects', $discover['projects']);
        if (!$relink) {
            $runtimeState['onboardingStep'] = 'project';
            $plugin->getState()->saveState($runtimeState);
        }
        $plugin->getLogs()->log('info', 'Connection established and projects discovered', 'onboarding', 'system', null, [
            'projectsCount' => count($discover['projects']),
        ]);

        Craft::$app->getSession()->setNotice(Craft::t('burrow', 'Connection established.'));

        return $this->redirect($this->setupStepUrl('project', $relink));
    }

    public function actionSelectProject(): ?Response
    {
        $this->requirePostRequest();
        $this->requirePermission('accessPlugin-burrow');

        $request = Craft::$app->getRequest();
        $plugin = Plugin::getInstance();
        $runtimeState = $plugin->getState()->getState();

        $rawSelection = (string)$request->getBodyParam('projectSelection', '');
        $decoded = json_decode($rawSelection, true);
        $selection = [
            'organizationId' => trim((string)($decoded['organizationId'] ?? '')),
            'clientId' => trim((string)($decoded['clientId'] ?? '')),
            'projectId' => trim((string)($decoded['projectId'] ?? '')),
        ];
        $discoveredProjects = (array)Craft::$app->getSession()->get('burrow.discoveredProjects', []);
        if ($selection['projectId'] !== '' && ($selection['organizationId'] === '' || $selection['clientId'] === '')) {
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

        if ($selection['projectId'] === '') {
            Craft::$app->getSession()->setError(Craft::t('burrow', 'Please choose a project.'));
            return $this->redirect($this->setupStepUrl('project', $relink));
        }

        $runtimeState['capabilities'] = $plugin->getIntegrations()->buildCapabilities(
            (array)($runtimeState['selectedIntegrations'] ?? [])
        );

        $link = $plugin->getBurrowApi()->link(
            $plugin->getBurrowBaseUrl(),
            $plugin->getBurrowApiKey(),
            $selection,
            (array)$runtimeState['capabilities'],
            $runtimeState
        );
        if (!$link['ok']) {
            $plugin->getLogs()->log('error', 'Project link failed', 'onboarding', 'system', null, ['error' => $link['error']]);
            Craft::$app->getSession()->setError(Craft::t('burrow', 'Project linking failed: {error}', ['error' => $link['error']]));
            return $this->redirect($this->setupStepUrl('project', $relink));
        }

        $runtimeState = $plugin->getBurrowApi()->applyLinkResult($runtimeState, $link);

        if (!$plugin->runtimeStateHasIngestionKey($runtimeState)) {
            $plugin->getLogs()->log('error', 'Project link succeeded but no ingestion key was returned', 'onboarding', 'system', null, $selection);
            Craft::$app->getSession()->setError(Craft::t('burrow', 'Project linking succeeded but Burrow did not return an ingestion key. Try again or re-enter your account API key.'));
            return $this->redirect($this->setupStepUrl('project', $relink));
        }

        $runtimeState['connectionApiKey'] = '';
        $plugin->clearAccountApiKeyFromProjectConfigIfAllowed();

        if ($relink) {
            $syncResult = $plugin->getIntegrations()->syncConfiguration($runtimeState, false);
            $runtimeState = $syncResult['runtimeState'];
            $plugin->getState()->saveState($runtimeState);
            $plugin->getLogs()->log('info', 'Project re-linked', 'settings', 'system', null, $selection);

            if ($syncResult['ok']) {
                Craft::$app->getSession()->setNotice(Craft::t('burrow', 'Project re-linked. A new ingestion key is active.'));
            } else {
                Craft::$app->getSession()->setError($syncResult['error'] !== ''
                    ? Craft::t('burrow', 'Project re-linked, but sync failed: {error}', ['error' => $syncResult['error']])
                    : Craft::t('burrow', 'Project re-linked, but sync to Burrow failed.'));
            }

            return $this->redirect($this->configureSectionUrl('connection'));
        }

        $runtimeState['onboardingStep'] = 'integrations';
        $plugin->getState()->saveState($runtimeState);
        $plugin->getLogs()->log('info', 'Project linked', 'onboarding', 'system', null, $selection);

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
        $runtimeState['onboardingCompleted'] = true;
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
        $syncResult = $plugin->getBurrowApi()->publishSystemSnapshot(
            $plugin->getBurrowBaseUrl(),
            $plugin->getBurrowApiKey(),
            $runtimeState,
            $runtimeState['lastSnapshot']
        );
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
        return $this->redirect('burrow/setup?step=finish');
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
        if (trim((string)($runtimeState['projectId'] ?? '')) === '') {
            Craft::$app->getSession()->setError(Craft::t('burrow', 'Project is not linked yet.'));
            return $this->redirect('burrow/dashboard');
        }

        $request = Craft::$app->getRequest();
        $windowPreset = trim((string)$request->getBodyParam('backfillWindowPreset', 'last_30_days'));
        $sources = (array)$request->getBodyParam('backfillSources', []);

        $integrationSettings = is_array($runtimeState['integrationSettings'] ?? null) ? $runtimeState['integrationSettings'] : [];
        $existingBackfill = is_array($integrationSettings['backfill'] ?? null) ? $integrationSettings['backfill'] : [];
        $existingStatus = (string)($existingBackfill['status'] ?? '');
        if ($existingStatus === 'queued' || $existingStatus === 'running') {
            Craft::$app->getSession()->setError(Craft::t('burrow', 'A backfill is already running. Wait for it to finish or check the queue before starting another.'));
            return $this->redirect('burrow/dashboard#data-backfill');
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
            return $this->redirect('burrow/dashboard#data-backfill');
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
        $plugin->getState()->saveState($runtimeState);

        Craft::$app->getQueue()->push(new \burrow\Burrow\jobs\BackfillChunkJob());

        Craft::$app->getSession()->setNotice(Craft::t('burrow', 'Backfill queued. It will run via Craft’s queue in the background; keep your queue worker running until it finishes. Progress appears on this dashboard.'));
        return $this->redirect('burrow/dashboard#data-backfill');
    }
}
