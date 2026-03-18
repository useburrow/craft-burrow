<?php
namespace burrow\Burrow\controllers;

use Craft;
use craft\web\Controller;
use yii\web\Response;

use burrow\Burrow\Plugin;

class SettingsController extends Controller
{
    protected array|bool|int $allowAnonymous = false;

    public function actionIndex(): Response
    {
        $this->requirePermission('accessPlugin-burrow');

        $plugin = Plugin::getInstance();
        $integrationsService = $plugin->getIntegrations();
        $settings = $plugin->getSettings();
        $runtimeState = $plugin->getState()->getState();
        $availableIntegrations = $integrationsService->getAvailableIntegrations();
        $selectedIntegrations = array_values(array_filter(array_map('strval', (array)($runtimeState['selectedIntegrations'] ?? []))));
        $wizardSteps = $integrationsService->buildWizardSteps($selectedIntegrations);
        $requestedStep = (string)Craft::$app->getRequest()->getQueryParam('step', '');
        $currentStep = array_key_exists((string)($runtimeState['onboardingStep'] ?? ''), $wizardSteps)
            ? (string)$runtimeState['onboardingStep']
            : 'connection';
        $wizardStepOrder = array_keys($wizardSteps);
        $currentStepIndex = array_search($currentStep, $wizardStepOrder, true);
        if ($currentStepIndex === false) {
            $currentStepIndex = 0;
        }
        $step = $currentStep;
        if ($requestedStep !== '' && array_key_exists($requestedStep, $wizardSteps)) {
            $requestedIndex = array_search($requestedStep, $wizardStepOrder, true);
            if ($requestedIndex !== false && $requestedIndex <= $currentStepIndex) {
                $step = $requestedStep;
            }
        }
        $projects = (array)Craft::$app->getSession()->get('burrow.discoveredProjects', []);
        $freeformForms = $integrationsService->getFreeformForms();
        $freeformFieldsByFormId = [];
        foreach ($freeformForms as $freeformForm) {
            $formId = (string)($freeformForm['id'] ?? '');
            if ($formId === '') {
                continue;
            }
            $freeformFieldsByFormId[$formId] = $integrationsService->getFreeformFields($formId);
        }
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
            if (in_array($integrationKey, ['freeform', 'formie'], true)) {
                $count = (int)($contractsByProvider[$integrationKey] ?? 0);
                $detail = $count . ' form' . ($count === 1 ? '' : 's');
            } elseif ($integrationKey === 'commerce') {
                $commerce = is_array($integrationSettings['commerce'] ?? null) ? $integrationSettings['commerce'] : [];
                $mode = (string)($commerce['mode'] ?? 'track');
                $funnel = !empty($commerce['ecommerceFunnel']);
                if ($mode !== 'track') {
                    $detail = 'Off';
                } elseif ($funnel) {
                    $detail = 'Orders + Items + Funnel';
                } else {
                    $detail = 'Orders + Items';
                }
            }
            $integrationSummaryRows[] = [
                'key' => $integrationKey,
                'label' => (string)($integrationLabels[$integrationKey] ?? $integrationKey),
                'detail' => $detail,
                'iconDataUri' => (string)($availableIntegrations[$integrationKey]['iconDataUri'] ?? ''),
            ];
        }
        $projectUrl = trim((string)($runtimeState['burrowProject']['url'] ?? ''));
        if ($projectUrl === '') {
            $path = trim((string)($runtimeState['burrowProject']['path'] ?? ''));
            if ($path !== '' && trim((string)$settings->baseUrl) !== '') {
                $parts = parse_url((string)$settings->baseUrl);
                if (is_array($parts) && !empty($parts['scheme']) && !empty($parts['host'])) {
                    $host = (string)$parts['host'];
                    if (str_starts_with($host, 'api.')) {
                        $host = 'app.' . substr($host, 4);
                    }
                    $projectUrl = (string)$parts['scheme'] . '://' . $host . '/' . ltrim($path, '/');
                }
            }
        }

        return $this->renderTemplate('burrow/settings/index', [
            'settings' => $settings,
            'state' => $runtimeState,
            'step' => $step,
            'currentStep' => $currentStep,
            'currentStepIndex' => $currentStepIndex,
            'wizardSteps' => $wizardSteps,
            'nextStep' => $integrationsService->nextWizardStep($step, $selectedIntegrations),
            'projects' => $projects,
            'availableIntegrations' => $availableIntegrations,
            'integrationStepData' => [
                'freeformForms' => $freeformForms,
                'freeformFieldsByFormId' => $freeformFieldsByFormId,
                'formieForms' => $integrationsService->getFormieForms(),
                'settings' => is_array($runtimeState['integrationSettings'] ?? null) ? $runtimeState['integrationSettings'] : [],
            ],
            'integrationReadinessRows' => $integrationReadinessRows,
            'contractRows' => $contractRows,
            'contractSyncMeta' => $syncMeta,
            'integrationSummaryRows' => $integrationSummaryRows,
            'projectUrl' => $projectUrl,
            'sdkAvailable' => $plugin->getBurrowApi()->isSdkAvailable(),
            'queueStats' => $plugin->getQueue()->stats(),
            'logs' => $plugin->getLogs()->latest(25),
        ]);
    }

    public function actionDashboard(): Response
    {
        $this->requirePermission('accessPlugin-burrow');

        $plugin = Plugin::getInstance();
        $settings = $plugin->getSettings();
        $state = $plugin->getState()->getState();
        $queueStats = $plugin->getQueue()->stats();
        $logs = $plugin->getLogs()->latest(50);
        $snapshot = is_array($state['lastSnapshot'] ?? null) ? $state['lastSnapshot'] : [];
        $formsContracts = $plugin->getIntegrations()->buildFormsContracts($state);
        $integrationSettings = is_array($state['integrationSettings'] ?? null) ? $state['integrationSettings'] : [];
        $backfillState = is_array($integrationSettings['backfill'] ?? null) ? $integrationSettings['backfill'] : [];
        $availableSources = $plugin->getBackfill()->availableSources($state);
        $backfillSources = array_values(array_filter(array_map('strval', (array)($backfillState['sources'] ?? $availableSources))));
        $backfillPresets = $plugin->getBackfill()->presetOptions();

        $contractsByProvider = [];
        foreach ($formsContracts as $contract) {
            $providerKey = trim((string)($contract['provider'] ?? ''));
            if ($providerKey === '' || empty($contract['enabled'])) {
                continue;
            }
            $contractsByProvider[$providerKey] = (int)($contractsByProvider[$providerKey] ?? 0) + 1;
        }
        $integrationSummaryRows = [];
        $integrationLabels = $plugin->getIntegrations()->integrationLabels();
        $availableIntegrations = $plugin->getIntegrations()->getAvailableIntegrations();
        foreach ((array)($state['selectedIntegrations'] ?? []) as $integrationKey) {
            $integrationKey = (string)$integrationKey;
            $detail = '';
            if (in_array($integrationKey, ['freeform', 'formie'], true)) {
                $count = (int)($contractsByProvider[$integrationKey] ?? 0);
                $detail = $count . ' form' . ($count === 1 ? '' : 's');
            } elseif ($integrationKey === 'commerce') {
                $commerce = is_array($integrationSettings['commerce'] ?? null) ? $integrationSettings['commerce'] : [];
                $mode = (string)($commerce['mode'] ?? 'track');
                $funnel = !empty($commerce['ecommerceFunnel']);
                if ($mode !== 'track') {
                    $detail = 'Off';
                } elseif ($funnel) {
                    $detail = 'Orders + Items + Funnel';
                } else {
                    $detail = 'Orders + Items';
                }
            }
            $integrationSummaryRows[] = [
                'label' => (string)($integrationLabels[$integrationKey] ?? $integrationKey),
                'detail' => $detail,
                'iconDataUri' => (string)($availableIntegrations[$integrationKey]['iconDataUri'] ?? ''),
            ];
        }

        return $this->renderTemplate('burrow/dashboard/index', [
            'settings' => $settings,
            'state' => $state,
            'queueStats' => $queueStats,
            'logs' => $logs,
            'snapshot' => $snapshot,
            'integrationSummaryRows' => $integrationSummaryRows,
            'contractRows' => array_values(array_map(static function (array $contract): array {
                $mode = trim((string)($contract['mode'] ?? 'count_only'));
                $modeLabel = $mode === 'custom_fields'
                    ? 'Custom fields'
                    : ($mode === 'off' ? 'Off' : 'Count-only');
                return [
                    'provider' => (string)($contract['provider'] ?? ''),
                    'formName' => trim((string)($contract['formName'] ?? '')),
                    'externalFormId' => trim((string)($contract['externalFormId'] ?? '')),
                    'mode' => $modeLabel,
                    'mappingCount' => is_array($contract['fieldMappings'] ?? null) ? count((array)$contract['fieldMappings']) : 0,
                ];
            }, $formsContracts)),
            'backfillState' => $backfillState,
            'backfillSources' => $backfillSources,
            'availableBackfillSources' => $availableSources,
            'backfillPresets' => $backfillPresets,
            'selectedSubnavItem' => 'dashboard',
        ]);
    }

    public function actionOutbox(): Response
    {
        $this->requirePermission('accessPlugin-burrow');

        $plugin = Plugin::getInstance();

        return $this->renderTemplate('burrow/outbox/index', [
            'rows' => $plugin->getQueue()->listRecent(200),
            'queueStats' => $plugin->getQueue()->stats(),
            'selectedSubnavItem' => 'outbox',
        ]);
    }

    public function actionRetryOutbox(): ?Response
    {
        $this->requirePostRequest();
        $this->requirePermission('accessPlugin-burrow');

        $id = (string)Craft::$app->getRequest()->getBodyParam('id', '');
        if ($id === '') {
            Craft::$app->getSession()->setError(Craft::t('burrow', 'Invalid outbox id.'));
            return $this->redirect('burrow/outbox');
        }

        $ok = Plugin::getInstance()->getQueue()->retryNow($id);
        Craft::$app->getSession()->setNotice($ok ? Craft::t('burrow', 'Outbox record queued for retry.') : Craft::t('burrow', 'Unable to retry outbox record.'));

        return $this->redirect('burrow/outbox');
    }

    public function actionDeleteOutbox(): ?Response
    {
        $this->requirePostRequest();
        $this->requirePermission('accessPlugin-burrow');

        $id = (string)Craft::$app->getRequest()->getBodyParam('id', '');
        if ($id === '') {
            Craft::$app->getSession()->setError(Craft::t('burrow', 'Invalid outbox id.'));
            return $this->redirect('burrow/outbox');
        }

        $ok = Plugin::getInstance()->getQueue()->deleteRecord($id);
        Craft::$app->getSession()->setNotice($ok ? Craft::t('burrow', 'Outbox record deleted.') : Craft::t('burrow', 'Unable to delete outbox record.'));

        return $this->redirect('burrow/outbox');
    }

    public function actionSaveConnection(): ?Response
    {
        $this->requirePostRequest();
        $this->requirePermission('accessPlugin-burrow');

        $request = Craft::$app->getRequest();
        $plugin = Plugin::getInstance();
        $settings = $plugin->getSettings();
        $settings->baseUrl = trim((string)$request->getBodyParam('baseUrl', $settings->baseUrl));
        $settings->apiKey = trim((string)$request->getBodyParam('apiKey', $settings->apiKey));

        if ($settings->baseUrl === '' || $settings->apiKey === '') {
            Craft::$app->getSession()->setError(Craft::t('burrow', 'Base URL and API key are required.'));
            return $this->redirect('burrow/settings?step=connection');
        }

        if (!Craft::$app->getConfig()->getGeneral()->allowAdminChanges) {
            Craft::$app->getSession()->setError(Craft::t('burrow', 'Changes to plugin settings are not permitted in this environment.'));
            return $this->redirect('burrow/settings?step=connection');
        }

        if (!Craft::$app->getPlugins()->savePluginSettings($plugin, $settings->toArray())) {
            $errors = $settings->getFirstErrors();
            $message = Craft::t('burrow', 'Could not save base URL/API key.');
            if (!empty($errors)) {
                $message .= ' ' . implode(' ', array_values($errors));
            }
            Craft::error('Burrow settings save failed: ' . json_encode($errors), __METHOD__);
            Craft::$app->getSession()->setError($message);
            return $this->redirect('burrow/settings?step=connection');
        }

        $runtimeState = $plugin->getState()->getState();
        $discover = $plugin->getBurrowApi()->discover($settings->baseUrl, $settings->apiKey, (array)($runtimeState['capabilities'] ?? []));
        if (!$discover['ok']) {
            $plugin->getLogs()->log('error', 'Connection discover failed', 'onboarding', 'system', null, ['error' => $discover['error']]);
            Craft::$app->getSession()->setError(Craft::t('burrow', 'Connection failed: {error}', ['error' => $discover['error']]));
            return $this->redirect('burrow/settings?step=connection');
        }

        Craft::$app->getSession()->set('burrow.discoveredProjects', $discover['projects']);
        $runtimeState['onboardingStep'] = 'project';
        $plugin->getState()->saveState($runtimeState);
        $plugin->getLogs()->log('info', 'Connection established and projects discovered', 'onboarding', 'system', null, [
            'projectsCount' => count($discover['projects']),
        ]);

        Craft::$app->getSession()->setNotice(Craft::t('burrow', 'Connection established.'));

        return $this->redirect('burrow/settings?step=project');
    }

    public function actionSelectProject(): ?Response
    {
        $this->requirePostRequest();
        $this->requirePermission('accessPlugin-burrow');

        $request = Craft::$app->getRequest();
        $plugin = Plugin::getInstance();
        $settings = $plugin->getSettings();
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

        if ($selection['projectId'] === '') {
            Craft::$app->getSession()->setError(Craft::t('burrow', 'Please choose a project.'));
            return $this->redirect('burrow/settings?step=project');
        }

        $runtimeState['capabilities'] = $plugin->getIntegrations()->buildCapabilities(
            (array)($runtimeState['selectedIntegrations'] ?? [])
        );

        $link = $plugin->getBurrowApi()->link(
            $settings->baseUrl,
            $settings->apiKey,
            $selection,
            (array)$runtimeState['capabilities'],
            $runtimeState
        );
        if (!$link['ok']) {
            $plugin->getLogs()->log('error', 'Project link failed', 'onboarding', 'system', null, ['error' => $link['error']]);
            Craft::$app->getSession()->setError(Craft::t('burrow', 'Project linking failed: {error}', ['error' => $link['error']]));
            return $this->redirect('burrow/settings?step=project');
        }

        $runtimeState = $plugin->getBurrowApi()->applyLinkResult($runtimeState, $link);
        $runtimeState['onboardingStep'] = 'integrations';
        $plugin->getState()->saveState($runtimeState);
        $plugin->getLogs()->log('info', 'Project linked', 'onboarding', 'system', null, $selection);

        Craft::$app->getSession()->setNotice(Craft::t('burrow', 'Project selected and linked.'));

        return $this->redirect('burrow/settings?step=integrations');
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
            return $this->redirect('burrow/settings?step=integrations');
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

        return $this->redirect('burrow/settings?step=' . urlencode($nextStep));
    }

    public function actionSaveIntegrationStep(): ?Response
    {
        $this->requirePostRequest();
        $this->requirePermission('accessPlugin-burrow');

        $plugin = Plugin::getInstance();
        $runtimeState = $plugin->getState()->getState();
        $selected = array_values(array_filter(array_map('strval', (array)($runtimeState['selectedIntegrations'] ?? []))));
        $integration = trim((string)Craft::$app->getRequest()->getBodyParam('integration', ''));

        if (!$plugin->getIntegrations()->isIntegrationStep($integration) || !in_array($integration, $selected, true)) {
            Craft::$app->getSession()->setError(Craft::t('burrow', 'Invalid integration step.'));
            return $this->redirect('burrow/settings?step=integrations');
        }

        $integrationSettings = is_array($runtimeState['integrationSettings'] ?? null) ? $runtimeState['integrationSettings'] : [];
        if ($integration === 'freeform') {
            $payload = (array)Craft::$app->getRequest()->getBodyParam('freeform', []);
            $formsPayload = is_array($payload['forms'] ?? null) ? (array)$payload['forms'] : [];
            $normalizedForms = [];
            foreach ($formsPayload as $formId => $formConfig) {
                if (!is_array($formConfig)) {
                    continue;
                }
                $mode = trim((string)($formConfig['mode'] ?? 'off'));
                if (!in_array($mode, ['off', 'count_only', 'custom_fields'], true)) {
                    $mode = 'off';
                }
                $fieldsPayload = is_array($formConfig['fields'] ?? null) ? (array)$formConfig['fields'] : [];
                $normalizedFields = [];
                foreach ($fieldsPayload as $fieldId => $fieldConfig) {
                    if (!is_array($fieldConfig) || empty($fieldConfig['include'])) {
                        continue;
                    }
                    $target = trim((string)($fieldConfig['target'] ?? ''));
                    $normalizedFields[(string)$fieldId] = [
                        'externalFieldId' => trim((string)($fieldConfig['externalFieldId'] ?? $fieldId)),
                        'sourceLabel' => trim((string)($fieldConfig['sourceLabel'] ?? '')),
                        'dataType' => trim((string)($fieldConfig['dataType'] ?? 'string')),
                        'canonicalKey' => trim((string)($fieldConfig['canonicalKey'] ?? '')),
                        'target' => in_array($target, ['properties', 'tags'], true) ? $target : '',
                        'displayLabelOverride' => trim((string)($fieldConfig['displayLabelOverride'] ?? '')),
                    ];
                }
                $normalizedForms[(string)$formId] = [
                    'externalFormId' => trim((string)($formConfig['externalFormId'] ?? $formId)),
                    'formName' => trim((string)($formConfig['formName'] ?? '')),
                    'mode' => $mode,
                    'fields' => $normalizedFields,
                ];
            }
            $integrationSettings['freeform'] = [
                'forms' => $normalizedForms,
            ];
        } elseif ($integration === 'formie') {
            $selectedForms = (array)Craft::$app->getRequest()->getBodyParam('formieFormIds', []);
            $selectedForms = array_values(array_filter(array_map('strval', $selectedForms)));
            $mode = trim((string)Craft::$app->getRequest()->getBodyParam('formieMode', 'count_only'));
            if (!in_array($mode, ['off', 'count_only', 'custom_fields'], true)) {
                $mode = 'count_only';
            }
            $integrationSettings['formie'] = [
                'mode' => $mode,
                'formIds' => $selectedForms,
            ];
        } elseif ($integration === 'commerce') {
            $mode = trim((string)Craft::$app->getRequest()->getBodyParam('commerceMode', 'track'));
            if (!in_array($mode, ['track', 'off'], true)) {
                $mode = 'track';
            }
            $integrationSettings['commerce'] = [
                'mode' => $mode,
                'ecommerceFunnel' => (bool)Craft::$app->getRequest()->getBodyParam('ecommerceFunnel', false),
            ];
            $runtimeState['capabilities']['ecommerce_funnel'] = $mode === 'track' && !empty($integrationSettings['commerce']['ecommerceFunnel']);
        }

        $runtimeState['integrationSettings'] = $integrationSettings;
        $nextStep = $plugin->getIntegrations()->nextWizardStep($integration, $selected);
        $runtimeState['onboardingStep'] = $nextStep;
        $plugin->getState()->saveState($runtimeState);
        $plugin->getLogs()->log('info', 'Integration setup step completed', 'onboarding', 'system', $integration);

        return $this->redirect('burrow/settings?step=' . urlencode($nextStep));
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
        return $this->redirect('burrow/settings?step=review');
    }

    public function actionSyncContracts(): ?Response
    {
        $this->requirePostRequest();
        $this->requirePermission('accessPlugin-burrow');

        $plugin = Plugin::getInstance();
        $settings = $plugin->getSettings();
        $runtimeState = $plugin->getState()->getState();
        if (trim((string)($runtimeState['projectId'] ?? '')) === '') {
            Craft::$app->getSession()->setError(Craft::t('burrow', 'Project is not linked yet. Complete Step 2 before syncing contracts.'));
            return $this->redirect('burrow/settings?step=review');
        }

        $contracts = $plugin->getIntegrations()->buildFormsContracts($runtimeState);
        if (empty($contracts)) {
            Craft::$app->getSession()->setError(Craft::t('burrow', 'No enabled contracts found. Configure at least one form integration before syncing.'));
            return $this->redirect('burrow/settings?step=review');
        }

        $result = $plugin->getBurrowApi()->submitFormsContracts(
            $settings->baseUrl,
            $settings->apiKey,
            $runtimeState,
            $contracts
        );
        if (!$result['ok']) {
            $plugin->getLogs()->log('error', 'Forms contract sync failed', 'onboarding', 'system', null, ['error' => $result['error']]);
            Craft::$app->getSession()->setError(Craft::t('burrow', 'Contract sync failed: {error}', ['error' => $result['error']]));
            return $this->redirect('burrow/settings?step=review');
        }

        $runtimeState['sdkState'] = is_array($result['sdkState'] ?? null) ? $result['sdkState'] : (array)($runtimeState['sdkState'] ?? []);
        $projectSourceId = trim((string)($result['projectSourceId'] ?? ''));
        if ($projectSourceId !== '') {
            $runtimeState['projectSourceId'] = $projectSourceId;
            $sourceIds = is_array($runtimeState['sourceIds'] ?? null) ? $runtimeState['sourceIds'] : [];
            $sourceIds['forms'] = $projectSourceId;
            if (trim((string)($sourceIds['system'] ?? '')) === '') {
                $sourceIds['system'] = $projectSourceId;
            }
            $runtimeState['sourceIds'] = $sourceIds;
        }
        $integrationSettings = is_array($runtimeState['integrationSettings'] ?? null) ? $runtimeState['integrationSettings'] : [];
        $integrationSettings['contractSync'] = [
            'version' => trim((string)($result['contractsVersion'] ?? '')),
            'syncedAt' => gmdate('c'),
            'mappingCount' => is_array($result['contractMappings'] ?? null) ? count((array)$result['contractMappings']) : 0,
        ];
        $runtimeState['integrationSettings'] = $integrationSettings;
        $runtimeState['onboardingStep'] = 'finish';
        $plugin->getState()->saveState($runtimeState);
        $plugin->getLogs()->log('info', 'Contracts synced to Burrow', 'onboarding', 'system', null, [
            'contractsVersion' => $integrationSettings['contractSync']['version'],
            'contractsCount' => count($contracts),
        ]);

        Craft::$app->getSession()->setNotice(Craft::t('burrow', 'Contracts synced to Burrow.'));
        return $this->redirect('burrow/settings?step=finish');
    }

    public function actionFinish(): ?Response
    {
        $this->requirePostRequest();
        $this->requirePermission('accessPlugin-burrow');

        $plugin = Plugin::getInstance();
        $settings = $plugin->getSettings();
        $runtimeState = $plugin->getState()->getState();
        $runtimeState['lastSnapshot'] = $plugin->getSnapshot()->collectSnapshot();
        $syncResult = $plugin->getBurrowApi()->publishSystemSnapshot(
            $settings->baseUrl,
            $settings->apiKey,
            $runtimeState,
            $runtimeState['lastSnapshot']
        );
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
        return $this->redirect('burrow/settings?step=finish');
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
        $result = $plugin->getBackfill()->runBackfill($runtimeState, $windowPreset, $sources);

        $integrationSettings = is_array($runtimeState['integrationSettings'] ?? null) ? $runtimeState['integrationSettings'] : [];
        $integrationSettings['backfill'] = [
            'status' => $result['ok'] ? 'completed' : 'failed',
            'windowPreset' => $windowPreset,
            'windowStart' => (string)$result['windowStart'],
            'windowEnd' => (string)$result['windowEnd'],
            'sources' => (array)$result['sources'],
            'requested' => (int)$result['requested'],
            'accepted' => (int)$result['accepted'],
            'rejected' => (int)$result['rejected'],
            'validationRejected' => (int)$result['validationRejected'],
            'latestCursor' => (string)$result['latestCursor'],
            'breakdown' => (array)$result['breakdown'],
            'startedAt' => gmdate('c'),
            'completedAt' => gmdate('c'),
            'error' => (string)$result['error'],
        ];
        $runtimeState['integrationSettings'] = $integrationSettings;
        $plugin->getState()->saveState($runtimeState);

        if ($result['ok']) {
            $plugin->getLogs()->log('info', 'Backfill completed', 'backfill', 'system', null, [
                'requested' => $result['requested'],
                'accepted' => $result['accepted'],
                'rejected' => $result['rejected'],
                'sources' => $result['sources'],
                'windowStart' => $result['windowStart'],
            ]);
            Craft::$app->getSession()->setNotice(Craft::t('burrow', 'Backfill completed. Requested: {requested}, accepted: {accepted}, rejected: {rejected}.', [
                'requested' => (string)$result['requested'],
                'accepted' => (string)$result['accepted'],
                'rejected' => (string)$result['rejected'],
            ]));
        } else {
            $plugin->getLogs()->log('error', 'Backfill failed', 'backfill', 'system', null, [
                'error' => $result['error'],
                'sources' => $sources,
                'windowPreset' => $windowPreset,
            ]);
            Craft::$app->getSession()->setError(Craft::t('burrow', 'Backfill failed: {error}', [
                'error' => (string)$result['error'],
            ]));
        }

        return $this->redirect('burrow/dashboard');
    }
}
