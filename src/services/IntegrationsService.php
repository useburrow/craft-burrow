<?php
namespace burrow\Burrow\services;

use burrow\Burrow\integrations\forms\FormIntegrationAdapter;
use burrow\Burrow\integrations\forms\FormIntegrationsRegistry;
use burrow\Burrow\Plugin;
use Craft;
use craft\base\Component;

class IntegrationsService extends Component
{
    private ?FormIntegrationsRegistry $_formIntegrations = null;

    public function getFormIntegrations(): FormIntegrationsRegistry
    {
        if ($this->_formIntegrations === null) {
            $this->_formIntegrations = new FormIntegrationsRegistry();
        }

        return $this->_formIntegrations;
    }

    public function getFormIntegration(string $id): ?FormIntegrationAdapter
    {
        return $this->getFormIntegrations()->get($id);
    }

    public function isFormIntegration(string $step): bool
    {
        return $this->getFormIntegrations()->has($step);
    }

    /**
     * @return string[]
     */
    public function integrationOrder(): array
    {
        return array_merge($this->getFormIntegrations()->ids(), ['commerce', 'shopify']);
    }

    /**
     * @return array<string,string>
     */
    public function integrationLabels(): array
    {
        $labels = [];
        foreach ($this->getFormIntegrations()->all() as $adapter) {
            $labels[$adapter->getId()] = $adapter->getLabel();
        }
        $labels['commerce'] = 'Craft Commerce';
        $labels['shopify'] = 'Shopify (Headless)';

        return $labels;
    }

    /**
     * @param string[] $selected
     * @return array<string,string>
     *
     * @author Burrow Analytics, LLC
     * @since 5.0.0
     */
    public function buildWizardSteps(array $selected): array
    {
        $steps = [
            'connection' => 'Connection',
        ];

        if (count(Craft::$app->getSites()->getAllSites()) > 1) {
            $steps['sites'] = 'Sites';
        }

        $steps['project'] = 'Project';
        $steps['integrations'] = 'Integrations';

        $labels = $this->integrationLabels();
        foreach ($this->integrationOrder() as $integration) {
            if (!in_array($integration, $selected, true)) {
                continue;
            }
            $steps[$integration] = (string)($labels[$integration] ?? $integration);
        }

        $steps['review'] = 'Review';
        $steps['finish'] = 'Finish';

        return $steps;
    }

    /**
     * @param string[] $selected
     * @return array<string,string>
     */
    public function buildSettingsSections(array $selected): array
    {
        $sections = [
            'overview' => 'Overview',
            'integrations' => 'Integrations',
        ];

        $labels = $this->integrationLabels();
        foreach ($this->integrationOrder() as $integration) {
            if (!in_array($integration, $selected, true)) {
                continue;
            }
            $sections[$integration] = (string)($labels[$integration] ?? $integration);
        }

        $sections['connection'] = 'Connection';

        return $sections;
    }

    /**
     * @param array<string,mixed> $capabilities
     */
    public function capabilitiesFingerprint(array $capabilities): string
    {
        return json_encode($capabilities, JSON_THROW_ON_ERROR);
    }

    /**
     * Syncs forms contracts (and optionally re-links / publishes a snapshot) for each linked Craft site.
     *
     * Shared Formie/Freeform forms are registered under every linked project. Install-level settings
     * (selected integrations, contract sync meta) are shared; per-site routing/keys stay isolated.
     *
     * @param array<string,mixed> $runtimeState
     * @param bool $forceRelink When true, re-link each linked project with current capabilities (e.g. after integration selection changes).
     * @param bool $publishSnapshot When true, publish a system stack snapshot per linked site (onboarding only; routine settings saves skip this).
     * @return array{
     *     ok:bool,
     *     error:string,
     *     runtimeState:array<string,mixed>,
     *     relinked:bool,
     *     contractsSynced:bool,
     *     contractsCount:int,
     *     snapshotSynced:bool,
     *     notice:string
     * }
     *
     * @author Burrow Analytics, LLC
     * @since 5.0.0
     */
    public function syncConfiguration(array $runtimeState, bool $forceRelink = false, bool $publishSnapshot = false): array
    {
        $plugin = Plugin::getInstance();
        $stateService = $plugin->getState();
        $linkedSites = $stateService->getLinkedSiteStates();

        // Legacy / mid-onboarding: flat project fields without siteStates entries yet.
        if ($linkedSites === [] && trim((string)($runtimeState['projectId'] ?? '')) !== '') {
            $siteId = (int)($runtimeState['craftSiteId'] ?? Craft::$app->getSites()->getPrimarySite()?->id ?? 0);
            if ($siteId > 0) {
                $linkedSites[(string)$siteId] = [
                    'siteId' => $siteId,
                    'projectId' => (string)$runtimeState['projectId'],
                    'enabled' => true,
                    'linked' => true,
                ];
            }
        }

        if ($linkedSites === []) {
            return [
                'ok' => false,
                'error' => Craft::t('burrow', 'Project is not linked yet.'),
                'runtimeState' => $runtimeState,
                'relinked' => false,
                'contractsSynced' => false,
                'contractsCount' => 0,
                'snapshotSynced' => false,
                'notice' => '',
            ];
        }

        $relinked = false;
        $contractsSynced = false;
        $contractsCount = 0;
        $snapshotSynced = false;
        $lastSnapshotError = '';
        $anySiteOk = false;
        $lastError = '';

        foreach ($linkedSites as $siteKey => $_meta) {
            $siteId = (int)$siteKey;
            $siteRuntime = $stateService->getSiteState($siteId);
            // Keep install-level fields from the caller (may include unsaved integration edits).
            foreach (['selectedIntegrations', 'capabilities', 'integrationSettings', 'onboardingStep', 'onboardingCompleted', 'connectionBaseUrl', 'connectionApiKey'] as $installKey) {
                if (array_key_exists($installKey, $runtimeState)) {
                    $siteRuntime[$installKey] = $runtimeState[$installKey];
                }
            }

            if (!$plugin->canDispatchToBurrow($siteRuntime)) {
                $lastError = Craft::t('burrow', 'Burrow connection is not ready for site {site}. Re-link the project if you recently rotated credentials.', [
                    'site' => (string)($siteRuntime['siteHandle'] ?? $siteId),
                ]);
                continue;
            }

            if ($forceRelink) {
                $selection = [
                    'organizationId' => trim((string)($siteRuntime['organizationId'] ?? '')),
                    'clientId' => trim((string)($siteRuntime['clientId'] ?? '')),
                    'projectId' => trim((string)($siteRuntime['projectId'] ?? '')),
                ];
                if ($selection['projectId'] === '') {
                    $lastError = Craft::t('burrow', 'Project is not linked yet.');
                    continue;
                }

                $siteUrl = trim((string)($siteRuntime['siteUrl'] ?? ''));
                $link = $plugin->getBurrowApi()->link(
                    $plugin->getBurrowBaseUrl(),
                    $plugin->getBurrowApiKey(),
                    $selection,
                    (array)($siteRuntime['capabilities'] ?? []),
                    $siteRuntime,
                    $siteUrl !== '' ? $siteUrl : null,
                    false
                );
                if (!$link['ok']) {
                    $plugin->getLogs()->log('error', 'Project re-link failed during configuration sync', 'settings', 'system', null, [
                        'error' => $link['error'],
                        'siteId' => $siteId,
                        'code' => $link['code'] ?? '',
                    ]);
                    $lastError = Craft::t('burrow', 'Project re-link failed: {error}', ['error' => $link['error']]);
                    continue;
                }

                $siteRuntime = $plugin->getBurrowApi()->applyLinkResult($siteRuntime, $link);
                $relinked = true;
                $stateService->saveSiteState($siteId, [
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
                    'siteUrl' => (string)($siteRuntime['siteUrl'] ?? ''),
                ], [
                    'selectedIntegrations' => (array)($runtimeState['selectedIntegrations'] ?? []),
                    'capabilities' => (array)($runtimeState['capabilities'] ?? []),
                    'integrationSettings' => (array)($runtimeState['integrationSettings'] ?? []),
                    'connectionBaseUrl' => (string)($runtimeState['connectionBaseUrl'] ?? ''),
                    'connectionApiKey' => (string)($runtimeState['connectionApiKey'] ?? ''),
                ]);
            }

            $contracts = $this->buildFormsContracts($siteRuntime);
            $siteContractsCount = count($contracts);
            $contractsCount = max($contractsCount, $siteContractsCount);

            if ($siteContractsCount > 0) {
                $result = $plugin->getBurrowApi()->submitFormsContracts(
                    $plugin->getBurrowBaseUrl(),
                    $plugin->getBurrowApiKey(),
                    $siteRuntime,
                    $contracts
                );
                if (!$result['ok']) {
                    $plugin->getLogs()->log('error', 'Forms contract sync failed', 'settings', 'system', null, [
                        'error' => $result['error'],
                        'siteId' => $siteId,
                    ]);
                    $lastError = Craft::t('burrow', 'Contract sync failed: {error}', ['error' => $result['error']]);
                    continue;
                }

                $siteRuntime['sdkState'] = is_array($result['sdkState'] ?? null) ? $result['sdkState'] : (array)($siteRuntime['sdkState'] ?? []);
                $contractMappings = is_array($result['contractMappings'] ?? null) ? $result['contractMappings'] : [];
                if ($contractMappings !== []) {
                    $siteRuntime['sdkState']['contractMappings'] = $contractMappings;
                }
                $projectSourceId = trim((string)($result['projectSourceId'] ?? ''));
                if ($projectSourceId !== '') {
                    $siteRuntime['projectSourceId'] = $projectSourceId;
                    $sourceIds = is_array($siteRuntime['sourceIds'] ?? null) ? $siteRuntime['sourceIds'] : [];
                    $sourceIds['forms'] = $projectSourceId;
                    if (trim((string)($sourceIds['ecommerce'] ?? '')) === '') {
                        $sourceIds['ecommerce'] = $projectSourceId;
                    }
                    if (trim((string)($sourceIds['system'] ?? '')) === '') {
                        $sourceIds['system'] = $projectSourceId;
                    }
                    $siteRuntime['sourceIds'] = $sourceIds;
                }
                $integrationSettings = is_array($runtimeState['integrationSettings'] ?? null) ? $runtimeState['integrationSettings'] : [];
                $integrationSettings['contractSync'] = [
                    'version' => trim((string)($result['contractsVersion'] ?? '')),
                    'syncedAt' => gmdate('c'),
                    'mappingCount' => is_array($result['contractMappings'] ?? null) ? count((array)$result['contractMappings']) : 0,
                ];
                $runtimeState['integrationSettings'] = $integrationSettings;
                $siteRuntime['integrationSettings'] = $integrationSettings;
                $contractsSynced = true;

                $plugin->getLogs()->log('info', 'Forms contracts synced to Burrow', 'settings', 'system', null, [
                    'contractsCount' => $siteContractsCount,
                    'contractsVersion' => trim((string)($result['contractsVersion'] ?? '')),
                    'siteId' => $siteId,
                ]);
            }

            if ($publishSnapshot) {
                $siteRuntime['lastSnapshot'] = $plugin->getSnapshot()->collectSnapshot();
                $snapshotResult = $plugin->getBurrowApi()->publishSystemSnapshot(
                    $plugin->getBurrowBaseUrl(),
                    $plugin->getBurrowApiKey(),
                    $siteRuntime,
                    $siteRuntime['lastSnapshot']
                );
                if (!empty($snapshotResult['ok'])) {
                    $snapshotSynced = true;
                    $integrationSettings = is_array($runtimeState['integrationSettings'] ?? null) ? $runtimeState['integrationSettings'] : [];
                    $systemJobs = is_array($integrationSettings['systemJobs'] ?? null) ? $integrationSettings['systemJobs'] : [];
                    $systemJobs['snapshotLastRunAt'] = gmdate('c');
                    $systemJobs['snapshotQueuedAt'] = '';
                    $systemJobs['snapshotLastError'] = '';
                    $integrationSettings['systemJobs'] = $systemJobs;
                    $runtimeState['integrationSettings'] = $integrationSettings;
                    $siteRuntime['integrationSettings'] = $integrationSettings;
                } else {
                    $lastSnapshotError = (string)($snapshotResult['error'] ?? '');
                }
            }

            $stateService->saveSiteState($siteId, [
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
                'lastSnapshot' => (array)($siteRuntime['lastSnapshot'] ?? []),
                'siteUrl' => (string)($siteRuntime['siteUrl'] ?? ''),
            ], [
                'selectedIntegrations' => (array)($runtimeState['selectedIntegrations'] ?? []),
                'capabilities' => (array)($runtimeState['capabilities'] ?? []),
                'integrationSettings' => (array)($runtimeState['integrationSettings'] ?? []),
                'onboardingStep' => (string)($runtimeState['onboardingStep'] ?? 'connection'),
                'onboardingCompleted' => (bool)($runtimeState['onboardingCompleted'] ?? false),
                'connectionBaseUrl' => (string)($runtimeState['connectionBaseUrl'] ?? ''),
                'connectionApiKey' => (string)($runtimeState['connectionApiKey'] ?? ''),
            ]);

            $anySiteOk = true;
        }

        $runtimeState = $stateService->getState();
        // Preserve caller-provided install fields that may not yet be flushed if no site saved.
        // getState() after saveSiteState is authoritative when anySiteOk.

        if (!$anySiteOk) {
            return [
                'ok' => false,
                'error' => $lastError !== ''
                    ? $lastError
                    : Craft::t('burrow', 'Burrow connection is not ready. Check your linked project and credentials.'),
                'runtimeState' => $runtimeState,
                'relinked' => false,
                'contractsSynced' => false,
                'contractsCount' => 0,
                'snapshotSynced' => false,
                'notice' => '',
            ];
        }

        if (!$contractsSynced && !$relinked && !$snapshotSynced) {
            return [
                'ok' => false,
                'error' => Craft::t('burrow', 'Nothing to sync to Burrow. Enable at least one integration or form contract.'),
                'runtimeState' => $runtimeState,
                'relinked' => $relinked,
                'contractsSynced' => false,
                'contractsCount' => 0,
                'snapshotSynced' => false,
                'notice' => '',
            ];
        }

        if ($publishSnapshot && !$snapshotSynced && ($contractsSynced || $relinked)) {
            $plugin->getLogs()->log('warning', 'Configuration synced but snapshot publish failed', 'settings', 'system', null, [
                'error' => $lastSnapshotError,
            ]);

            return [
                'ok' => true,
                'error' => '',
                'runtimeState' => $runtimeState,
                'relinked' => $relinked,
                'contractsSynced' => $contractsSynced,
                'contractsCount' => $contractsCount,
                'snapshotSynced' => false,
                'notice' => Craft::t('burrow', 'Configuration synced to Burrow. Snapshot sync pending: {error}', [
                    'error' => $lastSnapshotError,
                ]),
            ];
        }

        $plugin->getLogs()->log('info', 'Configuration synced to Burrow', 'settings', 'system', null, [
            'relinked' => $relinked,
            'contractsSynced' => $contractsSynced,
            'contractsCount' => $contractsCount,
            'snapshotSynced' => $snapshotSynced,
            'linkedSites' => count($linkedSites),
        ]);

        if ($contractsSynced) {
            $notice = Craft::t('burrow', 'Settings saved and synced to Burrow ({count} contract(s) across {sites} site(s)).', [
                'count' => (string)$contractsCount,
                'sites' => (string)count($linkedSites),
            ]);
        } elseif ($relinked) {
            $notice = Craft::t('burrow', 'Settings saved and project capabilities updated in Burrow.');
        } else {
            $notice = Craft::t('burrow', 'Settings saved and system snapshot published to Burrow.');
        }

        return [
            'ok' => true,
            'error' => '',
            'runtimeState' => $runtimeState,
            'relinked' => $relinked,
            'contractsSynced' => $contractsSynced,
            'contractsCount' => $contractsCount,
            'snapshotSynced' => $snapshotSynced,
            'notice' => $notice,
        ];
    }

    /**
     * @param string[] $selected
     */
    public function nextWizardStep(string $fromStep, array $selected): string
    {
        $keys = array_keys($this->buildWizardSteps($selected));
        $index = array_search($fromStep, $keys, true);
        if ($index === false) {
            return 'review';
        }

        return (string)($keys[$index + 1] ?? 'review');
    }

    /**
     * @param string[] $selected
     */
    public function previousWizardStep(string $fromStep, array $selected): string
    {
        $keys = array_keys($this->buildWizardSteps($selected));
        $index = array_search($fromStep, $keys, true);
        if ($index === false || $index === 0) {
            return '';
        }

        return (string)$keys[$index - 1];
    }

    public function isIntegrationStep(string $step): bool
    {
        return in_array($step, $this->integrationOrder(), true);
    }

    /**
     * @return array<string,array<string,mixed>>
     */
    public function getAvailableIntegrations(): array
    {
        $result = [];
        foreach ($this->getFormIntegrations()->all() as $adapter) {
            $result[$adapter->getId()] = $this->pluginStatus($adapter->getCraftPluginHandle(), $adapter->getLabel());
        }
        $result['commerce'] = $this->pluginStatus('commerce', 'Craft Commerce');
        $result['shopify'] = $this->pluginStatus('shopify', 'Shopify (Headless)');

        return $result;
    }

    /**
     * @param array<string,mixed> $runtimeState
     * @return array<string, array{
     *     forms: array<int, array{id: string, name: string, handle: string, submissionCount120d: int, lastSubmittedAt: string, isActive: bool}>,
     *     fieldsByFormId: array<string, array<int, array<string, string>>>
     * }>
     */
    public function buildFormAdapterViewData(array $runtimeState): array
    {
        $data = [];
        foreach ($this->getFormIntegrations()->all() as $adapter) {
            $id = $adapter->getId();
            $forms = $adapter->discoverForms();
            $activity = $adapter->discoverFormActivity(120);
            $fieldsByFormId = [];
            $enriched = [];
            foreach ($forms as $form) {
                $formId = (string)($form['id'] ?? '');
                if ($formId === '') {
                    continue;
                }
                $stats = is_array($activity[$formId] ?? null) ? $activity[$formId] : [];
                $count = (int)($stats['count'] ?? 0);
                $lastSubmittedAt = trim((string)($stats['lastSubmittedAt'] ?? ''));
                $enriched[] = [
                    'id' => $formId,
                    'name' => (string)($form['name'] ?? ''),
                    'handle' => (string)($form['handle'] ?? ''),
                    'submissionCount120d' => $count,
                    'lastSubmittedAt' => $lastSubmittedAt,
                    'isActive' => $count > 0,
                ];
                $fieldsByFormId[$formId] = $adapter->discoverFields($formId);
            }
            usort($enriched, static function (array $a, array $b): int {
                $countCmp = ($b['submissionCount120d'] ?? 0) <=> ($a['submissionCount120d'] ?? 0);
                if ($countCmp !== 0) {
                    return $countCmp;
                }

                return strcasecmp((string)($a['name'] ?? ''), (string)($b['name'] ?? ''));
            });
            $data[$id] = [
                'id' => $id,
                'label' => $adapter->getLabel(),
                'defaultPrefix' => $adapter->getDefaultPrefix(),
                'forms' => $enriched,
                'fieldsByFormId' => $fieldsByFormId,
            ];
        }

        return $data;
    }

    /**
     * @return array<int,array{handle: string, name: string, color: string, id: string}>
     */
    public function getCommerceOrderStatuses(): array
    {
        $commerceClass = '\craft\commerce\Plugin';
        if (!class_exists($commerceClass) || !method_exists($commerceClass, 'getInstance')) {
            return [];
        }

        try {
            $commerce = $commerceClass::getInstance();
            if ($commerce === null || !method_exists($commerce, 'getOrderStatuses')) {
                return [];
            }

            $statusService = $commerce->getOrderStatuses();
            $statuses = [];

            // Commerce 5.x: per-store statuses.
            if (method_exists($commerce, 'getStores')) {
                $stores = $commerce->getStores();
                $store = method_exists($stores, 'getCurrentStore') ? $stores->getCurrentStore() : null;
                if ($store !== null && method_exists($statusService, 'getAllOrderStatusesForStore')) {
                    $allStatuses = $statusService->getAllOrderStatusesForStore($store);
                    foreach ($allStatuses as $status) {
                        if (!is_object($status)) {
                            continue;
                        }
                        $statuses[] = [
                            'id' => (string)($status->id ?? ''),
                            'handle' => (string)($status->handle ?? ''),
                            'name' => (string)($status->name ?? ''),
                            'color' => (string)($status->color ?? ''),
                        ];
                    }
                    if ($statuses !== []) {
                        return $statuses;
                    }
                }
            }

            // Commerce 4.x fallback.
            if (method_exists($statusService, 'getAllOrderStatuses')) {
                $allStatuses = $statusService->getAllOrderStatuses();
                foreach ($allStatuses as $status) {
                    if (!is_object($status)) {
                        continue;
                    }
                    $statuses[] = [
                        'id' => (string)($status->id ?? ''),
                        'handle' => (string)($status->handle ?? ''),
                        'name' => (string)($status->name ?? ''),
                        'color' => (string)($status->color ?? ''),
                    ];
                }
            }

            return $statuses;
        } catch (\Throwable) {
            return [];
        }
    }

    /**
     * @param string[] $selected
     * @return array<string,mixed>
     */
    public function buildCapabilities(array $selected): array
    {
        $forms = [];
        foreach ($this->getFormIntegrations()->all() as $adapter) {
            if (in_array($adapter->getId(), $selected, true)) {
                $forms[] = $adapter->getId();
            }
        }

        $ecommerce = [];
        if (in_array('commerce', $selected, true)) {
            $ecommerce[] = 'craft-commerce';
        }
        if (in_array('shopify', $selected, true)) {
            $ecommerce[] = 'shopify';
        }

        return [
            'forms' => $forms,
            'ecommerce' => $ecommerce,
            'ecommerce_funnel' => $ecommerce !== [],
        ];
    }

    /**
     * Effective `ecommerce_funnel` capability across every configured ecommerce integration.
     *
     * @param array<string,mixed> $integrationSettings
     * @param string[] $selected
     */
    public function resolveFunnelCapability(array $integrationSettings, array $selected): bool
    {
        foreach (['commerce', 'shopify'] as $integration) {
            if (!in_array($integration, $selected, true)) {
                continue;
            }
            $config = is_array($integrationSettings[$integration] ?? null) ? $integrationSettings[$integration] : [];
            if ((string)($config['mode'] ?? 'track') === 'track' && !empty($config['ecommerceFunnel'])) {
                return true;
            }
        }

        return false;
    }

    /**
     * @param array<string,mixed> $runtimeState
     * @return array<int,array<string,mixed>>
     */
    public function buildFormsContracts(array $runtimeState): array
    {
        $contracts = [];
        $integrationSettings = is_array($runtimeState['integrationSettings'] ?? null)
            ? $runtimeState['integrationSettings']
            : [];

        foreach ($this->getFormIntegrations()->all() as $adapter) {
            $config = is_array($integrationSettings[$adapter->getId()] ?? null)
                ? $integrationSettings[$adapter->getId()]
                : [];
            $contracts = array_merge($contracts, $adapter->buildContracts($config, $runtimeState));
        }

        if ($contracts === []) {
            return [];
        }

        return Plugin::getInstance()->getBurrowApi()->enrichFormsContracts($runtimeState, $contracts);
    }

    /**
     * @param array<string,mixed> $runtimeState
     * @return array<int,array{name:string,status:string}>
     */
    public function buildIntegrationReadinessRows(array $runtimeState): array
    {
        $selected = array_values(array_filter(array_map('strval', (array)($runtimeState['selectedIntegrations'] ?? []))));
        $contracts = $this->buildFormsContracts($runtimeState);
        $countsByProvider = [];
        foreach ($contracts as $contract) {
            $provider = trim((string)($contract['provider'] ?? ''));
            if ($provider === '') {
                continue;
            }
            $countsByProvider[$provider] = (int)($countsByProvider[$provider] ?? 0) + 1;
        }

        $labels = $this->integrationLabels();
        $integrationSettings = is_array($runtimeState['integrationSettings'] ?? null)
            ? $runtimeState['integrationSettings']
            : [];
        $rows = [];
        foreach ($selected as $integration) {
            $status = 'Configured';
            if ($this->isFormIntegration($integration)) {
                $status = !empty($countsByProvider[$integration]) ? 'Configured' : 'Needs setup';
            } elseif ($integration === 'commerce') {
                $commerce = is_array($integrationSettings['commerce'] ?? null) ? $integrationSettings['commerce'] : [];
                $status = isset($commerce['mode']) ? 'Configured' : 'Needs setup';
            } elseif ($integration === 'shopify') {
                $shopify = is_array($integrationSettings['shopify'] ?? null) ? $integrationSettings['shopify'] : [];
                $status = isset($shopify['mode']) ? 'Configured' : 'Needs setup';
            }
            $rows[] = [
                'name' => (string)($labels[$integration] ?? $integration),
                'status' => $status,
            ];
        }

        return $rows;
    }

    /**
     * @return array<int,array<string,mixed>>
     */
    public function collectPluginVersionSnapshot(): array
    {
        $pluginsService = Craft::$app->getPlugins();
        $updates = Craft::$app->getUpdates()->getUpdates(true);
        $updateMap = is_array($updates->plugins ?? null) ? $updates->plugins : [];

        $snapshot = [];
        foreach ($pluginsService->getAllPlugins() as $plugin) {
            $handle = $plugin->id;
            $update = $updateMap[$handle] ?? null;
            $latest = $update?->getLatest()?->version ?? $plugin->getVersion();
            $snapshot[] = [
                'handle' => $handle,
                'name' => $plugin->name,
                'version' => $plugin->getVersion(),
                'latest' => $latest,
                'updateAvailable' => version_compare($latest, $plugin->getVersion(), '>'),
            ];
        }

        return $snapshot;
    }

    /**
     * @return array<string,mixed>
     */
    private function pluginStatus(string $handle, string $label): array
    {
        $pluginsService = Craft::$app->getPlugins();
        $plugin = $pluginsService->getPlugin($handle);
        $installed = $pluginsService->isPluginInstalled($handle);
        $enabled = $installed && $pluginsService->isPluginEnabled($handle);

        if ($plugin === null) {
            return [
                'handle' => $handle,
                'label' => $label,
                'installed' => false,
                'enabled' => false,
                'version' => '',
                'iconDataUri' => '',
            ];
        }

        $iconDataUri = '';
        foreach (['icon.svg', 'icon-mask.svg'] as $iconFile) {
            $iconPath = rtrim((string)$plugin->getBasePath(), DIRECTORY_SEPARATOR) . DIRECTORY_SEPARATOR . $iconFile;
            if (!is_file($iconPath) || !is_readable($iconPath)) {
                continue;
            }
            $svg = @file_get_contents($iconPath);
            if (!is_string($svg) || trim($svg) === '') {
                continue;
            }
            $iconDataUri = 'data:image/svg+xml;base64,' . base64_encode($svg);
            break;
        }

        return [
            'handle' => $handle,
            'label' => $label,
            'installed' => $installed,
            'enabled' => $enabled,
            'version' => $plugin->getVersion(),
            'iconDataUri' => $iconDataUri,
        ];
    }
}
