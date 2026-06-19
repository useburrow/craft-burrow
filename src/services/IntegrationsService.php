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
        return array_merge($this->getFormIntegrations()->ids(), ['commerce']);
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

        return $labels;
    }

    /**
     * @param string[] $selected
     * @return array<string,string>
     */
    public function buildWizardSteps(array $selected): array
    {
        $steps = [
            'connection' => 'Connection',
            'project' => 'Project',
            'integrations' => 'Integrations',
        ];

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
     * @param array<string,mixed> $runtimeState
     * @param bool $forceRelink When true, re-link the project with current capabilities (e.g. after integration selection changes).
     * @param bool $publishSnapshot When true, publish a system stack snapshot (onboarding only; routine settings saves skip this).
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
     */
    public function syncConfiguration(array $runtimeState, bool $forceRelink = false, bool $publishSnapshot = false): array
    {
        $plugin = Plugin::getInstance();

        if (trim((string)($runtimeState['projectId'] ?? '')) === '') {
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

        if (!$plugin->canDispatchToBurrow($runtimeState)) {
            $missing = [];
            if ($plugin->getBurrowBaseUrl() === '') {
                $missing[] = Craft::t('burrow', 'base URL');
            }
            if (trim((string)($runtimeState['projectId'] ?? '')) === '') {
                $missing[] = Craft::t('burrow', 'linked project');
            }
            if (!$plugin->runtimeStateHasIngestionKey($runtimeState) && $plugin->getBurrowApiKey() === '') {
                $missing[] = Craft::t('burrow', 'ingestion key');
            }

            return [
                'ok' => false,
                'error' => $missing !== []
                    ? Craft::t('burrow', 'Burrow connection is not ready. Missing: {items}. Re-link the project if you recently rotated credentials.', [
                        'items' => implode(', ', $missing),
                    ])
                    : Craft::t('burrow', 'Burrow connection is not ready. Check your linked project and credentials.'),
                'runtimeState' => $runtimeState,
                'relinked' => false,
                'contractsSynced' => false,
                'contractsCount' => 0,
                'snapshotSynced' => false,
                'notice' => '',
            ];
        }

        $relinked = false;
        if ($forceRelink) {
            $selection = [
                'organizationId' => trim((string)($runtimeState['organizationId'] ?? '')),
                'clientId' => trim((string)($runtimeState['clientId'] ?? '')),
                'projectId' => trim((string)($runtimeState['projectId'] ?? '')),
            ];
            if ($selection['projectId'] === '') {
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

            $link = $plugin->getBurrowApi()->link(
                $plugin->getBurrowBaseUrl(),
                $plugin->getBurrowApiKey(),
                $selection,
                (array)($runtimeState['capabilities'] ?? []),
                $runtimeState
            );
            if (!$link['ok']) {
                $plugin->getLogs()->log('error', 'Project re-link failed during configuration sync', 'settings', 'system', null, [
                    'error' => $link['error'],
                ]);

                return [
                    'ok' => false,
                    'error' => Craft::t('burrow', 'Project re-link failed: {error}', ['error' => $link['error']]),
                    'runtimeState' => $runtimeState,
                    'relinked' => false,
                    'contractsSynced' => false,
                    'contractsCount' => 0,
                    'snapshotSynced' => false,
                    'notice' => '',
                ];
            }

            $runtimeState = $plugin->getBurrowApi()->applyLinkResult($runtimeState, $link);
            $relinked = true;
        }

        $contracts = $this->buildFormsContracts($runtimeState);
        $contractsSynced = false;
        $contractsCount = count($contracts);

        if ($contractsCount > 0) {
            $result = $plugin->getBurrowApi()->submitFormsContracts(
                $plugin->getBurrowBaseUrl(),
                $plugin->getBurrowApiKey(),
                $runtimeState,
                $contracts
            );
            if (!$result['ok']) {
                $plugin->getLogs()->log('error', 'Forms contract sync failed', 'settings', 'system', null, [
                    'error' => $result['error'],
                ]);

                return [
                    'ok' => false,
                    'error' => Craft::t('burrow', 'Contract sync failed: {error}', ['error' => $result['error']]),
                    'runtimeState' => $runtimeState,
                    'relinked' => $relinked,
                    'contractsSynced' => false,
                    'contractsCount' => $contractsCount,
                    'snapshotSynced' => false,
                    'notice' => '',
                ];
            }

            $runtimeState['sdkState'] = is_array($result['sdkState'] ?? null) ? $result['sdkState'] : (array)($runtimeState['sdkState'] ?? []);
            $contractMappings = is_array($result['contractMappings'] ?? null) ? $result['contractMappings'] : [];
            if ($contractMappings !== []) {
                $runtimeState['sdkState']['contractMappings'] = $contractMappings;
            }
            $projectSourceId = trim((string)($result['projectSourceId'] ?? ''));
            if ($projectSourceId !== '') {
                $runtimeState['projectSourceId'] = $projectSourceId;
                $sourceIds = is_array($runtimeState['sourceIds'] ?? null) ? $runtimeState['sourceIds'] : [];
                $sourceIds['forms'] = $projectSourceId;
                if (trim((string)($sourceIds['ecommerce'] ?? '')) === '') {
                    $sourceIds['ecommerce'] = $projectSourceId;
                }
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
            $contractsSynced = true;

            $plugin->getLogs()->log('info', 'Forms contracts synced to Burrow', 'settings', 'system', null, [
                'contractsCount' => $contractsCount,
                'contractsVersion' => trim((string)($result['contractsVersion'] ?? '')),
                'forms' => array_map(static function (array $contract): array {
                    $customKeys = [];
                    foreach ((array)($contract['fieldMappings'] ?? []) as $mapping) {
                        if (!is_array($mapping)) {
                            continue;
                        }
                        $key = trim((string)($mapping['canonicalKey'] ?? ''));
                        if ($key === '' || in_array($key, ['submissionId', 'submittedAt', 'formId'], true)) {
                            continue;
                        }
                        $customKeys[] = $key;
                    }

                    return [
                        'provider' => trim((string)($contract['provider'] ?? '')),
                        'externalFormId' => trim((string)($contract['externalFormId'] ?? '')),
                        'contractId' => trim((string)($contract['contractId'] ?? '')),
                        'fieldCount' => count($customKeys),
                        'canonicalKeys' => $customKeys,
                    ];
                }, is_array($result['formsContracts'] ?? null) && $result['formsContracts'] !== []
                    ? $result['formsContracts']
                    : $contracts),
            ]);
        }

        $snapshotSynced = false;
        $snapshotResult = ['ok' => false, 'error' => ''];
        if ($publishSnapshot) {
            $runtimeState['lastSnapshot'] = $plugin->getSnapshot()->collectSnapshot();
            $snapshotResult = $plugin->getBurrowApi()->publishSystemSnapshot(
                $plugin->getBurrowBaseUrl(),
                $plugin->getBurrowApiKey(),
                $runtimeState,
                $runtimeState['lastSnapshot']
            );
            $snapshotSynced = (bool)($snapshotResult['ok'] ?? false);
            if ($snapshotSynced) {
                $integrationSettings = is_array($runtimeState['integrationSettings'] ?? null) ? $runtimeState['integrationSettings'] : [];
                $systemJobs = is_array($integrationSettings['systemJobs'] ?? null) ? $integrationSettings['systemJobs'] : [];
                $systemJobs['snapshotLastRunAt'] = gmdate('c');
                $systemJobs['snapshotQueuedAt'] = '';
                $systemJobs['snapshotLastError'] = '';
                $integrationSettings['systemJobs'] = $systemJobs;
                $runtimeState['integrationSettings'] = $integrationSettings;
            }
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
                'error' => $snapshotResult['error'] ?? '',
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
                    'error' => (string)($snapshotResult['error'] ?? ''),
                ]),
            ];
        }

        $plugin->getLogs()->log('info', 'Configuration synced to Burrow', 'settings', 'system', null, [
            'relinked' => $relinked,
            'contractsSynced' => $contractsSynced,
            'contractsCount' => $contractsCount,
            'snapshotSynced' => $snapshotSynced,
        ]);

        if ($contractsSynced) {
            $mappingCount = 0;
            foreach ($contracts as $contract) {
                if (!is_array($contract['fieldMappings'] ?? null)) {
                    continue;
                }
                foreach ((array)$contract['fieldMappings'] as $mapping) {
                    if (!is_array($mapping)) {
                        continue;
                    }
                    $key = trim((string)($mapping['canonicalKey'] ?? ''));
                    if ($key === '' || in_array($key, ['submissionId', 'submittedAt', 'formId'], true)) {
                        continue;
                    }
                    $mappingCount++;
                }
            }
            $notice = Craft::t('burrow', 'Settings saved and synced to Burrow ({count} contract(s), {mappings} field mapping(s)).', [
                'count' => (string)$contractsCount,
                'mappings' => (string)$mappingCount,
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

        return $result;
    }

    /**
     * @param array<string,mixed> $runtimeState
     * @return array<string, array{forms: array<int, array<string, string>>, fieldsByFormId: array<string, array<int, array<string, string>>>}>
     */
    public function buildFormAdapterViewData(array $runtimeState): array
    {
        $data = [];
        foreach ($this->getFormIntegrations()->all() as $adapter) {
            $id = $adapter->getId();
            $forms = $adapter->discoverForms();
            $fieldsByFormId = [];
            foreach ($forms as $form) {
                $formId = (string)($form['id'] ?? '');
                if ($formId === '') {
                    continue;
                }
                $fieldsByFormId[$formId] = $adapter->discoverFields($formId);
            }
            $data[$id] = [
                'id' => $id,
                'label' => $adapter->getLabel(),
                'defaultPrefix' => $adapter->getDefaultPrefix(),
                'forms' => $forms,
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

        return [
            'forms' => $forms,
            'ecommerce' => in_array('commerce', $selected, true) ? ['craft-commerce'] : [],
            'ecommerce_funnel' => in_array('commerce', $selected, true),
        ];
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
