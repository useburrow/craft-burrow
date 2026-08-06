<?php
namespace burrow\Burrow\services;

use Craft;
use craft\base\Component;
use craft\models\Site;

use burrow\Burrow\helpers\CredentialCrypto;
use burrow\Burrow\Plugin;
use burrow\Burrow\records\RuntimeStateRecord;

/**
 * Persists install-level Burrow connection state and per-Craft-site project routing.
 *
 * An instance is available via `Plugin::getInstance()->getState()`.
 *
 * @author Burrow Analytics, LLC
 * @since 5.0.0
 */
class StateService extends Component
{
    // =========================================================================
    // Public Methods
    // =========================================================================

    /**
     * Returns install-level fields plus a flattened primary/current linked site view.
     *
     * Always includes a `siteStates` map keyed by Craft site ID string. Legacy single-site
     * installs with a flat `projectId` and empty `siteStates` are hydrated from the primary site.
     *
     * @return array<string,mixed>
     *
     * @author Burrow Analytics, LLC
     * @since 5.0.0
     */
    public function getState(): array
    {
        $record = RuntimeStateRecord::find()->one();
        if (!$record) {
            return $this->defaultState();
        }

        $install = $this->_readInstallFields($record);
        $siteStates = $this->_decodeSiteStates($record);
        if ($siteStates === [] && trim((string)($record->projectId ?? '')) !== '') {
            $siteStates = $this->_synthesizeLegacySiteStates($record);
        }

        $flattened = $this->_flattenPrimarySiteView($install, $siteStates);

        return array_merge($this->defaultState(), $flattened, [
            'siteStates' => $siteStates,
        ]);
    }

    /**
     * Returns install-level fields merged with routing for a specific Craft site.
     *
     * @param int $siteId the Craft site ID
     * @return array<string,mixed>
     *
     * @author Burrow Analytics, LLC
     * @since 5.4.0
     */
    public function getSiteState(int $siteId): array
    {
        $state = $this->getState();
        $siteKey = (string)$siteId;
        $siteStates = is_array($state['siteStates'] ?? null) ? $state['siteStates'] : [];
        $sitePatch = is_array($siteStates[$siteKey] ?? null) ? $siteStates[$siteKey] : $this->defaultSiteState($siteId);

        return $this->_mergeInstallAndSite($state, $sitePatch);
    }

    /**
     * Updates one site's routing without wiping other sites, and mirrors the primary into legacy columns.
     *
     * @param int $siteId the Craft site ID
     * @param array<string,mixed> $sitePatch site-scoped fields to merge
     * @param array<string,mixed>|null $installState optional install-level fields to merge
     * @return bool
     *
     * @author Burrow Analytics, LLC
     * @since 5.4.0
     */
    public function saveSiteState(int $siteId, array $sitePatch, ?array $installState = null): bool
    {
        $current = $this->getState();
        if (is_array($installState)) {
            foreach ($this->_installFieldKeys() as $key) {
                if (array_key_exists($key, $installState)) {
                    $current[$key] = $installState[$key];
                }
            }
        }

        $siteStates = is_array($current['siteStates'] ?? null) ? $current['siteStates'] : [];
        $siteKey = (string)$siteId;
        $existing = is_array($siteStates[$siteKey] ?? null)
            ? $siteStates[$siteKey]
            : $this->defaultSiteState($siteId);
        $mergedSite = array_merge($existing, $sitePatch, [
            'siteId' => $siteId,
        ]);
        $siteMeta = $this->_craftSiteMeta($siteId);
        if ($siteMeta !== null) {
            $mergedSite['siteUid'] = $siteMeta['uid'];
            $mergedSite['siteHandle'] = $siteMeta['handle'];
            if (!array_key_exists('siteUrl', $sitePatch) || trim((string)$sitePatch['siteUrl']) === '') {
                $mergedSite['siteUrl'] = $siteMeta['baseUrl'];
            }
        }
        $siteStates[$siteKey] = $this->_normalizeSiteState($mergedSite, $siteId);
        $current['siteStates'] = $siteStates;

        return $this->saveState($current);
    }

    /**
     * Persists the full runtime state including `siteStates` and legacy flat mirrors.
     *
     * @param array<string,mixed> $state
     * @return bool
     *
     * @author Burrow Analytics, LLC
     * @since 5.0.0
     */
    public function saveState(array $state): bool
    {
        $siteStates = is_array($state['siteStates'] ?? null) ? $state['siteStates'] : [];
        foreach ($siteStates as $siteKey => $siteState) {
            if (!is_array($siteState)) {
                unset($siteStates[$siteKey]);
                continue;
            }
            $siteId = (int)($siteState['siteId'] ?? $siteKey);
            $resolvedIngestion = Plugin::getInstance()->resolveIngestionKey($siteState);
            if ($resolvedIngestion['key'] !== '') {
                $siteState['ingestionKey'] = $resolvedIngestion;
            }
            $siteStates[$siteKey] = $this->_normalizeSiteState($siteState, $siteId);
        }

        // Keep legacy flat columns aligned with the mirrored primary/default linked site.
        $flattened = $this->_flattenPrimarySiteView($state, $siteStates);
        $resolvedFlatIngestion = Plugin::getInstance()->resolveIngestionKey($flattened);
        if ($resolvedFlatIngestion['key'] !== '') {
            $flattened['ingestionKey'] = $resolvedFlatIngestion;
        }

        $record = RuntimeStateRecord::find()->one();
        if (!$record) {
            $record = new RuntimeStateRecord();
        }

        $record->projectId = (string)($flattened['projectId'] ?? '');
        $record->clientId = (string)($flattened['clientId'] ?? '');
        $record->organizationId = (string)($flattened['organizationId'] ?? '');
        $record->projectSourceId = (string)($flattened['projectSourceId'] ?? '');
        $record->sourceIds = (array)($flattened['sourceIds'] ?? ['forms' => '', 'ecommerce' => '', 'system' => '']);
        $record->sdkState = (array)($flattened['sdkState'] ?? []);
        $ingestionPlain = (array)($flattened['ingestionKey'] ?? ['key' => '', 'projectId' => '', 'keyPrefix' => '']);
        $record->ingestionKey = [
            'key' => CredentialCrypto::seal(trim((string)($ingestionPlain['key'] ?? '')), CredentialCrypto::INFO_INGESTION_KEY),
            'projectId' => (string)($ingestionPlain['projectId'] ?? ''),
            'keyPrefix' => (string)($ingestionPlain['keyPrefix'] ?? ''),
        ];
        $record->burrowProject = (array)($flattened['burrowProject'] ?? ['name' => '', 'path' => '', 'url' => '']);
        $record->selectedIntegrations = array_values(array_map('strval', (array)($state['selectedIntegrations'] ?? [])));
        $record->capabilities = (array)($state['capabilities'] ?? ['forms' => [], 'ecommerce' => [], 'ecommerce_funnel' => false]);
        $record->integrationSettings = (array)($state['integrationSettings'] ?? []);
        $record->lastSnapshot = (array)($flattened['lastSnapshot'] ?? []);
        $record->onboardingStep = (string)($state['onboardingStep'] ?? 'connection');
        $record->onboardingCompleted = (bool)($state['onboardingCompleted'] ?? false);
        $record->connectionBaseUrl = (string)($state['connectionBaseUrl'] ?? '');
        $record->connectionApiKey = CredentialCrypto::seal(trim((string)($state['connectionApiKey'] ?? '')), CredentialCrypto::INFO_CONNECTION_API_KEY);
        $record->siteStates = $this->_encodeSiteStatesForStorage($siteStates);

        return (bool)$record->save();
    }

    /**
     * Lists all Craft sites with identifiers useful for onboarding UI.
     *
     * @return array<int,array{id:int,uid:string,handle:string,name:string,baseUrl:string,primary:bool}>
     *
     * @author Burrow Analytics, LLC
     * @since 5.4.0
     */
    public function listCraftSites(): array
    {
        $primaryId = (int)(Craft::$app->getSites()->getPrimarySite()?->id ?? 0);
        $rows = [];
        foreach (Craft::$app->getSites()->getAllSites() as $site) {
            if (!$site instanceof Site) {
                continue;
            }
            $rows[] = [
                'id' => (int)$site->id,
                'uid' => (string)$site->uid,
                'handle' => (string)$site->handle,
                'name' => (string)$site->name,
                'baseUrl' => rtrim((string)$site->getBaseUrl(), '/') . '/',
                'primary' => (int)$site->id === $primaryId,
            ];
        }

        return $rows;
    }

    /**
     * Returns site states that are enabled and linked to a Burrow project.
     *
     * @return array<string,array<string,mixed>>
     *
     * @author Burrow Analytics, LLC
     * @since 5.4.0
     */
    public function getLinkedSiteStates(): array
    {
        $state = $this->getState();
        $siteStates = is_array($state['siteStates'] ?? null) ? $state['siteStates'] : [];
        $linked = [];
        foreach ($siteStates as $siteKey => $siteState) {
            if (!is_array($siteState)) {
                continue;
            }
            if (empty($siteState['enabled'])) {
                continue;
            }
            if (trim((string)($siteState['projectId'] ?? '')) === '') {
                continue;
            }
            if (!$this->_siteHasIngestionKey($siteState)) {
                continue;
            }
            $linked[(string)$siteKey] = $siteState;
        }

        return $linked;
    }

    /**
     * Finds the site state linked to a Burrow project ID, if any.
     *
     * @param string $projectId the Burrow project ID
     * @return array<string,mixed>|null
     *
     * @author Burrow Analytics, LLC
     * @since 5.4.0
     */
    public function findSiteStateByProjectId(string $projectId): ?array
    {
        $projectId = trim($projectId);
        if ($projectId === '') {
            return null;
        }

        $state = $this->getState();
        $siteStates = is_array($state['siteStates'] ?? null) ? $state['siteStates'] : [];
        foreach ($siteStates as $siteState) {
            if (!is_array($siteState)) {
                continue;
            }
            if (trim((string)($siteState['projectId'] ?? '')) === $projectId) {
                return $siteState;
            }
        }

        return null;
    }

    /**
     * Whether a Burrow project is already linked to a different Craft site in this install.
     *
     * @param string $projectId the Burrow project ID
     * @param int $exceptSiteId Craft site ID to ignore (the site currently being linked)
     * @return bool
     *
     * @author Burrow Analytics, LLC
     * @since 5.4.0
     */
    public function isProjectLinkedToOtherSite(string $projectId, int $exceptSiteId): bool
    {
        $found = $this->findSiteStateByProjectId($projectId);
        if ($found === null) {
            return false;
        }

        return (int)($found['siteId'] ?? 0) !== $exceptSiteId;
    }

    /**
     * Finds a linked site state whose sealed ingestion key matches the bearer token.
     *
     * @param string $token the bearer token from the request
     * @return array<string,mixed>|null install+site merged state, or null
     *
     * @author Burrow Analytics, LLC
     * @since 5.4.0
     */
    public function findSiteStateByIngestionKey(string $token): ?array
    {
        $token = trim($token);
        if ($token === '') {
            return null;
        }

        foreach ($this->getLinkedSiteStates() as $siteKey => $siteState) {
            $key = trim((string)(
                is_array($siteState['ingestionKey'] ?? null)
                    ? ($siteState['ingestionKey']['key'] ?? '')
                    : ''
            ));
            if ($key !== '' && hash_equals($key, $token)) {
                return $this->getSiteState((int)$siteKey);
            }
        }

        // Legacy single-site fallback when siteStates is empty / not yet migrated in memory.
        $state = $this->getState();
        $legacyKey = trim((string)(
            is_array($state['ingestionKey'] ?? null)
                ? ($state['ingestionKey']['key'] ?? '')
                : ''
        ));
        if ($legacyKey !== '' && hash_equals($legacyKey, $token)) {
            return $state;
        }

        return null;
    }

    /**
     * Resolves runtime state for outbox delivery from persisted project/site routing.
     *
     * @param string|null $projectId outbox project_id column or payload projectId
     * @param int|null $siteId outbox site_id column
     * @return array<string,mixed>
     *
     * @author Burrow Analytics, LLC
     * @since 5.4.0
     */
    public function resolveRuntimeStateForDelivery(?string $projectId = null, ?int $siteId = null): array
    {
        if ($siteId !== null && $siteId > 0) {
            $siteState = $this->getSiteState($siteId);
            if (trim((string)($siteState['projectId'] ?? '')) !== '') {
                return $siteState;
            }
        }

        $projectId = trim((string)$projectId);
        if ($projectId !== '') {
            $found = $this->findSiteStateByProjectId($projectId);
            if ($found !== null) {
                return $this->getSiteState((int)($found['siteId'] ?? 0));
            }
        }

        return $this->getState();
    }

    /**
     * Normalizes a site URL for comparison (ignore scheme, www, trailing slash; keep path).
     *
     * @param string $url the site URL
     * @return string
     *
     * @author Burrow Analytics, LLC
     * @since 5.4.0
     */
    public function normalizeSiteUrl(string $url): string
    {
        $url = trim($url);
        if ($url === '') {
            return '';
        }

        $parts = parse_url($url);
        if (!is_array($parts)) {
            return strtolower(rtrim($url, '/'));
        }

        $host = strtolower((string)($parts['host'] ?? ''));
        if (str_starts_with($host, 'www.')) {
            $host = substr($host, 4);
        }
        $path = (string)($parts['path'] ?? '');
        $path = rtrim($path, '/');
        if ($path === '/') {
            $path = '';
        }

        $port = isset($parts['port']) ? (':' . (int)$parts['port']) : '';

        return $host . $port . $path;
    }

    /**
     * Default empty site-scoped state for a Craft site.
     *
     * @param int $siteId the Craft site ID
     * @return array<string,mixed>
     *
     * @author Burrow Analytics, LLC
     * @since 5.4.0
     */
    public function defaultSiteState(int $siteId = 0): array
    {
        $meta = $siteId > 0 ? $this->_craftSiteMeta($siteId) : null;

        return [
            'enabled' => false,
            'linked' => false,
            'siteId' => $siteId,
            'siteUid' => (string)($meta['uid'] ?? ''),
            'siteHandle' => (string)($meta['handle'] ?? ''),
            'siteUrl' => (string)($meta['baseUrl'] ?? ''),
            'projectId' => '',
            'clientId' => '',
            'organizationId' => '',
            'projectSourceId' => '',
            'sourceIds' => ['forms' => '', 'ecommerce' => '', 'system' => ''],
            'sdkState' => [],
            'ingestionKey' => ['key' => '', 'projectId' => '', 'keyPrefix' => ''],
            'burrowProject' => ['name' => '', 'path' => '', 'url' => ''],
            'lastSnapshot' => [],
        ];
    }

    /**
     * @return array<string,mixed>
     *
     * @author Burrow Analytics, LLC
     * @since 5.0.0
     */
    public function defaultState(): array
    {
        return [
            'projectId' => '',
            'clientId' => '',
            'organizationId' => '',
            'projectSourceId' => '',
            'sourceIds' => ['forms' => '', 'ecommerce' => '', 'system' => ''],
            'sdkState' => [],
            'ingestionKey' => ['key' => '', 'projectId' => '', 'keyPrefix' => ''],
            'burrowProject' => ['name' => '', 'path' => '', 'url' => ''],
            'selectedIntegrations' => [],
            'capabilities' => ['forms' => [], 'ecommerce' => [], 'ecommerce_funnel' => false],
            'integrationSettings' => [],
            'lastSnapshot' => [],
            'onboardingStep' => 'connection',
            'onboardingCompleted' => false,
            'connectionBaseUrl' => '',
            'connectionApiKey' => '',
            'siteStates' => [],
        ];
    }

    // =========================================================================
    // Private Methods
    // =========================================================================

    /**
     * @return string[]
     */
    private function _installFieldKeys(): array
    {
        return [
            'connectionBaseUrl',
            'connectionApiKey',
            'selectedIntegrations',
            'capabilities',
            'integrationSettings',
            'onboardingStep',
            'onboardingCompleted',
        ];
    }

    /**
     * @return array<string,mixed>
     */
    private function _readInstallFields(RuntimeStateRecord $record): array
    {
        return [
            'selectedIntegrations' => is_array($record->selectedIntegrations) ? $record->selectedIntegrations : [],
            'capabilities' => is_array($record->capabilities) ? $record->capabilities : ['forms' => [], 'ecommerce' => [], 'ecommerce_funnel' => false],
            'integrationSettings' => is_array($record->integrationSettings) ? $record->integrationSettings : [],
            'onboardingStep' => (string)($record->onboardingStep ?? 'connection'),
            'onboardingCompleted' => (bool)($record->onboardingCompleted ?? false),
            'connectionBaseUrl' => (string)($record->connectionBaseUrl ?? ''),
            'connectionApiKey' => CredentialCrypto::unseal((string)($record->connectionApiKey ?? ''), CredentialCrypto::INFO_CONNECTION_API_KEY),
        ];
    }

    /**
     * @return array<string,array<string,mixed>>
     */
    private function _decodeSiteStates(RuntimeStateRecord $record): array
    {
        $raw = $record->siteStates ?? null;
        if (is_string($raw) && $raw !== '') {
            $decoded = json_decode($raw, true);
            $raw = is_array($decoded) ? $decoded : [];
        }
        if (!is_array($raw)) {
            return [];
        }

        $result = [];
        foreach ($raw as $siteKey => $siteState) {
            if (!is_array($siteState)) {
                continue;
            }
            $siteId = (int)($siteState['siteId'] ?? $siteKey);
            $ingestionStored = is_array($siteState['ingestionKey'] ?? null)
                ? $siteState['ingestionKey']
                : ['key' => '', 'projectId' => '', 'keyPrefix' => ''];
            $siteState['ingestionKey'] = [
                'key' => CredentialCrypto::unseal((string)($ingestionStored['key'] ?? ''), CredentialCrypto::INFO_INGESTION_KEY),
                'projectId' => (string)($ingestionStored['projectId'] ?? ''),
                'keyPrefix' => (string)($ingestionStored['keyPrefix'] ?? ''),
            ];
            $result[(string)$siteKey] = $this->_normalizeSiteState($siteState, $siteId);
        }

        return $result;
    }

    /**
     * @param array<string,array<string,mixed>> $siteStates
     * @return array<string,array<string,mixed>>
     */
    private function _encodeSiteStatesForStorage(array $siteStates): array
    {
        $encoded = [];
        foreach ($siteStates as $siteKey => $siteState) {
            if (!is_array($siteState)) {
                continue;
            }
            $ingestionPlain = is_array($siteState['ingestionKey'] ?? null)
                ? $siteState['ingestionKey']
                : ['key' => '', 'projectId' => '', 'keyPrefix' => ''];
            $siteState['ingestionKey'] = [
                'key' => CredentialCrypto::seal(trim((string)($ingestionPlain['key'] ?? '')), CredentialCrypto::INFO_INGESTION_KEY),
                'projectId' => (string)($ingestionPlain['projectId'] ?? ''),
                'keyPrefix' => (string)($ingestionPlain['keyPrefix'] ?? ''),
            ];
            $encoded[(string)$siteKey] = $siteState;
        }

        return $encoded;
    }

    /**
     * @return array<string,array<string,mixed>>
     */
    private function _synthesizeLegacySiteStates(RuntimeStateRecord $record): array
    {
        $primary = Craft::$app->getSites()->getPrimarySite();
        $siteId = (int)($primary?->id ?? 0);
        if ($siteId <= 0) {
            return [];
        }

        $ingestionStored = is_array($record->ingestionKey)
            ? $record->ingestionKey
            : ['key' => '', 'projectId' => '', 'keyPrefix' => ''];

        $siteState = $this->defaultSiteState($siteId);
        $siteState['enabled'] = true;
        $siteState['linked'] = trim((string)($record->projectId ?? '')) !== '';
        $siteState['projectId'] = (string)($record->projectId ?? '');
        $siteState['clientId'] = (string)($record->clientId ?? '');
        $siteState['organizationId'] = (string)($record->organizationId ?? '');
        $siteState['projectSourceId'] = (string)($record->projectSourceId ?? '');
        $siteState['sourceIds'] = is_array($record->sourceIds) ? $record->sourceIds : ['forms' => '', 'ecommerce' => '', 'system' => ''];
        $siteState['sdkState'] = is_array($record->sdkState) ? $record->sdkState : [];
        $siteState['ingestionKey'] = [
            'key' => CredentialCrypto::unseal((string)($ingestionStored['key'] ?? ''), CredentialCrypto::INFO_INGESTION_KEY),
            'projectId' => (string)($ingestionStored['projectId'] ?? ''),
            'keyPrefix' => (string)($ingestionStored['keyPrefix'] ?? ''),
        ];
        $siteState['burrowProject'] = is_array($record->burrowProject) ? $record->burrowProject : ['name' => '', 'path' => '', 'url' => ''];
        $siteState['lastSnapshot'] = is_array($record->lastSnapshot) ? $record->lastSnapshot : [];

        return [(string)$siteId => $siteState];
    }

    /**
     * @param array<string,mixed> $install
     * @param array<string,array<string,mixed>> $siteStates
     * @return array<string,mixed>
     */
    private function _flattenPrimarySiteView(array $install, array $siteStates): array
    {
        $primaryId = (int)(Craft::$app->getSites()->getPrimarySite()?->id ?? 0);
        $chosen = null;

        if ($primaryId > 0 && isset($siteStates[(string)$primaryId]) && is_array($siteStates[(string)$primaryId])) {
            $candidate = $siteStates[(string)$primaryId];
            if (!empty($candidate['enabled']) && trim((string)($candidate['projectId'] ?? '')) !== '') {
                $chosen = $candidate;
            }
        }

        if ($chosen === null) {
            try {
                $currentId = (int)(Craft::$app->getSites()->getCurrentSite()->id ?? 0);
            } catch (\Throwable) {
                $currentId = 0;
            }
            if ($currentId > 0 && isset($siteStates[(string)$currentId]) && is_array($siteStates[(string)$currentId])) {
                $candidate = $siteStates[(string)$currentId];
                if (!empty($candidate['enabled']) && trim((string)($candidate['projectId'] ?? '')) !== '') {
                    $chosen = $candidate;
                }
            }
        }

        if ($chosen === null) {
            foreach ($siteStates as $siteState) {
                if (!is_array($siteState)) {
                    continue;
                }
                if (!empty($siteState['enabled']) && trim((string)($siteState['projectId'] ?? '')) !== '') {
                    $chosen = $siteState;
                    break;
                }
            }
        }

        if ($chosen === null && $primaryId > 0 && isset($siteStates[(string)$primaryId])) {
            $chosen = is_array($siteStates[(string)$primaryId]) ? $siteStates[(string)$primaryId] : null;
        }

        if ($chosen === null) {
            $chosen = $this->defaultSiteState($primaryId);
        }

        return $this->_mergeInstallAndSite($install, $chosen);
    }

    /**
     * @param array<string,mixed> $install
     * @param array<string,mixed> $site
     * @return array<string,mixed>
     */
    private function _mergeInstallAndSite(array $install, array $site): array
    {
        return [
            'projectId' => (string)($site['projectId'] ?? ''),
            'clientId' => (string)($site['clientId'] ?? ''),
            'organizationId' => (string)($site['organizationId'] ?? ''),
            'projectSourceId' => (string)($site['projectSourceId'] ?? ''),
            'sourceIds' => is_array($site['sourceIds'] ?? null) ? $site['sourceIds'] : ['forms' => '', 'ecommerce' => '', 'system' => ''],
            'sdkState' => is_array($site['sdkState'] ?? null) ? $site['sdkState'] : [],
            'ingestionKey' => is_array($site['ingestionKey'] ?? null)
                ? $site['ingestionKey']
                : ['key' => '', 'projectId' => '', 'keyPrefix' => ''],
            'burrowProject' => is_array($site['burrowProject'] ?? null)
                ? $site['burrowProject']
                : ['name' => '', 'path' => '', 'url' => ''],
            'lastSnapshot' => is_array($site['lastSnapshot'] ?? null) ? $site['lastSnapshot'] : [],
            'craftSiteId' => (int)($site['siteId'] ?? 0),
            'siteUrl' => (string)($site['siteUrl'] ?? ''),
            'selectedIntegrations' => array_values(array_map('strval', (array)($install['selectedIntegrations'] ?? []))),
            'capabilities' => is_array($install['capabilities'] ?? null)
                ? $install['capabilities']
                : ['forms' => [], 'ecommerce' => [], 'ecommerce_funnel' => false],
            'integrationSettings' => is_array($install['integrationSettings'] ?? null) ? $install['integrationSettings'] : [],
            'onboardingStep' => (string)($install['onboardingStep'] ?? 'connection'),
            'onboardingCompleted' => (bool)($install['onboardingCompleted'] ?? false),
            'connectionBaseUrl' => (string)($install['connectionBaseUrl'] ?? ''),
            'connectionApiKey' => (string)($install['connectionApiKey'] ?? ''),
            'siteStates' => is_array($install['siteStates'] ?? null) ? $install['siteStates'] : [],
        ];
    }

    /**
     * @param array<string,mixed> $siteState
     * @return array<string,mixed>
     */
    private function _normalizeSiteState(array $siteState, int $siteId): array
    {
        $defaults = $this->defaultSiteState($siteId);
        $merged = array_merge($defaults, $siteState);
        $merged['siteId'] = $siteId > 0 ? $siteId : (int)($merged['siteId'] ?? 0);
        $merged['enabled'] = (bool)($merged['enabled'] ?? false);
        $projectId = trim((string)($merged['projectId'] ?? ''));
        $merged['linked'] = $projectId !== '' && $this->_siteHasIngestionKey($merged);
        $merged['sourceIds'] = is_array($merged['sourceIds'] ?? null)
            ? $merged['sourceIds']
            : ['forms' => '', 'ecommerce' => '', 'system' => ''];
        $merged['sdkState'] = is_array($merged['sdkState'] ?? null) ? $merged['sdkState'] : [];
        $merged['ingestionKey'] = is_array($merged['ingestionKey'] ?? null)
            ? $merged['ingestionKey']
            : ['key' => '', 'projectId' => '', 'keyPrefix' => ''];
        $merged['burrowProject'] = is_array($merged['burrowProject'] ?? null)
            ? $merged['burrowProject']
            : ['name' => '', 'path' => '', 'url' => ''];
        $merged['lastSnapshot'] = is_array($merged['lastSnapshot'] ?? null) ? $merged['lastSnapshot'] : [];

        return $merged;
    }

    /**
     * @param array<string,mixed> $siteState
     */
    private function _siteHasIngestionKey(array $siteState): bool
    {
        $key = trim((string)(
            is_array($siteState['ingestionKey'] ?? null)
                ? ($siteState['ingestionKey']['key'] ?? '')
                : ''
        ));
        if ($key !== '') {
            return true;
        }

        return trim((string)($siteState['sdkState']['ingestionKey'] ?? '')) !== '';
    }

    /**
     * @return array{uid:string,handle:string,baseUrl:string}|null
     */
    private function _craftSiteMeta(int $siteId): ?array
    {
        if ($siteId <= 0) {
            return null;
        }
        $site = Craft::$app->getSites()->getSiteById($siteId);
        if ($site === null) {
            return null;
        }

        return [
            'uid' => (string)$site->uid,
            'handle' => (string)$site->handle,
            'baseUrl' => rtrim((string)$site->getBaseUrl(), '/') . '/',
        ];
    }
}
