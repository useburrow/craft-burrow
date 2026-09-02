<?php
namespace burrow\Burrow;

use Craft;
use craft\helpers\App;
use yii\base\Event;

use burrow\Burrow\base\PluginTrait;
use burrow\Burrow\elements\OutboxElement;
use burrow\Burrow\models\Settings;

use craft\base\Model;
use craft\base\Plugin as CraftPlugin;
use craft\events\PluginEvent;
use craft\events\RegisterComponentTypesEvent;
use craft\events\RegisterUrlRulesEvent;
use craft\helpers\UrlHelper;
use craft\queue\Queue as CraftQueue;
use craft\services\Elements;
use craft\services\Plugins;
use craft\web\UrlManager;
use craft\web\View;

class Plugin extends CraftPlugin
{
    use PluginTrait;

    public static ?Plugin $plugin = null;

    public string $schemaVersion = '5.4.0';
    public bool $hasCpSettings = true;
    public bool $hasCpSection = true;

    public function init(): void
    {
        parent::init();
        self::$plugin = $this;

        $this->_ensureProcessWorkingDirectory();
        $this->_registerQueueWorkingDirectoryGuard();
        $this->_setPluginComponents();
        $this->_maybeHandlePostInstallRedirect();
        $this->_registerRoutes();
        $this->_registerPostInstallRedirect();
        $this->_registerElementTypes();
        $this->_registerCommerceHooks();
        $this->_registerFormHooks();
        $this->_registerShopifyCollector();
        $this->_scheduleSystemJobs();

        Craft::info(
            Craft::t('burrow', '{name} plugin loaded', ['name' => $this->name]),
            __METHOD__
        );
    }

    protected function createSettingsModel(): ?Model
    {
        return new Settings();
    }

    /**
     * Resolves the Burrow API base URL for outbound requests.
     *
     * Precedence: `BURROW_BASE_URL` env → parsed runtime `connectionBaseUrl` → parsed project-config `baseUrl`.
     * Stored values may be literals or Craft env references (`$BURROW_BASE_URL`).
     * Use this (not raw {@see getSettings()}) so production saves work when `allowAdminChanges` is false.
     */
    public function getBurrowBaseUrl(): string
    {
        $fromEnv = trim((string)App::env('BURROW_BASE_URL'));
        if ($fromEnv !== '') {
            return $fromEnv;
        }

        $fromState = $this->parseEnvValue($this->getRawBurrowBaseUrlFromState());
        if ($fromState !== '') {
            return $fromState;
        }

        return $this->parseEnvValue((string)$this->getSettings()->baseUrl);
    }

    /**
     * Account-level (organization) API key for onboarding (discover/link).
     *
     * Precedence: `BURROW_API_KEY` env → parsed runtime `connectionApiKey` → parsed project-config `apiKey`.
     * Stored values may be literals or Craft env references (`$BURROW_API_KEY`).
     * Retained after project linking so additional Craft sites can be linked with the same org key.
     * Does not fall back to project-config `apiKey` when an ingestion key exists and the runtime connection key is empty.
     */
    public function getBurrowApiKey(): string
    {
        $fromEnv = trim((string)App::env('BURROW_API_KEY'));
        if ($fromEnv !== '') {
            return $fromEnv;
        }

        $state = $this->getState()->getState();
        $fromState = $this->parseEnvValue((string)($state['connectionApiKey'] ?? ''));
        if ($fromState !== '') {
            return $fromState;
        }

        if ($this->runtimeStateHasIngestionKey($state)) {
            return '';
        }

        return $this->parseEnvValue((string)$this->getSettings()->apiKey);
    }

    /**
     * Unparsed base URL for Control Panel forms (may be `$BURROW_BASE_URL`).
     */
    public function getRawBurrowBaseUrl(): string
    {
        $fromState = $this->getRawBurrowBaseUrlFromState();
        if ($fromState !== '') {
            return $fromState;
        }

        return trim((string)$this->getSettings()->baseUrl);
    }

    /**
     * Unparsed organization API key for Control Panel forms (may be `$BURROW_API_KEY`).
     */
    public function getRawBurrowApiKey(): string
    {
        $state = $this->getState()->getState();
        $fromState = trim((string)($state['connectionApiKey'] ?? ''));
        if ($fromState !== '') {
            return $fromState;
        }

        if ($this->runtimeStateHasIngestionKey($state)) {
            return '';
        }

        return trim((string)$this->getSettings()->apiKey);
    }

    /**
     * Whether the organization API key is supplied by process env or a `$VAR` reference (not a pasted secret).
     */
    public function isBurrowApiKeyFromEnvironment(): bool
    {
        if (trim((string)App::env('BURROW_API_KEY')) !== '') {
            return true;
        }

        return str_starts_with($this->getRawBurrowApiKey(), '$');
    }

    /**
     * Whether the Burrow base URL is supplied by process env or a `$VAR` reference.
     */
    public function isBurrowBaseUrlFromEnvironment(): bool
    {
        if (trim((string)App::env('BURROW_BASE_URL')) !== '') {
            return true;
        }

        return str_starts_with($this->getRawBurrowBaseUrl(), '$');
    }

    /**
     * Craft application environment name (`CRAFT_ENVIRONMENT`), lowercased.
     */
    public function getCraftEnvironment(): string
    {
        $env = Craft::$app->env ?? App::env('CRAFT_ENVIRONMENT');

        return strtolower(trim((string)$env));
    }

    /**
     * Whether Craft is running outside `production` / `prod` (local, dev, staging, etc.).
     */
    public function isNonProductionEnvironment(): bool
    {
        return !in_array($this->getCraftEnvironment(), ['production', 'prod'], true);
    }

    /**
     * Whether runtime state has a project ingestion key from Burrow (used for event dispatch and related API calls).
     *
     * @param array<string,mixed>|null $state
     */
    public function runtimeStateHasIngestionKey(?array $state = null): bool
    {
        return $this->resolveIngestionKey($state)['key'] !== '';
    }

    /**
     * Resolves the project ingestion key from runtime state and SDK state.
     *
     * @param array<string,mixed>|null $state
     * @return array{key:string,projectId:string,keyPrefix:string}
     */
    public function resolveIngestionKey(?array $state = null): array
    {
        $state ??= $this->getState()->getState();
        $stored = is_array($state['ingestionKey'] ?? null) ? $state['ingestionKey'] : [];
        $key = trim((string)($stored['key'] ?? ''));
        $projectId = trim((string)($stored['projectId'] ?? ''));
        $keyPrefix = trim((string)($stored['keyPrefix'] ?? ''));

        if ($key === '' && is_array($state['sdkState'] ?? null)) {
            $key = trim((string)($state['sdkState']['ingestionKey'] ?? ''));
        }

        if ($projectId === '') {
            $projectId = trim((string)($state['projectId'] ?? ''));
        }

        return [
            'key' => $key,
            'projectId' => $projectId,
            'keyPrefix' => $keyPrefix,
        ];
    }

    /**
     * Whether outbound Burrow API calls can authenticate (ingestion key and/or temporary account key), given routing context.
     *
     * @param array<string,mixed>|null $runtimeState
     */
    public function canDispatchToBurrow(?array $runtimeState = null): bool
    {
        $runtimeState ??= $this->getState()->getState();
        if ($this->getBurrowBaseUrl() === '' || trim((string)($runtimeState['projectId'] ?? '')) === '') {
            return false;
        }
        if ($this->runtimeStateHasIngestionKey($runtimeState)) {
            return true;
        }

        return $this->getBurrowApiKey() !== '';
    }

    /**
     * Removes account-level API key from project config when CP admin changes are allowed (ingestion key is sufficient after link).
     */
    public function clearAccountApiKeyFromProjectConfigIfAllowed(): void
    {
        if (!Craft::$app->getConfig()->getGeneral()->allowAdminChanges) {
            return;
        }
        $settings = $this->getSettings();
        if (trim((string)$settings->apiKey) === '') {
            return;
        }
        $settings->apiKey = '';
        Craft::$app->getPlugins()->savePluginSettings($this, $settings->toArray());
    }

    /**
     * Plugin settings with connection fields merged for CP display (avoids mutating the cached settings model).
     *
     * Returns unparsed environmental values so the form can show `$BURROW_API_KEY` instead of the secret.
     * When process env supplies credentials and no raw value is stored, suggests the `$BURROW_*` alias.
     */
    public function getConnectionSettingsForDisplay(): Settings
    {
        $model = new Settings();
        $stored = $this->getSettings();
        $model->pluginName = $stored->pluginName;
        $model->baseUrl = $this->getRawBurrowBaseUrl();
        $model->apiKey = $this->getRawBurrowApiKey();

        if ($model->apiKey === '' && trim((string)App::env('BURROW_API_KEY')) !== '') {
            $model->apiKey = '$BURROW_API_KEY';
        }
        if (trim((string)App::env('BURROW_BASE_URL')) !== '' && !str_starts_with($model->baseUrl, '$')) {
            $model->baseUrl = '$BURROW_BASE_URL';
        }

        return $model;
    }

    /**
     * Unparsed `connectionBaseUrl` from runtime state only.
     */
    private function getRawBurrowBaseUrlFromState(): string
    {
        $state = $this->getState()->getState();

        return trim((string)($state['connectionBaseUrl'] ?? ''));
    }

    /**
     * Parses a Craft environmental setting value (`$ENV_NAME` or literal).
     */
    private function parseEnvValue(string $value): string
    {
        return trim((string)App::parseEnv(trim($value)));
    }

    public function getSettingsResponse(): mixed
    {
        $state = $this->getState()->getState();
        $url = !empty($state['onboardingCompleted']) ? 'burrow/settings' : 'burrow/setup';

        return Craft::$app->getResponse()->redirect(UrlHelper::cpUrl($url));
    }

    public function isOnboardingCompleted(): bool
    {
        $state = $this->getState()->getState();

        return !empty($state['onboardingCompleted']);
    }

    public function getCpNavItem(): ?array
    {
        $item = parent::getCpNavItem();

        $item['label'] = $this->getSettings()->pluginName;
        $item['url'] = 'burrow';

        $item['subnav']['dashboard'] = [
            'label' => Craft::t('burrow', 'Dashboard'),
            'url' => 'burrow/dashboard',
        ];
        if ($this->isOnboardingCompleted()) {
            $item['subnav']['settings'] = [
                'label' => Craft::t('burrow', 'Settings'),
                'url' => 'burrow/settings',
            ];
        } else {
            $item['subnav']['setup'] = [
                'label' => Craft::t('burrow', 'Setup'),
                'url' => 'burrow/setup',
            ];
        }
        $item['subnav']['outbox'] = [
            'label' => Craft::t('burrow', 'Outbox'),
            'url' => 'burrow/outbox',
        ];

        return $item;
    }

    /**
     * Restores PHP's working directory when `getcwd()` is empty or points at a removed path.
     *
     * Yii queue isolation (`queue/listen --isolate`) spawns each job via Symfony Process using
     * the worker's cwd. After a deploy the worker can keep running from a deleted release
     * directory, which surfaces as "The provided cwd "" does not exist."
     *
     * @author Burrow Analytics, LLC
     * @since 5.5.2
     */
    private function _ensureProcessWorkingDirectory(): void
    {
        $cwd = getcwd();
        if (is_string($cwd) && $cwd !== '' && is_dir($cwd)) {
            return;
        }

        $candidates = [];
        if (defined('CRAFT_BASE_PATH')) {
            $candidates[] = CRAFT_BASE_PATH;
        }

        $root = Craft::getAlias('@root', false);
        if (is_string($root) && $root !== '') {
            $candidates[] = $root;
        }

        /** @var \craft\web\Application|\craft\console\Application $app */
        $app = Craft::$app;
        $vendorPath = $app->getPath()->getVendorPath();
        if (is_string($vendorPath) && $vendorPath !== '') {
            $candidates[] = dirname($vendorPath);
        }

        foreach ($candidates as $path) {
            $resolved = realpath($path);
            if (!is_string($resolved) || $resolved === '' || !is_dir($resolved)) {
                continue;
            }

            chdir($resolved);
            return;
        }
    }

    /**
     * Re-applies a valid working directory on each queue worker loop, before isolate-spawn.
     *
     * @author Burrow Analytics, LLC
     * @since 5.5.2
     */
    private function _registerQueueWorkingDirectoryGuard(): void
    {
        $restore = function(): void {
            $this->_ensureProcessWorkingDirectory();
        };

        Event::on(CraftQueue::class, CraftQueue::EVENT_WORKER_START, $restore);
        Event::on(CraftQueue::class, CraftQueue::EVENT_WORKER_LOOP, $restore);
    }

    private function _registerRoutes(): void
    {
        Event::on(
            UrlManager::class,
            UrlManager::EVENT_REGISTER_CP_URL_RULES,
            static function(RegisterUrlRulesEvent $event): void {
                $event->rules = array_merge($event->rules, [
                    'burrow' => 'burrow/settings/dashboard',
                    'burrow/dashboard' => 'burrow/settings/dashboard',
                    'burrow/setup' => 'burrow/settings/setup',
                    'burrow/backfill-probe' => 'burrow/settings/backfill-probe',
                    'burrow/outbox' => 'burrow/settings/outbox',
                    'burrow/outbox/<elementId:\d+>' => 'elements/edit',
                    'burrow/settings/outbox' => 'burrow/settings/outbox',
                    'burrow/settings' => 'burrow/settings/configure',
                    'burrow/settings/index' => 'burrow/settings/index',
                ]);
            }
        );
    }

    private function _registerPostInstallRedirect(): void
    {
        Event::on(
            Plugins::class,
            Plugins::EVENT_AFTER_INSTALL_PLUGIN,
            function(PluginEvent $event): void {
                $installedPlugin = $event->plugin ?? null;
                $installedHandle = '';
                if (is_object($installedPlugin)) {
                    $installedHandle = strtolower(trim((string)($installedPlugin->id ?? '')));
                }
                if ($installedHandle !== 'burrow') {
                    return;
                }
                if (Craft::$app->getRequest()->getIsConsoleRequest()) {
                    return;
                }
                if (!Craft::$app->getRequest()->getIsCpRequest()) {
                    return;
                }
                if (Craft::$app->getRequest()->getAcceptsJson()) {
                    return;
                }

                // Craft's plugin installer can override direct redirects in this same request,
                // so set a one-time session flag and handle the redirect on the next CP GET.
                Craft::$app->getSession()->set('burrow.postInstallRedirectPending', true);
            }
        );
    }

    private function _maybeHandlePostInstallRedirect(): void
    {
        if (Craft::$app->getRequest()->getIsConsoleRequest()) {
            return;
        }

        $request = Craft::$app->getRequest();
        if (!$request->getIsCpRequest() || !$request->getIsGet()) {
            return;
        }
        if ($request->getAcceptsJson() || $request->getIsAjax()) {
            return;
        }

        $session = Craft::$app->getSession();
        if (!(bool)$session->get('burrow.postInstallRedirectPending', false)) {
            return;
        }

        $pathInfo = trim((string)$request->getPathInfo(), '/');
        if (str_starts_with($pathInfo, 'burrow')) {
            $session->remove('burrow.postInstallRedirectPending');
            return;
        }

        $session->remove('burrow.postInstallRedirectPending');
        $redirectUrl = $this->isOnboardingCompleted() ? 'burrow/dashboard' : 'burrow/setup';
        Craft::$app->getResponse()->redirect(UrlHelper::cpUrl($redirectUrl));
        Craft::$app->end();
    }

    private function _registerElementTypes(): void
    {
        Event::on(
            Elements::class,
            Elements::EVENT_REGISTER_ELEMENT_TYPES,
            static function (RegisterComponentTypesEvent $event): void {
                $event->types[] = OutboxElement::class;
            }
        );
    }

    private function _registerCommerceHooks(): void
    {
        $orderClass = '\craft\commerce\elements\Order';
        if (!class_exists($orderClass)) {
            return;
        }

        $hookMap = [
            // Prefer payment/authorization completion hooks for confirmed orders.
            'EVENT_AFTER_ORDER_PAID' => 'handleCompletedOrderEvent',
            'EVENT_AFTER_ORDER_AUTHORIZED' => 'handleCompletedOrderEvent',
            'EVENT_AFTER_ADD_LINE_ITEM' => 'handleCartLineItemAddedEvent',
            'EVENT_AFTER_REMOVE_LINE_ITEM' => 'handleCartLineItemRemovedEvent',
        ];
        foreach ($hookMap as $eventConstant => $handler) {
            $eventConst = $orderClass . '::' . $eventConstant;
            if (!defined($eventConst)) {
                continue;
            }
            /** @var string $eventName */
            $eventName = constant($eventConst);
            Event::on(
                $orderClass,
                $eventName,
                function (\yii\base\Event $event) use ($handler): void {
                    try {
                        $this->getCommerceTracking()->{$handler}($event);
                    } catch (\Throwable $e) {
                        $this->getLogs()->log('warning', 'Commerce order event dispatch failed', 'commerce', 'ecommerce', null, [
                            'handler' => $handler,
                            'error' => $e->getMessage(),
                        ]);
                    }
                }
            );
        }

        $elementClass = '\craft\base\Element';
        if (class_exists($elementClass) && defined($elementClass . '::EVENT_AFTER_SAVE')) {
            /** @var string $saveEventName */
            $saveEventName = constant($elementClass . '::EVENT_AFTER_SAVE');
            Event::on(
                $orderClass,
                $saveEventName,
                function (\yii\base\Event $event): void {
                    try {
                        $this->getCommerceTracking()->handleOrderSavedEvent($event);
                    } catch (\Throwable $e) {
                        $this->getLogs()->log('warning', 'Commerce checkout detection dispatch failed', 'commerce', 'ecommerce', null, [
                            'handler' => 'handleOrderSavedEvent',
                            'error' => $e->getMessage(),
                        ]);
                    }
                }
            );
        }

        // Order status changes fire on OrderHistories (see Commerce OrderHistories::EVENT_ORDER_STATUS_CHANGE),
        // not OrderStatuses (which only exposes EVENT_ORDER_STATUS_CHANGE_EMAILS).
        $orderHistoriesClass = '\craft\commerce\services\OrderHistories';
        if (class_exists($orderHistoriesClass)) {
            $statusChangeConst = $orderHistoriesClass . '::EVENT_ORDER_STATUS_CHANGE';
            if (defined($statusChangeConst)) {
                /** @var string $statusChangeEventName */
                $statusChangeEventName = constant($statusChangeConst);
                Event::on(
                    $orderHistoriesClass,
                    $statusChangeEventName,
                    function (\yii\base\Event $event): void {
                        try {
                            $this->getCommerceTracking()->handleOrderStatusChangeEvent($event);
                        } catch (\Throwable $e) {
                            $this->getLogs()->log('warning', 'Commerce order status change dispatch failed', 'commerce', 'ecommerce', null, [
                                'handler' => 'handleOrderStatusChangeEvent',
                                'error' => $e->getMessage(),
                            ]);
                        }
                    }
                );
            }
        }

        $paymentsClass = '\craft\commerce\services\Payments';
        if (class_exists($paymentsClass)) {
            $paymentConst = $paymentsClass . '::EVENT_AFTER_PROCESS_PAYMENT';
            if (defined($paymentConst)) {
                /** @var string $paymentEventName */
                $paymentEventName = constant($paymentConst);
                Event::on(
                    $paymentsClass,
                    $paymentEventName,
                    function (\yii\base\Event $event): void {
                        try {
                            $this->getCommerceTracking()->handlePaymentProcessedEvent($event);
                        } catch (\Throwable $e) {
                            $this->getLogs()->log('warning', 'Commerce payment event dispatch failed', 'commerce', 'ecommerce', null, [
                                'handler' => 'handlePaymentProcessedEvent',
                                'error' => $e->getMessage(),
                            ]);
                        }
                    }
                );
            }
        }
    }

    private function _registerFormHooks(): void
    {
        foreach ($this->getIntegrations()->getFormIntegrations()->all() as $adapter) {
            $adapter->registerEventHooks(function (\yii\base\Event $event) use ($adapter): void {
                try {
                    $this->getFormTracking()->handleAdapterSubmissionEvent($adapter, $event);
                } catch (\Throwable $e) {
                    $this->getLogs()->log('warning', $adapter->getLabel() . ' submission event dispatch failed', $adapter->getId(), 'forms', null, [
                        'error' => $e->getMessage(),
                    ]);
                }
            });
        }
    }

    /**
     * Injects the headless-Shopify frontend collector into site pages when the
     * Shopify integration's funnel capture is enabled. The collector relays
     * cart interactions to the same-origin `burrow/collect` action; no Burrow
     * credential is ever exposed to the page.
     */
    private function _registerShopifyCollector(): void
    {
        $request = Craft::$app->getRequest();
        if (
            $request->getIsConsoleRequest()
            || !$request->getIsSiteRequest()
            || !$request->getIsGet()
            || $request->getIsActionRequest()
            || $request->getIsAjax()
        ) {
            return;
        }

        try {
            if (!$this->getShopifyTracking()->shouldInjectCollector()) {
                return;
            }

            $collectorJs = @file_get_contents(__DIR__ . DIRECTORY_SEPARATOR . 'resources' . DIRECTORY_SEPARATOR . 'collector.js');
            if (!is_string($collectorJs) || trim($collectorJs) === '') {
                return;
            }

            $config = json_encode([
                'endpoint' => UrlHelper::actionUrl('burrow/collect'),
                'sessionInfoUrl' => UrlHelper::actionUrl('users/session-info'),
            ], JSON_UNESCAPED_SLASHES);
            if (!is_string($config)) {
                return;
            }

            Craft::$app->getView()->registerJs(
                'window.__burrowCollectorConfig = ' . $config . ';' . "\n" . $collectorJs,
                View::POS_END
            );
        } catch (\Throwable $e) {
            $this->getLogs()->log('warning', 'Shopify collector injection failed', 'shopify', 'ecommerce', null, [
                'error' => $e->getMessage(),
            ]);
        }
    }

    private function _scheduleSystemJobs(): void
    {
        $db = Craft::$app->getDb();
        if ($db->getSchema()->getTableSchema('{{%burrow_runtime_state}}', true) === null) {
            return;
        }

        $mutex = Craft::$app->getMutex();
        $lockKey = 'burrow-system-jobs-scheduler';
        if (!$mutex->acquire($lockKey, 0)) {
            return;
        }

        try {
            $runtimeState = $this->getState()->getState();
            if (empty($runtimeState['onboardingCompleted']) || trim((string)($runtimeState['projectId'] ?? '')) === '') {
                return;
            }
            if (!$this->canDispatchToBurrow($runtimeState)) {
                return;
            }

            $integrationSettings = is_array($runtimeState['integrationSettings'] ?? null) ? $runtimeState['integrationSettings'] : [];
            $systemJobs = is_array($integrationSettings['systemJobs'] ?? null) ? $integrationSettings['systemJobs'] : [];
            $now = time();

            // Weekly system snapshot publish cadence (7 days).
            $snapshotInterval = 7 * 24 * 3600;
            $snapshotLastRun = $this->_timestampFromState((string)($systemJobs['snapshotLastRunAt'] ?? ''));
            $snapshotQueued = $this->_timestampFromState((string)($systemJobs['snapshotQueuedAt'] ?? ''));
            $relinkInProgress = trim((string)($runtimeState['connectionApiKey'] ?? '')) !== '';
            $snapshotPublishedRecently = $this->getSnapshot()->wasPublishedRecently($runtimeState);
            if (
                !$relinkInProgress
                && !$snapshotPublishedRecently
                && ($snapshotLastRun === 0 || ($now - $snapshotLastRun) >= $snapshotInterval)
                && ($snapshotQueued === 0 || ($now - $snapshotQueued) > 1800)
            ) {
                Craft::$app->getQueue()->push(new \burrow\Burrow\jobs\PublishSystemSnapshotJob());
                $systemJobs['snapshotQueuedAt'] = gmdate('c');
            }

            // Hourly system heartbeat publish cadence (1 hour).
            $heartbeatInterval = 3600;
            $heartbeatLastRun = $this->_timestampFromState((string)($systemJobs['heartbeatLastRunAt'] ?? ''));
            $heartbeatQueued = $this->_timestampFromState((string)($systemJobs['heartbeatQueuedAt'] ?? ''));
            if (($heartbeatLastRun === 0 || ($now - $heartbeatLastRun) >= $heartbeatInterval) && ($heartbeatQueued === 0 || ($now - $heartbeatQueued) > 900)) {
                Craft::$app->getQueue()->push(new \burrow\Burrow\jobs\PublishSystemHeartbeatJob());
                $systemJobs['heartbeatQueuedAt'] = gmdate('c');
            }

            // Cart abandonment scan cadence (30 minutes), gated by funnel opt-in.
            $commerceConfig = is_array($integrationSettings['commerce'] ?? null) ? $integrationSettings['commerce'] : [];
            $funnelEnabled = (string)($commerceConfig['mode'] ?? 'off') === 'track' && !empty($commerceConfig['ecommerceFunnel']);
            if ($funnelEnabled) {
                $cartAbandonmentInterval = 1800;
                $cartAbandonmentLastRun = $this->_timestampFromState((string)($systemJobs['cartAbandonmentLastRunAt'] ?? ''));
                $cartAbandonmentQueued = $this->_timestampFromState((string)($systemJobs['cartAbandonmentQueuedAt'] ?? ''));
                if (($cartAbandonmentLastRun === 0 || ($now - $cartAbandonmentLastRun) >= $cartAbandonmentInterval) && ($cartAbandonmentQueued === 0 || ($now - $cartAbandonmentQueued) > 900)) {
                    Craft::$app->getQueue()->push(new \burrow\Burrow\jobs\DetectAbandonedCartsJob());
                    $systemJobs['cartAbandonmentQueuedAt'] = gmdate('c');
                }
            }

            $integrationSettings['systemJobs'] = $systemJobs;
            $runtimeState['integrationSettings'] = $integrationSettings;
            $this->getState()->saveState($runtimeState);
        } catch (\Throwable $e) {
            $this->getLogs()->log('warning', 'System job scheduler check failed', 'system', 'system', null, [
                'error' => $e->getMessage(),
            ]);
        } finally {
            $mutex->release($lockKey);
        }
    }

    private function _timestampFromState(string $value): int
    {
        $trimmed = trim($value);
        if ($trimmed === '') {
            return 0;
        }
        $ts = strtotime($trimmed);
        return $ts === false ? 0 : (int)$ts;
    }
}
