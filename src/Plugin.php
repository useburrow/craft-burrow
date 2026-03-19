<?php
namespace burrow\Burrow;

use Craft;
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
use craft\services\Elements;
use craft\services\Plugins;
use craft\web\UrlManager;

class Plugin extends CraftPlugin
{
    use PluginTrait;

    public static ?Plugin $plugin = null;

    public string $schemaVersion = '5.0.0';
    public bool $hasCpSettings = true;
    public bool $hasCpSection = true;

    public function init(): void
    {
        parent::init();
        self::$plugin = $this;

        $this->_maybeHandlePostInstallRedirect();
        $this->_registerRoutes();
        $this->_registerPostInstallRedirect();
        $this->_setPluginComponents();
        $this->_registerElementTypes();
        $this->_registerCommerceHooks();
        $this->_registerFormHooks();
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

    public function getSettingsResponse(): mixed
    {
        return Craft::$app->getResponse()->redirect(UrlHelper::cpUrl('burrow/settings'));
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
        $item['subnav']['settings'] = [
            'label' => Craft::t('burrow', 'Setup'),
            'url' => 'burrow/settings',
        ];
        $item['subnav']['outbox'] = [
            'label' => Craft::t('burrow', 'Outbox'),
            'url' => 'burrow/outbox',
        ];

        return $item;
    }

    private function _registerRoutes(): void
    {
        Event::on(
            UrlManager::class,
            UrlManager::EVENT_REGISTER_CP_URL_RULES,
            static function(RegisterUrlRulesEvent $event): void {
                $event->rules = array_merge($event->rules, [
                    'burrow' => 'burrow/settings/index',
                    'burrow/dashboard' => 'burrow/settings/dashboard',
                    'burrow/backfill-probe' => 'burrow/settings/backfill-probe',
                    'burrow/outbox' => 'burrow/settings/outbox',
                    'burrow/settings/outbox' => 'burrow/settings/outbox',
                    'burrow/settings' => 'burrow/settings/index',
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
        Craft::$app->getResponse()->redirect(UrlHelper::cpUrl('burrow'));
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
    }

    private function _registerFormHooks(): void
    {
        $freeformServiceClass = '\Solspace\Freeform\Services\SubmissionsService';
        $freeformEventConst = $freeformServiceClass . '::EVENT_AFTER_SUBMIT';
        if (class_exists($freeformServiceClass) && defined($freeformEventConst)) {
            /** @var string $eventName */
            $eventName = constant($freeformEventConst);
            Event::on(
                $freeformServiceClass,
                $eventName,
                function (\yii\base\Event $event): void {
                    try {
                        $this->getFormTracking()->handleFreeformSubmissionEvent($event);
                    } catch (\Throwable $e) {
                        $this->getLogs()->log('warning', 'Freeform submission event dispatch failed', 'freeform', 'forms', null, [
                            'error' => $e->getMessage(),
                        ]);
                    }
                }
            );
        }

        $formieServiceClass = '\verbb\formie\services\Submissions';
        $formieEventConst = $formieServiceClass . '::EVENT_AFTER_SUBMISSION';
        if (class_exists($formieServiceClass) && defined($formieEventConst)) {
            /** @var string $eventName */
            $eventName = constant($formieEventConst);
            Event::on(
                $formieServiceClass,
                $eventName,
                function (\yii\base\Event $event): void {
                    try {
                        $this->getFormTracking()->handleFormieSubmissionEvent($event);
                    } catch (\Throwable $e) {
                        $this->getLogs()->log('warning', 'Formie submission event dispatch failed', 'formie', 'forms', null, [
                            'error' => $e->getMessage(),
                        ]);
                    }
                }
            );
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
            $settings = $this->getSettings();
            if (trim((string)$settings->baseUrl) === '' || trim((string)$settings->apiKey) === '') {
                return;
            }

            $integrationSettings = is_array($runtimeState['integrationSettings'] ?? null) ? $runtimeState['integrationSettings'] : [];
            $systemJobs = is_array($integrationSettings['systemJobs'] ?? null) ? $integrationSettings['systemJobs'] : [];
            $now = time();

            // Weekly system snapshot publish cadence (7 days).
            $snapshotInterval = 7 * 24 * 3600;
            $snapshotLastRun = $this->_timestampFromState((string)($systemJobs['snapshotLastRunAt'] ?? ''));
            $snapshotQueued = $this->_timestampFromState((string)($systemJobs['snapshotQueuedAt'] ?? ''));
            if (($snapshotLastRun === 0 || ($now - $snapshotLastRun) >= $snapshotInterval) && ($snapshotQueued === 0 || ($now - $snapshotQueued) > 1800)) {
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
