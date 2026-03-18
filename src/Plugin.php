<?php
namespace amici\Burrow;

use Craft;
use yii\base\Event;

use amici\Burrow\base\PluginTrait;
use amici\Burrow\models\Settings;

use craft\base\Model;
use craft\base\Plugin as CraftPlugin;
use craft\events\RegisterUrlRulesEvent;
use craft\helpers\UrlHelper;
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

        $this->_registerRoutes();
        $this->_setPluginComponents();

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
        $state = $this->getState()->getState();
        $onboardingComplete = !empty($state['onboardingCompleted']);

        $item['label'] = $this->getSettings()->pluginName;
        $item['url'] = $onboardingComplete ? 'burrow/dashboard' : 'burrow/settings';

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
                    'burrow/outbox' => 'burrow/settings/outbox',
                    'burrow/settings' => 'burrow/settings/index',
                ]);
            }
        );
    }
}
