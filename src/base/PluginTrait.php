<?php
namespace burrow\Burrow\base;

use burrow\Burrow\services\BurrowApiService;
use burrow\Burrow\services\BackfillService;
use burrow\Burrow\services\CommerceTrackingService;
use burrow\Burrow\services\EventLogService;
use burrow\Burrow\services\FormTrackingService;
use burrow\Burrow\services\IntegrationsService;
use burrow\Burrow\services\QueueService;
use burrow\Burrow\services\ShopifyTrackingService;
use burrow\Burrow\services\StateService;
use burrow\Burrow\services\SystemSnapshotService;

trait PluginTrait
{
    private bool $_pluginComponentsInitialized = false;

    private function _setPluginComponents(): void
    {
        if ($this->_pluginComponentsInitialized) {
            return;
        }

        $this->setComponents([
            'burrowApi' => BurrowApiService::class,
            'backfill' => BackfillService::class,
            'commerceTracking' => CommerceTrackingService::class,
            'formTracking' => FormTrackingService::class,
            'integrations' => IntegrationsService::class,
            'logs' => EventLogService::class,
            'queue' => QueueService::class,
            'shopifyTracking' => ShopifyTrackingService::class,
            'state' => StateService::class,
            'snapshot' => SystemSnapshotService::class,
        ]);

        $this->_pluginComponentsInitialized = true;
    }

    public function getBurrowApi(): BurrowApiService
    {
        $this->_setPluginComponents();

        return $this->get('burrowApi');
    }

    public function getBackfill(): BackfillService
    {
        $this->_setPluginComponents();

        return $this->get('backfill');
    }

    public function getIntegrations(): IntegrationsService
    {
        $this->_setPluginComponents();

        return $this->get('integrations');
    }

    public function getCommerceTracking(): CommerceTrackingService
    {
        $this->_setPluginComponents();

        return $this->get('commerceTracking');
    }

    public function getFormTracking(): FormTrackingService
    {
        $this->_setPluginComponents();

        return $this->get('formTracking');
    }

    public function getLogs(): EventLogService
    {
        $this->_setPluginComponents();

        return $this->get('logs');
    }

    public function getQueue(): QueueService
    {
        $this->_setPluginComponents();

        return $this->get('queue');
    }

    public function getShopifyTracking(): ShopifyTrackingService
    {
        $this->_setPluginComponents();

        return $this->get('shopifyTracking');
    }

    public function getState(): StateService
    {
        // Craft can call into the plugin during install before init() runs, so
        // register components lazily when a service is first requested.
        $this->_setPluginComponents();

        return $this->get('state');
    }

    public function getSnapshot(): SystemSnapshotService
    {
        $this->_setPluginComponents();

        return $this->get('snapshot');
    }
}
