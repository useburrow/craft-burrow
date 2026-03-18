<?php
namespace burrow\Burrow\base;

use burrow\Burrow\services\BurrowApiService;
use burrow\Burrow\services\BackfillService;
use burrow\Burrow\services\EventLogService;
use burrow\Burrow\services\IntegrationsService;
use burrow\Burrow\services\QueueService;
use burrow\Burrow\services\StateService;
use burrow\Burrow\services\SystemSnapshotService;

trait PluginTrait
{
    private function _setPluginComponents(): void
    {
        $this->setComponents([
            'burrowApi' => BurrowApiService::class,
            'backfill' => BackfillService::class,
            'integrations' => IntegrationsService::class,
            'logs' => EventLogService::class,
            'queue' => QueueService::class,
            'state' => StateService::class,
            'snapshot' => SystemSnapshotService::class,
        ]);
    }

    public function getBurrowApi(): BurrowApiService
    {
        return $this->get('burrowApi');
    }

    public function getBackfill(): BackfillService
    {
        return $this->get('backfill');
    }

    public function getIntegrations(): IntegrationsService
    {
        return $this->get('integrations');
    }

    public function getLogs(): EventLogService
    {
        return $this->get('logs');
    }

    public function getQueue(): QueueService
    {
        return $this->get('queue');
    }

    public function getState(): StateService
    {
        return $this->get('state');
    }

    public function getSnapshot(): SystemSnapshotService
    {
        return $this->get('snapshot');
    }
}
