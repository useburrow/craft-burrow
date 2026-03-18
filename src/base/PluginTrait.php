<?php
namespace amici\Burrow\base;

use amici\Burrow\services\BurrowApiService;
use amici\Burrow\services\BackfillService;
use amici\Burrow\services\EventLogService;
use amici\Burrow\services\IntegrationsService;
use amici\Burrow\services\QueueService;
use amici\Burrow\services\StateService;
use amici\Burrow\services\SystemSnapshotService;

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
