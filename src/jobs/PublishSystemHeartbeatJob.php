<?php
namespace burrow\Burrow\jobs;

use Craft;
use craft\queue\BaseJob;
use yii\queue\Queue;

class PublishSystemHeartbeatJob extends BaseJob
{
    protected function defaultDescription(): ?string
    {
        return 'Publish Burrow system heartbeat';
    }

    public function execute($queue): void
    {
        /** @var Queue $queue */
        $plugin = \burrow\Burrow\Plugin::getInstance();
        $runtimeState = $plugin->getState()->getState();

        $integrationSettings = is_array($runtimeState['integrationSettings'] ?? null) ? $runtimeState['integrationSettings'] : [];
        $systemJobs = is_array($integrationSettings['systemJobs'] ?? null) ? $integrationSettings['systemJobs'] : [];
        $systemJobs['heartbeatQueuedAt'] = '';
        $systemJobs['heartbeatLastAttemptAt'] = gmdate('c');

        if (!$plugin->canDispatchToBurrow($runtimeState)) {
            $systemJobs['heartbeatLastError'] = 'Missing Burrow connection/routing context.';
            $integrationSettings['systemJobs'] = $systemJobs;
            $runtimeState['integrationSettings'] = $integrationSettings;
            $plugin->getState()->saveState($runtimeState);
            return;
        }

        $result = $plugin->getBurrowApi()->publishSystemHeartbeat(
            $plugin->getBurrowBaseUrl(),
            $plugin->getBurrowApiKey(),
            $runtimeState,
            0.0
        );

        if ($result['ok']) {
            $systemJobs['heartbeatLastRunAt'] = gmdate('c');
            $systemJobs['heartbeatLastError'] = '';
            $plugin->getLogs()->log('info', 'Scheduled heartbeat published', 'system', 'system');
        } else {
            $systemJobs['heartbeatLastError'] = (string)$result['error'];
            $plugin->getLogs()->log('warning', 'Scheduled heartbeat publish failed', 'system', 'system', null, [
                'error' => $result['error'],
            ]);
        }

        $integrationSettings['systemJobs'] = $systemJobs;
        $runtimeState['integrationSettings'] = $integrationSettings;
        $plugin->getState()->saveState($runtimeState);
    }
}
