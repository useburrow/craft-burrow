<?php
namespace burrow\Burrow\jobs;

use Craft;
use craft\queue\BaseJob;
use yii\queue\Queue;

class PublishSystemSnapshotJob extends BaseJob
{
    protected function defaultDescription(): ?string
    {
        return 'Publish Burrow system snapshot';
    }

    public function execute($queue): void
    {
        /** @var Queue $queue */
        $plugin = \burrow\Burrow\Plugin::getInstance();
        $runtimeState = $plugin->getState()->getState();

        $integrationSettings = is_array($runtimeState['integrationSettings'] ?? null) ? $runtimeState['integrationSettings'] : [];
        $systemJobs = is_array($integrationSettings['systemJobs'] ?? null) ? $integrationSettings['systemJobs'] : [];
        $systemJobs['snapshotQueuedAt'] = '';
        $systemJobs['snapshotLastAttemptAt'] = gmdate('c');

        if (!$plugin->canDispatchToBurrow($runtimeState)) {
            $systemJobs['snapshotLastError'] = 'Missing Burrow connection/routing context.';
            $integrationSettings['systemJobs'] = $systemJobs;
            $runtimeState['integrationSettings'] = $integrationSettings;
            $plugin->getState()->saveState($runtimeState);
            return;
        }

        $runtimeState['lastSnapshot'] = $plugin->getSnapshot()->collectSnapshot();
        $result = $plugin->getBurrowApi()->publishSystemSnapshot(
            $plugin->getBurrowBaseUrl(),
            $plugin->getBurrowApiKey(),
            $runtimeState,
            $runtimeState['lastSnapshot']
        );

        if ($result['ok']) {
            $systemJobs['snapshotLastRunAt'] = gmdate('c');
            $systemJobs['snapshotLastError'] = '';
            $plugin->getLogs()->log('info', 'Scheduled snapshot published', 'system', 'system');
        } else {
            $systemJobs['snapshotLastError'] = (string)$result['error'];
            $plugin->getLogs()->log('warning', 'Scheduled snapshot publish failed', 'system', 'system', null, [
                'error' => $result['error'],
            ]);
        }

        $integrationSettings['systemJobs'] = $systemJobs;
        $runtimeState['integrationSettings'] = $integrationSettings;
        $plugin->getState()->saveState($runtimeState);
    }
}
