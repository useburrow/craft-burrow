<?php
namespace burrow\Burrow\jobs;

use Craft;
use craft\queue\BaseJob;
use yii\queue\Queue;

/**
 * Publishes a system heartbeat to each linked Burrow project.
 *
 * @author Burrow Analytics, LLC
 * @since 5.0.0
 */
class PublishSystemHeartbeatJob extends BaseJob
{
    // =========================================================================
    // Public Methods
    // =========================================================================

    /**
     * @inheritdoc
     */
    protected function defaultDescription(): ?string
    {
        return 'Publish Burrow system heartbeat';
    }

    /**
     * @inheritdoc
     */
    public function execute($queue): void
    {
        /** @var Queue $queue */
        $plugin = \burrow\Burrow\Plugin::getInstance();
        $runtimeState = $plugin->getState()->getState();

        $integrationSettings = is_array($runtimeState['integrationSettings'] ?? null) ? $runtimeState['integrationSettings'] : [];
        $systemJobs = is_array($integrationSettings['systemJobs'] ?? null) ? $integrationSettings['systemJobs'] : [];
        $systemJobs['heartbeatQueuedAt'] = '';
        $systemJobs['heartbeatLastAttemptAt'] = gmdate('c');

        $linkedSites = $plugin->getState()->getLinkedSiteStates();
        if ($linkedSites === []) {
            if (!$plugin->canDispatchToBurrow($runtimeState)) {
                $systemJobs['heartbeatLastError'] = 'Missing Burrow connection/routing context.';
                $integrationSettings['systemJobs'] = $systemJobs;
                $runtimeState['integrationSettings'] = $integrationSettings;
                $plugin->getState()->saveState($runtimeState);
                return;
            }
            $linkedSites = ['0' => ['siteId' => (int)($runtimeState['craftSiteId'] ?? 0)]];
        }

        $anyOk = false;
        $lastError = '';
        foreach ($linkedSites as $siteKey => $_meta) {
            $siteId = (int)$siteKey;
            $siteRuntime = $siteId > 0
                ? $plugin->getState()->getSiteState($siteId)
                : $runtimeState;
            if (!$plugin->canDispatchToBurrow($siteRuntime)) {
                continue;
            }
            $result = $plugin->getBurrowApi()->publishSystemHeartbeat(
                $plugin->getBurrowBaseUrl(),
                $plugin->getBurrowApiKey(),
                $siteRuntime,
                0.0
            );
            if (!empty($result['ok'])) {
                $anyOk = true;
            } else {
                $lastError = (string)($result['error'] ?? '');
            }
        }

        if ($anyOk) {
            $systemJobs['heartbeatLastRunAt'] = gmdate('c');
            $systemJobs['heartbeatLastError'] = '';
            $plugin->getLogs()->log('info', 'Scheduled heartbeat published', 'system', 'system', null, [
                'linkedSites' => count($linkedSites),
            ]);
        } else {
            $systemJobs['heartbeatLastError'] = $lastError !== '' ? $lastError : 'Missing Burrow connection/routing context.';
            $plugin->getLogs()->log('warning', 'Scheduled heartbeat publish failed', 'system', 'system', null, [
                'error' => $systemJobs['heartbeatLastError'],
            ]);
        }

        $integrationSettings['systemJobs'] = $systemJobs;
        $runtimeState['integrationSettings'] = $integrationSettings;
        $plugin->getState()->saveState($runtimeState);
    }
}
