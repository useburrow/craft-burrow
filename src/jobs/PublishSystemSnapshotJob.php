<?php
namespace burrow\Burrow\jobs;

use Craft;
use craft\queue\BaseJob;
use yii\queue\Queue;

/**
 * Publishes a system stack snapshot to each linked Burrow project.
 *
 * @author Burrow Analytics, LLC
 * @since 5.0.0
 */
class PublishSystemSnapshotJob extends BaseJob
{
    // =========================================================================
    // Public Methods
    // =========================================================================

    /**
     * @inheritdoc
     */
    protected function defaultDescription(): ?string
    {
        return 'Publish Burrow system snapshot';
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
        $systemJobs['snapshotQueuedAt'] = '';
        $systemJobs['snapshotLastAttemptAt'] = gmdate('c');

        if (empty($runtimeState['onboardingCompleted'])) {
            $plugin->getLogs()->log('info', 'Skipped scheduled snapshot publish (onboarding not completed)', 'system', 'system');
            $integrationSettings['systemJobs'] = $systemJobs;
            $runtimeState['integrationSettings'] = $integrationSettings;
            $plugin->getState()->saveState($runtimeState);
            return;
        }

        $linkedSites = $plugin->getState()->getLinkedSiteStates();
        if ($linkedSites === []) {
            if (!$plugin->canDispatchToBurrow($runtimeState)) {
                $systemJobs['snapshotLastError'] = 'Missing Burrow connection/routing context.';
                $integrationSettings['systemJobs'] = $systemJobs;
                $runtimeState['integrationSettings'] = $integrationSettings;
                $plugin->getState()->saveState($runtimeState);
                return;
            }
            $linkedSites = ['0' => ['siteId' => (int)($runtimeState['craftSiteId'] ?? 0)]];
        }

        if ($plugin->getSnapshot()->wasPublishedRecently($runtimeState)) {
            $plugin->getLogs()->log('info', 'Skipped scheduled snapshot publish (recent snapshot already sent)', 'system', 'system');
            $integrationSettings['systemJobs'] = $systemJobs;
            $runtimeState['integrationSettings'] = $integrationSettings;
            $plugin->getState()->saveState($runtimeState);
            return;
        }

        $snapshot = $plugin->getSnapshot()->collectSnapshot();
        $runtimeState['lastSnapshot'] = $snapshot;
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
            $siteRuntime['lastSnapshot'] = $snapshot;
            $result = $plugin->getBurrowApi()->publishSystemSnapshot(
                $plugin->getBurrowBaseUrl(),
                $plugin->getBurrowApiKey(),
                $siteRuntime,
                $snapshot
            );
            if (!empty($result['ok'])) {
                $anyOk = true;
                if ($siteId > 0) {
                    $plugin->getState()->saveSiteState($siteId, [
                        'lastSnapshot' => $snapshot,
                    ]);
                }
            } else {
                $lastError = (string)($result['error'] ?? '');
            }
        }

        if ($anyOk) {
            $systemJobs['snapshotLastRunAt'] = gmdate('c');
            $systemJobs['snapshotLastError'] = '';
            $plugin->getLogs()->log('info', 'Scheduled snapshot published', 'system', 'system', null, [
                'linkedSites' => count($linkedSites),
            ]);
        } else {
            $systemJobs['snapshotLastError'] = $lastError !== '' ? $lastError : 'Missing Burrow connection/routing context.';
            $plugin->getLogs()->log('warning', 'Scheduled snapshot publish failed', 'system', 'system', null, [
                'error' => $systemJobs['snapshotLastError'],
            ]);
        }

        $integrationSettings['systemJobs'] = $systemJobs;
        $runtimeState['integrationSettings'] = $integrationSettings;
        $plugin->getState()->saveState($runtimeState);
    }
}
