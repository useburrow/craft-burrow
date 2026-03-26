<?php
namespace burrow\Burrow\jobs;

use Craft;
use burrow\Burrow\Plugin;
use craft\queue\BaseJob;
use yii\queue\Queue;
use yii\queue\RetryableJobInterface;

class CleanupOutboxRetentionJob extends BaseJob implements RetryableJobInterface
{
    /**
     * Retention window in days for sent/failed rows (ignored when {@see $forcePurge} is true).
     */
    public int $retentionDays = 30;

    /**
     * When true, deletes all sent/failed outbox rows and truncates the dedupe table (retention save with 0 days).
     */
    public bool $forcePurge = false;

    public function getTtr(): int
    {
        return 900;
    }

    public function canRetry($attempt, $error): bool
    {
        return false;
    }

    protected function defaultDescription(): ?string
    {
        return Craft::t('burrow', 'Burrow outbox retention cleanup');
    }

    public function execute($queue): void
    {
        /** @var Queue $queue */
        $plugin = Plugin::getInstance();
        $days = $this->forcePurge ? 0 : max(1, min(365, $this->retentionDays));
        $deleted = $plugin->getQueue()->cleanupSentAndFailed($days);

        if ($this->forcePurge) {
            $runtimeState = $plugin->getState()->getState();
            $integrationSettings = is_array($runtimeState['integrationSettings'] ?? null) ? $runtimeState['integrationSettings'] : [];
            $operations = is_array($integrationSettings['operations'] ?? null) ? $integrationSettings['operations'] : [];
            $storedRetention = max(1, min(365, (int)($operations['outboxRetentionDays'] ?? 30)));

            $plugin->getLogs()->log('info', 'Outbox force-purged (retention save)', 'operations', 'system', null, [
                'outboxRetentionDays' => $storedRetention,
                'outboxPurgeAllSentAndFailed' => true,
                'deletedRecords' => $deleted,
            ]);
            return;
        }

        $plugin->getLogs()->log('info', 'Operations settings updated', 'operations', 'system', null, [
            'outboxRetentionDays' => $days,
            'deletedRecords' => $deleted,
        ]);
    }
}
