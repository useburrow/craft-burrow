<?php
namespace burrow\Burrow\jobs;

use Craft;
use burrow\Burrow\Plugin;
use craft\helpers\Queue as QueueHelper;
use craft\queue\BaseJob;

/**
 * Reconciles CP outbox element rows for outbox records created by bulk backfill sends,
 * which skip per-row element saves for speed. Time-budgeted and self-requeuing so it
 * never runs past the queue TTR.
 */
class SyncOutboxElementIndexJob extends BaseJob
{
    private const TIME_BUDGET_SECONDS = 60;

    private const BATCH_SIZE = 200;

    public const TTR = 300;

    protected function defaultDescription(): ?string
    {
        return Craft::t('burrow', 'Index Burrow outbox activity');
    }

    public function execute($queue): void
    {
        $queueService = Plugin::getInstance()->getQueue();
        $deadline = microtime(true) + self::TIME_BUDGET_SECONDS;

        // Requeue only while `remaining` keeps shrinking, so rows whose element save
        // persistently fails can't produce an endless requeue loop.
        $previousRemaining = null;
        do {
            $result = $queueService->syncElementIndexBatch(self::BATCH_SIZE);
            $madeProgress = $previousRemaining === null || $result['remaining'] < $previousRemaining;
            $previousRemaining = $result['remaining'];
        } while ($result['synced'] > 0 && $result['remaining'] > 0 && $madeProgress && microtime(true) < $deadline);

        if ($result['remaining'] > 0 && $madeProgress) {
            QueueHelper::push(new self(), null, 0, self::TTR);
        }
    }
}
