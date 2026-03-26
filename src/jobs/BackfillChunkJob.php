<?php
namespace burrow\Burrow\jobs;

use Craft;
use burrow\Burrow\Plugin;
use craft\queue\BaseJob;
use yii\queue\Queue;

class BackfillChunkJob extends BaseJob
{
    protected function defaultDescription(): ?string
    {
        return Craft::t('burrow', 'Process Burrow historical backfill (chunk)');
    }

    public function execute($queue): void
    {
        /** @var Queue $queue */
        $plugin = Plugin::getInstance();
        $runtimeState = $plugin->getState()->getState();
        $integrationSettings = is_array($runtimeState['integrationSettings'] ?? null) ? $runtimeState['integrationSettings'] : [];
        $backfill = is_array($integrationSettings['backfill'] ?? null) ? $integrationSettings['backfill'] : [];
        $status = (string)($backfill['status'] ?? '');

        if ($status !== 'queued' && $status !== 'running') {
            return;
        }

        if ($status === 'queued') {
            $backfill['status'] = 'running';
            $integrationSettings['backfill'] = $backfill;
            $runtimeState['integrationSettings'] = $integrationSettings;
            $plugin->getState()->saveState($runtimeState);
        }

        $checkpoint = is_array($backfill['checkpoint'] ?? null) ? $backfill['checkpoint'] : [];
        if ($checkpoint === []) {
            $backfill['status'] = 'failed';
            $backfill['error'] = 'Backfill checkpoint missing.';
            $backfill['completedAt'] = gmdate('c');
            $integrationSettings['backfill'] = $backfill;
            $runtimeState['integrationSettings'] = $integrationSettings;
            $plugin->getState()->saveState($runtimeState);

            return;
        }

        $chunk = $plugin->getBackfill()->runBackfillChunk($runtimeState, $checkpoint);

        $runtimeState = $plugin->getState()->getState();
        $integrationSettings = is_array($runtimeState['integrationSettings'] ?? null) ? $runtimeState['integrationSettings'] : [];
        $backfill = is_array($integrationSettings['backfill'] ?? null) ? $integrationSettings['backfill'] : [];

        $updatedCheckpoint = $chunk['checkpoint'];
        $backfill['checkpoint'] = $updatedCheckpoint;
        $backfill['windowStart'] = (string)($updatedCheckpoint['windowStart'] ?? '');
        $backfill['windowEnd'] = (string)($updatedCheckpoint['windowEnd'] ?? '');
        $backfill['windowPreset'] = (string)($updatedCheckpoint['windowPreset'] ?? '');
        $backfill['sources'] = array_values(array_filter(array_map('strval', (array)($updatedCheckpoint['sources'] ?? []))));
        $backfill['requested'] = (int)($updatedCheckpoint['requested'] ?? 0);
        $backfill['accepted'] = (int)($updatedCheckpoint['accepted'] ?? 0) + (int)($updatedCheckpoint['skippedDuplicates'] ?? 0);
        $backfill['rejected'] = (int)($updatedCheckpoint['rejected'] ?? 0);
        $backfill['validationRejected'] = (int)($updatedCheckpoint['validationRejected'] ?? 0);
        $backfill['latestCursor'] = (string)($updatedCheckpoint['latestCursor'] ?? '');
        $backfill['breakdown'] = is_array($updatedCheckpoint['breakdown'] ?? null) ? $updatedCheckpoint['breakdown'] : ['forms' => 0, 'ecommerce' => 0];

        if (!$chunk['ok']) {
            $backfill['status'] = 'failed';
            $backfill['error'] = (string)$chunk['error'];
            $backfill['completedAt'] = gmdate('c');
            $integrationSettings['backfill'] = $backfill;
            $runtimeState['integrationSettings'] = $integrationSettings;
            $plugin->getState()->saveState($runtimeState);
            $plugin->getLogs()->log('error', 'Backfill failed', 'backfill', 'system', null, [
                'error' => $chunk['error'],
            ]);

            return;
        }

        if ($chunk['done']) {
            $final = $plugin->getBackfill()->completeBackfillFromCheckpoint($runtimeState, $updatedCheckpoint);
            $backfill['status'] = $final['ok'] ? 'completed' : 'failed';
            $backfill['error'] = (string)$final['error'];
            $backfill['windowStart'] = (string)$final['windowStart'];
            $backfill['windowEnd'] = (string)$final['windowEnd'];
            $backfill['sources'] = (array)$final['sources'];
            $backfill['requested'] = (int)$final['requested'];
            $backfill['accepted'] = (int)$final['accepted'];
            $backfill['rejected'] = (int)$final['rejected'];
            $backfill['validationRejected'] = (int)$final['validationRejected'];
            $backfill['latestCursor'] = (string)$final['latestCursor'];
            $backfill['breakdown'] = (array)$final['breakdown'];
            $backfill['completedAt'] = gmdate('c');
            unset($backfill['checkpoint']);

            $integrationSettings['backfill'] = $backfill;
            $runtimeState['integrationSettings'] = $integrationSettings;
            $plugin->getState()->saveState($runtimeState);

            if ($final['ok']) {
                $plugin->getLogs()->log('info', 'Backfill completed', 'backfill', 'system', null, [
                    'requested' => $final['requested'],
                    'accepted' => $final['accepted'],
                    'rejected' => $final['rejected'],
                    'sources' => $final['sources'],
                    'windowStart' => $final['windowStart'],
                ]);
            } else {
                $plugin->getLogs()->log('error', 'Backfill failed', 'backfill', 'system', null, [
                    'error' => $final['error'],
                ]);
            }

            return;
        }

        $integrationSettings['backfill'] = $backfill;
        $runtimeState['integrationSettings'] = $integrationSettings;
        $plugin->getState()->saveState($runtimeState);

        Craft::$app->getQueue()->push(new BackfillChunkJob());
    }
}
