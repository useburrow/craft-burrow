<?php
namespace amici\Burrow\services;

use Craft;
use craft\base\Component;

class QueueService extends Component
{
    /**
     * @param array<string,mixed> $payload
     */
    public function enqueue(string $eventKey, array $payload, string $channel = '', string $eventName = '', int $maxAttempts = 6): bool
    {
        try {
            Craft::$app->getDb()->createCommand()->insert('{{%burrow_outbox}}', [
                'id' => bin2hex(random_bytes(16)),
                'event_key' => $eventKey,
                'channel' => $channel ?: null,
                'event_name' => $eventName ?: null,
                'status' => 'pending',
                'attempt_count' => 0,
                'max_attempts' => max(1, $maxAttempts),
                'payload' => $payload,
                'last_error' => null,
                'next_attempt_at' => null,
                'sent_at' => null,
                'created_at' => gmdate('Y-m-d H:i:s'),
                'updated_at' => gmdate('Y-m-d H:i:s'),
            ])->execute();
            return true;
        } catch (\Throwable) {
            return false;
        }
    }

    /**
     * @return array<string,int>
     */
    public function stats(): array
    {
        $db = Craft::$app->getDb();

        return [
            'pending' => (int)$db->createCommand("SELECT COUNT(*) FROM {{%burrow_outbox}} WHERE status = 'pending'")->queryScalar(),
            'retrying' => (int)$db->createCommand("SELECT COUNT(*) FROM {{%burrow_outbox}} WHERE status = 'retrying'")->queryScalar(),
            'sent' => (int)$db->createCommand("SELECT COUNT(*) FROM {{%burrow_outbox}} WHERE status = 'sent'")->queryScalar(),
            'failed' => (int)$db->createCommand("SELECT COUNT(*) FROM {{%burrow_outbox}} WHERE status = 'failed'")->queryScalar(),
        ];
    }

    /**
     * @return array<int,array<string,mixed>>
     */
    public function listRecent(int $limit = 100): array
    {
        $rows = Craft::$app->getDb()->createCommand(
            'SELECT id, event_key, channel, event_name, status, attempt_count, max_attempts, last_error, next_attempt_at, sent_at, created_at, updated_at
             FROM {{%burrow_outbox}}
             ORDER BY created_at DESC
             LIMIT :limit',
            [':limit' => max(1, $limit)]
        )->queryAll();

        return is_array($rows) ? $rows : [];
    }

    public function retryNow(string $id): bool
    {
        return (bool)Craft::$app->getDb()->createCommand()->update(
            '{{%burrow_outbox}}',
            [
                'status' => 'pending',
                'next_attempt_at' => null,
                'updated_at' => gmdate('Y-m-d H:i:s'),
            ],
            ['id' => $id]
        )->execute();
    }

    public function deleteRecord(string $id): bool
    {
        return (bool)Craft::$app->getDb()->createCommand()->delete('{{%burrow_outbox}}', ['id' => $id])->execute();
    }
}
