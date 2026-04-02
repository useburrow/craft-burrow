<?php
namespace burrow\Burrow\services;

use burrow\Burrow\elements\OutboxElement;
use burrow\Burrow\jobs\DeliverOutboxRowJob;
use burrow\Burrow\Plugin;
use Craft;
use craft\base\Component;
use craft\db\Query;
use craft\helpers\Db;
use craft\helpers\Queue as QueueHelper;

class QueueService extends Component
{
    /**
     * Matches WordPress Burrow plugin default: automatic delayed retries after failed realtime delivery.
     */
    public const DEFAULT_MAX_ATTEMPTS = 6;
    private bool $deferOutboxElementSearchIndex = false;

    /** @var array<int, true> */
    private array $deferredOutboxElementIds = [];

    /**
     * Skip updating the Craft search index on each outbox element save; pair with
     * {@see flushDeferredOutboxElementSearchIndex()} after bulk work (e.g. backfill jobs).
     */
    public function beginDeferringOutboxElementSearchIndex(): void
    {
        $this->deferOutboxElementSearchIndex = true;
        $this->deferredOutboxElementIds = [];
    }

    /**
     * Applies search indexing for outbox elements saved while deferral was active.
     */
    public function flushDeferredOutboxElementSearchIndex(): void
    {
        $ids = array_keys($this->deferredOutboxElementIds);
        $this->deferredOutboxElementIds = [];
        $this->deferOutboxElementSearchIndex = false;

        foreach ($ids as $elementId) {
            if ($elementId <= 0) {
                continue;
            }
            try {
                $element = Craft::$app->getElements()->getElementById($elementId, OutboxElement::class);
                if ($element instanceof OutboxElement) {
                    Craft::$app->getSearch()->indexElementAttributes($element);
                }
            } catch (\Throwable) {
                // Best-effort; CP element search may lag until the next resave.
            }
        }
    }

    /**
     * @param array<string,mixed> $payload
     */
    public function enqueue(string $eventKey, array $payload, string $channel = '', string $eventName = '', int $maxAttempts = 6): bool
    {
        try {
            $id = bin2hex(random_bytes(16));
            Craft::$app->getDb()->createCommand()->insert('{{%burrow_outbox}}', [
                'id' => $id,
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
            $this->syncElementIndexRecordByOutboxId($id);
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
    public function listRecords(string $status = '', int $limit = 50, int $offset = 0, string $search = ''): array
    {
        $where = [];
        $params = [
            ':limit' => max(1, $limit),
            ':offset' => max(0, $offset),
        ];

        $normalizedStatus = trim($status);
        if ($normalizedStatus !== '' && in_array($normalizedStatus, ['pending', 'retrying', 'sent', 'failed'], true)) {
            $where[] = 'status = :status';
            $params[':status'] = $normalizedStatus;
        }

        $queryText = trim($search);
        if ($queryText !== '') {
            $where[] = '(event_key LIKE :q OR channel LIKE :q OR event_name LIKE :q OR last_error LIKE :q)';
            $params[':q'] = '%' . str_replace(['%', '_'], ['\\%', '\\_'], $queryText) . '%';
        }

        $whereSql = $where ? ('WHERE ' . implode(' AND ', $where)) : '';
        $rows = Craft::$app->getDb()->createCommand(
            "SELECT id, event_key, channel, event_name, status, attempt_count, max_attempts, last_error, next_attempt_at, sent_at, created_at, updated_at
             FROM {{%burrow_outbox}}
             {$whereSql}
             ORDER BY created_at DESC
             LIMIT :limit OFFSET :offset",
            $params
        )->queryAll();

        return is_array($rows) ? $rows : [];
    }

    public function countRecords(string $status = '', string $search = ''): int
    {
        $where = [];
        $params = [];

        $normalizedStatus = trim($status);
        if ($normalizedStatus !== '' && in_array($normalizedStatus, ['pending', 'retrying', 'sent', 'failed'], true)) {
            $where[] = 'status = :status';
            $params[':status'] = $normalizedStatus;
        }

        $queryText = trim($search);
        if ($queryText !== '') {
            $where[] = '(event_key LIKE :q OR channel LIKE :q OR event_name LIKE :q OR last_error LIKE :q)';
            $params[':q'] = '%' . str_replace(['%', '_'], ['\\%', '\\_'], $queryText) . '%';
        }

        $whereSql = $where ? ('WHERE ' . implode(' AND ', $where)) : '';
        return (int)Craft::$app->getDb()->createCommand(
            "SELECT COUNT(*) FROM {{%burrow_outbox}} {$whereSql}",
            $params
        )->queryScalar();
    }

    public function wasSent(string $eventKey): bool
    {
        $eventKey = trim($eventKey);
        if ($eventKey === '') {
            return false;
        }
        $result = Craft::$app->getDb()->createCommand(
            'SELECT 1 FROM {{%burrow_outbox_sent}} WHERE event_key = :key LIMIT 1',
            [':key' => $eventKey]
        )->queryScalar();
        return $result !== false && $result !== null;
    }

    public function retryNow(string $id): bool
    {
        $ok = (bool)Craft::$app->getDb()->createCommand()->update(
            '{{%burrow_outbox}}',
            [
                'status' => 'pending',
                'attempt_count' => 0,
                'last_error' => null,
                'next_attempt_at' => null,
                'updated_at' => gmdate('Y-m-d H:i:s'),
            ],
            ['id' => $id]
        )->execute();
        if ($ok) {
            $this->syncElementIndexRecordByOutboxId($id);
            if (Plugin::getInstance()->canDispatchToBurrow()) {
                QueueHelper::push(new DeliverOutboxRowJob(['outboxId' => $id]), null, 0, 120);
            }
        }
        return $ok;
    }

    public function deleteRecord(string $id): bool
    {
        $ok = (bool)Craft::$app->getDb()->createCommand()->delete('{{%burrow_outbox}}', ['id' => $id])->execute();
        if ($ok) {
            $this->removeElementIndexRecordByOutboxId($id);
        }
        return $ok;
    }

    /**
     * @param array<string,mixed> $payload
     */
    public function markSent(string $eventKey, array $payload, string $channel = '', string $eventName = ''): bool
    {
        $ok = $this->upsertDeliveryRecord(
            $eventKey,
            $payload,
            $channel,
            $eventName,
            'sent',
            null,
            gmdate('Y-m-d H:i:s'),
            false
        );
        if (!$ok) {
            return false;
        }
        $this->syncElementIndexRecordByEventKey($eventKey);

        try {
            Craft::$app->getDb()->createCommand()->upsert(
                '{{%burrow_outbox_sent}}',
                [
                    'event_key' => $eventKey,
                    'sent_at' => gmdate('Y-m-d H:i:s'),
                ],
                [
                    'sent_at' => gmdate('Y-m-d H:i:s'),
                ]
            )->execute();
        } catch (\Throwable) {
            // Outbox row already reflects send status; keep best-effort sent-index write.
        }
        return true;
    }

    /**
     * @param array<string,mixed> $payload
     */
    public function markFailed(string $eventKey, array $payload, string $error, string $channel = '', string $eventName = '', bool $scheduleAutoRetry = true): bool
    {
        $ok = $this->upsertDeliveryRecord(
            $eventKey,
            $payload,
            $channel,
            $eventName,
            'failed',
            $error,
            null,
            $scheduleAutoRetry
        );
        if ($ok) {
            $this->syncElementIndexRecordByEventKey($eventKey);
        }
        return $ok;
    }

    /**
     * Remove completed outbox rows. Use {@see $days} = 0 to delete every `sent` and `failed` row immediately
     * and reset the `burrow_outbox_sent` dedupe table (operations “force purge” path).
     */
    public function cleanupSentAndFailed(int $days): int
    {
        if ($days === 0) {
            $deletedOutbox = (int)Craft::$app->getDb()->createCommand()->delete(
                '{{%burrow_outbox}}',
                ['in', 'status', ['sent', 'failed']]
            )->execute();
            try {
                Db::truncateTable('{{%burrow_outbox_sent}}', Craft::$app->getDb());
            } catch (\Throwable) {
                // Best-effort; table may be missing in partial installs.
            }
            $this->cleanupElementIndexOrphans();

            return $deletedOutbox;
        }

        $days = max(1, $days);
        $cutoff = gmdate('Y-m-d H:i:s', strtotime('-' . $days . ' days'));
        $deletedOutbox = (int)Craft::$app->getDb()->createCommand()->delete(
            '{{%burrow_outbox}}',
            [
                'and',
                ['in', 'status', ['sent', 'failed']],
                ['<', 'updated_at', $cutoff],
            ]
        )->execute();
        try {
            Craft::$app->getDb()->createCommand()->delete(
                '{{%burrow_outbox_sent}}',
                ['<', 'sent_at', $cutoff]
            )->execute();
        } catch (\Throwable) {
            // Best-effort cleanup for sent-index table.
        }
        $this->cleanupElementIndexOrphans();
        return $deletedOutbox;
    }

    public function syncElementIndex(): void
    {
        $rows = Craft::$app->getDb()->createCommand(
            'SELECT id FROM {{%burrow_outbox}} ORDER BY created_at DESC LIMIT 5000'
        )->queryAll();
        if (!is_array($rows)) {
            return;
        }
        foreach ($rows as $row) {
            if (!is_array($row)) {
                continue;
            }
            $id = trim((string)($row['id'] ?? ''));
            if ($id === '') {
                continue;
            }
            $this->syncElementIndexRecordByOutboxId($id);
        }
        $this->cleanupElementIndexOrphans();
    }

    /**
     * @param array<string,mixed> $payload
     */
    private function upsertDeliveryRecord(
        string $eventKey,
        array $payload,
        string $channel,
        string $eventName,
        string $status,
        ?string $lastError,
        ?string $sentAt,
        bool $scheduleAutoRetry = true
    ): bool {
        $eventKey = trim($eventKey);
        if ($eventKey === '') {
            return false;
        }

        $db = Craft::$app->getDb();
        $transaction = $db->beginTransaction();
        try {
            $row = (new Query())
                ->from('{{%burrow_outbox}}')
                ->where(['event_key' => $eventKey])
                ->one($db);
            $now = gmdate('Y-m-d H:i:s');
            $maxAttempts = self::DEFAULT_MAX_ATTEMPTS;

            if ($row === false || $row === null) {
                $id = bin2hex(random_bytes(16));
                $attemptCount = 1;
                $storedMax = $maxAttempts;
                if ($status === 'sent') {
                    $newStatus = 'sent';
                } else {
                    $canAuto = $scheduleAutoRetry && Plugin::getInstance()->canDispatchToBurrow();
                    $newStatus = ($canAuto && $attemptCount < $storedMax) ? 'retrying' : 'failed';
                }
                $db->createCommand()->insert('{{%burrow_outbox}}', [
                    'id' => $id,
                    'event_key' => $eventKey,
                    'channel' => $channel ?: null,
                    'event_name' => $eventName ?: null,
                    'status' => $newStatus,
                    'attempt_count' => $attemptCount,
                    'max_attempts' => $storedMax,
                    'payload' => $payload,
                    'last_error' => $lastError,
                    'next_attempt_at' => null,
                    'sent_at' => $sentAt,
                    'created_at' => $now,
                    'updated_at' => $now,
                ])->execute();
                $transaction->commit();
                $this->syncElementIndexRecordByOutboxId($id);
                if ($status !== 'sent' && $newStatus === 'retrying') {
                    $this->pushDelayedOutboxDelivery($id, $attemptCount, $scheduleAutoRetry);
                }
                return true;
            }

            $id = trim((string)($row['id'] ?? ''));
            $prevAttempts = (int)($row['attempt_count'] ?? 0);
            $attemptCount = $prevAttempts + 1;
            $storedMax = max((int)($row['max_attempts'] ?? 1), $maxAttempts);
            $queueDelayedRetryAfterCommit = false;

            if ($status === 'sent') {
                $newStatus = 'sent';
                // Do not increment on success: attempt_count reflects tries that occurred before delivery succeeded.
                $successAttemptCount = max(1, $prevAttempts);
                $db->createCommand()->update(
                    '{{%burrow_outbox}}',
                    [
                        'channel' => $channel ?: null,
                        'event_name' => $eventName ?: null,
                        'status' => $newStatus,
                        'attempt_count' => $successAttemptCount,
                        'max_attempts' => $storedMax,
                        'payload' => $payload,
                        'last_error' => null,
                        'next_attempt_at' => null,
                        'sent_at' => $sentAt,
                        'updated_at' => $now,
                    ],
                    ['id' => $id]
                )->execute();
            } else {
                $canAuto = $scheduleAutoRetry && Plugin::getInstance()->canDispatchToBurrow();
                if (!$scheduleAutoRetry) {
                    $newStatus = 'failed';
                } elseif ($attemptCount >= $storedMax) {
                    $newStatus = 'failed';
                } else {
                    $newStatus = $canAuto ? 'retrying' : 'failed';
                }
                $db->createCommand()->update(
                    '{{%burrow_outbox}}',
                    [
                        'channel' => $channel ?: null,
                        'event_name' => $eventName ?: null,
                        'status' => $newStatus,
                        'attempt_count' => $attemptCount,
                        'max_attempts' => $storedMax,
                        'payload' => $payload,
                        'last_error' => $lastError,
                        'next_attempt_at' => null,
                        'sent_at' => null,
                        'updated_at' => $now,
                    ],
                    ['id' => $id]
                )->execute();
                if ($newStatus === 'retrying') {
                    $queueDelayedRetryAfterCommit = true;
                }
            }

            $transaction->commit();
            if ($id !== '') {
                $this->syncElementIndexRecordByOutboxId($id);
            }
            if ($queueDelayedRetryAfterCommit && $id !== '') {
                $this->pushDelayedOutboxDelivery($id, $attemptCount, $scheduleAutoRetry);
            }
            return true;
        } catch (\Throwable) {
            $transaction->rollBack();
            return false;
        }
    }

    private function pushDelayedOutboxDelivery(string $outboxId, int $attemptCountAfterFailure, bool $scheduleAutoRetry): void
    {
        if (!$scheduleAutoRetry || !Plugin::getInstance()->canDispatchToBurrow()) {
            return;
        }
        $delay = min(600, 30 * max(1, $attemptCountAfterFailure));
        QueueHelper::push(new DeliverOutboxRowJob(['outboxId' => $outboxId]), null, $delay, 120);
    }

    private function syncElementIndexRecordByEventKey(string $eventKey): void
    {
        $eventKey = trim($eventKey);
        if ($eventKey === '') {
            return;
        }
        $row = Craft::$app->getDb()->createCommand(
            'SELECT id FROM {{%burrow_outbox}} WHERE event_key = :eventKey LIMIT 1',
            [':eventKey' => $eventKey]
        )->queryOne();
        if (!is_array($row)) {
            return;
        }
        $id = trim((string)($row['id'] ?? ''));
        if ($id === '') {
            return;
        }
        $this->syncElementIndexRecordByOutboxId($id);
    }

    private function syncElementIndexRecordByOutboxId(string $outboxId): void
    {
        if (!$this->outboxElementTableExists()) {
            return;
        }
        $row = $this->fetchOutboxRow($outboxId);
        if ($row === null) {
            $this->removeElementIndexRecordByOutboxId($outboxId);
            return;
        }

        /** @var OutboxElement|null $element */
        $element = OutboxElement::find()->status(null)->outboxId($outboxId)->one();
        if ($element === null) {
            $element = new OutboxElement();
        }

        $element->outboxId = trim((string)($row['id'] ?? ''));
        $element->eventKey = trim((string)($row['event_key'] ?? ''));
        $element->channel = trim((string)($row['channel'] ?? ''));
        $element->eventName = trim((string)($row['event_name'] ?? ''));
        $element->outboxStatus = trim((string)($row['status'] ?? 'pending')) ?: 'pending';
        $element->attemptCount = (int)($row['attempt_count'] ?? 0);
        $element->maxAttempts = (int)($row['max_attempts'] ?? 1);
        $element->lastError = isset($row['last_error']) ? (string)$row['last_error'] : null;
        $element->nextAttemptAt = isset($row['next_attempt_at']) ? (string)$row['next_attempt_at'] : null;
        $element->sentAt = isset($row['sent_at']) ? (string)$row['sent_at'] : null;
        $element->outboxCreatedAt = isset($row['created_at']) ? (string)$row['created_at'] : gmdate('Y-m-d H:i:s');
        $element->outboxUpdatedAt = isset($row['updated_at']) ? (string)$row['updated_at'] : gmdate('Y-m-d H:i:s');

        $updateSearchIndex = !$this->deferOutboxElementSearchIndex;
        try {
            Craft::$app->getElements()->saveElement($element, false, false, $updateSearchIndex);
            if ($this->deferOutboxElementSearchIndex && (int)$element->id > 0) {
                $this->deferredOutboxElementIds[(int)$element->id] = true;
            }
        } catch (\Throwable) {
            // Best-effort sync; outbox delivery should never fail due to index sync.
        }
    }

    private function removeElementIndexRecordByOutboxId(string $outboxId): void
    {
        if (!$this->outboxElementTableExists()) {
            return;
        }
        try {
            $id = Craft::$app->getDb()->createCommand(
                'SELECT id FROM {{%burrow_outbox_elements}} WHERE outboxId = :outboxId LIMIT 1',
                [':outboxId' => $outboxId]
            )->queryScalar();
            if ($id !== false && $id !== null) {
                Craft::$app->getDb()->createCommand()->delete('{{%burrow_outbox_elements}}', ['outboxId' => $outboxId])->execute();
                Craft::$app->getDb()->createCommand()->delete('{{%elements}}', ['id' => (int)$id])->execute();
            }
        } catch (\Throwable) {
            // Best-effort cleanup.
        }
    }

    private function cleanupElementIndexOrphans(): void
    {
        if (!$this->outboxElementTableExists()) {
            return;
        }
        try {
            $rows = Craft::$app->getDb()->createCommand(
                'SELECT e.id, e.outboxId
                 FROM {{%burrow_outbox_elements}} e
                 LEFT JOIN {{%burrow_outbox}} o ON o.id = e.outboxId
                 WHERE o.id IS NULL'
            )->queryAll();
            if (!is_array($rows)) {
                return;
            }
            foreach ($rows as $row) {
                if (!is_array($row)) {
                    continue;
                }
                $elementId = (int)($row['id'] ?? 0);
                $outboxId = trim((string)($row['outboxId'] ?? ''));
                if ($outboxId !== '') {
                    Craft::$app->getDb()->createCommand()->delete('{{%burrow_outbox_elements}}', ['outboxId' => $outboxId])->execute();
                }
                if ($elementId > 0) {
                    Craft::$app->getDb()->createCommand()->delete('{{%elements}}', ['id' => $elementId])->execute();
                }
            }
        } catch (\Throwable) {
            // Best-effort cleanup.
        }
    }

    private function outboxElementTableExists(): bool
    {
        return Craft::$app->getDb()->getSchema()->getTableSchema('{{%burrow_outbox_elements}}', true) !== null;
    }

    /**
     * @return array<string,mixed>|null
     */
    private function fetchOutboxRow(string $outboxId): ?array
    {
        $row = Craft::$app->getDb()->createCommand(
            'SELECT id, event_key, channel, event_name, status, attempt_count, max_attempts, last_error, next_attempt_at, sent_at, created_at, updated_at
             FROM {{%burrow_outbox}}
             WHERE id = :id
             LIMIT 1',
            [':id' => $outboxId]
        )->queryOne();
        return is_array($row) ? $row : null;
    }
}
