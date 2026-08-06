<?php
namespace burrow\Burrow\jobs;

use Craft;
use craft\queue\BaseJob;

/**
 * Republishes a single outbox payload to Burrow (manual retry or automatic backoff).
 *
 * Resolves credentials from the row's persisted project_id/site_id so delivery never
 * depends on a mutable "current" global settings view alone.
 *
 * @author Burrow Analytics, LLC
 * @since 5.0.0
 */
class DeliverOutboxRowJob extends BaseJob
{
    // =========================================================================
    // Properties
    // =========================================================================

    /**
     * @var string
     */
    public string $outboxId = '';

    // =========================================================================
    // Public Methods
    // =========================================================================

    /**
     * @inheritdoc
     */
    protected function defaultDescription(): ?string
    {
        return Craft::t('burrow', 'Deliver Burrow outbox event');
    }

    /**
     * @inheritdoc
     */
    public function execute($queue): void
    {
        $outboxId = trim($this->outboxId);
        if ($outboxId === '') {
            return;
        }

        $plugin = \burrow\Burrow\Plugin::getInstance();
        $row = Craft::$app->getDb()->createCommand(
            'SELECT id, event_key, channel, event_name, status, attempt_count, max_attempts, payload, project_id, site_id
             FROM {{%burrow_outbox}}
             WHERE id = :id
             LIMIT 1',
            [':id' => $outboxId]
        )->queryOne();

        if (!is_array($row)) {
            return;
        }

        $status = trim((string)($row['status'] ?? ''));
        if (!in_array($status, ['pending', 'retrying'], true)) {
            return;
        }

        $payload = $row['payload'] ?? null;
        if (is_string($payload)) {
            $decoded = json_decode($payload, true);
            $payload = is_array($decoded) ? $decoded : null;
        }
        if (!is_array($payload) || empty($payload['event']) || empty($payload['channel'])) {
            return;
        }

        $eventKey = trim((string)($row['event_key'] ?? ''));
        if ($eventKey === '') {
            return;
        }

        $projectId = trim((string)($row['project_id'] ?? ''));
        if ($projectId === '') {
            $projectId = trim((string)($payload['projectId'] ?? ''));
        }
        $siteId = isset($row['site_id']) && $row['site_id'] !== null && $row['site_id'] !== ''
            ? (int)$row['site_id']
            : null;

        $runtimeState = $plugin->getState()->resolveRuntimeStateForDelivery(
            $projectId !== '' ? $projectId : null,
            $siteId
        );
        if (!$plugin->canDispatchToBurrow($runtimeState)) {
            $plugin->getQueue()->markFailed(
                $eventKey,
                $payload,
                'Missing Burrow connection or credentials.',
                trim((string)($row['channel'] ?? '')),
                trim((string)($row['event_name'] ?? '')),
                false
            );
            return;
        }

        $result = $plugin->getBurrowApi()->publishEvents(
            $plugin->getBurrowBaseUrl(),
            $plugin->getBurrowApiKey(),
            $runtimeState,
            [$payload]
        );

        $channel = trim((string)($row['channel'] ?? ''));
        $eventName = trim((string)($row['event_name'] ?? ''));

        if ($result['ok']) {
            $plugin->getQueue()->markSent($eventKey, $payload, $channel, $eventName);
            return;
        }

        $error = trim((string)($result['error'] ?? 'Outbox publish failed.'));
        $plugin->getQueue()->markFailed($eventKey, $payload, $error, $channel, $eventName);
    }
}
