<?php
namespace burrow\Burrow\elements;

use burrow\Burrow\elements\actions\RetryOutboxAction;
use burrow\Burrow\elements\db\OutboxElementQuery;
use Craft;
use craft\base\Element;
use craft\elements\actions\Delete;
use craft\helpers\Html;
use craft\helpers\UrlHelper;

class OutboxElement extends Element
{
    public string $outboxId = '';
    public string $eventKey = '';
    public string $channel = '';
    public string $eventName = '';
    public string $outboxStatus = 'pending';
    public int $attemptCount = 0;
    public int $maxAttempts = 1;
    public ?string $lastError = null;
    public ?string $nextAttemptAt = null;
    public ?string $sentAt = null;
    public ?string $outboxCreatedAt = null;
    public ?string $outboxUpdatedAt = null;

    public static function displayName(): string
    {
        return 'Outbox record';
    }

    public static function pluralDisplayName(): string
    {
        return 'Outbox records';
    }

    public static function lowerDisplayName(): string
    {
        return 'outbox record';
    }

    public static function refHandle(): ?string
    {
        return 'outbox-record';
    }

    public static function hasContent(): bool
    {
        return false;
    }

    public static function isLocalized(): bool
    {
        return false;
    }

    public static function hasTitles(): bool
    {
        return false;
    }

    public static function find(): OutboxElementQuery
    {
        return new OutboxElementQuery(static::class);
    }

    protected static function defineSources(string $context = 'index'): array
    {
        return [
            [
                'key' => '*',
                'label' => 'All',
                'criteria' => ['status' => null],
            ],
            [
                'key' => 'pending',
                'label' => 'Pending',
                'criteria' => ['status' => 'pending'],
            ],
            [
                'key' => 'retrying',
                'label' => 'Retrying',
                'criteria' => ['status' => 'retrying'],
            ],
            [
                'key' => 'failed',
                'label' => 'Failed',
                'criteria' => ['status' => 'failed'],
            ],
            [
                'key' => 'sent',
                'label' => 'Sent',
                'criteria' => ['status' => 'sent'],
            ],
        ];
    }

    protected static function defineTableAttributes(): array
    {
        return [
            'eventKey' => ['label' => 'Event Key'],
            'channelEvent' => ['label' => 'Channel/Event'],
            'status' => ['label' => 'Status'],
            'attempts' => ['label' => 'Attempts'],
            'lastError' => ['label' => 'Last Error'],
            'outboxCreatedAt' => ['label' => 'Created'],
            'outboxUpdatedAt' => ['label' => 'Updated'],
        ];
    }

    protected static function defineDefaultTableAttributes(string $source): array
    {
        return ['eventKey', 'channelEvent', 'status', 'attempts', 'lastError', 'outboxCreatedAt'];
    }

    protected static function defineSortOptions(): array
    {
        return [
            'outboxCreatedAt' => ['label' => 'Created', 'orderBy' => 'burrow_outbox_elements.outboxCreatedAt'],
            'outboxUpdatedAt' => ['label' => 'Updated', 'orderBy' => 'burrow_outbox_elements.outboxUpdatedAt'],
            'status' => ['label' => 'Status', 'orderBy' => 'burrow_outbox_elements.outboxStatus'],
            'eventKey' => ['label' => 'Event Key', 'orderBy' => 'burrow_outbox_elements.eventKey'],
            'attemptCount' => ['label' => 'Attempts', 'orderBy' => 'burrow_outbox_elements.attemptCount'],
        ];
    }

    protected static function defineActions(string $source): array
    {
        return [
            RetryOutboxAction::class,
            Delete::class,
        ];
    }

    public static function hasStatuses(): bool
    {
        return true;
    }

    public static function statuses(): array
    {
        return [
            'pending' => ['label' => 'Pending', 'color' => 'orange'],
            'retrying' => ['label' => 'Retrying', 'color' => 'yellow'],
            'failed' => ['label' => 'Failed', 'color' => 'red'],
            'sent' => ['label' => 'Sent', 'color' => 'green'],
        ];
    }

    public function getStatus(): ?string
    {
        $status = trim($this->outboxStatus);
        return $status !== '' ? $status : 'pending';
    }

    public function getUriFormat(): ?string
    {
        return null;
    }

    public function getCpEditUrl(): ?string
    {
        return UrlHelper::cpUrl('burrow/outbox');
    }

    public function getFieldLayout(): ?\craft\models\FieldLayout
    {
        return null;
    }

    public static function searchableAttributes(): array
    {
        return ['eventKey', 'channel', 'eventName', 'lastError', 'outboxStatus'];
    }

    public function getSearchKeywords(mixed $attribute): string
    {
        return match ($attribute) {
            'eventKey' => $this->eventKey,
            'channel' => $this->channel,
            'eventName' => $this->eventName,
            'lastError' => (string)$this->lastError,
            'outboxStatus' => $this->outboxStatus,
            default => parent::getSearchKeywords($attribute),
        };
    }

    protected function tableAttributeHtml(string $attribute): string
    {
        return match ($attribute) {
            'channelEvent' => Html::encode($this->channel ?: '-') . ' / ' . Html::encode($this->eventName ?: '-'),
            'attempts' => (string)$this->attemptCount . ' / ' . (string)$this->maxAttempts,
            default => parent::tableAttributeHtml($attribute),
        };
    }

    public function beforeDelete(): bool
    {
        if (!parent::beforeDelete()) {
            return false;
        }

        if ($this->outboxId !== '') {
            try {
                Craft::$app->getDb()->createCommand()->delete('{{%burrow_outbox}}', ['id' => $this->outboxId])->execute();
            } catch (\Throwable) {
                return false;
            }
        }

        return true;
    }

    public function afterSave(bool $isNew): void
    {
        $now = gmdate('Y-m-d H:i:s');
        Craft::$app->getDb()->createCommand()->upsert('{{%burrow_outbox_elements}}', [
            'id' => $this->id,
            'outboxId' => $this->outboxId,
            'eventKey' => $this->eventKey,
            'channel' => $this->channel !== '' ? $this->channel : null,
            'eventName' => $this->eventName !== '' ? $this->eventName : null,
            'outboxStatus' => $this->outboxStatus,
            'attemptCount' => $this->attemptCount,
            'maxAttempts' => $this->maxAttempts,
            'lastError' => $this->lastError,
            'nextAttemptAt' => $this->nextAttemptAt,
            'sentAt' => $this->sentAt,
            'outboxCreatedAt' => $this->outboxCreatedAt ?? $now,
            'outboxUpdatedAt' => $this->outboxUpdatedAt ?? $now,
        ], [
            'outboxId' => $this->outboxId,
            'eventKey' => $this->eventKey,
            'channel' => $this->channel !== '' ? $this->channel : null,
            'eventName' => $this->eventName !== '' ? $this->eventName : null,
            'outboxStatus' => $this->outboxStatus,
            'attemptCount' => $this->attemptCount,
            'maxAttempts' => $this->maxAttempts,
            'lastError' => $this->lastError,
            'nextAttemptAt' => $this->nextAttemptAt,
            'sentAt' => $this->sentAt,
            'outboxCreatedAt' => $this->outboxCreatedAt ?? $now,
            'outboxUpdatedAt' => $this->outboxUpdatedAt ?? $now,
        ])->execute();

        parent::afterSave($isNew);
    }
}
