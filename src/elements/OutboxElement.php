<?php
namespace burrow\Burrow\elements;

use burrow\Burrow\elements\actions\RetryOutboxAction;
use burrow\Burrow\elements\conditions\OutboxCondition;
use burrow\Burrow\elements\db\OutboxElementQuery;
use burrow\Burrow\Plugin;
use burrow\Burrow\services\QueueService;
use Craft;
use craft\base\Element;
use craft\elements\actions\Delete;
use craft\elements\conditions\ElementConditionInterface;
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

    public static function createCondition(): ElementConditionInterface
    {
        return \Craft::createObject(OutboxCondition::class, [static::class]);
    }

    protected static function defineSources(string $context = 'index'): array
    {
        return [
            [
                'key' => '*',
                'label' => 'All',
                // Explicitly clear Craft's built-in element status filter.
                // Outbox uses `outboxStatus` (pending/retrying/failed/sent), and "All" must include all of them.
                'criteria' => ['status' => null, 'outboxStatus' => null],
            ],
            [
                'key' => 'pending',
                'label' => 'Pending',
                'criteria' => ['status' => null, 'outboxStatus' => 'pending'],
            ],
            [
                'key' => 'retrying',
                'label' => 'Retrying',
                'criteria' => ['status' => null, 'outboxStatus' => 'retrying'],
            ],
            [
                'key' => 'failed',
                'label' => 'Failed',
                'criteria' => ['status' => null, 'outboxStatus' => 'failed'],
            ],
            [
                'key' => 'sent',
                'label' => 'Sent',
                'criteria' => ['status' => null, 'outboxStatus' => 'sent'],
            ],
        ];
    }

    protected static function defineTableAttributes(): array
    {
        return [
            'eventKey' => ['label' => 'Event Key'],
            'channel' => ['label' => 'Channel'],
            'eventName' => ['label' => 'Event'],
            'outboxStatus' => ['label' => 'Status'],
            'attemptCount' => ['label' => 'Attempts'],
            'lastError' => ['label' => 'Last Error'],
            'outboxCreatedAt' => ['label' => 'Created'],
            'outboxUpdatedAt' => ['label' => 'Updated'],
        ];
    }

    protected static function defineDefaultTableAttributes(string $source): array
    {
        return ['outboxStatus', 'attemptCount', 'lastError', 'outboxCreatedAt'];
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
            'pending' => ['label' => 'Pending', 'color' => 'blue'],
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
        return UrlHelper::cpUrl("burrow/outbox/{$this->id}");
    }

    public function getFieldLayout(): ?\craft\models\FieldLayout
    {
        return null;
    }

    public function canView(\craft\elements\User $user): bool
    {
        return $user->can('accessPlugin-burrow');
    }

    public function canSave(\craft\elements\User $user): bool
    {
        return false;
    }

    public function canDelete(\craft\elements\User $user): bool
    {
        return $user->can('accessPlugin-burrow');
    }

    public function prepareEditScreen(\yii\web\Response $response, string $containerId): void
    {
        /** @var \craft\web\CpScreenResponseBehavior $response */
        $response->title(Craft::t('burrow', 'Event {id}', ['id' => $this->outboxId ?: $this->id]));
        $response->crumbs([
            [
                'label' => Craft::t('burrow', 'Burrow'),
                'url' => UrlHelper::cpUrl('burrow'),
            ],
            [
                'label' => Craft::t('burrow', 'Outbox'),
                'url' => UrlHelper::cpUrl('burrow/outbox'),
            ],
        ]);
    }

    public function getPostEditUrl(): ?string
    {
        return UrlHelper::cpUrl('burrow/outbox');
    }

    public function getUiLabel(): string
    {
        $channel = trim($this->channel);
        $event = trim($this->eventName);
        if ($channel !== '' || $event !== '') {
            return ($channel ?: '-') . ' / ' . ($event ?: '-');
        }
        $key = trim($this->eventKey);
        if ($key !== '') {
            return '...' . substr($key, -12);
        }
        return 'Outbox #' . ($this->outboxId ?: $this->id);
    }

    protected function metadata(): array
    {
        $meta = [
            Craft::t('burrow', 'Channel') => $this->channel ?: '-',
            Craft::t('burrow', 'Event') => $this->eventName ?: '-',
            Craft::t('burrow', 'Status') => ucfirst($this->outboxStatus ?: 'pending'),
            Craft::t('burrow', 'Attempts') => $this->attemptCount . ' / ' . $this->maxAttempts,
        ];
        if ($this->lastError) {
            $meta[Craft::t('burrow', 'Last Error')] = $this->lastError;
        }
        if ($this->sentAt) {
            $meta[Craft::t('burrow', 'Sent At')] = $this->sentAt;
        }
        $meta[Craft::t('burrow', 'Created')] = $this->outboxCreatedAt ?: '-';
        $meta[Craft::t('burrow', 'Updated')] = $this->outboxUpdatedAt ?: '-';
        $meta[Craft::t('burrow', 'Event Key')] = Html::tag('code', Html::encode($this->eventKey), ['style' => 'word-break:break-all; font-size:11px;']);
        return $meta;
    }

    protected function metaFieldsHtml(bool $static): string
    {
        $html = parent::metaFieldsHtml($static);

        if (!$static && $this->outboxId !== '' && $this->id) {
            $status = $this->outboxStatus ?: 'pending';
            $canRetryState = in_array($status, ['failed', 'retrying', 'pending'], true);
            if ($canRetryState) {
                $html .= Html::beginTag('div', [
                    'class' => 'meta',
                    'style' => 'padding-top:14px;border-top:1px solid var(--hairline-color, #e5e7eb);',
                ]);
                $html .= Html::tag('h3', Craft::t('burrow', 'Delivery'), ['class' => 'heading', 'style' => 'margin-bottom:8px;']);
                $html .= Html::tag('p', Craft::t('burrow', 'Automatic retries use up to {n} attempts (same default as the WordPress plugin).', ['n' => (string)QueueService::DEFAULT_MAX_ATTEMPTS]), [
                    'style' => 'font-size:12px;opacity:0.85;margin:0 0 10px;',
                ]);
                if (Plugin::getInstance()->canDispatchToBurrow()) {
                    $html .= Html::beginForm(UrlHelper::actionUrl('burrow/settings/retry-outbox'), 'post', ['style' => 'margin:0;']);
                    $html .= Html::hiddenInput(Craft::$app->getConfig()->getGeneral()->csrfTokenName, Craft::$app->getRequest()->getCsrfToken());
                    $html .= Html::hiddenInput('id', $this->outboxId);
                    $html .= Html::hiddenInput('return', 'burrow/outbox/' . (int)$this->id);
                    $html .= Html::submitButton(Craft::t('burrow', 'Retry now'), ['class' => 'btn submit']);
                    $html .= Html::endForm();
                } else {
                    $html .= Html::tag('p', Craft::t('burrow', 'Configure the Burrow connection and ingestion key in Settings to retry delivery.'), [
                        'class' => 'warning',
                        'style' => 'font-size:12px;margin:0;',
                    ]);
                }
                $html .= Html::endTag('div');
            }
        }

        $payload = $this->loadPayloadJson();
        if ($payload !== null) {
            $json = json_encode($payload, JSON_PRETTY_PRINT | JSON_UNESCAPED_SLASHES | JSON_UNESCAPED_UNICODE);
            $html .= Html::tag('div',
                Html::tag('h3', Craft::t('burrow', 'Payload'), ['class' => 'heading', 'style' => 'margin-bottom:8px;'])
                . Html::tag('pre', Html::tag('code', Html::encode($json)), [
                    'style' => 'background:#1e1e2e; color:#cdd6f4; padding:14px; border-radius:6px; font-size:11px; line-height:1.5; overflow:auto; max-height:420px; white-space:pre-wrap; word-break:break-all;',
                ]),
                ['style' => 'padding:14px 0;']
            );
        }

        return $html;
    }

    private function statusBadgeHtml(): string
    {
        $status = $this->outboxStatus ?: 'pending';
        [$dotColor, $bgColor, $textColor] = match ($status) {
            'sent' => ['#16a34a', '#dcfce7', '#166534'],
            'pending' => ['#2563eb', '#dbeafe', '#1e40af'],
            'retrying' => ['#ca8a04', '#fef9c3', '#854d0e'],
            'failed' => ['#dc2626', '#fee2e2', '#991b1b'],
            default => ['#6b7280', '#f3f4f6', '#374151'],
        };
        $dot = Html::tag('span', '', [
            'style' => "display:inline-block; width:8px; height:8px; border-radius:50%; background:{$dotColor}; margin-right:6px; flex-shrink:0;",
        ]);
        return Html::tag('span', $dot . Html::encode(strtoupper($status)), [
            'style' => "display:inline-flex; align-items:center; padding:2px 10px; border-radius:10px; font-size:11px; font-weight:600; letter-spacing:0.03em; background:{$bgColor}; color:{$textColor};",
        ]);
    }

    private function loadPayloadJson(): mixed
    {
        if ($this->outboxId === '') {
            return null;
        }
        try {
            $row = Craft::$app->getDb()->createCommand(
                'SELECT payload FROM {{%burrow_outbox}} WHERE id = :id LIMIT 1',
                [':id' => $this->outboxId]
            )->queryOne();
            if (!is_array($row)) {
                return null;
            }
            $raw = $row['payload'] ?? null;
            if (is_string($raw)) {
                $decoded = json_decode($raw, true);
                return is_array($decoded) ? $decoded : $raw;
            }
            return $raw;
        } catch (\Throwable) {
            return null;
        }
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
            'eventKey' => Html::tag('code', Html::encode($this->eventKey !== '' ? ('...' . substr($this->eventKey, -12)) : '-'), ['style' => 'font-size:11px;']),
            'attemptCount' => (string)$this->attemptCount . ' / ' . (string)$this->maxAttempts,
            'outboxStatus' => $this->statusBadgeHtml(),
            'channel' => Html::encode($this->channel ?: '-'),
            'eventName' => Html::encode($this->eventName ?: '-'),
            'lastError' => Html::encode($this->lastError !== null && $this->lastError !== '' ? (mb_strlen($this->lastError) > 60 ? mb_substr($this->lastError, 0, 60) . '...' : $this->lastError) : '-'),
            'outboxCreatedAt' => Html::encode($this->outboxCreatedAt ?: '-'),
            'outboxUpdatedAt' => Html::encode($this->outboxUpdatedAt ?: '-'),
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
