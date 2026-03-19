<?php
namespace burrow\Burrow\elements\db;

use craft\elements\db\ElementQuery;

class OutboxElementQuery extends ElementQuery
{
    public ?string $outboxId = null;
    public ?string $outboxStatus = null;

    public function outboxId(?string $value): static
    {
        $this->outboxId = $value;
        return $this;
    }

    public function outboxStatus(?string $value): static
    {
        $this->outboxStatus = $value;
        return $this;
    }

    protected function beforePrepare(): bool
    {
        $this->joinElementTable('{{%burrow_outbox_elements}}');

        $this->query->select([
            'burrow_outbox_elements.outboxId',
            'burrow_outbox_elements.eventKey',
            'burrow_outbox_elements.channel',
            'burrow_outbox_elements.eventName',
            'burrow_outbox_elements.outboxStatus',
            'burrow_outbox_elements.attemptCount',
            'burrow_outbox_elements.maxAttempts',
            'burrow_outbox_elements.lastError',
            'burrow_outbox_elements.nextAttemptAt',
            'burrow_outbox_elements.sentAt',
            'burrow_outbox_elements.outboxCreatedAt',
            'burrow_outbox_elements.outboxUpdatedAt',
        ]);

        if ($this->outboxId !== null && $this->outboxId !== '') {
            $this->subQuery->andWhere(['burrow_outbox_elements.outboxId' => $this->outboxId]);
        }

        if ($this->outboxStatus !== null && $this->outboxStatus !== '') {
            $this->subQuery->andWhere(['burrow_outbox_elements.outboxStatus' => $this->outboxStatus]);
        }

        $this->subQuery->andWhere($this->statusCondition('burrow_outbox_elements.outboxStatus'));

        return parent::beforePrepare();
    }
}
