<?php
namespace burrow\Burrow\elements\actions;

use burrow\Burrow\elements\OutboxElement;
use burrow\Burrow\Plugin;
use craft\base\ElementAction;
use craft\elements\db\ElementQueryInterface;

class RetryOutboxAction extends ElementAction
{
    public function getTriggerLabel(): string
    {
        return 'Retry selected';
    }

    public function performAction(ElementQueryInterface $query): bool
    {
        $count = 0;
        foreach ($query->all() as $element) {
            if (!$element instanceof OutboxElement) {
                continue;
            }
            if ($element->outboxId === '') {
                continue;
            }
            if (Plugin::getInstance()->getQueue()->retryNow($element->outboxId)) {
                $count++;
            }
        }

        $this->setMessage($count > 0
            ? 'Queued ' . $count . ' record' . ($count === 1 ? '' : 's') . ' for retry.'
            : 'No records were queued for retry.');

        return true;
    }
}
