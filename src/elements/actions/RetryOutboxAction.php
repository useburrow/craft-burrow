<?php
namespace burrow\Burrow\elements\actions;

use burrow\Burrow\elements\OutboxElement;
use burrow\Burrow\Plugin;
use Craft;
use craft\base\ElementAction;
use craft\base\ElementInterface;
use craft\elements\db\ElementQueryInterface;

class RetryOutboxAction extends ElementAction
{
    public function getTriggerLabel(): string
    {
        return Craft::t('burrow', 'Retry selected');
    }

    public function isAvailable(?string $source): bool
    {
        return parent::isAvailable($source) && Plugin::getInstance()->canDispatchToBurrow();
    }

    public function isAvailableForElement(ElementInterface $element): bool
    {
        if (!$element instanceof OutboxElement || $element->outboxId === '') {
            return false;
        }

        return in_array($element->outboxStatus, ['failed', 'retrying', 'pending'], true);
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

        $message = $count === 1
            ? Craft::t('burrow', 'Queued 1 record for retry.')
            : Craft::t('burrow', 'Queued {count} records for retry.', ['count' => $count]);
        $this->setMessage($count > 0 ? $message : Craft::t('burrow', 'No records were queued for retry.'));

        return true;
    }
}
