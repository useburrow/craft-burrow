<?php

namespace burrow\Burrow\elements\conditions;

use Craft;
use craft\base\conditions\BaseTextConditionRule;
use craft\base\ElementInterface;
use craft\elements\conditions\ElementConditionRuleInterface;
use craft\elements\db\ElementQueryInterface;

class EventNameConditionRule extends BaseTextConditionRule implements ElementConditionRuleInterface
{
    public function getLabel(): string
    {
        return Craft::t('burrow', 'Event Name');
    }

    public function getExclusiveQueryParams(): array
    {
        return ['eventName'];
    }

    public function modifyQuery(ElementQueryInterface $query): void
    {
        $query->eventName($this->paramValue());
    }

    public function matchElement(ElementInterface $element): bool
    {
        return $this->matchValue($element->eventName ?? '');
    }
}
