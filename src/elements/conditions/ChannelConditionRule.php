<?php

namespace burrow\Burrow\elements\conditions;

use Craft;
use craft\base\conditions\BaseTextConditionRule;
use craft\base\ElementInterface;
use craft\elements\conditions\ElementConditionRuleInterface;
use craft\elements\db\ElementQueryInterface;

class ChannelConditionRule extends BaseTextConditionRule implements ElementConditionRuleInterface
{
    public function getLabel(): string
    {
        return Craft::t('burrow', 'Channel');
    }

    public function getExclusiveQueryParams(): array
    {
        return ['channel'];
    }

    public function modifyQuery(ElementQueryInterface $query): void
    {
        $query->channel($this->paramValue());
    }

    public function matchElement(ElementInterface $element): bool
    {
        return $this->matchValue($element->channel ?? '');
    }
}
