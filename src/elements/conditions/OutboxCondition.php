<?php

namespace burrow\Burrow\elements\conditions;

use craft\elements\conditions\ElementCondition;

class OutboxCondition extends ElementCondition
{
    protected function selectableConditionRules(): array
    {
        return array_merge(parent::selectableConditionRules(), [
            ChannelConditionRule::class,
            EventNameConditionRule::class,
        ]);
    }
}
