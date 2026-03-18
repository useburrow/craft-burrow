<?php
namespace burrow\Burrow\records;

use craft\db\ActiveRecord;

class RuntimeStateRecord extends ActiveRecord
{
    public static function tableName(): string
    {
        return '{{%burrow_runtime_state}}';
    }
}
