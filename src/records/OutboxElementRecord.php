<?php
namespace burrow\Burrow\records;

use craft\db\ActiveRecord;

class OutboxElementRecord extends ActiveRecord
{
    public static function tableName(): string
    {
        return '{{%burrow_outbox_elements}}';
    }
}
