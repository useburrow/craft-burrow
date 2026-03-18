<?php
namespace burrow\Burrow\records;

use craft\db\ActiveRecord;

class EventLogRecord extends ActiveRecord
{
    public static function tableName(): string
    {
        return '{{%burrow_event_logs}}';
    }
}
