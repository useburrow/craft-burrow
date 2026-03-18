<?php
namespace amici\Burrow\services;

use craft\base\Component;

use amici\Burrow\records\EventLogRecord;

class EventLogService extends Component
{
    /**
     * @param array<string,mixed> $context
     */
    public function log(string $type, string $message, string $provider = '', string $channel = '', ?string $eventKey = null, array $context = []): void
    {
        $record = new EventLogRecord();
        $record->type = $type;
        $record->provider = $provider;
        $record->channel = $channel;
        $record->eventKey = $eventKey;
        $record->message = $message;
        $record->context = $context;
        $record->save();
    }

    /**
     * @return array<int,array<string,mixed>>
     */
    public function latest(int $limit = 100): array
    {
        $rows = EventLogRecord::find()
            ->orderBy(['id' => SORT_DESC])
            ->limit(max(1, $limit))
            ->all();

        $result = [];
        foreach ($rows as $row) {
            $result[] = [
                'id' => (int)$row->id,
                'type' => (string)$row->type,
                'provider' => (string)$row->provider,
                'channel' => (string)$row->channel,
                'eventKey' => (string)$row->eventKey,
                'message' => (string)$row->message,
                'context' => is_array($row->context) ? $row->context : [],
                'dateCreated' => (string)$row->dateCreated,
            ];
        }

        return $result;
    }
}
