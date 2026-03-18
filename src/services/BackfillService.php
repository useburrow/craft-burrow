<?php
namespace burrow\Burrow\services;

use Craft;
use craft\base\Component;
use craft\db\Query;

class BackfillService extends Component
{
    /**
     * @param array<string,mixed> $runtimeState
     * @param array<int,string> $sources
     * @return array{ok:bool,error:string,windowStart:string,windowEnd:string,sources:array<int,string>,requested:int,accepted:int,rejected:int,validationRejected:int,latestCursor:string,breakdown:array<string,int>}
     */
    public function runBackfill(array $runtimeState, string $windowPreset, array $sources): array
    {
        $sources = $this->normalizeSources($sources);
        if (empty($sources)) {
            return $this->errorResult('Choose at least one source for backfill.');
        }

        $windowStart = $this->windowStartForPreset($windowPreset);
        $windowEnd = gmdate('c');
        $events = [];
        $breakdown = [
            'forms' => 0,
            'ecommerce' => 0,
        ];

        if (in_array('forms', $sources, true)) {
            $formEvents = $this->buildFormsEvents($runtimeState, $windowStart);
            $events = [...$events, ...$formEvents];
            $breakdown['forms'] = count($formEvents);
        }
        if (in_array('ecommerce', $sources, true)) {
            $ecommerceEvents = $this->buildEcommerceEvents($runtimeState, $windowStart);
            $events = [...$events, ...$ecommerceEvents];
            $breakdown['ecommerce'] = count($ecommerceEvents);
        }

        if (empty($events)) {
            return $this->errorResult('No historical events found for the selected window and sources.');
        }

        $plugin = \burrow\Burrow\Plugin::getInstance();
        $settings = $plugin->getSettings();
        $sdkResult = $plugin->getBurrowApi()->submitBackfillEvents(
            $settings->baseUrl,
            $settings->apiKey,
            $runtimeState,
            $events,
            $windowStart,
            $windowEnd
        );
        if (!$sdkResult['ok']) {
            return [
                'ok' => false,
                'error' => (string)$sdkResult['error'],
                'windowStart' => $windowStart,
                'windowEnd' => $windowEnd,
                'sources' => $sources,
                'requested' => count($events),
                'accepted' => 0,
                'rejected' => 0,
                'validationRejected' => 0,
                'latestCursor' => '',
                'breakdown' => $breakdown,
            ];
        }

        return [
            'ok' => true,
            'error' => '',
            'windowStart' => $windowStart,
            'windowEnd' => $windowEnd,
            'sources' => $sources,
            'requested' => (int)($sdkResult['requestedCount'] ?? count($events)),
            'accepted' => (int)($sdkResult['acceptedCount'] ?? 0),
            'rejected' => (int)($sdkResult['rejectedCount'] ?? 0),
            'validationRejected' => (int)($sdkResult['validationRejectedCount'] ?? 0),
            'latestCursor' => trim((string)($sdkResult['latestCursor'] ?? '')),
            'breakdown' => $breakdown,
        ];
    }

    /**
     * @return array<string,string>
     */
    public function presetOptions(): array
    {
        return [
            'last_7_days' => 'Last 7 days',
            'last_30_days' => 'Last 30 days',
            'last_90_days' => 'Last 90 days',
            'last_365_days' => 'Past year',
            'last_730_days' => 'Two years',
            'all_time' => 'All time',
        ];
    }

    /**
     * @param array<string,mixed> $runtimeState
     * @return array<int,string>
     */
    public function availableSources(array $runtimeState): array
    {
        $selected = array_values(array_filter(array_map('strval', (array)($runtimeState['selectedIntegrations'] ?? []))));
        $available = [];
        if (in_array('freeform', $selected, true) || in_array('formie', $selected, true)) {
            $available[] = 'forms';
        }
        if (in_array('commerce', $selected, true)) {
            $available[] = 'ecommerce';
        }

        return $available;
    }

    private function windowStartForPreset(string $preset): string
    {
        $preset = trim($preset);
        return match ($preset) {
            'last_7_days' => gmdate('c', strtotime('-7 days')),
            'last_90_days' => gmdate('c', strtotime('-90 days')),
            'last_365_days' => gmdate('c', strtotime('-365 days')),
            'last_730_days' => gmdate('c', strtotime('-730 days')),
            'all_time' => '1970-01-01T00:00:00Z',
            default => gmdate('c', strtotime('-30 days')),
        };
    }

    /**
     * @param array<int,string> $sources
     * @return array<int,string>
     */
    private function normalizeSources(array $sources): array
    {
        $normalized = array_values(array_filter(array_map('strval', $sources)));
        $normalized = array_values(array_intersect($normalized, ['forms', 'ecommerce']));
        return array_values(array_unique($normalized));
    }

    /**
     * @param array<string,mixed> $runtimeState
     * @return array<int,array<string,mixed>>
     */
    private function buildFormsEvents(array $runtimeState, string $windowStart): array
    {
        $formsSourceId = trim((string)($runtimeState['sourceIds']['forms'] ?? $runtimeState['projectSourceId'] ?? ''));
        $projectId = trim((string)($runtimeState['projectId'] ?? ''));
        if ($projectId === '' || $formsSourceId === '') {
            return [];
        }

        $events = [];
        $events = [...$events, ...$this->buildFreeformSubmissionEvents($runtimeState, $windowStart, $projectId, $formsSourceId)];
        $events = [...$events, ...$this->buildFormieSubmissionEvents($runtimeState, $windowStart, $projectId, $formsSourceId)];

        return $events;
    }

    /**
     * @param array<string,mixed> $runtimeState
     * @return array<int,array<string,mixed>>
     */
    private function buildFreeformSubmissionEvents(array $runtimeState, string $windowStart, string $projectId, string $projectSourceId): array
    {
        $config = is_array($runtimeState['integrationSettings']['freeform']['forms'] ?? null)
            ? $runtimeState['integrationSettings']['freeform']['forms']
            : [];
        if (empty($config) || !$this->tableExists('{{%freeform_submissions}}')) {
            return [];
        }

        $enabledFormIds = [];
        $formNames = [];
        foreach ($config as $formId => $formConfig) {
            if (!is_array($formConfig)) {
                continue;
            }
            $mode = trim((string)($formConfig['mode'] ?? 'off'));
            if (!in_array($mode, ['count_only', 'custom_fields'], true)) {
                continue;
            }
            $stringFormId = trim((string)$formId);
            if ($stringFormId === '') {
                continue;
            }
            $enabledFormIds[] = (int)$stringFormId;
            $formNames[(int)$stringFormId] = trim((string)($formConfig['formName'] ?? ('Form ' . $stringFormId)));
        }
        if (empty($enabledFormIds)) {
            return [];
        }

        $dateColumn = $this->pickExistingColumn('{{%freeform_submissions}}', ['dateCreated', 'dateUpdated']);
        if ($dateColumn === '') {
            return [];
        }

        $rows = (new Query())
            ->select(['id', 'formId', $dateColumn])
            ->from('{{%freeform_submissions}}')
            ->where(['formId' => $enabledFormIds])
            ->andWhere(['>=', $dateColumn, $windowStart])
            ->orderBy([$dateColumn => SORT_ASC])
            ->limit(5000)
            ->all();

        $events = [];
        foreach ($rows as $row) {
            if (!is_array($row)) {
                continue;
            }
            $formId = (int)($row['formId'] ?? 0);
            $timestamp = $this->normalizeTimestamp((string)($row[$dateColumn] ?? ''));
            if ($formId <= 0 || $timestamp === '') {
                continue;
            }
            $submissionId = trim((string)($row['id'] ?? ''));
            $events[] = [
                'projectId' => $projectId,
                'projectSourceId' => $projectSourceId,
                'channel' => 'forms',
                'event' => 'forms.submission.received',
                'timestamp' => $timestamp,
                'source' => 'craft-freeform',
                'tags' => [
                    'provider' => 'freeform',
                    'formId' => (string)$formId,
                ],
                'properties' => [
                    'provider' => 'freeform',
                    'formId' => (string)$formId,
                    'formName' => (string)($formNames[$formId] ?? ('Form ' . $formId)),
                    'submissionId' => $submissionId,
                    'isBackfill' => true,
                ],
            ];
        }

        return $events;
    }

    /**
     * @param array<string,mixed> $runtimeState
     * @return array<int,array<string,mixed>>
     */
    private function buildFormieSubmissionEvents(array $runtimeState, string $windowStart, string $projectId, string $projectSourceId): array
    {
        $config = is_array($runtimeState['integrationSettings']['formie'] ?? null)
            ? $runtimeState['integrationSettings']['formie']
            : [];
        $mode = trim((string)($config['mode'] ?? 'off'));
        if (!in_array($mode, ['count_only', 'custom_fields'], true) || !$this->tableExists('{{%formie_submissions}}')) {
            return [];
        }

        $selectedFormIds = array_values(array_filter(array_map('intval', (array)($config['formIds'] ?? []))));
        if (empty($selectedFormIds)) {
            return [];
        }

        $dateColumn = $this->pickExistingColumn('{{%formie_submissions}}', ['dateCreated', 'dateUpdated']);
        if ($dateColumn === '') {
            return [];
        }

        $rows = (new Query())
            ->select(['id', 'formId', $dateColumn])
            ->from('{{%formie_submissions}}')
            ->where(['formId' => $selectedFormIds])
            ->andWhere(['>=', $dateColumn, $windowStart])
            ->orderBy([$dateColumn => SORT_ASC])
            ->limit(5000)
            ->all();

        $formNames = [];
        foreach (\burrow\Burrow\Plugin::getInstance()->getIntegrations()->getFormieForms() as $form) {
            if (!is_array($form)) {
                continue;
            }
            $id = (int)($form['id'] ?? 0);
            if ($id <= 0) {
                continue;
            }
            $formNames[$id] = (string)($form['name'] ?? ('Form ' . $id));
        }

        $events = [];
        foreach ($rows as $row) {
            if (!is_array($row)) {
                continue;
            }
            $formId = (int)($row['formId'] ?? 0);
            $timestamp = $this->normalizeTimestamp((string)($row[$dateColumn] ?? ''));
            if ($formId <= 0 || $timestamp === '') {
                continue;
            }
            $submissionId = trim((string)($row['id'] ?? ''));
            $events[] = [
                'projectId' => $projectId,
                'projectSourceId' => $projectSourceId,
                'channel' => 'forms',
                'event' => 'forms.submission.received',
                'timestamp' => $timestamp,
                'source' => 'craft-formie',
                'tags' => [
                    'provider' => 'formie',
                    'formId' => (string)$formId,
                ],
                'properties' => [
                    'provider' => 'formie',
                    'formId' => (string)$formId,
                    'formName' => (string)($formNames[$formId] ?? ('Form ' . $formId)),
                    'submissionId' => $submissionId,
                    'isBackfill' => true,
                ],
            ];
        }

        return $events;
    }

    /**
     * @param array<string,mixed> $runtimeState
     * @return array<int,array<string,mixed>>
     */
    private function buildEcommerceEvents(array $runtimeState, string $windowStart): array
    {
        $commerceConfig = is_array($runtimeState['integrationSettings']['commerce'] ?? null)
            ? $runtimeState['integrationSettings']['commerce']
            : [];
        if ((string)($commerceConfig['mode'] ?? 'off') !== 'track') {
            return [];
        }
        if (!$this->tableExists('{{%commerce_orders}}')) {
            return [];
        }

        $projectId = trim((string)($runtimeState['projectId'] ?? ''));
        if ($projectId === '') {
            return [];
        }

        $sourceIds = is_array($runtimeState['sourceIds'] ?? null) ? $runtimeState['sourceIds'] : [];
        $ecommerceSourceId = trim((string)($sourceIds['ecommerce'] ?? $runtimeState['projectSourceId'] ?? ''));
        if ($ecommerceSourceId === '') {
            return [];
        }

        $dateColumn = $this->pickExistingColumn('{{%commerce_orders}}', ['dateOrdered', 'dateCreated']);
        if ($dateColumn === '') {
            return [];
        }

        $currencyColumn = $this->pickExistingColumn('{{%commerce_orders}}', ['paymentCurrency', 'currency']);
        $currencySelect = $currencyColumn !== '' ? $currencyColumn : new \yii\db\Expression("'USD'");

        $rows = (new Query())
            ->select([
                'id',
                'number',
                'totalPrice',
                'itemSubtotal',
                'dateOrdered' => $dateColumn,
                'currency' => $currencySelect,
            ])
            ->from('{{%commerce_orders}}')
            ->where(['>=', $dateColumn, $windowStart])
            ->andWhere(['is not', 'dateOrdered', null])
            ->orderBy([$dateColumn => SORT_ASC])
            ->limit(2000)
            ->all();

        if (empty($rows)) {
            return [];
        }

        $orderIds = array_values(array_filter(array_map(static fn ($row) => (int)($row['id'] ?? 0), $rows)));
        $lineItemsByOrder = $this->loadCommerceLineItems($orderIds);

        $state = new \Burrow\Sdk\Events\ChannelRoutingState(
            projectId: $projectId,
            projectSourceIds: [
                'forms' => trim((string)($sourceIds['forms'] ?? $runtimeState['projectSourceId'] ?? '')),
                'ecommerce' => $ecommerceSourceId,
                'system' => trim((string)($sourceIds['system'] ?? $runtimeState['projectSourceId'] ?? '')),
            ],
            clientId: trim((string)($runtimeState['clientId'] ?? ''))
        );
        $routing = new \Burrow\Sdk\Events\ChannelRoutingResolver($state);

        $events = [];
        foreach ($rows as $row) {
            if (!is_array($row)) {
                continue;
            }
            $orderId = trim((string)($row['number'] ?? $row['id'] ?? ''));
            $submittedAt = $this->normalizeTimestamp((string)($row['dateOrdered'] ?? ''));
            if ($orderId === '' || $submittedAt === '') {
                continue;
            }
            $currency = trim((string)($row['currency'] ?? 'USD')) ?: 'USD';
            $items = $lineItemsByOrder[(int)($row['id'] ?? 0)] ?? [];
            $itemCount = count($items);

            try {
                $events[] = \Burrow\Sdk\Events\CanonicalEnvelopeBuilders::buildEcommerceOrderPlacedEvent([
                    'orderId' => $orderId,
                    'orderTotal' => (float)($row['totalPrice'] ?? 0),
                    'currency' => $currency,
                    'itemCount' => $itemCount,
                    'submittedAt' => $submittedAt,
                    'subtotal' => (float)($row['itemSubtotal'] ?? 0),
                    'timestamp' => $submittedAt,
                    'tags' => [
                        'provider' => 'craft-commerce',
                    ],
                ], $routing);
            } catch (\Throwable) {
                continue;
            }

            foreach ($items as $item) {
                try {
                    $events[] = \Burrow\Sdk\Events\CanonicalEnvelopeBuilders::buildEcommerceItemPurchasedEvent([
                        'orderId' => $orderId,
                        'productId' => trim((string)($item['productId'] ?? '')),
                        'productName' => trim((string)($item['productName'] ?? 'Item')),
                        'quantity' => (float)($item['quantity'] ?? 1),
                        'unitPrice' => (float)($item['unitPrice'] ?? 0),
                        'lineTotal' => (float)($item['lineTotal'] ?? 0),
                        'currency' => $currency,
                        'submittedAt' => $submittedAt,
                        'timestamp' => $submittedAt,
                        'tags' => [
                            'provider' => 'craft-commerce',
                        ],
                    ], $routing);
                } catch (\Throwable) {
                    continue;
                }
            }
        }

        return $events;
    }

    /**
     * @param array<int,int> $orderIds
     * @return array<int,array<int,array<string,mixed>>>
     */
    private function loadCommerceLineItems(array $orderIds): array
    {
        if (empty($orderIds) || !$this->tableExists('{{%commerce_lineitems}}')) {
            return [];
        }

        $rows = (new Query())
            ->select(['orderId', 'purchasableId', 'description', 'qty', 'salePrice', 'subtotal'])
            ->from('{{%commerce_lineitems}}')
            ->where(['orderId' => $orderIds])
            ->all();

        $result = [];
        foreach ($rows as $row) {
            if (!is_array($row)) {
                continue;
            }
            $orderId = (int)($row['orderId'] ?? 0);
            if ($orderId <= 0) {
                continue;
            }
            $result[$orderId][] = [
                'productId' => (string)($row['purchasableId'] ?? ''),
                'productName' => (string)($row['description'] ?? 'Item'),
                'quantity' => (float)($row['qty'] ?? 1),
                'unitPrice' => (float)($row['salePrice'] ?? 0),
                'lineTotal' => (float)($row['subtotal'] ?? 0),
            ];
        }

        return $result;
    }

    private function tableExists(string $tableName): bool
    {
        return Craft::$app->getDb()->getSchema()->getTableSchema($tableName, true) !== null;
    }

    /**
     * @param array<int,string> $candidates
     */
    private function pickExistingColumn(string $tableName, array $candidates): string
    {
        $schema = Craft::$app->getDb()->getSchema()->getTableSchema($tableName, true);
        if ($schema === null) {
            return '';
        }
        foreach ($candidates as $column) {
            if (isset($schema->columns[$column])) {
                return $column;
            }
        }
        return '';
    }

    private function normalizeTimestamp(string $value): string
    {
        $trimmed = trim($value);
        if ($trimmed === '') {
            return '';
        }
        try {
            return (new \DateTimeImmutable($trimmed))->setTimezone(new \DateTimeZone('UTC'))->format('c');
        } catch (\Throwable) {
            return '';
        }
    }

    /**
     * @return array{ok:bool,error:string,windowStart:string,windowEnd:string,sources:array<int,string>,requested:int,accepted:int,rejected:int,validationRejected:int,latestCursor:string,breakdown:array<string,int>}
     */
    private function errorResult(string $error): array
    {
        return [
            'ok' => false,
            'error' => $error,
            'windowStart' => '',
            'windowEnd' => '',
            'sources' => [],
            'requested' => 0,
            'accepted' => 0,
            'rejected' => 0,
            'validationRejected' => 0,
            'latestCursor' => '',
            'breakdown' => ['forms' => 0, 'ecommerce' => 0],
        ];
    }
}
