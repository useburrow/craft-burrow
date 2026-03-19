<?php
namespace burrow\Burrow\services;

use Craft;
use craft\base\Component;

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
            $debug = $this->buildDiscoveryDebug($runtimeState, $sources, $windowStart);
            \burrow\Burrow\Plugin::getInstance()->getLogs()->log(
                'warning',
                'Backfill discovery returned zero events',
                'backfill',
                'system',
                null,
                $debug
            );
            $summary = 'forms=' . (string)($debug['formsEvents'] ?? 0)
                . ', ecommerce=' . (string)($debug['ecommerceEvents'] ?? 0)
                . ', projectId=' . ((bool)($debug['projectIdPresent'] ?? false) ? 'yes' : 'no')
                . ', formsSource=' . ((bool)($debug['formsSourcePresent'] ?? false) ? 'yes' : 'no')
                . ', ecommerceSource=' . ((bool)($debug['ecommerceSourcePresent'] ?? false) ? 'yes' : 'no')
                . ', commerceSelected=' . ((bool)($debug['commerceSelected'] ?? false) ? 'yes' : 'no')
                . ', commerceMode=' . (string)($debug['commerceMode'] ?? 'unknown')
                . ', commerceScanned=' . (string)($debug['commerceScanned'] ?? 0)
                . ', commerceInWindow=' . (string)($debug['commerceInWindow'] ?? 0);
            return $this->errorResult('No historical events found for the selected window and sources. [' . $summary . ']');
        }

        $plugin = \burrow\Burrow\Plugin::getInstance();
        $settings = $plugin->getSettings();
        $eventKeys = [];
        $pendingEvents = [];
        $skippedDuplicates = 0;
        foreach ($events as $event) {
            if (!is_array($event)) {
                continue;
            }
            $eventKey = $this->buildBackfillEventKey($event);
            if ($plugin->getQueue()->wasSent($eventKey)) {
                $skippedDuplicates++;
                continue;
            }
            $eventKeys[] = $eventKey;
            $pendingEvents[] = $event;
        }
        if (empty($pendingEvents)) {
            return [
                'ok' => true,
                'error' => '',
                'windowStart' => $windowStart,
                'windowEnd' => $windowEnd,
                'sources' => $sources,
                'requested' => 0,
                'accepted' => 0,
                'rejected' => 0,
                'validationRejected' => 0,
                'latestCursor' => '',
                'breakdown' => $breakdown,
            ];
        }
        $sdkResult = $plugin->getBurrowApi()->submitBackfillEvents(
            $settings->baseUrl,
            $settings->apiKey,
            $runtimeState,
            $pendingEvents,
            $windowStart,
            $windowEnd
        );
        if (!$sdkResult['ok']) {
            foreach ($pendingEvents as $index => $event) {
                if (!is_array($event)) {
                    continue;
                }
                $plugin->getQueue()->markFailed(
                    (string)($eventKeys[$index] ?? $this->buildBackfillEventKey($event)),
                    $event,
                    (string)$sdkResult['error'],
                    (string)($event['channel'] ?? ''),
                    (string)($event['event'] ?? '')
                );
            }
            return [
                'ok' => false,
                'error' => (string)$sdkResult['error'],
                'windowStart' => $windowStart,
                'windowEnd' => $windowEnd,
                'sources' => $sources,
                'requested' => count($pendingEvents),
                'accepted' => 0,
                'rejected' => 0,
                'validationRejected' => 0,
                'latestCursor' => '',
                'breakdown' => $breakdown,
            ];
        }

        foreach ($pendingEvents as $index => $event) {
            if (!is_array($event)) {
                continue;
            }
            $plugin->getQueue()->markSent(
                (string)($eventKeys[$index] ?? $this->buildBackfillEventKey($event)),
                $event,
                (string)($event['channel'] ?? ''),
                (string)($event['event'] ?? '')
            );
        }

        return [
            'ok' => true,
            'error' => '',
            'windowStart' => $windowStart,
            'windowEnd' => $windowEnd,
            'sources' => $sources,
            'requested' => (int)($sdkResult['requestedCount'] ?? count($pendingEvents)),
            'accepted' => (int)($sdkResult['acceptedCount'] ?? 0) + $skippedDuplicates,
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
     * @return array<string,mixed>
     */
    public function debugProbe(array $runtimeState, string $windowPreset = 'last_90_days'): array
    {
        $windowStart = $this->windowStartForPreset($windowPreset);
        $windowStartTs = strtotime($windowStart) ?: 0;

        $formsEvents = $this->buildFormsEvents($runtimeState, $windowStart);
        $ecommerceEvents = $this->buildEcommerceEvents($runtimeState, $windowStart);

        $freeform = $this->probeFreeformSubmissions($windowStartTs);
        $formie = $this->probeFormieSubmissions($windowStartTs);
        $commerce = $this->probeCommerceOrders($windowStartTs);

        $sourceIds = is_array($runtimeState['sourceIds'] ?? null) ? $runtimeState['sourceIds'] : [];
        $projectSourceId = trim((string)($runtimeState['projectSourceId'] ?? ''));

        return [
            'windowPreset' => $windowPreset,
            'windowStart' => $windowStart,
            'windowEnd' => gmdate('c'),
            'projectId' => trim((string)($runtimeState['projectId'] ?? '')),
            'sourceIds' => [
                'forms' => trim((string)($sourceIds['forms'] ?? $projectSourceId)),
                'ecommerce' => trim((string)($sourceIds['ecommerce'] ?? $sourceIds['forms'] ?? $projectSourceId)),
                'system' => trim((string)($sourceIds['system'] ?? $projectSourceId)),
            ],
            'availableSources' => $this->availableSources($runtimeState),
            'eventCounts' => [
                'forms' => count($formsEvents),
                'ecommerce' => count($ecommerceEvents),
            ],
            'freeform' => $freeform,
            'formie' => $formie,
            'commerce' => $commerce,
        ];
    }

    /**
     * @param array<string,mixed> $runtimeState
     * @return array<int,string>
     */
    public function availableSources(array $runtimeState): array
    {
        $selected = array_values(array_filter(array_map('strval', (array)($runtimeState['selectedIntegrations'] ?? []))));
        $integrationSettings = is_array($runtimeState['integrationSettings'] ?? null) ? $runtimeState['integrationSettings'] : [];
        $available = [];
        if ((in_array('freeform', $selected, true) || in_array('formie', $selected, true)) && $this->hasConfiguredFormsBackfill($integrationSettings)) {
            $available[] = 'forms';
        }
        $commerceSettings = is_array($integrationSettings['commerce'] ?? null) ? $integrationSettings['commerce'] : [];
        if (in_array('commerce', $selected, true) && (string)($commerceSettings['mode'] ?? 'off') === 'track') {
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
        $events = [];
        $events = [...$events, ...$this->buildFreeformSubmissionEvents($runtimeState, $windowStart)];
        $events = [...$events, ...$this->buildFormieSubmissionEvents($runtimeState, $windowStart)];

        return $events;
    }

    /**
     * @param array<string,mixed> $runtimeState
     * @return array<int,array<string,mixed>>
     */
    private function buildFreeformSubmissionEvents(array $runtimeState, string $windowStart): array
    {
        $config = is_array($runtimeState['integrationSettings']['freeform']['forms'] ?? null)
            ? $runtimeState['integrationSettings']['freeform']['forms']
            : [];
        $submissionClass = '\Solspace\Freeform\Elements\Submission';
        if (!class_exists($submissionClass) || !method_exists($submissionClass, 'find')) {
            return [];
        }

        $enabledFormIds = [];
        $formNames = [];
        $formConfigsById = [];
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
            $formConfigsById[(int)$stringFormId] = $formConfig;
        }
        if (empty($enabledFormIds)) {
            foreach (\burrow\Burrow\Plugin::getInstance()->getIntegrations()->getFreeformForms() as $form) {
                if (!is_array($form)) {
                    continue;
                }
                $id = (int)($form['id'] ?? 0);
                if ($id <= 0) {
                    continue;
                }
                $enabledFormIds[] = $id;
                $formNames[$id] = trim((string)($form['name'] ?? ('Form ' . $id)));
            }
            $enabledFormIds = array_values(array_unique($enabledFormIds));
        }
        $enabledFormIdMap = array_fill_keys($enabledFormIds, true);
        $windowStartTs = strtotime($windowStart) ?: 0;
        try {
            $rows = $submissionClass::find()
                ->status(null)
                ->site('*')
                ->orderBy(['dateCreated' => SORT_DESC])
                ->limit(null)
                ->all();
        } catch (\Throwable) {
            return [];
        }

        $api = \burrow\Burrow\Plugin::getInstance()->getBurrowApi();
        $events = [];
        foreach ($rows as $row) {
            if (!is_object($row)) {
                continue;
            }
            $formId = $this->extractSubmissionFormId($row);
            if ($formId <= 0 || !isset($enabledFormIdMap[$formId])) {
                continue;
            }
            $timestamp = $this->normalizeTimestamp($this->objectDateValue($row, ['dateCreated', 'dateUpdated']));
            if ($timestamp === '') {
                continue;
            }
            $submittedTs = strtotime($timestamp) ?: 0;
            if ($submittedTs < $windowStartTs) {
                continue;
            }
            $submissionId = $this->objectStringValue($row, ['id']);
            $prefixedFormId = $this->resolveProviderPrefix($runtimeState, 'freeform', 'FF') . max(0, $formId);
            $baseTags = [
                'provider' => 'freeform',
                'formId' => $prefixedFormId,
            ];
            $baseProperties = [
                'provider' => 'freeform',
                'formId' => $prefixedFormId,
                'formName' => (string)($formNames[$formId] ?? ($formId > 0 ? ('Form ' . $formId) : 'Unknown Form')),
                'submissionId' => $submissionId,
                'submittedAt' => $timestamp,
                'isBackfill' => true,
            ];
            $formConfig = is_array($formConfigsById[$formId] ?? null) ? $formConfigsById[$formId] : [];
            $mode = trim((string)($formConfig['mode'] ?? 'count_only'));
            if ($mode === 'custom_fields') {
                $submissionPayload = $this->extractSubmissionScalarValues($row);
                if (!empty($submissionPayload)) {
                    $baseProperties = array_merge($submissionPayload, $baseProperties);
                }
                $mapped = $this->extractMappedSubmissionPayload($row, is_array($formConfig['fields'] ?? null) ? $formConfig['fields'] : []);
                if (!empty($mapped['tags']) && is_array($mapped['tags'])) {
                    $baseTags = array_merge($baseTags, $mapped['tags']);
                }
                if (!empty($mapped['properties']) && is_array($mapped['properties'])) {
                    $baseProperties = array_merge($baseProperties, $mapped['properties']);
                }
            }
            $event = $api->buildFormsSubmissionEvent($runtimeState, [
                'timestamp' => $timestamp,
                'source' => 'craft-freeform',
                'tags' => $baseTags,
                'properties' => $baseProperties,
            ]);
            if (!empty($event)) {
                $events[] = $event;
            }
        }

        return $events;
    }

    /**
     * @param array<string,mixed> $runtimeState
     * @return array<int,array<string,mixed>>
     */
    private function buildFormieSubmissionEvents(array $runtimeState, string $windowStart): array
    {
        $config = is_array($runtimeState['integrationSettings']['formie'] ?? null)
            ? $runtimeState['integrationSettings']['formie']
            : [];
        $mode = trim((string)($config['mode'] ?? 'count_only'));
        if (!in_array($mode, ['count_only', 'custom_fields'], true)) {
            return [];
        }
        if ($mode === 'custom_fields') {
            // Formie custom field mapping is not implemented yet; treat as count-only.
            $mode = 'count_only';
        }
        $submissionClass = '\verbb\formie\elements\Submission';
        if (!class_exists($submissionClass) || !method_exists($submissionClass, 'find')) {
            return [];
        }

        $selectedFormIds = array_values(array_filter(array_map('intval', (array)($config['formIds'] ?? []))));
        $windowStartTs = strtotime($windowStart) ?: 0;
        try {
            $rows = $submissionClass::find()
                ->status(null)
                ->site('*')
                ->orderBy(['dateCreated' => SORT_DESC])
                ->limit(null)
                ->all();
        } catch (\Throwable) {
            return [];
        }

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
        if (empty($selectedFormIds) && !empty($formNames)) {
            $selectedFormIds = array_keys($formNames);
        }
        if (empty($selectedFormIds)) {
            return [];
        }
        $selectedFormIdMap = array_fill_keys($selectedFormIds, true);

        $api = \burrow\Burrow\Plugin::getInstance()->getBurrowApi();
        $events = [];
        foreach ($rows as $row) {
            if (!is_object($row)) {
                continue;
            }
            $formId = $this->extractSubmissionFormId($row);
            if ($formId <= 0 || !isset($selectedFormIdMap[$formId])) {
                continue;
            }
            $timestamp = $this->normalizeTimestamp($this->objectDateValue($row, ['dateCreated', 'dateUpdated']));
            if ($timestamp === '') {
                continue;
            }
            $submittedTs = strtotime($timestamp) ?: 0;
            if ($submittedTs < $windowStartTs) {
                continue;
            }
            $submissionId = $this->objectStringValue($row, ['id']);
            $prefixedFormId = $this->resolveProviderPrefix($runtimeState, 'formie', 'FRM') . max(0, $formId);
            $baseProperties = [
                'provider' => 'formie',
                'formId' => $prefixedFormId,
                'formName' => (string)($formNames[$formId] ?? ($formId > 0 ? ('Form ' . $formId) : 'Unknown Form')),
                'submissionId' => $submissionId,
                'submittedAt' => $timestamp,
                'isBackfill' => true,
            ];
            $event = $api->buildFormsSubmissionEvent($runtimeState, [
                'timestamp' => $timestamp,
                'source' => 'craft-formie',
                'tags' => [
                    'provider' => 'formie',
                    'formId' => $prefixedFormId,
                ],
                'properties' => $baseProperties,
            ]);
            if (!empty($event)) {
                $events[] = $event;
            }
        }

        return $events;
    }

    /**
     * @param array<string,mixed> $runtimeState
     * @return array<int,array<string,mixed>>
     */
    private function buildEcommerceEvents(array $runtimeState, string $windowStart): array
    {
        $orderClass = '\craft\commerce\elements\Order';
        if (!class_exists($orderClass) || !method_exists($orderClass, 'find')) {
            return [];
        }

        $windowStartTs = strtotime($windowStart) ?: 0;

        try {
            $query = $orderClass::find()
                ->status(null)
                ->site('*')
                ->orderBy(['dateCreated' => SORT_DESC]);
            $orders = $query->limit(null)->all();
        } catch (\Throwable) {
            return [];
        }
        if (empty($orders)) {
            return [];
        }

        $events = [];
        $api = \burrow\Burrow\Plugin::getInstance()->getBurrowApi();
        foreach ($orders as $order) {
            if (!is_object($order)) {
                continue;
            }
            $submittedAt = $this->normalizeTimestamp($this->objectDateValue($order, ['dateOrdered', 'datePaid', 'dateAuthorized', 'dateCreated']));
            if ($submittedAt === '') {
                continue;
            }
            $submittedTs = strtotime($submittedAt) ?: 0;
            if ($submittedTs < $windowStartTs) {
                continue;
            }
            $orderId = $this->extractCommerceOrderIdentifier($order);
            if ($orderId === '') {
                continue;
            }
            $orderReference = $this->objectStringValue($order, ['reference', 'shortNumber']);
            $orderLookupNumber = $this->objectStringValue($order, ['number', 'id']);
            $currency = $this->objectStringValue($order, ['paymentCurrency', 'currency']) ?: 'USD';
            $items = $this->extractCommerceLineItemsFromOrderElement($order);
            $orderTotal = $this->extractCommerceOrderTotal($order);
            $itemCount = count($items);
            if ($itemCount <= 0) {
                $itemCount = max(0, (int)round($this->objectFloatValue($order, ['totalQty', 'totalQuantity', 'itemQty'])));
            }
            $shippingMethod = $this->extractCommerceShippingMethod($order);
            $shippingAddress = $this->extractCommerceShippingAddress($order);
            $paymentMethod = $this->extractCommercePaymentMethod($order);
            $customerToken = $this->extractCommerceCustomerToken($order);
            $isGuest = $this->extractCommerceIsGuest($order);
            $couponCode = $this->objectStringValue($order, ['couponCode']);

            $tags = [
                'provider' => 'craft-commerce',
                'currency' => $currency,
            ];
            if ($orderReference !== '') {
                $tags['orderReference'] = $orderReference;
            }
            if ($orderLookupNumber !== '') {
                $tags['orderLookupNumber'] = $orderLookupNumber;
            }
            if ($shippingMethod !== '') {
                $tags['shippingMethod'] = $shippingMethod;
            }
            if ($shippingAddress['country'] !== '') {
                $tags['shippingCountry'] = $shippingAddress['country'];
            }
            if ($shippingAddress['region'] !== '') {
                $tags['shippingRegion'] = $shippingAddress['region'];
            }
            if ($paymentMethod !== '') {
                $tags['paymentMethod'] = $paymentMethod;
            }
            if ($customerToken !== '') {
                $tags['customerToken'] = $customerToken;
            }
            if ($isGuest !== '') {
                $tags['isGuest'] = $isGuest;
            }
            if ($couponCode !== '') {
                $tags['couponCode'] = $couponCode;
            }

            $built = $api->buildEcommerceOrderAndItemEvents($runtimeState, [
                'orderId' => $orderId,
                'orderTotal' => $orderTotal,
                'currency' => $currency,
                'itemCount' => $itemCount,
                'submittedAt' => $submittedAt,
                'timestamp' => $submittedAt,
                'subtotal' => $this->objectFloatValue($order, ['itemSubtotal', 'subtotal']),
                'tax' => $this->objectFloatValue($order, ['totalTax', 'taxTotal']),
                'shipping' => $this->objectFloatValue($order, ['totalShippingCost', 'adjustmentSubtotal']),
                'externalEntityId' => 'craft_order_' . $orderId,
                'customerToken' => $customerToken,
                'tags' => $tags,
                'items' => $items,
            ]);
            if (!empty($built)) {
                $events = [...$events, ...$built];
            } else {
                \burrow\Burrow\Plugin::getInstance()->getLogs()->log(
                    'warning',
                    'Commerce order envelope build returned no events during backfill',
                    'backfill',
                    'ecommerce',
                    null,
                    [
                        'orderId' => $orderId,
                        'orderTotal' => $orderTotal,
                        'itemCount' => $itemCount,
                        'submittedAt' => $submittedAt,
                        'currency' => $currency,
                    ]
                );
            }
        }

        return $events;
    }

    /**
     * @param array<string,mixed> $integrationSettings
     */
    private function hasConfiguredFormsBackfill(array $integrationSettings): bool
    {
        $freeform = is_array($integrationSettings['freeform'] ?? null) ? $integrationSettings['freeform'] : [];
        $freeformForms = is_array($freeform['forms'] ?? null) ? $freeform['forms'] : [];
        foreach ($freeformForms as $config) {
            if (!is_array($config)) {
                continue;
            }
            $mode = trim((string)($config['mode'] ?? 'off'));
            if (in_array($mode, ['count_only', 'custom_fields'], true)) {
                return true;
            }
        }

        $formie = is_array($integrationSettings['formie'] ?? null) ? $integrationSettings['formie'] : [];
        $formieMode = trim((string)($formie['mode'] ?? 'count_only'));
        $formieIds = array_values(array_filter(array_map('strval', (array)($formie['formIds'] ?? []))));
        if (in_array($formieMode, ['count_only', 'custom_fields'], true)) {
            return true;
        }

        return false;
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
     * @param array<string,mixed> $event
     */
    private function buildBackfillEventKey(array $event): string
    {
        $normalized = $this->normalizeForHash($event);
        $channel = trim((string)($event['channel'] ?? 'unknown'));
        $eventName = trim((string)($event['event'] ?? 'event'));
        return 'backfill:' . $channel . ':' . $eventName . ':' . hash('sha256', json_encode($normalized));
    }

    /**
     * @param mixed $value
     * @return mixed
     */
    private function normalizeForHash($value)
    {
        if (!is_array($value)) {
            return $value;
        }

        if ($this->isAssoc($value)) {
            ksort($value);
        }

        foreach ($value as $key => $item) {
            $value[$key] = $this->normalizeForHash($item);
        }

        return $value;
    }

    /**
     * @param array<int|string,mixed> $value
     */
    private function isAssoc(array $value): bool
    {
        return array_keys($value) !== range(0, count($value) - 1);
    }

    /**
     * @param array<int,string> $keys
     */
    private function objectStringValue(object $source, array $keys): string
    {
        foreach ($keys as $key) {
            if (method_exists($source, 'get' . ucfirst($key))) {
                $value = $source->{'get' . ucfirst($key)}();
                $text = trim((string)$value);
                if ($text !== '') {
                    return $text;
                }
            }
            if (isset($source->{$key})) {
                $text = trim((string)$source->{$key});
                if ($text !== '') {
                    return $text;
                }
            }
        }
        return '';
    }

    /**
     * @param array<int,string> $keys
     */
    private function objectFloatValue(object $source, array $keys): float
    {
        $fallback = null;
        foreach ($keys as $key) {
            if (method_exists($source, 'get' . ucfirst($key))) {
                $value = $this->normalizeNumericValue($source->{'get' . ucfirst($key)}());
                if ($value !== 0.0) {
                    return $value;
                }
                if ($fallback === null) {
                    $fallback = $value;
                }
                continue;
            }
            if (isset($source->{$key})) {
                $value = $this->normalizeNumericValue($source->{$key});
                if ($value !== 0.0) {
                    return $value;
                }
                if ($fallback === null) {
                    $fallback = $value;
                }
            }
        }
        return $fallback ?? 0.0;
    }

    /**
     * @param array<int,string> $keys
     */
    private function objectDateValue(object $source, array $keys): string
    {
        foreach ($keys as $key) {
            $value = null;
            if (method_exists($source, 'get' . ucfirst($key))) {
                $value = $source->{'get' . ucfirst($key)}();
            } elseif (isset($source->{$key})) {
                $value = $source->{$key};
            }
            if ($value instanceof \DateTimeInterface) {
                return $value->format('c');
            }
            if (is_string($value) && trim($value) !== '') {
                return $value;
            }
        }
        return '';
    }

    /**
     * @return array<int,array<string,mixed>>
     */
    private function extractCommerceLineItemsFromOrderElement(object $order): array
    {
        $lineItems = [];
        $items = [];
        if (method_exists($order, 'getLineItems')) {
            $items = (array)$order->getLineItems();
        } elseif (property_exists($order, 'lineItems')) {
            $items = (array)$order->lineItems;
        }
        foreach ($items as $item) {
            if (!is_object($item)) {
                continue;
            }
            $lineItems[] = [
                'productId' => $this->objectStringValue($item, ['purchasableId', 'id']),
                'productName' => $this->objectStringValue($item, ['description', 'sku']) ?: 'Item',
                'quantity' => $this->objectFloatValue($item, ['qty', 'quantity']),
                'unitPrice' => $this->objectFloatValue($item, ['salePrice', 'price']),
                'lineTotal' => $this->objectFloatValue($item, ['subtotal', 'lineTotal', 'total']),
            ];
        }
        return $lineItems;
    }

    private function resolveProviderPrefix(array $runtimeState, string $provider, string $default): string
    {
        $integrationSettings = is_array($runtimeState['integrationSettings'] ?? null) ? $runtimeState['integrationSettings'] : [];
        $providerConfig = is_array($integrationSettings[$provider] ?? null) ? $integrationSettings[$provider] : [];
        $prefix = strtoupper(trim((string)($providerConfig['prefix'] ?? '')));
        return $prefix !== '' ? $prefix : $default;
    }

    private function extractSubmissionFormId(object $submission): int
    {
        $direct = (int)$this->objectStringValue($submission, ['formId', 'formID']);
        if ($direct > 0) {
            return $direct;
        }

        $form = null;
        if (method_exists($submission, 'getForm')) {
            $form = $submission->getForm();
        } elseif (isset($submission->form)) {
            $form = $submission->form;
        }
        if (is_object($form)) {
            $nested = (int)$this->objectStringValue($form, ['id']);
            if ($nested > 0) {
                return $nested;
            }
        }

        return 0;
    }

    /**
     * @param array<string,mixed> $mappedFields
     * @return array{tags:array<string,mixed>,properties:array<string,mixed>}
     */
    private function extractMappedSubmissionPayload(object $submission, array $mappedFields): array
    {
        $values = $this->extractSubmissionScalarValues($submission);
        if ($values === []) {
            return ['tags' => [], 'properties' => []];
        }
        $normalizedValues = [];
        foreach ($values as $key => $value) {
            $normalizedValues[$this->normalizeFieldKey($key)] = $value;
        }

        $tags = [];
        $properties = [];
        foreach ($mappedFields as $fieldConfig) {
            if (!is_array($fieldConfig)) {
                continue;
            }
            $target = trim((string)($fieldConfig['target'] ?? ''));
            if (!in_array($target, ['tags', 'properties'], true)) {
                continue;
            }
            $canonicalKey = trim((string)($fieldConfig['canonicalKey'] ?? ''));
            if ($canonicalKey === '') {
                continue;
            }
            $candidates = array_values(array_filter([
                trim((string)($fieldConfig['externalFieldId'] ?? '')),
                trim((string)($fieldConfig['sourceLabel'] ?? '')),
                $canonicalKey,
            ], static fn(string $value): bool => $value !== ''));

            $value = $this->findSubmissionValue($values, $normalizedValues, $candidates);
            if ($value === null || $value === '') {
                continue;
            }
            if ($target === 'tags') {
                $tags[$canonicalKey] = $value;
            } else {
                $properties[$canonicalKey] = $value;
            }
        }

        return [
            'tags' => $tags,
            'properties' => $properties,
        ];
    }

    /**
     * @return array<string,mixed>
     */
    private function extractSubmissionScalarValues(object $submission): array
    {
        $out = [];
        $sources = [];
        foreach (['getFieldValues', 'getValues', 'getContent', 'toArray'] as $method) {
            if (!method_exists($submission, $method)) {
                continue;
            }
            try {
                $value = $submission->{$method}();
                if (is_array($value)) {
                    $sources[] = $value;
                }
            } catch (\Throwable) {
                continue;
            }
        }

        if (property_exists($submission, 'fieldValues') && is_array($submission->fieldValues)) {
            $sources[] = $submission->fieldValues;
        }

        foreach ($sources as $source) {
            foreach ($source as $key => $value) {
                if (in_array((string)$key, ['id', 'formId', 'formID', 'submissionId', 'submittedAt'], true)) {
                    continue;
                }
                $normalized = $this->normalizeSubmissionValue($value);
                if ($normalized === null) {
                    continue;
                }
                $safeKey = trim((string)$key);
                if ($safeKey === '') {
                    continue;
                }
                $out[$safeKey] = $normalized;
            }
        }

        return $out;
    }

    /**
     * @param mixed $value
     * @return mixed
     */
    private function normalizeSubmissionValue($value)
    {
        if (is_scalar($value)) {
            return $value;
        }
        if ($value instanceof \DateTimeInterface) {
            return $value->format('c');
        }
        if (is_array($value)) {
            $flattened = [];
            foreach ($value as $item) {
                if (is_scalar($item)) {
                    $flattened[] = (string)$item;
                }
            }
            if ($flattened !== []) {
                return implode(', ', $flattened);
            }
            return null;
        }
        if (is_object($value) && method_exists($value, '__toString')) {
            return (string)$value;
        }
        return null;
    }

    /**
     * @param array<string,mixed> $values
     * @param array<string,mixed> $normalizedValues
     * @param array<int,string> $candidates
     * @return mixed
     */
    private function findSubmissionValue(array $values, array $normalizedValues, array $candidates)
    {
        foreach ($candidates as $candidate) {
            if (array_key_exists($candidate, $values)) {
                return $values[$candidate];
            }
            $normalized = $this->normalizeFieldKey($candidate);
            if ($normalized !== '' && array_key_exists($normalized, $normalizedValues)) {
                return $normalizedValues[$normalized];
            }
        }
        return null;
    }

    private function normalizeFieldKey(string $key): string
    {
        $normalized = preg_replace('/[^a-z0-9]+/i', '', strtolower(trim($key))) ?? '';
        return $normalized;
    }

    private function extractCommerceOrderIdentifier(object $order): string
    {
        return $this->objectStringValue($order, ['id', 'number', 'reference', 'shortNumber']);
    }

    private function extractCommerceOrderTotal(object $order): float
    {
        $total = $this->objectFloatValue($order, ['totalPaid', 'totalPrice', 'total']);
        if ($total > 0.0) {
            return $total;
        }
        return $this->objectFloatValue($order, ['itemSubtotal', 'subtotal']);
    }

    private function extractCommerceShippingMethod(object $order): string
    {
        $method = $this->objectStringValue($order, ['shippingMethodName', 'shippingMethodHandle']);
        if ($method !== '') {
            return $method;
        }

        $shippingMethod = null;
        if (method_exists($order, 'getShippingMethod')) {
            $shippingMethod = $order->getShippingMethod();
        } elseif (isset($order->shippingMethod)) {
            $shippingMethod = $order->shippingMethod;
        }
        if (is_object($shippingMethod)) {
            return $this->objectStringValue($shippingMethod, ['name', 'handle', 'id']);
        }

        return '';
    }

    /**
     * @return array{country: string, region: string}
     */
    private function extractCommerceShippingAddress(object $order): array
    {
        $result = ['country' => '', 'region' => ''];
        $address = null;
        if (method_exists($order, 'getShippingAddress')) {
            $address = $order->getShippingAddress();
        } elseif (isset($order->shippingAddress)) {
            $address = $order->shippingAddress;
        }
        if (!is_object($address)) {
            return $result;
        }
        $result['country'] = $this->objectStringValue($address, ['countryCode', 'country']);
        $result['region'] = $this->objectStringValue($address, ['administrativeArea', 'stateText', 'state', 'province']);
        return $result;
    }

    private function extractCommercePaymentMethod(object $order): string
    {
        if (method_exists($order, 'getGateway')) {
            $gateway = $order->getGateway();
            if (is_object($gateway)) {
                $name = $this->objectStringValue($gateway, ['name', 'handle']);
                if ($name !== '') {
                    return $name;
                }
            }
        }
        $gatewayId = $this->objectStringValue($order, ['gatewayId']);
        if ($gatewayId !== '') {
            return $gatewayId;
        }
        return $this->objectStringValue($order, ['paymentMethodName', 'paymentSource']);
    }

    private function extractCommerceCustomerToken(object $order): string
    {
        $email = $this->objectStringValue($order, ['email']);
        if ($email !== '') {
            return 'craft_' . hash('sha256', strtolower(trim($email)));
        }
        $customerId = $this->objectStringValue($order, ['customerId']);
        if ($customerId !== '') {
            return 'craft_cust_' . $customerId;
        }
        return '';
    }

    private function extractCommerceIsGuest(object $order): string
    {
        if (method_exists($order, 'getUser')) {
            return $order->getUser() === null ? 'true' : 'false';
        }
        if (method_exists($order, 'getCustomer')) {
            return $order->getCustomer() === null ? 'true' : 'false';
        }
        if (isset($order->isGuest)) {
            return $order->isGuest ? 'true' : 'false';
        }
        return '';
    }

    private function extractCommerceOrderStatusLabel(object $order): string
    {
        $status = null;
        if (method_exists($order, 'getOrderStatus')) {
            $status = $order->getOrderStatus();
        } elseif (isset($order->orderStatus)) {
            $status = $order->orderStatus;
        }
        if (is_object($status)) {
            return $this->objectStringValue($status, ['handle', 'name', 'id']);
        }
        return '';
    }

    /**
     * @param mixed $value
     */
    private function normalizeNumericValue($value): float
    {
        if (is_int($value) || is_float($value)) {
            return (float)$value;
        }
        if (is_string($value)) {
            $trimmed = trim($value);
            if ($trimmed === '') {
                return 0.0;
            }
            if (is_numeric($trimmed)) {
                return (float)$trimmed;
            }
            if (preg_match('/-?\d+(?:\.\d+)?/', str_replace(',', '', $trimmed), $match)) {
                return (float)$match[0];
            }
            return 0.0;
        }
        if (is_object($value)) {
            foreach (['getAmount', 'getValue', 'amount', 'value'] as $probe) {
                if (str_starts_with($probe, 'get') && method_exists($value, $probe)) {
                    return $this->normalizeNumericValue($value->{$probe}());
                }
                if (!str_starts_with($probe, 'get') && isset($value->{$probe})) {
                    return $this->normalizeNumericValue($value->{$probe});
                }
            }
            if (method_exists($value, '__toString')) {
                return $this->normalizeNumericValue((string)$value);
            }
        }
        return 0.0;
    }

    /**
     * @return array<string,mixed>
     */
    private function probeFreeformSubmissions(int $windowStartTs): array
    {
        $submissionClass = '\Solspace\Freeform\Elements\Submission';
        if (!class_exists($submissionClass) || !method_exists($submissionClass, 'find')) {
            return ['available' => false, 'scanned' => 0, 'inWindow' => 0, 'samples' => []];
        }
        try {
            $rows = $submissionClass::find()->status(null)->site('*')->orderBy(['dateCreated' => SORT_DESC])->limit(200)->all();
        } catch (\Throwable $e) {
            return ['available' => true, 'error' => $e->getMessage(), 'scanned' => 0, 'inWindow' => 0, 'samples' => []];
        }

        $inWindow = 0;
        $samples = [];
        foreach ($rows as $row) {
            if (!is_object($row)) {
                continue;
            }
            $timestamp = $this->normalizeTimestamp($this->objectDateValue($row, ['dateCreated', 'dateUpdated']));
            $ts = strtotime($timestamp) ?: 0;
            if ($ts >= $windowStartTs) {
                $inWindow++;
            }
            if (count($samples) < 5) {
                $samples[] = [
                    'id' => $this->objectStringValue($row, ['id']),
                    'formId' => (string)$this->extractSubmissionFormId($row),
                    'timestamp' => $timestamp,
                ];
            }
        }

        return [
            'available' => true,
            'scanned' => count($rows),
            'inWindow' => $inWindow,
            'samples' => $samples,
        ];
    }

    /**
     * @return array<string,mixed>
     */
    private function probeFormieSubmissions(int $windowStartTs): array
    {
        $submissionClass = '\verbb\formie\elements\Submission';
        if (!class_exists($submissionClass) || !method_exists($submissionClass, 'find')) {
            return ['available' => false, 'scanned' => 0, 'inWindow' => 0, 'samples' => []];
        }
        try {
            $rows = $submissionClass::find()->status(null)->site('*')->orderBy(['dateCreated' => SORT_DESC])->limit(200)->all();
        } catch (\Throwable $e) {
            return ['available' => true, 'error' => $e->getMessage(), 'scanned' => 0, 'inWindow' => 0, 'samples' => []];
        }

        $inWindow = 0;
        $samples = [];
        foreach ($rows as $row) {
            if (!is_object($row)) {
                continue;
            }
            $timestamp = $this->normalizeTimestamp($this->objectDateValue($row, ['dateCreated', 'dateUpdated']));
            $ts = strtotime($timestamp) ?: 0;
            if ($ts >= $windowStartTs) {
                $inWindow++;
            }
            if (count($samples) < 5) {
                $samples[] = [
                    'id' => $this->objectStringValue($row, ['id']),
                    'formId' => (string)$this->extractSubmissionFormId($row),
                    'timestamp' => $timestamp,
                ];
            }
        }

        return [
            'available' => true,
            'scanned' => count($rows),
            'inWindow' => $inWindow,
            'samples' => $samples,
        ];
    }

    /**
     * @return array<string,mixed>
     */
    private function probeCommerceOrders(int $windowStartTs): array
    {
        $orderClass = '\craft\commerce\elements\Order';
        if (!class_exists($orderClass) || !method_exists($orderClass, 'find')) {
            return ['available' => false, 'scanned' => 0, 'inWindow' => 0, 'samples' => []];
        }
        try {
            $query = $orderClass::find()->status(null)->site('*')->orderBy(['dateCreated' => SORT_DESC]);
            $orders = $query->limit(200)->all();
        } catch (\Throwable $e) {
            return ['available' => true, 'error' => $e->getMessage(), 'scanned' => 0, 'inWindow' => 0, 'samples' => []];
        }

        $inWindow = 0;
        $placedEligible = 0;
        $backfillEligible = 0;
        $statusSignalPresent = 0;
        $samples = [];
        foreach ($orders as $order) {
            if (!is_object($order)) {
                continue;
            }
            $timestamp = $this->normalizeTimestamp($this->objectDateValue($order, ['dateOrdered', 'datePaid', 'dateAuthorized', 'dateCreated']));
            $ts = strtotime($timestamp) ?: 0;
            if ($ts >= $windowStartTs) {
                $inWindow++;
            }
            $rawStatus = strtolower(trim($this->objectStringValue($order, ['status', 'orderStatus'])));
            $normalizedStatus = preg_replace('/\s+/', '', $rawStatus) ?? $rawStatus;
            $statusLabel = $this->extractCommerceOrderStatusLabel($order);
            if ($normalizedStatus !== '' || $statusLabel !== '') {
                $statusSignalPresent++;
            }
            // Probe now mirrors native query-based eligibility: queried rows are what backfill will process.
            $placedEligible++;
            $backfillEligible++;
            if (count($samples) < 5) {
                $isCompleted = false;
                if (method_exists($order, 'getIsCompleted')) {
                    $isCompleted = (bool)$order->getIsCompleted();
                } elseif (isset($order->isCompleted)) {
                    $isCompleted = (bool)$order->isCompleted;
                }
                $samples[] = [
                    'id' => $this->objectStringValue($order, ['id']),
                    'number' => $this->objectStringValue($order, ['number']),
                    'reference' => $this->objectStringValue($order, ['reference', 'shortNumber']),
                    'statusLabel' => $statusLabel,
                    'statusRaw' => $rawStatus,
                    'statusNormalized' => $normalizedStatus,
                    'orderStatusId' => $this->objectStringValue($order, ['orderStatusId']),
                    'isCompleted' => $isCompleted ? 'yes' : 'no',
                    'placedSignal' => 'yes',
                    'statusSignalPresent' => ($normalizedStatus !== '' || $statusLabel !== '') ? 'yes' : 'no',
                    'backfillEligible' => 'yes',
                    'excludeReason' => '',
                    'timestamp' => $timestamp,
                    'total' => $this->extractCommerceOrderTotal($order),
                    'itemCount' => count($this->extractCommerceLineItemsFromOrderElement($order)),
                    'totalPaid' => $this->objectFloatValue($order, ['totalPaid']),
                    'totalPrice' => $this->objectFloatValue($order, ['totalPrice']),
                    'totalRaw' => $this->objectFloatValue($order, ['total']),
                    'itemSubtotal' => $this->objectFloatValue($order, ['itemSubtotal', 'subtotal']),
                ];
            }
        }

        return [
            'available' => true,
            'scanned' => count($orders),
            'inWindow' => $inWindow,
            'placedEligible' => $placedEligible,
            'backfillEligible' => $backfillEligible,
            'statusSignalPresent' => $statusSignalPresent,
            'samples' => $samples,
        ];
    }

    /**
     * @param array<string,mixed> $runtimeState
     * @param array<int,string> $sources
     * @return array<string,mixed>
     */
    private function buildDiscoveryDebug(array $runtimeState, array $sources, string $windowStart): array
    {
        $sourceIds = is_array($runtimeState['sourceIds'] ?? null) ? $runtimeState['sourceIds'] : [];
        $integrationSettings = is_array($runtimeState['integrationSettings'] ?? null) ? $runtimeState['integrationSettings'] : [];
        $commerceSettings = is_array($integrationSettings['commerce'] ?? null) ? $integrationSettings['commerce'] : [];
        $selected = array_values(array_filter(array_map('strval', (array)($runtimeState['selectedIntegrations'] ?? []))));
        $windowStartTs = strtotime($windowStart) ?: 0;
        $commerceProbe = $this->probeCommerceOrders($windowStartTs);
        $formsEvents = 0;
        $ecommerceEvents = 0;
        if (in_array('forms', $sources, true)) {
            $formsEvents = count($this->buildFormsEvents($runtimeState, $windowStart));
        }
        if (in_array('ecommerce', $sources, true)) {
            $ecommerceEvents = count($this->buildEcommerceEvents($runtimeState, $windowStart));
        }
        return [
            'sources' => $sources,
            'selectedIntegrations' => $selected,
            'projectIdPresent' => trim((string)($runtimeState['projectId'] ?? '')) !== '',
            'formsSourcePresent' => trim((string)($sourceIds['forms'] ?? $runtimeState['projectSourceId'] ?? '')) !== '',
            'ecommerceSourcePresent' => trim((string)($sourceIds['ecommerce'] ?? $sourceIds['forms'] ?? $runtimeState['projectSourceId'] ?? '')) !== '',
            'commerceSelected' => in_array('commerce', $selected, true),
            'commerceMode' => trim((string)($commerceSettings['mode'] ?? 'track')),
            'commerceScanned' => (int)($commerceProbe['scanned'] ?? 0),
            'commerceInWindow' => (int)($commerceProbe['inWindow'] ?? 0),
            'formsEvents' => $formsEvents,
            'ecommerceEvents' => $ecommerceEvents,
        ];
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
