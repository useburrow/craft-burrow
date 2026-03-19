<?php
namespace burrow\Burrow\services;

use craft\base\Component;
use yii\base\Event;

class CommerceTrackingService extends Component
{
    public function handleCompletedOrderEvent(Event $event): void
    {
        $order = is_object($event->sender ?? null) ? $event->sender : null;
        if ($order === null) {
            return;
        }

        $plugin = \burrow\Burrow\Plugin::getInstance();
        $runtimeState = $plugin->getState()->getState();
        if (!$this->isCommerceTrackingEnabled($runtimeState)) {
            return;
        }

        $orderId = $this->extractOrderIdentifier($order);
        if ($orderId === '') {
            $plugin->getLogs()->log('warning', 'Commerce order skipped: missing order id', 'commerce', 'ecommerce');
            return;
        }
        $orderReference = $this->stringValue($order, ['reference', 'shortNumber']);
        $orderLookupNumber = $this->stringValue($order, ['number', 'id']);

        $submittedAt = $this->dateValue($order, ['dateOrdered', 'dateCreated']) ?: gmdate('c');
        $currency = $this->stringValue($order, ['paymentCurrency', 'currency']);
        if ($currency === '') {
            $currency = 'USD';
        }

        $lineItems = $this->extractLineItems($order);
        $orderTotal = $this->extractOrderTotal($order);
        $itemCount = count($lineItems);
        if ($itemCount <= 0) {
            $itemCount = max(0, (int)round($this->floatValue($order, ['totalQty', 'totalQuantity', 'itemQty'])));
        }
        $shippingMethod = $this->extractShippingMethod($order);
        $tags = ['provider' => 'craft-commerce'];
        if ($orderReference !== '') {
            $tags['orderReference'] = $orderReference;
        }
        if ($orderLookupNumber !== '') {
            $tags['orderLookupNumber'] = $orderLookupNumber;
        }
        if ($shippingMethod !== '') {
            $tags['shippingMethod'] = $shippingMethod;
        }
        $events = $plugin->getBurrowApi()->buildEcommerceOrderAndItemEvents($runtimeState, [
            'orderId' => $orderId,
            'orderTotal' => $orderTotal,
            'currency' => $currency,
            'itemCount' => $itemCount,
            'submittedAt' => $submittedAt,
            'timestamp' => $submittedAt,
            'subtotal' => $this->floatValue($order, ['itemSubtotal', 'subtotal']),
            'tax' => $this->floatValue($order, ['totalTax', 'taxTotal']),
            'externalEntityId' => 'craft_order_' . $orderId,
            'tags' => $tags,
            'items' => $lineItems,
        ]);

        if (empty($events)) {
            $plugin->getLogs()->log('warning', 'Commerce order envelope build failed', 'commerce', 'ecommerce', null, [
                'orderId' => $orderId,
            ]);
            return;
        }

        $settings = $plugin->getSettings();
        $result = $plugin->getBurrowApi()->publishEvents(
            $settings->baseUrl,
            $settings->apiKey,
            $runtimeState,
            $events
        );

        $plugin->getLogs()->log(
            $result['ok'] ? 'info' : 'warning',
            $result['ok'] ? 'Commerce order events published' : 'Commerce order events publish failed',
            'commerce',
            'ecommerce',
            null,
            [
                'orderId' => $orderId,
                'orderReference' => $orderReference,
                'orderLookupNumber' => $orderLookupNumber,
                'shippingMethod' => $shippingMethod,
                'requested' => $result['requestedCount'],
                'published' => $result['publishedCount'],
                'failed' => $result['failedCount'],
                'error' => $result['error'],
            ]
        );
    }

    private function extractOrderIdentifier(object $order): string
    {
        return $this->stringValue($order, ['id', 'number', 'reference', 'shortNumber']);
    }

    private function extractOrderTotal(object $order): float
    {
        $total = $this->floatValue($order, ['totalPaid', 'totalPrice', 'total']);
        if ($total > 0.0) {
            return $total;
        }
        return $this->floatValue($order, ['itemSubtotal', 'subtotal']);
    }

    private function extractShippingMethod(object $order): string
    {
        $method = $this->stringValue($order, ['shippingMethodName', 'shippingMethodHandle']);
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
            return $this->stringValue($shippingMethod, ['name', 'handle', 'id']);
        }
        return '';
    }

    /**
     * @param array<string,mixed> $runtimeState
     */
    private function isCommerceTrackingEnabled(array $runtimeState): bool
    {
        $selected = array_values(array_filter(array_map('strval', (array)($runtimeState['selectedIntegrations'] ?? []))));
        if (!in_array('commerce', $selected, true)) {
            return false;
        }

        $commerceConfig = is_array($runtimeState['integrationSettings']['commerce'] ?? null)
            ? $runtimeState['integrationSettings']['commerce']
            : [];

        return (string)($commerceConfig['mode'] ?? 'off') === 'track';
    }

    /**
     * @return array<int,array<string,mixed>>
     */
    private function extractLineItems(object $order): array
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
                'productId' => $this->stringValue($item, ['purchasableId', 'id']),
                'productName' => $this->stringValue($item, ['description', 'sku']) ?: 'Item',
                'quantity' => $this->floatValue($item, ['qty', 'quantity']),
                'unitPrice' => $this->floatValue($item, ['salePrice', 'price']),
                'lineTotal' => $this->floatValue($item, ['subtotal', 'total']),
            ];
        }

        return $lineItems;
    }

    /**
     * @param array<int,string> $keys
     */
    private function stringValue(object $source, array $keys): string
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
    private function floatValue(object $source, array $keys): float
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
     * @param array<int,string> $keys
     */
    private function dateValue(object $source, array $keys): string
    {
        foreach ($keys as $key) {
            $value = null;
            if (method_exists($source, 'get' . ucfirst($key))) {
                $value = $source->{'get' . ucfirst($key)}();
            } elseif (isset($source->{$key})) {
                $value = $source->{$key};
            }

            if ($value instanceof \DateTimeInterface) {
                return (new \DateTimeImmutable($value->format('c')))->setTimezone(new \DateTimeZone('UTC'))->format('c');
            }
            if (is_string($value) && trim($value) !== '') {
                try {
                    return (new \DateTimeImmutable($value))->setTimezone(new \DateTimeZone('UTC'))->format('c');
                } catch (\Throwable) {
                    continue;
                }
            }
        }

        return '';
    }
}
