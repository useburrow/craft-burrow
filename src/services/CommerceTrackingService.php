<?php
namespace burrow\Burrow\services;

use craft\base\Component;
use yii\base\Event;

class CommerceTrackingService extends Component
{
    /** @var array<string,bool> Order IDs that had a line item removed during this request */
    private array $ordersWithRemovalInFlight = [];

    public function handleCartLineItemAddedEvent(Event $event): void
    {
        $order = is_object($event->sender ?? null) ? $event->sender : null;
        $lineItem = is_object($event->lineItem ?? null) ? $event->lineItem : null;
        if ($order === null || $lineItem === null) {
            return;
        }

        $orderId = $this->extractOrderIdentifier($order);
        if ($orderId !== '' && isset($this->ordersWithRemovalInFlight[$orderId])) {
            return;
        }

        $plugin = \burrow\Burrow\Plugin::getInstance();
        $runtimeState = $plugin->getState()->getState();
        if (!$this->isCommerceFunnelEnabled($runtimeState)) {
            return;
        }

        $currency = $this->stringValue($order, ['paymentCurrency', 'currency']);
        if ($currency === '') {
            $currency = 'USD';
        }
        $productId = $this->stringValue($lineItem, ['purchasableId', 'id']);
        $productName = $this->stringValue($lineItem, ['description', 'sku']) ?: 'Item';
        $cartItemCount = max(0, (int)round($this->floatValue($order, ['totalQty', 'totalQuantity', 'itemQty'])));
        if ($cartItemCount <= 0 && method_exists($order, 'getLineItems')) {
            $cartItemCount = count((array)$order->getLineItems());
        }
        $eventEnvelope = $plugin->getBurrowApi()->buildEcommerceCartItemAddedEvent($runtimeState, [
            'productId' => $productId,
            'productName' => $productName,
            'variantName' => $this->stringValue($lineItem, ['optionsSignature', 'sku', 'description']) ?: $productName,
            'quantity' => $this->floatValue($lineItem, ['qty', 'quantity']),
            'unitPrice' => $this->floatValue($lineItem, ['salePrice', 'price']),
            'lineTotal' => $this->floatValue($lineItem, ['subtotal', 'total']),
            'currency' => $currency,
            'cartTotal' => $this->floatValue($order, ['totalPrice', 'total']),
            'cartItemCount' => $cartItemCount,
            'timestamp' => $this->dateValue($order, ['dateUpdated', 'dateCreated']) ?: gmdate('c'),
            'tags' => [
                'provider' => 'craft-commerce',
            ],
        ]);
        if (empty($eventEnvelope)) {
            return;
        }
        $this->publishAndTrackRealtimeEvent($eventEnvelope, $runtimeState, [
            'type' => 'cart_added',
            'orderId' => $this->extractOrderIdentifier($order),
            'productId' => $productId,
        ]);
    }

    public function handleCartLineItemRemovedEvent(Event $event): void
    {
        $order = is_object($event->sender ?? null) ? $event->sender : null;
        $lineItem = is_object($event->lineItem ?? null) ? $event->lineItem : null;
        if ($order === null || $lineItem === null) {
            return;
        }

        $orderId = $this->extractOrderIdentifier($order);
        if ($orderId !== '') {
            $this->ordersWithRemovalInFlight[$orderId] = true;
        }

        $plugin = \burrow\Burrow\Plugin::getInstance();
        $runtimeState = $plugin->getState()->getState();
        if (!$this->isCommerceFunnelEnabled($runtimeState)) {
            return;
        }

        $currency = $this->stringValue($order, ['paymentCurrency', 'currency']);
        if ($currency === '') {
            $currency = 'USD';
        }
        $productId = $this->stringValue($lineItem, ['purchasableId', 'id']);
        $productName = $this->stringValue($lineItem, ['description', 'sku']) ?: 'Item';
        $cartItemCount = max(0, (int)round($this->floatValue($order, ['totalQty', 'totalQuantity', 'itemQty'])));
        if ($cartItemCount <= 0 && method_exists($order, 'getLineItems')) {
            $cartItemCount = count((array)$order->getLineItems());
        }
        $eventEnvelope = $plugin->getBurrowApi()->buildEcommerceCartItemRemovedEvent($runtimeState, [
            'productId' => $productId,
            'productName' => $productName,
            'variantName' => $this->stringValue($lineItem, ['optionsSignature', 'sku', 'description']) ?: $productName,
            'quantity' => $this->floatValue($lineItem, ['qty', 'quantity']),
            'unitPrice' => $this->floatValue($lineItem, ['salePrice', 'price']),
            'lineTotal' => $this->floatValue($lineItem, ['subtotal', 'total']),
            'currency' => $currency,
            'cartTotal' => $this->floatValue($order, ['totalPrice', 'total']),
            'cartItemCount' => $cartItemCount,
            'timestamp' => $this->dateValue($order, ['dateUpdated', 'dateCreated']) ?: gmdate('c'),
            'tags' => [
                'provider' => 'craft-commerce',
            ],
        ]);
        if (empty($eventEnvelope)) {
            return;
        }
        $this->publishAndTrackRealtimeEvent($eventEnvelope, $runtimeState, [
            'type' => 'cart_removed',
            'orderId' => $this->extractOrderIdentifier($order),
            'productId' => $productId,
        ]);
    }

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
        $shippingAddress = $this->extractShippingAddress($order);
        $paymentMethod = $this->extractPaymentMethod($order);
        $customerToken = $this->extractCustomerToken($order);
        $isGuest = $this->extractIsGuest($order);
        $couponCode = $this->stringValue($order, ['couponCode']);

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

        $events = $plugin->getBurrowApi()->buildEcommerceOrderAndItemEvents($runtimeState, [
            'orderId' => $orderId,
            'orderTotal' => $orderTotal,
            'currency' => $currency,
            'itemCount' => $itemCount,
            'submittedAt' => $submittedAt,
            'timestamp' => $submittedAt,
            'subtotal' => $this->floatValue($order, ['itemSubtotal', 'subtotal']),
            'tax' => $this->floatValue($order, ['totalTax', 'taxTotal']),
            'shipping' => $this->floatValue($order, ['totalShippingCost', 'adjustmentSubtotal']),
            'externalEntityId' => 'craft_order_' . $orderId,
            'customerToken' => $customerToken,
            'tags' => $tags,
            'items' => $lineItems,
        ]);

        if (empty($events)) {
            $plugin->getLogs()->log('warning', 'Commerce order envelope build failed', 'commerce', 'ecommerce', null, [
                'orderId' => $orderId,
            ]);
            return;
        }

        $published = 0;
        $failed = 0;
        foreach ($events as $singleEvent) {
            if (!is_array($singleEvent)) {
                continue;
            }
            $ok = $this->publishAndTrackRealtimeEvent($singleEvent, $runtimeState, [
                'type' => 'order_completed',
                'orderId' => $orderId,
            ]);
            if ($ok) {
                $published++;
            } else {
                $failed++;
            }
        }

        if ($published > 0 && $this->isCommerceFunnelEnabled($runtimeState)) {
            $this->emitCartRecoveryIfApplicable($plugin, $runtimeState, $order, $orderId, $orderTotal, $currency, $customerToken, $submittedAt);
        }

        $plugin->getLogs()->log(
            $failed === 0 ? 'info' : 'warning',
            $failed === 0 ? 'Commerce order events published' : 'Commerce order events publish failed',
            'commerce',
            'ecommerce',
            null,
            [
                'orderId' => $orderId,
                'orderReference' => $orderReference,
                'orderLookupNumber' => $orderLookupNumber,
                'shippingMethod' => $shippingMethod,
                'requested' => count($events),
                'published' => $published,
                'failed' => $failed,
            ]
        );
    }

    public function handlePaymentProcessedEvent(Event $event): void
    {
        $transaction = is_object($event->transaction ?? null) ? $event->transaction : null;
        if ($transaction === null) {
            return;
        }

        $status = strtolower(trim($this->stringValue($transaction, ['status'])));
        if ($status === '' || $status === 'success' || $status === 'processing' || $status === 'redirect') {
            return;
        }

        $order = is_object($transaction->order ?? null) ? $transaction->order : null;
        if ($order === null) {
            $order = is_object($event->sender ?? null) ? $event->sender : null;
        }
        if ($order === null) {
            return;
        }

        $plugin = \burrow\Burrow\Plugin::getInstance();
        $runtimeState = $plugin->getState()->getState();
        if (!$this->isCommerceFunnelEnabled($runtimeState)) {
            return;
        }

        $orderId = $this->extractOrderIdentifier($order);
        $currency = $this->stringValue($order, ['paymentCurrency', 'currency']);
        if ($currency === '') {
            $currency = 'USD';
        }

        $cartTotal = $this->extractOrderTotal($order);
        $paymentMethod = $this->extractPaymentMethod($order);
        $customerToken = $this->extractCustomerToken($order);

        $failureReason = $this->stringValue($transaction, ['message', 'code']);
        if ($failureReason === '') {
            $response = is_object($transaction->response ?? null) ? $transaction->response : null;
            if ($response !== null) {
                $failureReason = $this->stringValue($response, ['getMessage', 'message', 'code']);
            }
        }
        if ($failureReason === '') {
            $failureReason = 'processing_error';
        }

        $tags = [
            'provider' => 'craft_commerce',
            'currency' => $currency,
        ];
        if ($customerToken !== '') {
            $tags['customerToken'] = $customerToken;
        }
        if ($paymentMethod !== '') {
            $tags['paymentMethod'] = $paymentMethod;
        }

        $eventEnvelope = $plugin->getBurrowApi()->buildEcommercePaymentFailedEvent($runtimeState, [
            'orderId' => $orderId,
            'cartTotal' => $cartTotal,
            'currency' => $currency,
            'failureReason' => $failureReason,
            'paymentMethod' => $paymentMethod,
            'timestamp' => gmdate('c'),
            'tags' => $tags,
        ]);
        if (empty($eventEnvelope)) {
            return;
        }

        $this->publishAndTrackRealtimeEvent($eventEnvelope, $runtimeState, [
            'type' => 'payment_failed',
            'orderId' => $orderId,
            'failureReason' => $failureReason,
        ]);
    }

    /**
     * Detect checkout initiation: fires on every Order save, but short-circuits
     * early and deduplicates so the event is emitted at most once per cart.
     * The heuristic is "incomplete order with email + line items".
     */
    public function handleOrderSavedEvent(Event $event): void
    {
        $order = is_object($event->sender ?? null) ? $event->sender : null;
        if ($order === null) {
            return;
        }

        if (isset($order->isCompleted) && $order->isCompleted) {
            return;
        }
        if (isset($order->dateOrdered) && $order->dateOrdered !== null) {
            return;
        }

        $email = $this->stringValue($order, ['email']);
        if ($email === '') {
            return;
        }

        $lineItems = [];
        if (method_exists($order, 'getLineItems')) {
            $lineItems = (array)$order->getLineItems();
        }
        if (empty($lineItems)) {
            return;
        }

        $plugin = \burrow\Burrow\Plugin::getInstance();
        $runtimeState = $plugin->getState()->getState();
        if (!$this->isCommerceFunnelEnabled($runtimeState)) {
            return;
        }

        $orderId = $this->extractOrderIdentifier($order);
        if ($orderId === '') {
            return;
        }

        $checkoutKey = 'checkout_started_' . $orderId;
        if ($plugin->getQueue()->wasSent($checkoutKey)) {
            return;
        }

        $currency = $this->stringValue($order, ['paymentCurrency', 'currency']);
        if ($currency === '') {
            $currency = 'USD';
        }

        $cartTotal = $this->floatValue($order, ['totalPrice', 'total', 'itemSubtotal']);
        $cartItemCount = count($lineItems);
        if ($cartItemCount <= 0) {
            $cartItemCount = max(0, (int)round($this->floatValue($order, ['totalQty', 'totalQuantity', 'itemQty'])));
        }

        $customerToken = $this->extractCustomerToken($order);
        $isGuest = $this->extractIsGuest($order);

        $tags = [
            'provider' => 'craft_commerce',
            'currency' => $currency,
        ];
        if ($customerToken !== '') {
            $tags['customerToken'] = $customerToken;
        }
        if ($isGuest !== '') {
            $tags['isGuest'] = $isGuest;
        }

        $eventEnvelope = $plugin->getBurrowApi()->buildEcommerceCheckoutStartedEvent($runtimeState, [
            'cartTotal' => $cartTotal,
            'cartItemCount' => $cartItemCount,
            'currency' => $currency,
            'timestamp' => $this->dateValue($order, ['dateUpdated', 'dateCreated']) ?: gmdate('c'),
            'tags' => $tags,
        ]);

        if (empty($eventEnvelope)) {
            return;
        }

        $result = $plugin->getBurrowApi()->publishEvents(
            $plugin->getBurrowBaseUrl(),
            $plugin->getBurrowApiKey(),
            $runtimeState,
            [$eventEnvelope]
        );

        $channel = trim((string)($eventEnvelope['channel'] ?? ''));
        $eventName = trim((string)($eventEnvelope['event'] ?? ''));

        if ($result['ok']) {
            $plugin->getQueue()->markSent($checkoutKey, $eventEnvelope, $channel, $eventName);
        } else {
            $error = trim((string)($result['error'] ?? 'Checkout started publish failed.'));
            $plugin->getQueue()->markFailed($checkoutKey, $eventEnvelope, $error, $channel, $eventName);
        }
    }

    private function emitCartRecoveryIfApplicable(
        object $plugin,
        array $runtimeState,
        object $order,
        string $orderId,
        float $orderTotal,
        string $currency,
        string $customerToken,
        string $timestamp
    ): void {
        if ($customerToken === '') {
            return;
        }

        $abandonmentDetails = $this->getAbandonmentDetails($customerToken);
        if ($abandonmentDetails === null) {
            return;
        }

        $recoveryEnvelope = $plugin->getBurrowApi()->buildEcommerceCartRecoveredEvent($runtimeState, [
            'orderId' => $orderId,
            'orderTotal' => $orderTotal,
            'originalCartTotal' => $abandonmentDetails['originalCartTotal'],
            'currency' => $currency,
            'minutesSinceAbandonment' => $abandonmentDetails['minutesSinceAbandonment'],
            'timestamp' => $timestamp,
            'tags' => [
                'provider' => 'craft_commerce',
                'currency' => $currency,
                'customerToken' => $customerToken,
            ],
        ]);

        if (empty($recoveryEnvelope)) {
            return;
        }

        $ok = $this->publishAndTrackRealtimeEvent($recoveryEnvelope, $runtimeState, [
            'type' => 'cart_recovered',
            'orderId' => $orderId,
        ]);

        if ($ok) {
            $this->clearAbandonmentSignal($customerToken);
            $plugin->getLogs()->log('info', 'Cart recovery detected at order completion', 'commerce', 'ecommerce', null, [
                'orderId' => $orderId,
                'minutesSinceAbandonment' => $abandonmentDetails['minutesSinceAbandonment'],
            ]);
        }
    }

    /**
     * Looks up the abandonment signal and enriches with timing and original cart total.
     *
     * @return array{originalCartTotal:float,minutesSinceAbandonment:int}|null
     */
    private function getAbandonmentDetails(string $customerToken): ?array
    {
        if ($customerToken === '') {
            return null;
        }

        try {
            $signalRow = \Craft::$app->getDb()->createCommand(
                'SELECT sent_at FROM {{%burrow_outbox_sent}} WHERE event_key = :key LIMIT 1',
                [':key' => 'abandonment_' . $customerToken]
            )->queryOne();
        } catch (\Throwable) {
            return null;
        }

        if (!is_array($signalRow) || empty($signalRow['sent_at'])) {
            return null;
        }

        $minutesSinceAbandonment = 0;
        try {
            $abandonedAt = new \DateTimeImmutable($signalRow['sent_at'], new \DateTimeZone('UTC'));
            $now = new \DateTimeImmutable('now', new \DateTimeZone('UTC'));
            $minutesSinceAbandonment = max(0, (int)round(($now->getTimestamp() - $abandonedAt->getTimestamp()) / 60));
        } catch (\Throwable) {
        }

        $originalCartTotal = 0.0;
        try {
            $outboxRow = \Craft::$app->getDb()->createCommand(
                "SELECT payload FROM {{%burrow_outbox}}
                 WHERE event_name IN ('ecommerce.cart.abandoned', 'ecommerce.checkout.abandoned')
                 AND status = 'sent'
                 ORDER BY updated_at DESC
                 LIMIT 1"
            )->queryOne();
            if (is_array($outboxRow) && !empty($outboxRow['payload'])) {
                $payload = $outboxRow['payload'];
                if (is_string($payload)) {
                    $payload = json_decode($payload, true);
                }
                if (is_array($payload)) {
                    $props = is_array($payload['properties'] ?? null) ? $payload['properties'] : [];
                    $originalCartTotal = (float)($props['cartTotal'] ?? 0);
                }
            }
        } catch (\Throwable) {
        }

        return [
            'originalCartTotal' => $originalCartTotal,
            'minutesSinceAbandonment' => $minutesSinceAbandonment,
        ];
    }

    private function clearAbandonmentSignal(string $customerToken): void
    {
        if ($customerToken === '') {
            return;
        }
        try {
            \Craft::$app->getDb()->createCommand()->delete(
                '{{%burrow_outbox_sent}}',
                ['event_key' => 'abandonment_' . $customerToken]
            )->execute();
        } catch (\Throwable) {
        }
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
     * @return array{country: string, region: string}
     */
    private function extractShippingAddress(object $order): array
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
        $result['country'] = $this->stringValue($address, ['countryCode', 'country']);
        $result['region'] = $this->stringValue($address, ['administrativeArea', 'stateText', 'state', 'province']);
        return $result;
    }

    private function extractPaymentMethod(object $order): string
    {
        if (method_exists($order, 'getGateway')) {
            $gateway = $order->getGateway();
            if (is_object($gateway)) {
                $name = $this->stringValue($gateway, ['name', 'handle']);
                if ($name !== '') {
                    return $name;
                }
            }
        }
        $gatewayId = $this->stringValue($order, ['gatewayId']);
        if ($gatewayId !== '') {
            return $gatewayId;
        }
        return $this->stringValue($order, ['paymentMethodName', 'paymentSource']);
    }

    private function extractCustomerToken(object $order): string
    {
        $email = $this->stringValue($order, ['email']);
        if ($email !== '') {
            return 'craft_' . hash('sha256', strtolower(trim($email)));
        }
        $customerId = $this->stringValue($order, ['customerId']);
        if ($customerId !== '') {
            return 'craft_cust_' . $customerId;
        }
        return '';
    }

    private function extractIsGuest(object $order): string
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
     * @param array<string,mixed> $runtimeState
     */
    private function isCommerceFunnelEnabled(array $runtimeState): bool
    {
        if (!$this->isCommerceTrackingEnabled($runtimeState)) {
            return false;
        }
        $commerceConfig = is_array($runtimeState['integrationSettings']['commerce'] ?? null)
            ? $runtimeState['integrationSettings']['commerce']
            : [];
        return !empty($commerceConfig['ecommerceFunnel']);
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

    /**
     * @param array<string,mixed> $eventEnvelope
     * @param array<string,mixed> $runtimeState
     * @param array<string,mixed> $identity
     */
    private function publishAndTrackRealtimeEvent(array $eventEnvelope, array $runtimeState, array $identity): bool
    {
        $plugin = \burrow\Burrow\Plugin::getInstance();
        $eventKey = $this->buildRealtimeEventKey($eventEnvelope, $identity);
        if ($plugin->getQueue()->wasSent($eventKey)) {
            return true;
        }

        $result = $plugin->getBurrowApi()->publishEvents(
            $plugin->getBurrowBaseUrl(),
            $plugin->getBurrowApiKey(),
            $runtimeState,
            [$eventEnvelope]
        );

        $channel = trim((string)($eventEnvelope['channel'] ?? ''));
        $eventName = trim((string)($eventEnvelope['event'] ?? ''));
        if ($result['ok']) {
            $plugin->getQueue()->markSent($eventKey, $eventEnvelope, $channel, $eventName);
            return true;
        }

        $error = trim((string)($result['error'] ?? 'Realtime publish failed.'));
        $plugin->getQueue()->markFailed($eventKey, $eventEnvelope, $error, $channel, $eventName);
        return false;
    }

    /**
     * @param array<string,mixed> $eventEnvelope
     * @param array<string,mixed> $identity
     */
    private function buildRealtimeEventKey(array $eventEnvelope, array $identity): string
    {
        $seed = [
            'channel' => trim((string)($eventEnvelope['channel'] ?? '')),
            'event' => trim((string)($eventEnvelope['event'] ?? '')),
            'timestamp' => trim((string)($eventEnvelope['timestamp'] ?? '')),
            'source' => trim((string)($eventEnvelope['source'] ?? '')),
            'identity' => $identity,
            'tags' => is_array($eventEnvelope['tags'] ?? null) ? $eventEnvelope['tags'] : [],
            'properties' => is_array($eventEnvelope['properties'] ?? null) ? $eventEnvelope['properties'] : [],
        ];
        return 'rt_' . hash('sha256', $this->stableJsonEncode($seed));
    }

    /**
     * @param array<string,mixed> $value
     */
    private function stableJsonEncode(array $value): string
    {
        $normalized = $this->normalizeForStableHash($value);
        $encoded = json_encode($normalized, JSON_UNESCAPED_SLASHES | JSON_UNESCAPED_UNICODE);
        return is_string($encoded) ? $encoded : '';
    }

    private function normalizeForStableHash(mixed $value): mixed
    {
        if (!is_array($value)) {
            return $value;
        }
        if (array_is_list($value)) {
            $normalized = [];
            foreach ($value as $item) {
                $normalized[] = $this->normalizeForStableHash($item);
            }
            return $normalized;
        }
        ksort($value);
        foreach ($value as $key => $item) {
            $value[$key] = $this->normalizeForStableHash($item);
        }
        return $value;
    }
}
