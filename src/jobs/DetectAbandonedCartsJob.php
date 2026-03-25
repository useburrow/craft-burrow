<?php
namespace burrow\Burrow\jobs;

use Craft;
use craft\queue\BaseJob;

class DetectAbandonedCartsJob extends BaseJob
{
    protected function defaultDescription(): ?string
    {
        return 'Detect abandoned Commerce carts';
    }

    public function execute($queue): void
    {
        $plugin = \burrow\Burrow\Plugin::getInstance();
        $runtimeState = $plugin->getState()->getState();

        $integrationSettings = is_array($runtimeState['integrationSettings'] ?? null) ? $runtimeState['integrationSettings'] : [];
        $systemJobs = is_array($integrationSettings['systemJobs'] ?? null) ? $integrationSettings['systemJobs'] : [];
        $systemJobs['cartAbandonmentQueuedAt'] = '';
        $systemJobs['cartAbandonmentLastAttemptAt'] = gmdate('c');

        if (!$this->isCommerceFunnelEnabled($runtimeState)) {
            $this->saveJobState($plugin, $runtimeState, $integrationSettings, $systemJobs, 'Commerce funnel not enabled.');
            return;
        }

        if (!$plugin->canDispatchToBurrow($runtimeState)) {
            $this->saveJobState($plugin, $runtimeState, $integrationSettings, $systemJobs, 'Missing Burrow connection/routing context.');
            return;
        }

        $orderClass = '\craft\commerce\elements\Order';
        if (!class_exists($orderClass)) {
            $this->saveJobState($plugin, $runtimeState, $integrationSettings, $systemJobs, 'Commerce not available.');
            return;
        }

        $commerceConfig = is_array($runtimeState['integrationSettings']['commerce'] ?? null)
            ? $runtimeState['integrationSettings']['commerce']
            : [];
        $thresholdMinutes = max(30, (int)($commerceConfig['cartAbandonmentThresholdMinutes'] ?? 120));
        $cutoff = new \DateTimeImmutable('-' . $thresholdMinutes . ' minutes', new \DateTimeZone('UTC'));

        $abandonedCarts = $this->findAbandonedCarts($orderClass, $cutoff);

        $published = 0;
        $skipped = 0;
        $failed = 0;

        foreach ($abandonedCarts as $cart) {
            $result = $this->processAbandonedCart($plugin, $cart, $runtimeState, $thresholdMinutes);
            if ($result === 'published') {
                $published++;
            } elseif ($result === 'skipped') {
                $skipped++;
            } else {
                $failed++;
            }
        }

        $error = $failed > 0
            ? "Published {$published}, skipped {$skipped}, failed {$failed}"
            : '';
        $systemJobs['cartAbandonmentLastRunAt'] = gmdate('c');
        $this->saveJobState($plugin, $runtimeState, $integrationSettings, $systemJobs, $error);

        $plugin->getLogs()->log(
            $failed === 0 ? 'info' : 'warning',
            "Cart abandonment scan: {$published} published, {$skipped} skipped, {$failed} failed",
            'commerce',
            'ecommerce',
            null,
            ['carts_scanned' => count($abandonedCarts), 'threshold_minutes' => $thresholdMinutes]
        );
    }

    /**
     * @return array<int,object>
     */
    private function findAbandonedCarts(string $orderClass, \DateTimeImmutable $cutoff): array
    {
        try {
            $query = $orderClass::find();
            if (method_exists($query, 'isCompleted')) {
                $query->isCompleted(false);
            }
            $query->dateUpdated('< ' . $cutoff->format('Y-m-d H:i:s'));
            $query->limit(200);
            $query->orderBy(['dateUpdated' => SORT_DESC]);
            $orders = $query->all();
        } catch (\Throwable) {
            return [];
        }

        $carts = [];
        foreach ($orders as $order) {
            if (!is_object($order)) {
                continue;
            }
            if (isset($order->isCompleted) && $order->isCompleted) {
                continue;
            }
            if (isset($order->dateOrdered) && $order->dateOrdered !== null) {
                continue;
            }

            $lineItems = [];
            if (method_exists($order, 'getLineItems')) {
                $lineItems = (array)$order->getLineItems();
            }
            if (empty($lineItems)) {
                continue;
            }

            $carts[] = $order;
        }

        return $carts;
    }

    private function processAbandonedCart(object $plugin, object $cart, array $runtimeState, int $thresholdMinutes): string
    {
        $cartNumber = $this->stringValue($cart, ['number', 'shortNumber', 'reference']);
        if ($cartNumber === '') {
            return 'skipped';
        }

        $externalEntityId = 'cc_cart_' . $cartNumber;
        $lifecycleKey = 'lifecycle_cart_abandoned_' . $externalEntityId;

        if ($plugin->getQueue()->wasSent($lifecycleKey)) {
            return 'skipped';
        }

        $currency = $this->stringValue($cart, ['paymentCurrency', 'currency']);
        if ($currency === '') {
            $currency = 'USD';
        }

        $cartTotal = $this->floatValue($cart, ['totalPrice', 'total', 'itemSubtotal']);
        $cartItemCount = 0;
        if (method_exists($cart, 'getLineItems')) {
            $cartItemCount = count((array)$cart->getLineItems());
        }
        if ($cartItemCount <= 0) {
            $cartItemCount = max(0, (int)round($this->floatValue($cart, ['totalQty', 'totalQuantity'])));
        }

        $customerToken = $this->extractCustomerToken($cart);

        $dateUpdated = null;
        if (isset($cart->dateUpdated) && $cart->dateUpdated instanceof \DateTimeInterface) {
            $dateUpdated = $cart->dateUpdated;
        } elseif (isset($cart->dateCreated) && $cart->dateCreated instanceof \DateTimeInterface) {
            $dateUpdated = $cart->dateCreated;
        }
        $minutesSinceLastActivity = 0;
        if ($dateUpdated !== null) {
            $now = new \DateTimeImmutable('now', new \DateTimeZone('UTC'));
            $minutesSinceLastActivity = max(0, (int)round(($now->getTimestamp() - $dateUpdated->getTimestamp()) / 60));
        }

        $tags = [
            'provider' => 'craft_commerce',
            'currency' => $currency,
        ];
        if ($customerToken !== '') {
            $tags['customerToken'] = $customerToken;
        }

        $eventEnvelope = $plugin->getBurrowApi()->buildEcommerceCartAbandonedEvent($runtimeState, [
            'externalEntityId' => $externalEntityId,
            'cartTotal' => $cartTotal,
            'cartItemCount' => $cartItemCount,
            'currency' => $currency,
            'minutesSinceLastActivity' => $minutesSinceLastActivity,
            'timestamp' => gmdate('c'),
            'tags' => $tags,
        ]);
        if (empty($eventEnvelope)) {
            return 'failed';
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
            $plugin->getQueue()->markSent($lifecycleKey, $eventEnvelope, $channel, $eventName);
            if ($customerToken !== '') {
                $this->recordAbandonmentSignal($customerToken);
            }
            return 'published';
        }

        $error = trim((string)($result['error'] ?? 'Cart abandonment publish failed.'));
        $plugin->getQueue()->markFailed($lifecycleKey, $eventEnvelope, $error, $channel, $eventName);
        return 'failed';
    }

    /**
     * Writes a sentinel into the outbox-sent table so that cart recovery
     * detection in CommerceTrackingService can look it up cheaply.
     */
    private function recordAbandonmentSignal(string $customerToken): void
    {
        try {
            Craft::$app->getDb()->createCommand()->upsert(
                '{{%burrow_outbox_sent}}',
                ['event_key' => 'abandonment_' . $customerToken, 'sent_at' => gmdate('Y-m-d H:i:s')],
                ['sent_at' => gmdate('Y-m-d H:i:s')]
            )->execute();
        } catch (\Throwable) {
        }
    }

    private function saveJobState(object $plugin, array $runtimeState, array $integrationSettings, array $systemJobs, string $error): void
    {
        $systemJobs['cartAbandonmentLastError'] = $error;
        $integrationSettings['systemJobs'] = $systemJobs;
        $runtimeState['integrationSettings'] = $integrationSettings;
        $plugin->getState()->saveState($runtimeState);
    }

    /**
     * @param array<string,mixed> $runtimeState
     */
    private function isCommerceFunnelEnabled(array $runtimeState): bool
    {
        $selected = array_values(array_filter(array_map('strval', (array)($runtimeState['selectedIntegrations'] ?? []))));
        if (!in_array('commerce', $selected, true)) {
            return false;
        }
        $commerceConfig = is_array($runtimeState['integrationSettings']['commerce'] ?? null)
            ? $runtimeState['integrationSettings']['commerce']
            : [];
        return (string)($commerceConfig['mode'] ?? 'off') === 'track' && !empty($commerceConfig['ecommerceFunnel']);
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

    /**
     * @param array<int,string> $keys
     */
    private function stringValue(object $source, array $keys): string
    {
        foreach ($keys as $key) {
            if (method_exists($source, 'get' . ucfirst($key))) {
                $text = trim((string)$source->{'get' . ucfirst($key)}());
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
        foreach ($keys as $key) {
            if (method_exists($source, 'get' . ucfirst($key))) {
                $value = (float)$source->{'get' . ucfirst($key)}();
                if ($value !== 0.0) {
                    return $value;
                }
            }
            if (isset($source->{$key})) {
                $value = (float)$source->{$key};
                if ($value !== 0.0) {
                    return $value;
                }
            }
        }
        return 0.0;
    }
}
