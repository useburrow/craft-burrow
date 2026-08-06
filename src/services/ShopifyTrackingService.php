<?php
namespace burrow\Burrow\services;

use burrow\Burrow\Plugin;
use Craft;
use craft\base\Component;
use craft\helpers\App;

/**
 * Headless Shopify funnel capture: the Craft frontend collects cart interactions
 * (Shopify cart/add form posts or Storefront-API carts) and relays them through
 * the plugin to Burrow. Checkout and order events are intentionally NOT emitted
 * here — the Shopify checkout pixel and Admin API integration own those stages,
 * so the funnel composes without double counting.
 */
class ShopifyTrackingService extends Component
{
    public const EVENT_TYPES = ['cart.added', 'cart.removed'];

    public function isShopifyPluginInstalled(): bool
    {
        $plugins = Craft::$app->getPlugins();

        return $plugins->isPluginInstalled('shopify') && $plugins->isPluginEnabled('shopify');
    }

    public function isCommercePluginInstalled(): bool
    {
        $plugins = Craft::$app->getPlugins();

        return $plugins->isPluginInstalled('commerce') && $plugins->isPluginEnabled('commerce');
    }

    /**
     * Headless-Shopify mode is auto-suggested when craftcms/shopify is installed and Craft Commerce is not.
     */
    public function isHeadlessShopifySuggested(): bool
    {
        return $this->isShopifyPluginInstalled() && !$this->isCommercePluginInstalled();
    }

    /**
     * @param array<string,mixed> $runtimeState
     * @return array<string,mixed>
     */
    public function getShopifyConfig(array $runtimeState): array
    {
        $integrationSettings = is_array($runtimeState['integrationSettings'] ?? null) ? $runtimeState['integrationSettings'] : [];

        return is_array($integrationSettings['shopify'] ?? null) ? $integrationSettings['shopify'] : [];
    }

    /**
     * @param array<string,mixed> $runtimeState
     */
    public function isShopifyTrackingEnabled(array $runtimeState): bool
    {
        $selected = array_values(array_filter(array_map('strval', (array)($runtimeState['selectedIntegrations'] ?? []))));
        if (!in_array('shopify', $selected, true)) {
            return false;
        }

        return (string)($this->getShopifyConfig($runtimeState)['mode'] ?? 'off') === 'track';
    }

    /**
     * Collector gate: local funnel opt-in AND the effective `ecommerce_funnel` capability
     * from the last onboarding link/sync (Burrow persists it to the ecommerce ProjectSource).
     *
     * @param array<string,mixed> $runtimeState
     */
    public function isShopifyFunnelEnabled(array $runtimeState): bool
    {
        if (!$this->isShopifyTrackingEnabled($runtimeState)) {
            return false;
        }
        if (empty($this->getShopifyConfig($runtimeState)['ecommerceFunnel'])) {
            return false;
        }

        $capabilities = is_array($runtimeState['capabilities'] ?? null) ? $runtimeState['capabilities'] : [];
        if (array_key_exists('ecommerce_funnel', $capabilities) && empty($capabilities['ecommerce_funnel'])) {
            return false;
        }

        return true;
    }

    /**
     * Shop domain for event tagging: configured value first, then the craftcms/shopify plugin settings.
     *
     * @param array<string,mixed>|null $runtimeState
     */
    public function resolveShopDomain(?array $runtimeState = null): string
    {
        $runtimeState ??= Plugin::getInstance()->getState()->getState();
        $configured = $this->normalizeShopDomain((string)($this->getShopifyConfig($runtimeState)['shopDomain'] ?? ''));
        if ($configured !== '') {
            return $configured;
        }

        return $this->detectShopDomainFromShopifyPlugin();
    }

    /**
     * Reads the shop domain from the craftcms/shopify plugin settings (`hostName`), defensively.
     */
    public function detectShopDomainFromShopifyPlugin(): string
    {
        try {
            $shopify = Craft::$app->getPlugins()->getPlugin('shopify');
            if ($shopify === null || !method_exists($shopify, 'getSettings')) {
                return '';
            }
            $settings = $shopify->getSettings();
            if (!is_object($settings)) {
                return '';
            }
            foreach (['hostName', 'shopDomain', 'shopUrl'] as $attribute) {
                if (!isset($settings->{$attribute})) {
                    continue;
                }
                $value = $this->normalizeShopDomain((string)App::parseEnv((string)$settings->{$attribute}));
                if ($value !== '') {
                    return $value;
                }
            }
        } catch (\Throwable) {
        }

        return '';
    }

    public function normalizeShopDomain(string $value): string
    {
        $value = strtolower(trim($value));
        if ($value === '') {
            return '';
        }
        $value = (string)preg_replace('#^https?://#', '', $value);
        $value = trim(explode('/', $value)[0] ?? '');
        if ($value === '' || !preg_match('/^[a-z0-9][a-z0-9.-]{1,250}$/', $value)) {
            return '';
        }

        return $value;
    }

    /**
     * Whether the frontend collector script should be injected into site pages.
     *
     * @param array<string,mixed>|null $runtimeState
     */
    public function shouldInjectCollector(?array $runtimeState = null): bool
    {
        $plugin = Plugin::getInstance();
        $runtimeState ??= $plugin->getState()->getState();
        if (empty($runtimeState['onboardingCompleted'])) {
            return false;
        }
        if (!$this->isShopifyFunnelEnabled($runtimeState)) {
            return false;
        }

        return $plugin->canDispatchToBurrow($runtimeState);
    }

    /**
     * Validate and normalize a collector payload from the browser.
     * Returns null when the payload is not a relayable cart event.
     *
     * @param array<string,mixed> $raw
     * @return array<string,mixed>|null
     */
    public function normalizeCollectedPayload(array $raw): ?array
    {
        $type = strtolower(trim((string)($raw['type'] ?? '')));
        if (!in_array($type, self::EVENT_TYPES, true)) {
            return null;
        }

        $quantity = $this->clampFloat($raw['quantity'] ?? 1, 0.0, 10000.0);
        if ($quantity <= 0.0) {
            $quantity = 1.0;
        }
        $unitPrice = $this->clampFloat($raw['unitPrice'] ?? 0, 0.0, 1000000.0);
        $lineTotal = $this->clampFloat($raw['lineTotal'] ?? 0, 0.0, 10000000.0);
        if ($lineTotal <= 0.0 && $unitPrice > 0.0) {
            $lineTotal = round($unitPrice * $quantity, 2);
        }

        $currency = strtoupper(trim((string)($raw['currency'] ?? '')));
        if (!preg_match('/^[A-Z]{3}$/', $currency)) {
            $currency = 'USD';
        }

        $productId = $this->cleanString($raw['productId'] ?? '', 100);
        $productName = $this->cleanString($raw['productName'] ?? '', 200) ?: 'Item';
        $variantName = $this->cleanString($raw['variantName'] ?? '', 200) ?: $productName;

        $externalEventId = trim((string)($raw['externalEventId'] ?? ''));
        if (!preg_match('/^craftplugin:[A-Za-z0-9][A-Za-z0-9.:_-]{0,220}$/', $externalEventId)) {
            $host = parse_url((string)(Craft::$app->getSites()->getPrimarySite()?->baseUrl ?? ''), PHP_URL_HOST) ?: 'site';
            $externalEventId = 'craftplugin:' . $host . ':cart:' . bin2hex(random_bytes(16));
        }

        $customerToken = '';
        $customerId = trim((string)($raw['customerId'] ?? ''));
        if (preg_match('/^\d{1,32}$/', $customerId)) {
            $customerToken = 'shopify_cust_' . $customerId;
        } else {
            $emailHash = strtolower(trim((string)($raw['emailHash'] ?? '')));
            if (preg_match('/^[a-f0-9]{64}$/', $emailHash)) {
                $customerToken = 'shopify_guest_' . substr($emailHash, 0, 12);
            }
        }

        return [
            'type' => $type,
            'productId' => $productId,
            'productName' => $productName,
            'variantName' => $variantName,
            'quantity' => $quantity,
            'unitPrice' => $unitPrice,
            'lineTotal' => $lineTotal,
            'currency' => $currency,
            'cartTotal' => $this->clampFloat($raw['cartTotal'] ?? 0, 0.0, 10000000.0),
            'cartItemCount' => max(0, min(10000, (int)($raw['cartItemCount'] ?? 0))),
            'externalEventId' => $externalEventId,
            'customerToken' => $customerToken,
        ];
    }

    /**
     * Build the Burrow envelope for a normalized collector payload and deliver it
     * (immediate send in-request, Craft outbox retry on failure).
     *
     * @param array<string,mixed> $payload Result of {@see normalizeCollectedPayload()}
     * @return array{ok:bool,error:string}
     */
    public function handleCollectedCartEvent(array $payload): array
    {
        $plugin = Plugin::getInstance();
        $siteId = 0;
        try {
            $siteId = (int)(\Craft::$app->getSites()->getCurrentSite()->id ?? 0);
        } catch (\Throwable) {
            $siteId = (int)(\Craft::$app->getSites()->getPrimarySite()?->id ?? 0);
        }
        $runtimeState = $siteId > 0
            ? $plugin->getState()->getSiteState($siteId)
            : $plugin->getState()->getState();
        if (empty($runtimeState['enabled']) || !$this->isShopifyFunnelEnabled($runtimeState)) {
            return ['ok' => false, 'error' => 'not_enabled'];
        }

        $tags = [
            'provider' => 'shopify',
            'currency' => (string)$payload['currency'],
        ];
        $customerToken = trim((string)($payload['customerToken'] ?? ''));
        if ($customerToken !== '') {
            $tags['customerToken'] = $customerToken;
        }

        $builderPayload = [
            'productId' => (string)$payload['productId'],
            'productName' => (string)$payload['productName'],
            'variantName' => (string)$payload['variantName'],
            'quantity' => (float)$payload['quantity'],
            'unitPrice' => (float)$payload['unitPrice'],
            'lineTotal' => (float)$payload['lineTotal'],
            'currency' => (string)$payload['currency'],
            'cartTotal' => (float)($payload['cartTotal'] ?? 0),
            'cartItemCount' => (int)($payload['cartItemCount'] ?? 0),
            'timestamp' => gmdate('c'),
            'tags' => $tags,
        ];

        $envelope = $payload['type'] === 'cart.removed'
            ? $plugin->getBurrowApi()->buildEcommerceCartItemRemovedEvent($runtimeState, $builderPayload)
            : $plugin->getBurrowApi()->buildEcommerceCartItemAddedEvent($runtimeState, $builderPayload);
        if (empty($envelope)) {
            return ['ok' => false, 'error' => 'envelope_build_failed'];
        }

        // The SDK cart builders whitelist tags and ignore externalEventId, so stamp both after build.
        $envelope['externalEventId'] = (string)$payload['externalEventId'];
        $shopDomain = $this->resolveShopDomain($runtimeState);
        if ($shopDomain !== '') {
            $envelope['tags'] = is_array($envelope['tags'] ?? null) ? $envelope['tags'] : [];
            $envelope['tags']['shopDomain'] = $shopDomain;
        }
        if ($siteId > 0) {
            $envelope['_burrowSiteId'] = $siteId;
        }
        $projectId = trim((string)($runtimeState['projectId'] ?? ''));
        if ($projectId !== '') {
            $envelope['_burrowProjectId'] = $projectId;
        }

        $eventKey = 'shopify_' . (string)$payload['externalEventId'];
        if ($plugin->getQueue()->wasSent($eventKey)) {
            return ['ok' => true, 'error' => ''];
        }

        $result = $plugin->getBurrowApi()->publishEvents(
            $plugin->getBurrowBaseUrl(),
            $plugin->getBurrowApiKey(),
            $runtimeState,
            [$envelope]
        );

        $channel = trim((string)($envelope['channel'] ?? ''));
        $eventName = trim((string)($envelope['event'] ?? ''));
        if ($result['ok']) {
            $plugin->getQueue()->markSent($eventKey, $envelope, $channel, $eventName);

            return ['ok' => true, 'error' => ''];
        }

        $error = trim((string)($result['error'] ?? 'Shopify cart event publish failed.'));
        $plugin->getQueue()->markFailed($eventKey, $envelope, $error, $channel, $eventName);

        return ['ok' => false, 'error' => $error];
    }

    private function cleanString(mixed $value, int $maxLength): string
    {
        $text = trim((string)(is_scalar($value) ? $value : ''));
        if ($text === '') {
            return '';
        }
        $text = (string)preg_replace('/[\x00-\x1F\x7F]/', '', $text);

        return function_exists('mb_substr') ? mb_substr($text, 0, $maxLength) : substr($text, 0, $maxLength);
    }

    private function clampFloat(mixed $value, float $min, float $max): float
    {
        if (!is_int($value) && !is_float($value) && !(is_string($value) && is_numeric(trim($value)))) {
            return $min;
        }

        return max($min, min($max, (float)$value));
    }
}
