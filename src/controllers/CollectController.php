<?php
namespace burrow\Burrow\controllers;

use Craft;
use craft\web\Controller;
use yii\web\Response;

use burrow\Burrow\Plugin;

/**
 * Same-origin relay for the headless-Shopify frontend collector.
 *
 * POST /actions/burrow/collect — CSRF-protected JSON carrying only funnel
 * telemetry (product/variant identifiers, quantity, price, currency). The
 * payload is validated/clamped server-side before relay to Burrow with the
 * per-project ingestion key; no Burrow credential is ever exposed to the page.
 */
class CollectController extends Controller
{
    private const MAX_BODY_BYTES = 8192;
    private const RATE_LIMIT_WINDOW_SECONDS = 60;
    private const RATE_LIMIT_MAX_EVENTS = 30;

    protected array|bool|int $allowAnonymous = ['index'];

    public function actionIndex(): Response
    {
        $this->requirePostRequest();

        $plugin = Plugin::getInstance();
        $siteId = 0;
        try {
            $siteId = (int)(Craft::$app->getSites()->getCurrentSite()->id ?? 0);
        } catch (\Throwable) {
            $siteId = (int)(Craft::$app->getSites()->getPrimarySite()?->id ?? 0);
        }
        $runtimeState = $siteId > 0
            ? $plugin->getState()->getSiteState($siteId)
            : $plugin->getState()->getState();
        if (
            empty($runtimeState['enabled'])
            || !$plugin->getShopifyTracking()->isShopifyFunnelEnabled($runtimeState)
            || !$plugin->canDispatchToBurrow($runtimeState)
        ) {
            return $this->jsonResponse(['ok' => false, 'error' => 'not_enabled'], 404);
        }

        if ($this->isRateLimited()) {
            return $this->jsonResponse(['ok' => false, 'error' => 'rate_limited'], 429);
        }

        $rawBody = (string)Craft::$app->getRequest()->getRawBody();
        if ($rawBody === '' || strlen($rawBody) > self::MAX_BODY_BYTES) {
            return $this->jsonResponse(['ok' => false, 'error' => 'invalid_payload'], 400);
        }

        $decoded = json_decode($rawBody, true);
        if (!is_array($decoded)) {
            return $this->jsonResponse(['ok' => false, 'error' => 'invalid_payload'], 400);
        }

        $normalized = $plugin->getShopifyTracking()->normalizeCollectedPayload($decoded);
        if ($normalized === null) {
            return $this->jsonResponse(['ok' => false, 'error' => 'invalid_event_type'], 400);
        }

        $result = $plugin->getShopifyTracking()->handleCollectedCartEvent($normalized);
        if (!$result['ok'] && $result['error'] === 'not_enabled') {
            return $this->jsonResponse(['ok' => false, 'error' => 'not_enabled'], 404);
        }

        // Delivery failures land in the outbox with automatic retries; from the
        // browser's perspective the event is accepted either way.
        return $this->jsonResponse(['ok' => true], 202);
    }

    /**
     * @param array<string,mixed> $data
     */
    private function jsonResponse(array $data, int $statusCode = 200): Response
    {
        Craft::$app->getResponse()->setStatusCode($statusCode);

        return $this->asJson($data);
    }

    /**
     * Bounded per-visitor noise: a fixed 60s window keyed by session (fallback: IP).
     */
    private function isRateLimited(): bool
    {
        $identity = '';
        try {
            $session = Craft::$app->getSession();
            if ($session->getIsActive() || $session->getHasSessionId()) {
                $identity = (string)$session->getId();
            }
        } catch (\Throwable) {
        }
        if ($identity === '') {
            $identity = (string)Craft::$app->getRequest()->getUserIP();
        }
        if ($identity === '') {
            return true;
        }

        $cache = Craft::$app->getCache();
        $cacheKey = 'burrow_collect_rate_' . hash('sha256', $identity);
        $count = $cache->get($cacheKey);
        $count = is_int($count) ? $count : 0;
        if ($count >= self::RATE_LIMIT_MAX_EVENTS) {
            return true;
        }

        $cache->set($cacheKey, $count + 1, self::RATE_LIMIT_WINDOW_SECONDS);

        return false;
    }
}
