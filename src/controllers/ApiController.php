<?php
namespace burrow\Burrow\controllers;

use Craft;
use craft\web\Controller;
use yii\web\Response;

use burrow\Burrow\Plugin;

/**
 * Anonymous Burrow callback endpoints (system snapshot refresh).
 *
 * @author Burrow Analytics, LLC
 * @since 5.0.0
 */
class ApiController extends Controller
{
    // =========================================================================
    // Properties
    // =========================================================================

    /**
     * @var array|bool|int
     */
    protected array|bool|int $allowAnonymous = ['stack-snapshot'];

    // =========================================================================
    // Public Methods
    // =========================================================================

    /**
     * @inheritdoc
     */
    public function beforeAction($action): bool
    {
        if ($action->id === 'stack-snapshot') {
            $this->enableCsrfValidation = false;
        }

        return parent::beforeAction($action);
    }

    /**
     * Publishes a system stack snapshot for the linked site matching the Bearer ingestion key.
     *
     * @return Response
     *
     * @author Burrow Analytics, LLC
     * @since 5.0.0
     */
    public function actionStackSnapshot(): Response
    {
        $this->requirePostRequest();

        $siteRuntime = $this->authenticateBearerToken();
        if ($siteRuntime === null) {
            return $this->jsonResponse(['ok' => false, 'error' => 'unauthorized'], 401);
        }

        if ($this->isRateLimited()) {
            return $this->jsonResponse(['ok' => false, 'error' => 'rate_limited'], 429);
        }

        $plugin = Plugin::getInstance();
        if (!$plugin->canDispatchToBurrow($siteRuntime)) {
            return $this->jsonResponse(['ok' => false, 'error' => 'not_configured'], 422);
        }

        $siteRuntime['lastSnapshot'] = $plugin->getSnapshot()->collectSnapshot();

        $result = $plugin->getBurrowApi()->publishSystemSnapshot(
            $plugin->getBurrowBaseUrl(),
            $plugin->getBurrowApiKey(),
            $siteRuntime,
            $siteRuntime['lastSnapshot']
        );

        $integrationSettings = is_array($siteRuntime['integrationSettings'] ?? null) ? $siteRuntime['integrationSettings'] : [];
        $systemJobs = is_array($integrationSettings['systemJobs'] ?? null) ? $integrationSettings['systemJobs'] : [];

        if ($result['ok']) {
            $systemJobs['snapshotLastRunAt'] = gmdate('c');
            $systemJobs['snapshotLastError'] = '';
            $plugin->getLogs()->log('info', 'Snapshot published via API request', 'system', 'system', null, [
                'siteId' => (int)($siteRuntime['craftSiteId'] ?? 0),
            ]);
        } else {
            $systemJobs['snapshotLastError'] = (string)$result['error'];
            $plugin->getLogs()->log('warning', 'Snapshot publish via API request failed', 'system', 'system', null, [
                'error' => $result['error'],
                'siteId' => (int)($siteRuntime['craftSiteId'] ?? 0),
            ]);
        }

        $integrationSettings['systemJobs'] = $systemJobs;
        $siteId = (int)($siteRuntime['craftSiteId'] ?? 0);
        if ($siteId > 0) {
            $plugin->getState()->saveSiteState($siteId, [
                'lastSnapshot' => (array)($siteRuntime['lastSnapshot'] ?? []),
            ], [
                'integrationSettings' => $integrationSettings,
            ]);
        } else {
            $runtimeState = $plugin->getState()->getState();
            $runtimeState['lastSnapshot'] = $siteRuntime['lastSnapshot'];
            $runtimeState['integrationSettings'] = $integrationSettings;
            $plugin->getState()->saveState($runtimeState);
        }

        if (!$result['ok']) {
            return $this->jsonResponse(['ok' => false, 'error' => 'publish_failed'], 502);
        }

        return $this->jsonResponse(['ok' => true]);
    }

    // =========================================================================
    // Private Methods
    // =========================================================================

    /**
     * @param array<string,mixed> $data
     */
    private function jsonResponse(array $data, int $statusCode = 200): Response
    {
        Craft::$app->getResponse()->setStatusCode($statusCode);
        return $this->asJson($data);
    }

    /**
     * Authenticates against any linked site's ingestion key and returns that site's merged state.
     *
     * @return array<string,mixed>|null
     */
    private function authenticateBearerToken(): ?array
    {
        $header = (string)Craft::$app->getRequest()->getHeaders()->get('authorization', '');

        if (stripos($header, 'Bearer ') !== 0) {
            return null;
        }

        $token = trim(substr($header, 7));
        if ($token === '') {
            return null;
        }

        return Plugin::getInstance()->getState()->findSiteStateByIngestionKey($token);
    }

    private function isRateLimited(): bool
    {
        $cacheKey = 'burrow_stack_snapshot_refresh_lock';
        $cache = Craft::$app->getCache();

        if ($cache->get($cacheKey) !== false) {
            return true;
        }

        $cache->set($cacheKey, 1, 60);
        return false;
    }
}
