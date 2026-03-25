<?php
namespace burrow\Burrow\controllers;

use Craft;
use craft\web\Controller;
use yii\web\Response;

use burrow\Burrow\Plugin;

class ApiController extends Controller
{
    protected array|bool|int $allowAnonymous = ['stack-snapshot'];

    public function beforeAction($action): bool
    {
        if ($action->id === 'stack-snapshot') {
            $this->enableCsrfValidation = false;
        }

        return parent::beforeAction($action);
    }

    public function actionStackSnapshot(): Response
    {
        $this->requirePostRequest();

        if (!$this->authenticateBearerToken()) {
            return $this->jsonResponse(['ok' => false, 'error' => 'unauthorized'], 401);
        }

        if ($this->isRateLimited()) {
            return $this->jsonResponse(['ok' => false, 'error' => 'rate_limited'], 429);
        }

        $plugin = Plugin::getInstance();
        $runtimeState = $plugin->getState()->getState();

        if (!$plugin->canDispatchToBurrow($runtimeState)) {
            return $this->jsonResponse(['ok' => false, 'error' => 'not_configured'], 422);
        }

        $runtimeState['lastSnapshot'] = $plugin->getSnapshot()->collectSnapshot();

        $result = $plugin->getBurrowApi()->publishSystemSnapshot(
            $plugin->getBurrowBaseUrl(),
            $plugin->getBurrowApiKey(),
            $runtimeState,
            $runtimeState['lastSnapshot']
        );

        $integrationSettings = is_array($runtimeState['integrationSettings'] ?? null) ? $runtimeState['integrationSettings'] : [];
        $systemJobs = is_array($integrationSettings['systemJobs'] ?? null) ? $integrationSettings['systemJobs'] : [];

        if ($result['ok']) {
            $systemJobs['snapshotLastRunAt'] = gmdate('c');
            $systemJobs['snapshotLastError'] = '';
            $plugin->getLogs()->log('info', 'Snapshot published via API request', 'system', 'system');
        } else {
            $systemJobs['snapshotLastError'] = (string)$result['error'];
            $plugin->getLogs()->log('warning', 'Snapshot publish via API request failed', 'system', 'system', null, [
                'error' => $result['error'],
            ]);
        }

        $integrationSettings['systemJobs'] = $systemJobs;
        $runtimeState['integrationSettings'] = $integrationSettings;
        $plugin->getState()->saveState($runtimeState);

        if (!$result['ok']) {
            return $this->jsonResponse(['ok' => false, 'error' => 'publish_failed'], 502);
        }

        return $this->jsonResponse(['ok' => true]);
    }

    /**
     * @param array<string,mixed> $data
     */
    private function jsonResponse(array $data, int $statusCode = 200): Response
    {
        Craft::$app->getResponse()->setStatusCode($statusCode);
        return $this->asJson($data);
    }

    private function authenticateBearerToken(): bool
    {
        $header = (string)Craft::$app->getRequest()->getHeaders()->get('authorization', '');

        if (stripos($header, 'Bearer ') !== 0) {
            return false;
        }

        $token = trim(substr($header, 7));
        if ($token === '') {
            return false;
        }

        $runtimeState = Plugin::getInstance()->getState()->getState();
        $ingestionKey = trim((string)(
            is_array($runtimeState['ingestionKey'] ?? null)
                ? ($runtimeState['ingestionKey']['key'] ?? '')
                : ''
        ));

        if ($ingestionKey === '') {
            return false;
        }

        return hash_equals($ingestionKey, $token);
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
