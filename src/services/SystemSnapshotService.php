<?php
namespace burrow\Burrow\services;

use Craft;
use craft\base\Component;

class SystemSnapshotService extends Component
{
    public const RECENT_PUBLISH_WINDOW_SECONDS = 900;

    /**
     * @param array<string,mixed> $runtimeState
     */
    public function snapshotLastRunTimestamp(array $runtimeState): int
    {
        $integrationSettings = is_array($runtimeState['integrationSettings'] ?? null) ? $runtimeState['integrationSettings'] : [];
        $systemJobs = is_array($integrationSettings['systemJobs'] ?? null) ? $integrationSettings['systemJobs'] : [];
        $value = trim((string)($systemJobs['snapshotLastRunAt'] ?? ''));
        if ($value === '') {
            return 0;
        }

        $timestamp = strtotime($value);

        return $timestamp === false ? 0 : (int)$timestamp;
    }

    /**
     * @param array<string,mixed> $runtimeState
     */
    public function wasPublishedRecently(array $runtimeState, ?int $windowSeconds = null): bool
    {
        $lastRun = $this->snapshotLastRunTimestamp($runtimeState);
        if ($lastRun === 0) {
            return false;
        }

        $window = $windowSeconds ?? self::RECENT_PUBLISH_WINDOW_SECONDS;

        return (time() - $lastRun) < $window;
    }

    /**
     * @return array<string,mixed>
     */
    public function collectSnapshot(): array
    {
        $updates = Craft::$app->getUpdates()->getUpdates(true);
        $cmsLatest = $updates->cms->getLatest()?->version ?? Craft::$app->getVersion();
        $plugins = \burrow\Burrow\Plugin::getInstance()->getIntegrations()->collectPluginVersionSnapshot();
        $updateCount = 0;
        foreach ($plugins as $plugin) {
            if (!empty($plugin['updateAvailable'])) {
                $updateCount++;
            }
        }
        if (version_compare($cmsLatest, Craft::$app->getVersion(), '>')) {
            $updateCount++;
        }

        return [
            'cms' => [
                'name' => 'craft',
                'version' => Craft::$app->getVersion(),
                'latestVersion' => $cmsLatest,
                'updateAvailable' => version_compare($cmsLatest, Craft::$app->getVersion(), '>'),
            ],
            'runtime' => [
                'php' => PHP_VERSION,
                'database' => Craft::$app->getDb()->getServerVersion(),
            ],
            'plugins' => $plugins,
            'updatesAvailable' => $updateCount,
            'totalPlugins' => count($plugins),
            'capturedAt' => gmdate('c'),
        ];
    }
}
