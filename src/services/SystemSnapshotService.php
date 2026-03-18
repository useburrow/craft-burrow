<?php
namespace burrow\Burrow\services;

use Craft;
use craft\base\Component;

class SystemSnapshotService extends Component
{
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
