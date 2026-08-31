<?php
namespace burrow\Burrow\models;

use craft\base\Model;
use craft\behaviors\EnvAttributeParserBehavior;

/**
 * Plugin settings model (project config).
 *
 * `baseUrl` and `apiKey` are environmental settings: they may store a literal value
 * or an environment variable reference such as `$BURROW_API_KEY`.
 *
 * @author Burrow Analytics, LLC
 * @since 5.0.0
 */
class Settings extends Model
{
    /**
     * @var string Display name in the Control Panel.
     */
    public string $pluginName = 'Burrow';

    /**
     * @var string Burrow API base URL, or an environment variable reference (e.g. `$BURROW_BASE_URL`).
     */
    public string $baseUrl = 'https://app.useburrow.com';

    /**
     * @var string Organization API key, or an environment variable reference (e.g. `$BURROW_API_KEY`).
     */
    public string $apiKey = '';

    /**
     * @inheritdoc
     */
    public function behaviors(): array
    {
        return [
            'parser' => [
                'class' => EnvAttributeParserBehavior::class,
                'attributes' => ['baseUrl', 'apiKey'],
            ],
        ];
    }

    /**
     * @inheritdoc
     */
    public function defineRules(): array
    {
        return [
            [['pluginName', 'baseUrl', 'apiKey'], 'string'],
        ];
    }
}
