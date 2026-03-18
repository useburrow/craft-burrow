<?php
namespace burrow\Burrow\models;

use craft\base\Model;

class Settings extends Model
{
    public string $pluginName = 'Burrow';
    public string $baseUrl = 'https://api.useburrow.com';
    public string $apiKey = '';

    public function defineRules(): array
    {
        return [
            [['pluginName', 'baseUrl', 'apiKey'], 'string'],
        ];
    }
}
