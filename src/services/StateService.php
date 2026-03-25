<?php
namespace burrow\Burrow\services;

use craft\base\Component;

use burrow\Burrow\helpers\CredentialCrypto;
use burrow\Burrow\records\RuntimeStateRecord;

class StateService extends Component
{
    /**
     * @return array<string,mixed>
     */
    public function getState(): array
    {
        $record = RuntimeStateRecord::find()->one();
        if (!$record) {
            return $this->defaultState();
        }

        $ingestionStored = is_array($record->ingestionKey) ? $record->ingestionKey : ['key' => '', 'projectId' => '', 'keyPrefix' => ''];

        return array_merge($this->defaultState(), [
            'projectId' => (string)($record->projectId ?? ''),
            'clientId' => (string)($record->clientId ?? ''),
            'organizationId' => (string)($record->organizationId ?? ''),
            'projectSourceId' => (string)($record->projectSourceId ?? ''),
            'sourceIds' => is_array($record->sourceIds) ? $record->sourceIds : ['forms' => '', 'ecommerce' => '', 'system' => ''],
            'sdkState' => is_array($record->sdkState) ? $record->sdkState : [],
            'ingestionKey' => [
                'key' => CredentialCrypto::unseal((string)($ingestionStored['key'] ?? ''), CredentialCrypto::INFO_INGESTION_KEY),
                'projectId' => (string)($ingestionStored['projectId'] ?? ''),
                'keyPrefix' => (string)($ingestionStored['keyPrefix'] ?? ''),
            ],
            'burrowProject' => is_array($record->burrowProject) ? $record->burrowProject : ['name' => '', 'path' => '', 'url' => ''],
            'selectedIntegrations' => is_array($record->selectedIntegrations) ? $record->selectedIntegrations : [],
            'capabilities' => is_array($record->capabilities) ? $record->capabilities : ['forms' => [], 'ecommerce' => [], 'ecommerce_funnel' => false],
            'integrationSettings' => is_array($record->integrationSettings) ? $record->integrationSettings : [],
            'lastSnapshot' => is_array($record->lastSnapshot) ? $record->lastSnapshot : [],
            'onboardingStep' => (string)($record->onboardingStep ?? 'connection'),
            'onboardingCompleted' => (bool)($record->onboardingCompleted ?? false),
            'connectionBaseUrl' => (string)($record->connectionBaseUrl ?? ''),
            'connectionApiKey' => CredentialCrypto::unseal((string)($record->connectionApiKey ?? ''), CredentialCrypto::INFO_CONNECTION_API_KEY),
        ]);
    }

    /**
     * @param array<string,mixed> $state
     */
    public function saveState(array $state): bool
    {
        $record = RuntimeStateRecord::find()->one();
        if (!$record) {
            $record = new RuntimeStateRecord();
        }

        $record->projectId = (string)($state['projectId'] ?? '');
        $record->clientId = (string)($state['clientId'] ?? '');
        $record->organizationId = (string)($state['organizationId'] ?? '');
        $record->projectSourceId = (string)($state['projectSourceId'] ?? '');
        $record->sourceIds = (array)($state['sourceIds'] ?? ['forms' => '', 'ecommerce' => '', 'system' => '']);
        $record->sdkState = (array)($state['sdkState'] ?? []);
        $ingestionPlain = (array)($state['ingestionKey'] ?? ['key' => '', 'projectId' => '', 'keyPrefix' => '']);
        $record->ingestionKey = [
            'key' => CredentialCrypto::seal(trim((string)($ingestionPlain['key'] ?? '')), CredentialCrypto::INFO_INGESTION_KEY),
            'projectId' => (string)($ingestionPlain['projectId'] ?? ''),
            'keyPrefix' => (string)($ingestionPlain['keyPrefix'] ?? ''),
        ];
        $record->burrowProject = (array)($state['burrowProject'] ?? ['name' => '', 'path' => '', 'url' => '']);
        $record->selectedIntegrations = array_values(array_map('strval', (array)($state['selectedIntegrations'] ?? [])));
        $record->capabilities = (array)($state['capabilities'] ?? ['forms' => [], 'ecommerce' => [], 'ecommerce_funnel' => false]);
        $record->integrationSettings = (array)($state['integrationSettings'] ?? []);
        $record->lastSnapshot = (array)($state['lastSnapshot'] ?? []);
        $record->onboardingStep = (string)($state['onboardingStep'] ?? 'connection');
        $record->onboardingCompleted = (bool)($state['onboardingCompleted'] ?? false);
        $record->connectionBaseUrl = (string)($state['connectionBaseUrl'] ?? '');
        $record->connectionApiKey = CredentialCrypto::seal(trim((string)($state['connectionApiKey'] ?? '')), CredentialCrypto::INFO_CONNECTION_API_KEY);

        return (bool)$record->save();
    }

    /**
     * @return array<string,mixed>
     */
    private function defaultState(): array
    {
        return [
            'projectId' => '',
            'clientId' => '',
            'organizationId' => '',
            'projectSourceId' => '',
            'sourceIds' => ['forms' => '', 'ecommerce' => '', 'system' => ''],
            'sdkState' => [],
            'ingestionKey' => ['key' => '', 'projectId' => '', 'keyPrefix' => ''],
            'burrowProject' => ['name' => '', 'path' => '', 'url' => ''],
            'selectedIntegrations' => [],
            'capabilities' => ['forms' => [], 'ecommerce' => [], 'ecommerce_funnel' => false],
            'integrationSettings' => [],
            'lastSnapshot' => [],
            'onboardingStep' => 'connection',
            'onboardingCompleted' => false,
            'connectionBaseUrl' => '',
            'connectionApiKey' => '',
        ];
    }
}
