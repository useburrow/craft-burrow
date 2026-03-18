<?php
namespace burrow\Burrow\services;

use Craft;
use craft\base\Component;

class BurrowApiService extends Component
{
    public function isSdkAvailable(): bool
    {
        return class_exists('\Burrow\Sdk\Client\BurrowClient');
    }

    /**
     * @return array{ok:bool,error:string,projects:array<int,array<string,mixed>>,raw:array<string,mixed>}
     */
    public function discover(string $baseUrl, string $apiKey, array $capabilities = []): array
    {
        try {
            $client = $this->createClient($baseUrl, $apiKey, [], [], false, true);
            $request = new \Burrow\Sdk\Contracts\OnboardingDiscoveryRequest(
                site: $this->buildSitePayload(),
                capabilities: $capabilities
            );
            $response = $client->discover($request);
            $body = is_array($response->body) ? $response->body : [];

            return [
                'ok' => $response->status >= 200 && $response->status < 300,
                'error' => '',
                'projects' => $this->extractProjects($body),
                'raw' => $body,
            ];
        } catch (\Throwable $e) {
            return [
                'ok' => false,
                'error' => $e->getMessage(),
                'projects' => [],
                'raw' => [],
            ];
        }
    }

    /**
     * @param array<string,string> $selection
     * @return array{ok:bool,error:string,routing:array<string,mixed>,project:array<string,mixed>,ingestionKey:array<string,mixed>,sdkState:array<string,mixed>}
     */
    public function link(string $baseUrl, string $apiKey, array $selection, array $capabilities = [], array $runtimeState = []): array
    {
        try {
            $client = $this->createClient($baseUrl, $apiKey, $runtimeState['sdkState'] ?? [], $runtimeState['ingestionKey'] ?? [], false, true);
            $request = new \Burrow\Sdk\Contracts\OnboardingLinkRequest(
                site: $this->buildSitePayload(),
                selection: $selection,
                platform: 'craft',
                capabilities: $capabilities
            );
            $response = $client->link($request);
            $project = $response->project;
            $ingestion = $response->ingestionKey;

            return [
                'ok' => true,
                'error' => '',
                'routing' => is_array($response->routing) ? $response->routing : [],
                'project' => [
                    'id' => $project?->id ?? '',
                    'name' => $project?->name ?? '',
                    'slug' => $project?->slug ?? '',
                    'burrowProjectPath' => $project?->burrowProjectPath ?? '',
                    'burrowProjectUrl' => $project?->burrowProjectUrl ?? '',
                ],
                'ingestionKey' => [
                    'key' => $ingestion?->key ?? '',
                    'projectId' => $ingestion?->projectId ?? '',
                    'keyPrefix' => $ingestion?->keyPrefix ?? '',
                ],
                'sdkState' => $client->getState()->toArray(),
            ];
        } catch (\Throwable $e) {
            return [
                'ok' => false,
                'error' => $e->getMessage(),
                'routing' => [],
                'project' => [],
                'ingestionKey' => [],
                'sdkState' => [],
            ];
        }
    }

    /**
     * @param array<string,mixed> $linkResult
     */
    public function applyLinkResult(array $runtimeState, array $linkResult): array
    {
        $routing = is_array($linkResult['routing'] ?? null) ? $linkResult['routing'] : [];
        $project = is_array($linkResult['project'] ?? null) ? $linkResult['project'] : [];
        $ingestionKey = is_array($linkResult['ingestionKey'] ?? null) ? $linkResult['ingestionKey'] : [];
        $sdkState = is_array($linkResult['sdkState'] ?? null) ? $linkResult['sdkState'] : [];

        if (!empty($sdkState)) {
            $runtimeState['sdkState'] = $sdkState;
        }

        if (!empty($routing['organizationId'])) {
            $runtimeState['organizationId'] = (string)$routing['organizationId'];
        }
        if (!empty($routing['clientId'])) {
            $runtimeState['clientId'] = (string)$routing['clientId'];
        }
        if (!empty($routing['projectId'])) {
            $runtimeState['projectId'] = (string)$routing['projectId'];
        }
        if (!empty($routing['projectSourceId'])) {
            $runtimeState['projectSourceId'] = (string)$routing['projectSourceId'];
        }
        if (!empty($routing['sourceIds']) && is_array($routing['sourceIds'])) {
            $runtimeState['sourceIds'] = array_merge(
                is_array($runtimeState['sourceIds'] ?? null) ? $runtimeState['sourceIds'] : [],
                $routing['sourceIds']
            );
        }

        $runtimeState['ingestionKey'] = [
            'key' => (string)($ingestionKey['key'] ?? ''),
            'projectId' => (string)($ingestionKey['projectId'] ?? ''),
            'keyPrefix' => (string)($ingestionKey['keyPrefix'] ?? ''),
        ];

        $runtimeState['burrowProject'] = [
            'path' => (string)($project['burrowProjectPath'] ?? ''),
            'url' => (string)($project['burrowProjectUrl'] ?? ''),
        ];

        return $runtimeState;
    }

    /**
     * @param array<string,mixed> $snapshot
     * @return array{ok:bool,error:string}
     */
    public function publishSystemSnapshot(string $baseUrl, string $apiKey, array $runtimeState, array $snapshot): array
    {
        try {
            $client = $this->createClient(
                $baseUrl,
                $apiKey,
                $runtimeState['sdkState'] ?? [],
                $runtimeState['ingestionKey'] ?? [],
                true,
                false
            );
            $resolver = $this->buildRoutingResolver($runtimeState);
            $event = \Burrow\Sdk\Events\CanonicalEnvelopeBuilders::buildSystemStackSnapshotEvent([
                'organizationId' => (string)($runtimeState['organizationId'] ?? ''),
                'cms' => (array)($snapshot['cms'] ?? []),
                'runtime' => (array)($snapshot['runtime'] ?? []),
                'plugins' => (array)($snapshot['plugins'] ?? []),
                'updatesAvailable' => (int)($snapshot['updatesAvailable'] ?? 0),
                'totalPlugins' => (int)($snapshot['totalPlugins'] ?? 0),
                'tags' => [
                    'provider' => 'craft-plugin',
                    'cmsVersion' => (string)($snapshot['cms']['version'] ?? ''),
                ],
            ], $resolver);
            $client->publishEvent($event);

            return ['ok' => true, 'error' => ''];
        } catch (\Throwable $e) {
            return ['ok' => false, 'error' => $e->getMessage()];
        }
    }

    /**
     * @param array<int,array<string,mixed>> $formsContracts
     * @return array{ok:bool,error:string,projectSourceId:string,contractsVersion:string,contractMappings:array<int,array<string,mixed>>,formsContracts:array<int,array<string,mixed>>,sdkState:array<string,mixed>}
     */
    public function submitFormsContracts(string $baseUrl, string $apiKey, array $runtimeState, array $formsContracts): array
    {
        try {
            $client = $this->createClient(
                $baseUrl,
                $apiKey,
                $runtimeState['sdkState'] ?? [],
                $runtimeState['ingestionKey'] ?? [],
                false,
                false
            );
            $request = new \Burrow\Sdk\Contracts\FormsContractSubmissionRequest($this->buildFormsContractPayload($runtimeState, $formsContracts));
            $response = $client->submitFormsContract($request);
            $contractMappings = [];
            foreach ((array)$response->contractMappings as $mapping) {
                if (!is_object($mapping) || !method_exists($mapping, 'toArray')) {
                    continue;
                }
                $contractMappings[] = (array)$mapping->toArray();
            }

            return [
                'ok' => true,
                'error' => '',
                'projectSourceId' => trim((string)($response->projectSourceId ?? '')),
                'contractsVersion' => trim((string)($response->contractsVersion ?? '')),
                'contractMappings' => $contractMappings,
                'formsContracts' => is_array($response->formsContracts) ? $response->formsContracts : [],
                'sdkState' => $client->getState()->toArray(),
            ];
        } catch (\Throwable $e) {
            return [
                'ok' => false,
                'error' => $e->getMessage(),
                'projectSourceId' => '',
                'contractsVersion' => '',
                'contractMappings' => [],
                'formsContracts' => [],
                'sdkState' => [],
            ];
        }
    }

    /**
     * @param array<int,array<string,mixed>> $events
     * @return array{ok:bool,error:string,requestedCount:int,acceptedCount:int,rejectedCount:int,validationRejectedCount:int,latestCursor:string}
     */
    public function submitBackfillEvents(
        string $baseUrl,
        string $apiKey,
        array $runtimeState,
        array $events,
        string $windowStart,
        string $windowEnd
    ): array {
        try {
            $client = $this->createClient(
                $baseUrl,
                $apiKey,
                $runtimeState['sdkState'] ?? [],
                $runtimeState['ingestionKey'] ?? [],
                true,
                false
            );

            $request = new \Burrow\Sdk\Contracts\BackfillEventsRequest(
                events: array_values($events),
                backfill: new \Burrow\Sdk\Contracts\BackfillWindow(
                    windowStart: $windowStart,
                    cursor: null,
                    windowEnd: $windowEnd,
                    source: 'craft-plugin'
                ),
                channel: null,
                source: 'craft-plugin',
                routing: [
                    'projectId' => trim((string)($runtimeState['projectId'] ?? '')),
                    'projectSourceId' => trim((string)($runtimeState['projectSourceId'] ?? '')),
                    'clientId' => trim((string)($runtimeState['clientId'] ?? '')),
                ]
            );
            $result = $client->backfillEvents(
                $request,
                new \Burrow\Sdk\Client\BackfillOptions(
                    batchSize: 100,
                    concurrency: 2,
                    maxAttempts: 3
                )
            );

            return [
                'ok' => true,
                'error' => '',
                'requestedCount' => (int)$result->requestedCount,
                'acceptedCount' => (int)$result->acceptedCount,
                'rejectedCount' => (int)$result->rejectedCount,
                'validationRejectedCount' => (int)$result->validationRejectedCount,
                'latestCursor' => trim((string)($result->latestCursor ?? '')),
            ];
        } catch (\Throwable $e) {
            return [
                'ok' => false,
                'error' => $e->getMessage(),
                'requestedCount' => count($events),
                'acceptedCount' => 0,
                'rejectedCount' => 0,
                'validationRejectedCount' => 0,
                'latestCursor' => '',
            ];
        }
    }

    /**
     * @param array<int,array<string,mixed>> $formsContracts
     */
    private function buildFormsContractPayload(array $runtimeState, array $formsContracts): array
    {
        $sdk = \Burrow\Sdk\Client\BurrowClientState::fromArray(
            is_array($runtimeState['sdkState'] ?? null) ? $runtimeState['sdkState'] : []
        );

        return [
            'platform' => 'craft',
            'pluginVersion' => \burrow\Burrow\Plugin::getInstance()->getVersion(),
            'site' => [
                'url' => (string)(Craft::$app->getSites()->getPrimarySite()?->baseUrl ?? ''),
                'cmsVersion' => Craft::$app->getVersion(),
            ],
            'routing' => [
                'organizationId' => (string)($runtimeState['organizationId'] ?? ''),
                'clientId' => (string)($sdk->clientId ?? $runtimeState['clientId'] ?? ''),
                'projectId' => (string)($sdk->projectId ?? $runtimeState['projectId'] ?? ''),
                'projectSourceId' => (string)($sdk->formsProjectSourceId ?? $runtimeState['projectSourceId'] ?? ''),
            ],
            'formsContracts' => array_values($formsContracts),
        ];
    }

    private function createClient(
        string $baseUrl,
        string $apiKey,
        array $sdkState = [],
        array $ingestionKey = [],
        bool $dispatchClient = false,
        bool $ignoreSdkStateIngestion = false
    ): \Burrow\Sdk\Client\BurrowClient
    {
        if (!$this->isSdkAvailable()) {
            throw new \RuntimeException('Burrow SDK not found. Install package "useburrow/sdk-php" to use Burrow onboarding.');
        }

        $transport = new \Burrow\Sdk\Transport\CurlHttpTransport(8);
        $state = is_array($sdkState) ? $sdkState : [];
        if ($ignoreSdkStateIngestion) {
            $state['ingestionKey'] = '';
        }
        $ingestionAuthKey = trim((string)($ingestionKey['key'] ?? ''));
        $primaryApiKey = trim($apiKey);
        $authKey = $dispatchClient
            ? ($ingestionAuthKey !== '' ? $ingestionAuthKey : $primaryApiKey)
            : $primaryApiKey;

        return new \Burrow\Sdk\Client\BurrowClient(
            baseUrl: rtrim($baseUrl, '/'),
            apiKey: $authKey,
            transport: $transport,
            state: \Burrow\Sdk\Client\BurrowClientState::fromArray($state)
        );
    }

    private function buildRoutingResolver(array $runtimeState): \Burrow\Sdk\Events\ChannelRoutingResolver
    {
        $sourceIds = is_array($runtimeState['sourceIds'] ?? null) ? $runtimeState['sourceIds'] : [];
        $fallback = (string)($runtimeState['projectSourceId'] ?? '');

        $channelSources = [];
        foreach (['forms', 'ecommerce', 'system'] as $channel) {
            $value = trim((string)($sourceIds[$channel] ?? ''));
            $channelSources[$channel] = $value !== '' ? $value : $fallback;
        }

        $state = new \Burrow\Sdk\Events\ChannelRoutingState(
            projectId: (string)($runtimeState['projectId'] ?? ''),
            projectSourceIds: $channelSources,
            clientId: (string)($runtimeState['clientId'] ?? '')
        );

        return new \Burrow\Sdk\Events\ChannelRoutingResolver($state);
    }

    /**
     * @param array<string,mixed> $body
     * @return array<int,array<string,mixed>>
     */
    private function extractProjects(array $body): array
    {
        $projects = $body['projects'] ?? $body['data']['projects'] ?? [];
        if (!is_array($projects)) {
            return [];
        }

        $result = [];
        foreach ($projects as $project) {
            if (!is_array($project)) {
                continue;
            }
            $result[] = [
                'organizationId' => (string)($project['organizationId'] ?? $project['orgId'] ?? ''),
                'clientId' => (string)($project['clientId'] ?? ''),
                'projectId' => (string)($project['projectId'] ?? $project['id'] ?? ''),
                'projectName' => (string)($project['projectName'] ?? $project['name'] ?? ''),
            ];
        }

        return $result;
    }

    /**
     * @return array<string,mixed>
     */
    private function buildSitePayload(): array
    {
        return [
            'url' => Craft::$app->getSites()->getPrimarySite()?->baseUrl ?? '',
            'cms' => 'craft',
            'cmsVersion' => Craft::$app->getVersion(),
            'phpVersion' => PHP_VERSION,
        ];
    }
}
