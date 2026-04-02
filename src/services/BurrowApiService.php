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
            $client = $this->createClient($baseUrl, $apiKey, [], [], false, true, null);
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
            $client = $this->createClient($baseUrl, $apiKey, $runtimeState['sdkState'] ?? [], $runtimeState['ingestionKey'] ?? [], false, true, null);
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
        $sourceIds = is_array($runtimeState['sourceIds'] ?? null) ? $runtimeState['sourceIds'] : [];
        $fallbackSource = trim((string)($runtimeState['projectSourceId'] ?? $sourceIds['forms'] ?? ''));
        if ($fallbackSource !== '') {
            if (trim((string)($sourceIds['forms'] ?? '')) === '') {
                $sourceIds['forms'] = $fallbackSource;
            }
            if (trim((string)($sourceIds['ecommerce'] ?? '')) === '') {
                $sourceIds['ecommerce'] = (string)($sourceIds['forms'] ?? $fallbackSource);
            }
            if (trim((string)($sourceIds['system'] ?? '')) === '') {
                $sourceIds['system'] = $fallbackSource;
            }
            $runtimeState['sourceIds'] = $sourceIds;
        }

        $runtimeState['ingestionKey'] = [
            'key' => (string)($ingestionKey['key'] ?? ''),
            'projectId' => (string)($ingestionKey['projectId'] ?? ''),
            'keyPrefix' => (string)($ingestionKey['keyPrefix'] ?? ''),
        ];

        $runtimeState['burrowProject'] = [
            'name' => (string)($project['name'] ?? ''),
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
                false,
                $runtimeState
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
            $event['source'] = 'craft-plugin';
            $client->publishEvent($event);

            return ['ok' => true, 'error' => ''];
        } catch (\Throwable $e) {
            return ['ok' => false, 'error' => $e->getMessage()];
        }
    }

    /**
     * @return array{ok:bool,error:string}
     */
    public function publishSystemHeartbeat(string $baseUrl, string $apiKey, array $runtimeState, float $responseMs = 0.0): array
    {
        try {
            $client = $this->createClient(
                $baseUrl,
                $apiKey,
                $runtimeState['sdkState'] ?? [],
                $runtimeState['ingestionKey'] ?? [],
                true,
                false,
                $runtimeState
            );
            $resolver = $this->buildRoutingResolver($runtimeState);
            $event = \Burrow\Sdk\Events\CanonicalEnvelopeBuilders::buildSystemHeartbeatEvent([
                'organizationId' => (string)($runtimeState['organizationId'] ?? ''),
                'responseMs' => max(0.0, $responseMs),
                'tags' => [
                    'provider' => 'craft-plugin',
                ],
            ], $resolver);
            $event['source'] = 'craft-plugin';
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
                true,
                false,
                $runtimeState
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
                false,
                $runtimeState,
                120
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
                    concurrency: 1,
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
     * @param array<int,array<string,mixed>> $events
     * @return array{ok:bool,error:string,requestedCount:int,publishedCount:int,failedCount:int}
     */
    public function publishEvents(
        string $baseUrl,
        string $apiKey,
        array $runtimeState,
        array $events
    ): array {
        $requested = count($events);
        if ($requested === 0) {
            return [
                'ok' => true,
                'error' => '',
                'requestedCount' => 0,
                'publishedCount' => 0,
                'failedCount' => 0,
            ];
        }

        try {
            $client = $this->createClient(
                $baseUrl,
                $apiKey,
                $runtimeState['sdkState'] ?? [],
                $runtimeState['ingestionKey'] ?? [],
                true,
                false,
                $runtimeState
            );

            $published = 0;
            $lastPublishError = '';
            foreach ($events as $event) {
                if (!is_array($event) || empty($event['event']) || empty($event['channel'])) {
                    continue;
                }
                try {
                    $client->publishEvent($event);
                    $published++;
                } catch (\Throwable $e) {
                    $lastPublishError = $e->getMessage();
                    continue;
                }
            }

            return [
                'ok' => $published > 0,
                'error' => $published > 0 ? '' : ($lastPublishError !== '' ? $lastPublishError : 'All event publishes failed.'),
                'requestedCount' => $requested,
                'publishedCount' => $published,
                'failedCount' => max(0, $requested - $published),
            ];
        } catch (\Throwable $e) {
            return [
                'ok' => false,
                'error' => $e->getMessage(),
                'requestedCount' => $requested,
                'publishedCount' => 0,
                'failedCount' => $requested,
            ];
        }
    }

    /**
     * Build a forms submission envelope from plugin runtime state.
     *
     * @param array<string,mixed> $runtimeState
     * @param array<string,mixed> $payload
     * @return array<string,mixed>
     */
    public function buildFormsSubmissionEvent(array $runtimeState, array $payload): array
    {
        $projectId = trim((string)($runtimeState['projectId'] ?? ''));
        $projectSourceId = $this->resolveChannelSourceId($runtimeState, 'forms');
        $timestamp = trim((string)($payload['timestamp'] ?? $payload['submittedAt'] ?? ''));
        if ($projectId === '' || $projectSourceId === '' || $timestamp === '') {
            return [];
        }

        return [
            'projectId' => $projectId,
            'projectSourceId' => $projectSourceId,
            'channel' => 'forms',
            'event' => 'forms.submission.received',
            'timestamp' => $timestamp,
            'source' => trim((string)($payload['source'] ?? 'craft-plugin')),
            'tags' => is_array($payload['tags'] ?? null) ? $payload['tags'] : [],
            'properties' => is_array($payload['properties'] ?? null) ? $payload['properties'] : [],
        ];
    }

    /**
     * Build ecommerce order + line item envelopes using SDK canonical builders.
     *
     * @param array<string,mixed> $runtimeState
     * @param array<string,mixed> $payload
     * @return array<int,array<string,mixed>>
     */
    public function buildEcommerceOrderAndItemEvents(array $runtimeState, array $payload): array
    {
        $projectId = trim((string)($runtimeState['projectId'] ?? ''));
        $ecommerceSourceId = $this->resolveChannelSourceId($runtimeState, 'ecommerce');
        $organizationId = trim((string)($runtimeState['organizationId'] ?? ''));
        $clientId = $this->resolveClientId($runtimeState);
        if ($projectId === '' || $ecommerceSourceId === '' || $organizationId === '' || $clientId === '') {
            \burrow\Burrow\Plugin::getInstance()->getLogs()->log(
                'warning',
                'Ecommerce envelope build skipped: missing routing context',
                'sdk',
                'ecommerce',
                null,
                [
                    'projectIdPresent' => $projectId !== '',
                    'ecommerceSourcePresent' => $ecommerceSourceId !== '',
                    'organizationIdPresent' => $organizationId !== '',
                    'clientIdPresent' => $clientId !== '',
                ]
            );
            return [];
        }

        $orderId = trim((string)($payload['orderId'] ?? ''));
        if ($orderId === '') {
            \burrow\Burrow\Plugin::getInstance()->getLogs()->log(
                'warning',
                'Ecommerce envelope build skipped: missing order id',
                'sdk',
                'ecommerce'
            );
            return [];
        }
        $submittedAt = trim((string)($payload['submittedAt'] ?? $payload['timestamp'] ?? ''));
        if ($submittedAt === '') {
            \burrow\Burrow\Plugin::getInstance()->getLogs()->log(
                'warning',
                'Ecommerce envelope build skipped: missing submitted timestamp',
                'sdk',
                'ecommerce',
                null,
                ['orderId' => $orderId]
            );
            return [];
        }

        $routing = $this->buildRoutingResolver($runtimeState);
        $currency = trim((string)($payload['currency'] ?? ''));
        if ($currency === '') {
            $currency = 'USD';
        }
        $tags = is_array($payload['tags'] ?? null) ? $payload['tags'] : [];
        if (!isset($tags['currency']) || trim((string)$tags['currency']) === '') {
            $tags['currency'] = $currency;
        }
        $events = [];
        $externalEntityId = trim((string)($payload['externalEntityId'] ?? ('craft_order_' . $orderId)));

        try {
            $orderPlaced = \Burrow\Sdk\Events\CanonicalEnvelopeBuilders::buildEcommerceOrderPlacedEvent([
                'organizationId' => $organizationId,
                'clientId' => $clientId,
                'orderId' => $orderId,
                'orderTotal' => (float)($payload['orderTotal'] ?? 0),
                'total' => (float)($payload['orderTotal'] ?? 0),
                'currency' => $currency,
                'itemCount' => (int)($payload['itemCount'] ?? 0),
                'submittedAt' => $submittedAt,
                'subtotal' => (float)($payload['subtotal'] ?? 0),
                'tax' => (float)($payload['tax'] ?? 0),
                'shippingTotal' => (float)($payload['shippingTotal'] ?? $payload['shipping'] ?? 0),
                'timestamp' => trim((string)($payload['timestamp'] ?? $submittedAt)),
                'externalEntityId' => $externalEntityId,
                'externalEventId' => trim((string)($payload['externalEventId'] ?? ($externalEntityId . '_placed'))),
                'tags' => $tags,
            ], $routing);
            $events[] = $this->withCraftPluginSource($orderPlaced);
        } catch (\Throwable $e) {
            \burrow\Burrow\Plugin::getInstance()->getLogs()->log(
                'warning',
                'Ecommerce order envelope build failed in SDK boundary',
                'sdk',
                'ecommerce',
                null,
                [
                    'orderId' => $orderId,
                    'submittedAt' => $submittedAt,
                    'currency' => $currency,
                    'itemCount' => (int)($payload['itemCount'] ?? 0),
                    'orderTotal' => (float)($payload['orderTotal'] ?? 0),
                    'error' => $e->getMessage(),
                ]
            );
            return [];
        }

        $items = is_array($payload['items'] ?? null) ? $payload['items'] : [];
        foreach ($items as $item) {
            if (!is_array($item)) {
                continue;
            }
            try {
                $events[] = \Burrow\Sdk\Events\CanonicalEnvelopeBuilders::buildEcommerceItemPurchasedEvent([
                    'organizationId' => $organizationId,
                    'clientId' => $clientId,
                    'orderId' => $orderId,
                    'productId' => trim((string)($item['productId'] ?? '')),
                    'productName' => trim((string)($item['productName'] ?? 'Item')),
                    'quantity' => (float)($item['quantity'] ?? 1),
                    'unitPrice' => (float)($item['unitPrice'] ?? 0),
                    'lineTotal' => (float)($item['lineTotal'] ?? 0),
                    'currency' => $currency,
                    'submittedAt' => $submittedAt,
                    'timestamp' => trim((string)($payload['timestamp'] ?? $submittedAt)),
                    'tags' => $tags,
                ], $routing);
            } catch (\Throwable $e) {
                \burrow\Burrow\Plugin::getInstance()->getLogs()->log(
                    'warning',
                    'Ecommerce line-item envelope build failed in SDK boundary',
                    'sdk',
                    'ecommerce',
                    null,
                    [
                        'orderId' => $orderId,
                        'productId' => trim((string)($item['productId'] ?? '')),
                        'productName' => trim((string)($item['productName'] ?? 'Item')),
                        'error' => $e->getMessage(),
                    ]
                );
                continue;
            }
        }

        return array_map(fn(array $envelope): array => $this->withCraftPluginSource($envelope), $events);
    }

    /**
     * Build ecommerce cart item added envelope using SDK canonical builder.
     *
     * @param array<string,mixed> $runtimeState
     * @param array<string,mixed> $payload
     * @return array<string,mixed>
     */
    public function buildEcommerceCartItemAddedEvent(array $runtimeState, array $payload): array
    {
        $projectId = trim((string)($runtimeState['projectId'] ?? ''));
        $ecommerceSourceId = $this->resolveChannelSourceId($runtimeState, 'ecommerce');
        $organizationId = trim((string)($runtimeState['organizationId'] ?? ''));
        $clientId = $this->resolveClientId($runtimeState);
        if ($projectId === '' || $ecommerceSourceId === '' || $organizationId === '' || $clientId === '') {
            return [];
        }

        $routing = $this->buildRoutingResolver($runtimeState);
        $currency = trim((string)($payload['currency'] ?? ''));
        if ($currency === '') {
            $currency = 'USD';
        }
        $tags = is_array($payload['tags'] ?? null) ? $payload['tags'] : [];
        if (!isset($tags['currency']) || trim((string)$tags['currency']) === '') {
            $tags['currency'] = $currency;
        }

        try {
            $envelope = \Burrow\Sdk\Events\CanonicalEnvelopeBuilders::buildEcommerceCartItemAddedEvent([
                'organizationId' => $organizationId,
                'clientId' => $clientId,
                'productId' => trim((string)($payload['productId'] ?? '')),
                'productName' => trim((string)($payload['productName'] ?? 'Item')),
                'variantName' => trim((string)($payload['variantName'] ?? $payload['productName'] ?? 'Item')),
                'quantity' => (float)($payload['quantity'] ?? 1),
                'unitPrice' => (float)($payload['unitPrice'] ?? 0),
                'lineTotal' => (float)($payload['lineTotal'] ?? 0),
                'currency' => $currency,
                'cartTotal' => (float)($payload['cartTotal'] ?? 0),
                'cartItemCount' => (int)($payload['cartItemCount'] ?? 0),
                'timestamp' => trim((string)($payload['timestamp'] ?? gmdate('c'))),
                'tags' => $tags,
            ], $routing);

            return $this->withCraftPluginSource($envelope);
        } catch (\Throwable $e) {
            \burrow\Burrow\Plugin::getInstance()->getLogs()->log(
                'warning',
                'Ecommerce cart.added envelope build failed in SDK boundary',
                'sdk',
                'ecommerce',
                null,
                [
                    'productId' => trim((string)($payload['productId'] ?? '')),
                    'productName' => trim((string)($payload['productName'] ?? '')),
                    'error' => $e->getMessage(),
                ]
            );
            return [];
        }
    }

    /**
     * Build ecommerce cart item removed envelope using SDK canonical builder.
     *
     * @param array<string,mixed> $runtimeState
     * @param array<string,mixed> $payload
     * @return array<string,mixed>
     */
    public function buildEcommerceCartItemRemovedEvent(array $runtimeState, array $payload): array
    {
        $projectId = trim((string)($runtimeState['projectId'] ?? ''));
        $ecommerceSourceId = $this->resolveChannelSourceId($runtimeState, 'ecommerce');
        $organizationId = trim((string)($runtimeState['organizationId'] ?? ''));
        $clientId = $this->resolveClientId($runtimeState);
        if ($projectId === '' || $ecommerceSourceId === '' || $organizationId === '' || $clientId === '') {
            return [];
        }

        $routing = $this->buildRoutingResolver($runtimeState);
        $currency = trim((string)($payload['currency'] ?? ''));
        if ($currency === '') {
            $currency = 'USD';
        }
        $tags = is_array($payload['tags'] ?? null) ? $payload['tags'] : [];
        if (!isset($tags['currency']) || trim((string)$tags['currency']) === '') {
            $tags['currency'] = $currency;
        }

        $builderClass = '\Burrow\Sdk\Events\CanonicalEnvelopeBuilders';
        $builderMethod = 'buildEcommerceCartItemRemovedEvent';
        if (!method_exists($builderClass, $builderMethod)) {
            \burrow\Burrow\Plugin::getInstance()->getLogs()->log(
                'warning',
                'SDK does not expose ecommerce cart.removed canonical builder',
                'sdk',
                'ecommerce',
                null,
                [
                    'builder' => $builderClass . '::' . $builderMethod,
                ]
            );
            return [];
        }

        try {
            $envelope = $builderClass::{$builderMethod}([
                'organizationId' => $organizationId,
                'clientId' => $clientId,
                'productId' => trim((string)($payload['productId'] ?? '')),
                'productName' => trim((string)($payload['productName'] ?? 'Item')),
                'variantName' => trim((string)($payload['variantName'] ?? $payload['productName'] ?? 'Item')),
                'quantity' => (float)($payload['quantity'] ?? 1),
                'unitPrice' => (float)($payload['unitPrice'] ?? 0),
                'lineTotal' => (float)($payload['lineTotal'] ?? 0),
                'currency' => $currency,
                'cartTotal' => (float)($payload['cartTotal'] ?? 0),
                'cartItemCount' => (int)($payload['cartItemCount'] ?? 0),
                'timestamp' => trim((string)($payload['timestamp'] ?? gmdate('c'))),
                'tags' => $tags,
            ], $routing);

            return $this->withCraftPluginSource($envelope);
        } catch (\Throwable $e) {
            \burrow\Burrow\Plugin::getInstance()->getLogs()->log(
                'warning',
                'Ecommerce cart.removed envelope build failed in SDK boundary',
                'sdk',
                'ecommerce',
                null,
                [
                    'productId' => trim((string)($payload['productId'] ?? '')),
                    'productName' => trim((string)($payload['productName'] ?? '')),
                    'error' => $e->getMessage(),
                ]
            );
            return [];
        }
    }

    /**
     * Build ecommerce cart abandoned lifecycle envelope.
     *
     * @param array<string,mixed> $runtimeState
     * @param array<string,mixed> $payload
     * @return array<string,mixed>
     */
    public function buildEcommerceCartAbandonedEvent(array $runtimeState, array $payload): array
    {
        $projectId = trim((string)($runtimeState['projectId'] ?? ''));
        $ecommerceSourceId = $this->resolveChannelSourceId($runtimeState, 'ecommerce');
        if ($projectId === '' || $ecommerceSourceId === '') {
            return [];
        }

        $builderClass = '\Burrow\Sdk\Events\CanonicalEnvelopeBuilders';
        $builderMethod = 'buildEcommerceCartAbandonedEvent';
        $routing = $this->buildRoutingResolver($runtimeState);

        $timestamp = trim((string)($payload['timestamp'] ?? gmdate('c')));
        $tags = is_array($payload['tags'] ?? null) ? $payload['tags'] : [];

        if (method_exists($builderClass, $builderMethod)) {
            try {
                $envelope = $builderClass::{$builderMethod}(array_merge($payload, [
                    'organizationId' => trim((string)($runtimeState['organizationId'] ?? '')),
                    'clientId' => $this->resolveClientId($runtimeState),
                    'timestamp' => $timestamp,
                ]), $routing);

                return $this->withCraftPluginSource($envelope);
            } catch (\Throwable $e) {
                \burrow\Burrow\Plugin::getInstance()->getLogs()->log(
                    'warning',
                    'Ecommerce cart.abandoned SDK builder failed, using manual envelope',
                    'sdk',
                    'ecommerce',
                    null,
                    ['error' => $e->getMessage()]
                );
            }
        }

        return [
            'projectId' => $projectId,
            'projectSourceId' => $ecommerceSourceId,
            'channel' => 'ecommerce',
            'event' => 'ecommerce.cart.abandoned',
            'timestamp' => $timestamp,
            'source' => 'craft-plugin',
            'isLifecycle' => true,
            'entityType' => 'cart',
            'state' => 'abandoned',
            'externalEntityId' => trim((string)($payload['externalEntityId'] ?? '')),
            'tags' => $tags,
            'properties' => [
                'cartTotal' => $payload['cartTotal'] ?? 0,
                'cartItemCount' => $payload['cartItemCount'] ?? 0,
                'currency' => $payload['currency'] ?? '',
                'minutesSinceLastActivity' => $payload['minutesSinceLastActivity'] ?? 0,
            ],
        ];
    }

    /**
     * Build ecommerce payment failed envelope (discrete, not lifecycle).
     *
     * @param array<string,mixed> $runtimeState
     * @param array<string,mixed> $payload
     * @return array<string,mixed>
     */
    public function buildEcommercePaymentFailedEvent(array $runtimeState, array $payload): array
    {
        $projectId = trim((string)($runtimeState['projectId'] ?? ''));
        $ecommerceSourceId = $this->resolveChannelSourceId($runtimeState, 'ecommerce');
        if ($projectId === '' || $ecommerceSourceId === '') {
            return [];
        }

        $builderClass = '\Burrow\Sdk\Events\CanonicalEnvelopeBuilders';
        $builderMethod = 'buildEcommercePaymentFailedEvent';
        $routing = $this->buildRoutingResolver($runtimeState);

        $timestamp = trim((string)($payload['timestamp'] ?? gmdate('c')));
        $tags = is_array($payload['tags'] ?? null) ? $payload['tags'] : [];

        if (method_exists($builderClass, $builderMethod)) {
            try {
                $envelope = $builderClass::{$builderMethod}(array_merge($payload, [
                    'organizationId' => trim((string)($runtimeState['organizationId'] ?? '')),
                    'clientId' => $this->resolveClientId($runtimeState),
                    'timestamp' => $timestamp,
                ]), $routing);

                return $this->withCraftPluginSource($envelope);
            } catch (\Throwable $e) {
                \burrow\Burrow\Plugin::getInstance()->getLogs()->log(
                    'warning',
                    'Ecommerce payment.failed SDK builder failed, using manual envelope',
                    'sdk',
                    'ecommerce',
                    null,
                    ['error' => $e->getMessage()]
                );
            }
        }

        return [
            'projectId' => $projectId,
            'projectSourceId' => $ecommerceSourceId,
            'channel' => 'ecommerce',
            'event' => 'ecommerce.payment.failed',
            'timestamp' => $timestamp,
            'source' => 'craft-plugin',
            'tags' => $tags,
            'properties' => [
                'orderId' => $payload['orderId'] ?? '',
                'cartTotal' => $payload['cartTotal'] ?? 0,
                'currency' => $payload['currency'] ?? '',
                'failureReason' => $payload['failureReason'] ?? 'processing_error',
                'paymentMethod' => $payload['paymentMethod'] ?? '',
            ],
        ];
    }

    /**
     * Build ecommerce cart recovered lifecycle envelope.
     *
     * @param array<string,mixed> $runtimeState
     * @param array<string,mixed> $payload
     * @return array<string,mixed>
     */
    public function buildEcommerceCartRecoveredEvent(array $runtimeState, array $payload): array
    {
        $projectId = trim((string)($runtimeState['projectId'] ?? ''));
        $ecommerceSourceId = $this->resolveChannelSourceId($runtimeState, 'ecommerce');
        if ($projectId === '' || $ecommerceSourceId === '') {
            return [];
        }

        $builderClass = '\Burrow\Sdk\Events\CanonicalEnvelopeBuilders';
        $builderMethod = 'buildEcommerceCartRecoveredEvent';
        $routing = $this->buildRoutingResolver($runtimeState);

        $timestamp = trim((string)($payload['timestamp'] ?? gmdate('c')));
        $tags = is_array($payload['tags'] ?? null) ? $payload['tags'] : [];

        if (method_exists($builderClass, $builderMethod)) {
            try {
                $envelope = $builderClass::{$builderMethod}(array_merge($payload, [
                    'organizationId' => trim((string)($runtimeState['organizationId'] ?? '')),
                    'clientId' => $this->resolveClientId($runtimeState),
                    'timestamp' => $timestamp,
                ]), $routing);

                return $this->withCraftPluginSource($envelope);
            } catch (\Throwable $e) {
                \burrow\Burrow\Plugin::getInstance()->getLogs()->log(
                    'warning',
                    'Ecommerce cart.recovered SDK builder failed, using manual envelope',
                    'sdk',
                    'ecommerce',
                    null,
                    ['error' => $e->getMessage()]
                );
            }
        }

        return [
            'projectId' => $projectId,
            'projectSourceId' => $ecommerceSourceId,
            'channel' => 'ecommerce',
            'event' => 'ecommerce.cart.recovered',
            'timestamp' => $timestamp,
            'source' => 'craft-plugin',
            'tags' => $tags,
            'properties' => [
                'orderId' => $payload['orderId'] ?? '',
                'orderTotal' => $payload['orderTotal'] ?? 0,
                'originalCartTotal' => $payload['originalCartTotal'] ?? 0,
                'currency' => $payload['currency'] ?? '',
                'minutesSinceAbandonment' => $payload['minutesSinceAbandonment'] ?? 0,
            ],
        ];
    }

    /**
     * Build ecommerce order lifecycle envelope (fulfilled, refunded, or cancelled).
     *
     * @param array<string,mixed> $runtimeState
     * @param array<string,mixed> $payload  Must include orderId, currency, orderTotal, and lifecycleState.
     * @return array<string,mixed>
     */
    public function buildEcommerceOrderLifecycleEvent(array $runtimeState, array $payload): array
    {
        $lifecycleState = trim((string)($payload['lifecycleState'] ?? ''));
        $builderMap = [
            'fulfilled' => 'buildEcommerceOrderFulfilledEvent',
            'refunded' => 'buildEcommerceOrderRefundedEvent',
            'cancelled' => 'buildEcommerceOrderCancelledEvent',
        ];
        if (!isset($builderMap[$lifecycleState])) {
            return [];
        }

        $projectId = trim((string)($runtimeState['projectId'] ?? ''));
        $ecommerceSourceId = $this->resolveChannelSourceId($runtimeState, 'ecommerce');
        if ($projectId === '' || $ecommerceSourceId === '') {
            return [];
        }

        $builderClass = '\Burrow\Sdk\Events\CanonicalEnvelopeBuilders';
        $builderMethod = $builderMap[$lifecycleState];
        $routing = $this->buildRoutingResolver($runtimeState);

        $timestamp = trim((string)($payload['timestamp'] ?? gmdate('c')));
        $tags = is_array($payload['tags'] ?? null) ? $payload['tags'] : [];

        if (method_exists($builderClass, $builderMethod)) {
            try {
                $envelope = $builderClass::{$builderMethod}(array_merge($payload, [
                    'organizationId' => trim((string)($runtimeState['organizationId'] ?? '')),
                    'clientId' => $this->resolveClientId($runtimeState),
                    'timestamp' => $timestamp,
                ]), $routing);

                return $this->withCraftPluginSource($envelope);
            } catch (\Throwable $e) {
                \burrow\Burrow\Plugin::getInstance()->getLogs()->log(
                    'warning',
                    'Ecommerce order.' . $lifecycleState . ' SDK builder failed, using manual envelope',
                    'sdk',
                    'ecommerce',
                    null,
                    ['error' => $e->getMessage()]
                );
            }
        }

        $eventName = 'ecommerce.order.' . $lifecycleState;
        $externalEntityId = trim((string)($payload['externalEntityId'] ?? ''));
        return [
            'projectId' => $projectId,
            'projectSourceId' => $ecommerceSourceId,
            'channel' => 'ecommerce',
            'event' => $eventName,
            'timestamp' => $timestamp,
            'source' => 'craft-plugin',
            'isLifecycle' => true,
            'entityType' => 'order',
            'state' => $lifecycleState,
            'externalEntityId' => $externalEntityId,
            'externalEventId' => trim((string)($payload['externalEventId'] ?? ($externalEntityId !== '' ? $externalEntityId . '_' . $lifecycleState : ''))),
            'tags' => $tags,
            'properties' => [
                'orderId' => $payload['orderId'] ?? '',
                'orderTotal' => $payload['orderTotal'] ?? 0,
                'currency' => $payload['currency'] ?? '',
            ],
        ];
    }

    /**
     * Build ecommerce checkout started envelope.
     *
     * @param array<string,mixed> $runtimeState
     * @param array<string,mixed> $payload
     * @return array<string,mixed>
     */
    public function buildEcommerceCheckoutStartedEvent(array $runtimeState, array $payload): array
    {
        $projectId = trim((string)($runtimeState['projectId'] ?? ''));
        $ecommerceSourceId = $this->resolveChannelSourceId($runtimeState, 'ecommerce');
        if ($projectId === '' || $ecommerceSourceId === '') {
            return [];
        }

        $builderClass = '\Burrow\Sdk\Events\CanonicalEnvelopeBuilders';
        $builderMethod = 'buildEcommerceCheckoutStartedEvent';
        $routing = $this->buildRoutingResolver($runtimeState);

        $timestamp = trim((string)($payload['timestamp'] ?? gmdate('c')));
        $tags = is_array($payload['tags'] ?? null) ? $payload['tags'] : [];

        if (method_exists($builderClass, $builderMethod)) {
            try {
                $envelope = $builderClass::{$builderMethod}(array_merge($payload, [
                    'organizationId' => trim((string)($runtimeState['organizationId'] ?? '')),
                    'clientId' => $this->resolveClientId($runtimeState),
                    'timestamp' => $timestamp,
                ]), $routing);

                return $this->withCraftPluginSource($envelope);
            } catch (\Throwable $e) {
                \burrow\Burrow\Plugin::getInstance()->getLogs()->log(
                    'warning',
                    'Ecommerce checkout.started SDK builder failed, using manual envelope',
                    'sdk',
                    'ecommerce',
                    null,
                    ['error' => $e->getMessage()]
                );
            }
        }

        return [
            'projectId' => $projectId,
            'projectSourceId' => $ecommerceSourceId,
            'channel' => 'ecommerce',
            'event' => 'ecommerce.checkout.started',
            'timestamp' => $timestamp,
            'source' => 'craft-plugin',
            'tags' => $tags,
            'properties' => [
                'cartTotal' => $payload['cartTotal'] ?? 0,
                'cartItemCount' => $payload['cartItemCount'] ?? 0,
                'currency' => $payload['currency'] ?? '',
            ],
        ];
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


    /**
     * Shared PHP SDK canonical builders default `source` for WordPress; Craft must emit craft-plugin.
     *
     * @param array<string,mixed> $envelope
     * @return array<string,mixed>
     */
    private function withCraftPluginSource(array $envelope): array
    {
        $envelope['source'] = 'craft-plugin';

        return $envelope;
    }

    /**
     * Merge top-level runtime state into SDK-persisted client state before {@see BurrowClientState::fromArray()}.
     *
     * Realtime POST /api/v1/events scopes the client with {@see BurrowClient::$scopedProjectId} from sdkState and
     * asserts it matches each envelope's projectId. Envelopes are built from runtime row fields (projectId,
     * sourceIds). Backfill uses explicit routing in the request body, so it can succeed while realtime fails if
     * sdkState lags after link/settings changes. Overlay keeps both paths aligned.
     *
     * @param array<string,mixed> $sdkState
     * @param array<string,mixed> $runtimeState
     * @return array<string,mixed>
     */
    private function overlayRuntimeScopeOnSdkState(array $sdkState, array $runtimeState): array
    {
        $merged = $sdkState;
        $projectId = trim((string)($runtimeState['projectId'] ?? ''));
        if ($projectId !== '') {
            $merged['projectId'] = $projectId;
        }
        $clientId = trim((string)($runtimeState['clientId'] ?? ''));
        if ($clientId !== '') {
            $merged['clientId'] = $clientId;
        }
        $ingestion = is_array($runtimeState['ingestionKey'] ?? null) ? trim((string)($runtimeState['ingestionKey']['key'] ?? '')) : '';
        if ($ingestion !== '') {
            $merged['ingestionKey'] = $ingestion;
        }
        $formsSourceId = $this->resolveChannelSourceId($runtimeState, 'forms');
        if ($formsSourceId !== '') {
            $merged['formsProjectSourceId'] = $formsSourceId;
        }

        return $merged;
    }

    private function createClient(
        string $baseUrl,
        string $apiKey,
        array $sdkState = [],
        array $ingestionKey = [],
        bool $dispatchClient = false,
        bool $ignoreSdkStateIngestion = false,
        ?array $runtimeScopeOverlay = null,
        int $httpTimeoutSeconds = 8
    ): \Burrow\Sdk\Client\BurrowClient
    {
        if (!$this->isSdkAvailable()) {
            throw new \RuntimeException('Burrow SDK not found. Install package "useburrow/sdk-php" to use Burrow onboarding.');
        }

        $transport = new \Burrow\Sdk\Transport\CurlHttpTransport(max(1, $httpTimeoutSeconds));
        $state = is_array($sdkState) ? $sdkState : [];
        if ($ignoreSdkStateIngestion) {
            $state['ingestionKey'] = '';
        }
        if (is_array($runtimeScopeOverlay) && $runtimeScopeOverlay !== []) {
            $state = $this->overlayRuntimeScopeOnSdkState($state, $runtimeScopeOverlay);
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
        $channelSources = [];
        foreach (['forms', 'ecommerce', 'system'] as $channel) {
            $channelSources[$channel] = $this->resolveChannelSourceId($runtimeState, $channel);
        }

        $state = new \Burrow\Sdk\Events\ChannelRoutingState(
            projectId: (string)($runtimeState['projectId'] ?? ''),
            projectSourceIds: $channelSources,
            clientId: $this->resolveClientId($runtimeState)
        );

        return new \Burrow\Sdk\Events\ChannelRoutingResolver($state);
    }

    /**
     * @param array<string,mixed> $runtimeState
     */
    private function resolveClientId(array $runtimeState): string
    {
        $runtimeClientId = trim((string)($runtimeState['clientId'] ?? ''));
        if ($runtimeClientId !== '') {
            return $runtimeClientId;
        }
        $sdk = \Burrow\Sdk\Client\BurrowClientState::fromArray(
            is_array($runtimeState['sdkState'] ?? null) ? $runtimeState['sdkState'] : []
        );
        return trim((string)($sdk->clientId ?? ''));
    }

    /**
     * @param array<string,mixed> $runtimeState
     */
    private function resolveChannelSourceId(array $runtimeState, string $channel): string
    {
        $sourceIds = is_array($runtimeState['sourceIds'] ?? null) ? $runtimeState['sourceIds'] : [];
        $value = trim((string)($sourceIds[$channel] ?? ''));
        if ($value !== '') {
            return $value;
        }
        if ($channel !== 'forms') {
            $formsFallback = trim((string)($sourceIds['forms'] ?? ''));
            if ($formsFallback !== '') {
                return $formsFallback;
            }
        }
        return trim((string)($runtimeState['projectSourceId'] ?? ''));
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
            $client = is_array($project['client'] ?? null) ? $project['client'] : [];
            $result[] = [
                'organizationId' => (string)($project['organizationId'] ?? $project['orgId'] ?? ''),
                'clientId' => (string)($project['clientId'] ?? $client['id'] ?? ''),
                'clientName' => (string)($project['clientName'] ?? $client['name'] ?? ''),
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
