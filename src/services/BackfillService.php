<?php
namespace burrow\Burrow\services;

use Craft;
use craft\base\Component;

class BackfillService extends Component
{
    private const BACKFILL_QUERY_BATCH = 200;

    private const BACKFILL_SUBMIT_CHUNK = 400;

    /** Max element query pages processed per queue job (each page is {@see BACKFILL_QUERY_BATCH} rows). */
    private const BACKFILL_JOB_MAX_PAGES = 25;

    /**
     * @param array<string,mixed> $runtimeState
     * @param array<int,string> $sources
     * @return array{ok:bool,error:string,windowStart:string,windowEnd:string,sources:array<int,string>,requested:int,accepted:int,rejected:int,validationRejected:int,latestCursor:string,breakdown:array<string,int>}
     */
    public function runBackfill(array $runtimeState, string $windowPreset, array $sources): array
    {
        $checkpoint = $this->createInitialCheckpoint($runtimeState, $windowPreset, $sources);
        if ($checkpoint === null) {
            $normalizedSources = $this->normalizeSources($sources);
            if ($normalizedSources === []) {
                return $this->errorResult('Choose at least one source for backfill.');
            }

            return $this->errorResult('No backfill source is available for the selected integrations.');
        }
        while (true) {
            $chunk = $this->runBackfillChunk($runtimeState, $checkpoint);
            if (!$chunk['ok']) {
                return $this->buildChunkFailureResult($checkpoint, $chunk['error'], (array)($checkpoint['sources'] ?? []));
            }
            $checkpoint = $chunk['checkpoint'];
            if ($chunk['done']) {
                return $this->completeBackfillFromCheckpoint($runtimeState, $checkpoint);
            }
        }
    }

    /**
     * @param array<int,string> $sources
     * @return array<string,mixed>|null Checkpoint payload persisted for queue jobs
     */
    public function createInitialCheckpoint(array $runtimeState, string $windowPreset, array $sources): ?array
    {
        $sources = $this->normalizeSources($sources);
        if ($sources === []) {
            return null;
        }
        $phase = $this->resolveStartingPhase($runtimeState, $sources);
        if ($phase === null) {
            return null;
        }
        $windowStart = $this->windowStartForPreset($windowPreset);
        $windowEnd = gmdate('c');

        return [
            'phase' => $phase,
            'offset' => 0,
            'windowStart' => $windowStart,
            'windowEnd' => $windowEnd,
            'windowPreset' => trim($windowPreset),
            'sources' => $sources,
            'requested' => 0,
            'accepted' => 0,
            'rejected' => 0,
            'validationRejected' => 0,
            'skippedDuplicates' => 0,
            'latestCursor' => '',
            'breakdown' => [
                'forms' => 0,
                'ecommerce' => 0,
            ],
        ];
    }

    /**
     * Processes up to {@see BACKFILL_JOB_MAX_PAGES} query pages for the current phase, then returns.
     *
     * @param array<string,mixed> $runtimeState
     * @param array<string,mixed> $checkpoint
     * @return array{ok:bool,error:string,done:bool,checkpoint:array<string,mixed>}
     */
    public function runBackfillChunk(array $runtimeState, array $checkpoint): array
    {
        $plugin = \burrow\Burrow\Plugin::getInstance();
        $sources = $this->normalizeSources((array)($checkpoint['sources'] ?? []));

        $phase = (string)($checkpoint['phase'] ?? 'complete');
        $offset = (int)($checkpoint['offset'] ?? 0);
        $windowStart = (string)($checkpoint['windowStart'] ?? '');
        $windowEnd = (string)($checkpoint['windowEnd'] ?? '');

        $pendingKeys = [];
        $pendingEvents = [];
        $pagesThisJob = 0;

        $flushPending = function () use (
            $plugin,
            $runtimeState,
            $windowStart,
            $windowEnd,
            &$pendingKeys,
            &$pendingEvents,
            &$checkpoint
        ): bool {
            if ($pendingEvents === []) {
                return true;
            }
            $keys = $pendingKeys;
            $events = $pendingEvents;
            $pendingKeys = [];
            $pendingEvents = [];
            $sdkResult = $plugin->getBurrowApi()->submitBackfillEvents(
                $plugin->getBurrowBaseUrl(),
                $plugin->getBurrowApiKey(),
                $runtimeState,
                $events,
                $windowStart,
                $windowEnd
            );
            if (!$sdkResult['ok']) {
                foreach ($events as $index => $event) {
                    if (!is_array($event)) {
                        continue;
                    }
                    $plugin->getQueue()->markFailed(
                        (string)($keys[$index] ?? $this->buildBackfillEventKey($event)),
                        $event,
                        (string)$sdkResult['error'],
                        (string)($event['channel'] ?? ''),
                        (string)($event['event'] ?? ''),
                        false
                    );
                }

                return false;
            }
            if (!$this->applyBackfillDeliveryResults($plugin, $keys, $events, $sdkResult)) {
                return false;
            }
            $checkpoint['requested'] = (int)($checkpoint['requested'] ?? 0) + (int)($sdkResult['requestedCount'] ?? count($events));
            $checkpoint['accepted'] = (int)($checkpoint['accepted'] ?? 0) + (int)($sdkResult['acceptedCount'] ?? 0);
            $checkpoint['rejected'] = (int)($checkpoint['rejected'] ?? 0) + (int)($sdkResult['rejectedCount'] ?? 0);
            $checkpoint['validationRejected'] = (int)($checkpoint['validationRejected'] ?? 0) + (int)($sdkResult['validationRejectedCount'] ?? 0);
            $cursor = trim((string)($sdkResult['latestCursor'] ?? ''));
            if ($cursor !== '') {
                $checkpoint['latestCursor'] = $cursor;
            }

            return true;
        };

        $appendEvent = function (array $event) use ($plugin, &$checkpoint, &$pendingKeys, &$pendingEvents, &$flushPending): bool {
            $channel = (string)($event['channel'] ?? '');
            if ($channel === 'forms') {
                $checkpoint['breakdown']['forms'] = (int)($checkpoint['breakdown']['forms'] ?? 0) + 1;
            } elseif ($channel === 'ecommerce') {
                $checkpoint['breakdown']['ecommerce'] = (int)($checkpoint['breakdown']['ecommerce'] ?? 0) + 1;
            }
            $eventKey = $this->buildBackfillEventKey($event);
            if ($plugin->getQueue()->wasSent($eventKey)) {
                $checkpoint['skippedDuplicates'] = (int)($checkpoint['skippedDuplicates'] ?? 0) + 1;

                return true;
            }
            $pendingKeys[] = $eventKey;
            $pendingEvents[] = $event;
            if (count($pendingEvents) >= self::BACKFILL_SUBMIT_CHUNK) {
                return $flushPending();
            }

            return true;
        };

        $done = false;
        while ($pagesThisJob < self::BACKFILL_JOB_MAX_PAGES && !$done) {
            if ($phase === 'complete') {
                $done = true;
                break;
            }

            $formAdapter = $this->getFormAdapterForPhase($phase);
            if ($formAdapter !== null) {
                $page = $formAdapter->fetchBackfillPage($runtimeState, $windowStart, $offset, self::BACKFILL_QUERY_BATCH);
                foreach ($page['events'] as $event) {
                    if (is_array($event) && !$appendEvent($event)) {
                        $checkpoint['phase'] = $phase;
                        $checkpoint['offset'] = $offset;

                        return [
                            'ok' => false,
                            'error' => 'Backfill submit failed mid-run. Earlier chunks may have been accepted.',
                            'done' => false,
                            'checkpoint' => $checkpoint,
                        ];
                    }
                }
                $offset = $page['nextOffset'];
                if ($page['exhausted']) {
                    $phase = $this->advancePhaseAfterExhaustion($runtimeState, $sources, $phase);
                    $offset = 0;
                    if ($phase === 'complete') {
                        $done = true;
                    }
                }
                $pagesThisJob++;

                continue;
            }

            if ($phase === 'ecommerce') {
                $page = $this->fetchEcommerceBackfillPage($runtimeState, $windowStart, $offset);
                foreach ($page['events'] as $event) {
                    if (is_array($event) && !$appendEvent($event)) {
                        $checkpoint['phase'] = $phase;
                        $checkpoint['offset'] = $offset;

                        return [
                            'ok' => false,
                            'error' => 'Backfill submit failed mid-run. Earlier chunks may have been accepted.',
                            'done' => false,
                            'checkpoint' => $checkpoint,
                        ];
                    }
                }
                $offset = $page['nextOffset'];
                if ($page['exhausted']) {
                    $phase = 'complete';
                    $done = true;
                }
                $pagesThisJob++;

                continue;
            }

            $phase = 'complete';
            $done = true;
        }

        if (!$flushPending()) {
            $checkpoint['phase'] = $phase;
            $checkpoint['offset'] = $offset;

            return [
                'ok' => false,
                'error' => 'Backfill submit failed on final chunk.',
                'done' => false,
                'checkpoint' => $checkpoint,
            ];
        }

        $checkpoint['phase'] = $phase;
        $checkpoint['offset'] = $offset;

        return [
            'ok' => true,
            'error' => '',
            'done' => $phase === 'complete',
            'checkpoint' => $checkpoint,
        ];
    }

    /**
     * @param array<string,mixed> $runtimeState
     * @param array<string,mixed> $checkpoint
     * @return array{ok:bool,error:string,windowStart:string,windowEnd:string,sources:array<int,string>,requested:int,accepted:int,rejected:int,validationRejected:int,latestCursor:string,breakdown:array<string,int>}
     */
    public function completeBackfillFromCheckpoint(array $runtimeState, array $checkpoint): array
    {
        $sources = $this->normalizeSources((array)($checkpoint['sources'] ?? []));
        $windowStart = (string)($checkpoint['windowStart'] ?? '');
        $windowEnd = (string)($checkpoint['windowEnd'] ?? '');
        $breakdown = is_array($checkpoint['breakdown'] ?? null) ? $checkpoint['breakdown'] : ['forms' => 0, 'ecommerce' => 0];
        $forms = (int)($breakdown['forms'] ?? 0);
        $ecommerce = (int)($breakdown['ecommerce'] ?? 0);

        if ($forms + $ecommerce === 0) {
            $debug = $this->buildDiscoveryDebug($runtimeState, $sources, $windowStart);
            \burrow\Burrow\Plugin::getInstance()->getLogs()->log(
                'warning',
                'Backfill discovery returned zero events',
                'backfill',
                'system',
                null,
                $debug
            );
            $summary = 'forms=' . (string)($debug['formsEvents'] ?? 0)
                . ', ecommerce=' . (string)($debug['ecommerceEvents'] ?? 0)
                . ', projectId=' . ((bool)($debug['projectIdPresent'] ?? false) ? 'yes' : 'no')
                . ', formsSource=' . ((bool)($debug['formsSourcePresent'] ?? false) ? 'yes' : 'no')
                . ', ecommerceSource=' . ((bool)($debug['ecommerceSourcePresent'] ?? false) ? 'yes' : 'no')
                . ', commerceSelected=' . ((bool)($debug['commerceSelected'] ?? false) ? 'yes' : 'no')
                . ', commerceMode=' . (string)($debug['commerceMode'] ?? 'unknown')
                . ', commerceScanned=' . (string)($debug['commerceScanned'] ?? 0)
                . ', commerceInWindow=' . (string)($debug['commerceInWindow'] ?? 0);
            $skippedDuplicates = (int)($checkpoint['skippedDuplicates'] ?? 0);
            $apiAccepted = (int)($checkpoint['accepted'] ?? 0);

            return [
                'ok' => false,
                'error' => 'No historical events found for the selected window and sources. [' . $summary . ']',
                'windowStart' => $windowStart,
                'windowEnd' => $windowEnd,
                'sources' => $sources,
                'requested' => (int)($checkpoint['requested'] ?? 0),
                'accepted' => $apiAccepted + $skippedDuplicates,
                'rejected' => (int)($checkpoint['rejected'] ?? 0),
                'validationRejected' => (int)($checkpoint['validationRejected'] ?? 0),
                'latestCursor' => (string)($checkpoint['latestCursor'] ?? ''),
                'breakdown' => $breakdown,
            ];
        }

        $requested = (int)($checkpoint['requested'] ?? 0);
        $accepted = (int)($checkpoint['accepted'] ?? 0);
        $skippedDuplicates = (int)($checkpoint['skippedDuplicates'] ?? 0);

        if ($requested === 0 && $skippedDuplicates > 0) {
            return [
                'ok' => true,
                'error' => '',
                'windowStart' => $windowStart,
                'windowEnd' => $windowEnd,
                'sources' => $sources,
                'requested' => 0,
                'accepted' => $skippedDuplicates,
                'rejected' => (int)($checkpoint['rejected'] ?? 0),
                'validationRejected' => (int)($checkpoint['validationRejected'] ?? 0),
                'latestCursor' => '',
                'breakdown' => $breakdown,
            ];
        }

        $rejected = (int)($checkpoint['rejected'] ?? 0);
        $completedOk = $accepted > 0 || ($requested === 0 && $rejected === 0);

        return [
            'ok' => $completedOk,
            'error' => $completedOk
                ? ''
                : sprintf('%d backfill event(s) were rejected by Burrow with 0 accepted. Check failed outbox rows for reasons.', $rejected),
            'windowStart' => $windowStart,
            'windowEnd' => $windowEnd,
            'sources' => $sources,
            'requested' => $requested,
            'accepted' => $accepted + $skippedDuplicates,
            'rejected' => $rejected,
            'validationRejected' => (int)($checkpoint['validationRejected'] ?? 0),
            'latestCursor' => (string)($checkpoint['latestCursor'] ?? ''),
            'breakdown' => $breakdown,
        ];
    }

    /**
     * @param array<string,mixed> $checkpoint
     * @param array<int,string> $sources
     * @return array{ok:bool,error:string,windowStart:string,windowEnd:string,sources:array<int,string>,requested:int,accepted:int,rejected:int,validationRejected:int,latestCursor:string,breakdown:array<string,int>}
     */
    private function buildChunkFailureResult(array $checkpoint, string $error, array $sources): array
    {
        $breakdown = is_array($checkpoint['breakdown'] ?? null) ? $checkpoint['breakdown'] : ['forms' => 0, 'ecommerce' => 0];
        $skippedDuplicates = (int)($checkpoint['skippedDuplicates'] ?? 0);
        $accepted = (int)($checkpoint['accepted'] ?? 0);

        return [
            'ok' => false,
            'error' => $error,
            'windowStart' => (string)($checkpoint['windowStart'] ?? ''),
            'windowEnd' => (string)($checkpoint['windowEnd'] ?? ''),
            'sources' => $sources,
            'requested' => (int)($checkpoint['requested'] ?? 0),
            'accepted' => $accepted + $skippedDuplicates,
            'rejected' => (int)($checkpoint['rejected'] ?? 0),
            'validationRejected' => (int)($checkpoint['validationRejected'] ?? 0),
            'latestCursor' => (string)($checkpoint['latestCursor'] ?? ''),
            'breakdown' => $breakdown,
        ];
    }

    /**
     * @param array<int,string> $sources
     */
    private function resolveStartingPhase(array $runtimeState, array $sources): ?string
    {
        if (in_array('forms', $sources, true)) {
            foreach ($this->formAdapters() as $adapter) {
                if ($adapter->prepareBackfillContext($runtimeState) !== null) {
                    return $adapter->getId();
                }
            }
        }
        if (in_array('ecommerce', $sources, true)) {
            $orderClass = '\craft\commerce\elements\Order';
            if (class_exists($orderClass) && method_exists($orderClass, 'find')) {
                return 'ecommerce';
            }
        }

        return null;
    }

    /**
     * @param array<int,string> $sources
     */
    private function advancePhaseAfterExhaustion(array $runtimeState, array $sources, string $finishedPhase): string
    {
        if ($this->getFormAdapterForPhase($finishedPhase) !== null) {
            $foundFinished = false;
            foreach ($this->formAdapters() as $adapter) {
                if ($foundFinished && in_array('forms', $sources, true) && $adapter->prepareBackfillContext($runtimeState) !== null) {
                    return $adapter->getId();
                }
                if ($adapter->getId() === $finishedPhase) {
                    $foundFinished = true;
                }
            }
            if (in_array('ecommerce', $sources, true)) {
                $orderClass = '\craft\commerce\elements\Order';
                if (class_exists($orderClass) && method_exists($orderClass, 'find')) {
                    return 'ecommerce';
                }
            }

            return 'complete';
        }

        return 'complete';
    }

    /**
     * @return array<string,string>
     */
    public function presetOptions(): array
    {
        return [
            'last_7_days' => 'Last 7 days',
            'last_30_days' => 'Last 30 days',
            'last_90_days' => 'Last 90 days',
            'year_to_date' => 'Year to date',
            'last_365_days' => 'Past year',
            'last_730_days' => 'Two years',
            'all_time' => 'All time',
        ];
    }

    /**
     * @param array<string,mixed> $runtimeState
     * @return array<string,mixed>
     */
    public function debugProbe(array $runtimeState, string $windowPreset = 'last_90_days'): array
    {
        $windowStart = $this->windowStartForPreset($windowPreset);
        $windowStartTs = strtotime($windowStart) ?: 0;

        $formProbes = [];
        foreach ($this->formAdapters() as $adapter) {
            $formProbes[$adapter->getId()] = $adapter->probeSubmissions($windowStartTs, 5);
        }
        $commerce = $this->probeCommerceOrders($windowStartTs);

        $sourceIds = is_array($runtimeState['sourceIds'] ?? null) ? $runtimeState['sourceIds'] : [];
        $projectSourceId = trim((string)($runtimeState['projectSourceId'] ?? ''));

        return [
            'windowPreset' => $windowPreset,
            'windowStart' => $windowStart,
            'windowEnd' => gmdate('c'),
            'projectId' => trim((string)($runtimeState['projectId'] ?? '')),
            'sourceIds' => [
                'forms' => trim((string)($sourceIds['forms'] ?? $projectSourceId)),
                'ecommerce' => trim((string)($sourceIds['ecommerce'] ?? $sourceIds['forms'] ?? $projectSourceId)),
                'system' => trim((string)($sourceIds['system'] ?? $projectSourceId)),
            ],
            'availableSources' => $this->availableSources($runtimeState),
            'eventCounts' => [
                'forms' => $this->countFormsBackfillEvents($runtimeState, $windowStart),
                'ecommerce' => $this->countEcommerceBackfillEvents($runtimeState, $windowStart),
            ],
            'formProbes' => $formProbes,
            'commerce' => $commerce,
        ];
    }

    /**
     * @param array<string,mixed> $runtimeState
     * @return array<int,string>
     */
    public function availableSources(array $runtimeState): array
    {
        $selected = array_values(array_filter(array_map('strval', (array)($runtimeState['selectedIntegrations'] ?? []))));
        $integrationSettings = is_array($runtimeState['integrationSettings'] ?? null) ? $runtimeState['integrationSettings'] : [];
        $available = [];
        $selectedHasForm = false;
        foreach ($this->formAdapters() as $adapter) {
            if (in_array($adapter->getId(), $selected, true)) {
                $selectedHasForm = true;
                break;
            }
        }
        if ($selectedHasForm && $this->hasConfiguredFormsBackfill($integrationSettings)) {
            $available[] = 'forms';
        }
        $commerceSettings = is_array($integrationSettings['commerce'] ?? null) ? $integrationSettings['commerce'] : [];
        if (in_array('commerce', $selected, true) && (string)($commerceSettings['mode'] ?? 'off') === 'track') {
            $available[] = 'ecommerce';
        }

        return $available;
    }

    private function windowStartForPreset(string $preset): string
    {
        $preset = trim($preset);
        return match ($preset) {
            'last_7_days' => gmdate('c', strtotime('-7 days')),
            'last_90_days' => gmdate('c', strtotime('-90 days')),
            'year_to_date' => gmdate('Y') . '-01-01T00:00:00+00:00',
            'last_365_days' => gmdate('c', strtotime('-365 days')),
            'last_730_days' => gmdate('c', strtotime('-730 days')),
            'all_time' => '1970-01-01T00:00:00Z',
            default => gmdate('c', strtotime('-30 days')),
        };
    }

    /**
     * @param array<int,string> $sources
     * @return array<int,string>
     */
    private function normalizeSources(array $sources): array
    {
        $normalized = array_values(array_filter(array_map('strval', $sources)));
        $normalized = array_values(array_intersect($normalized, ['forms', 'ecommerce']));
        return array_values(array_unique($normalized));
    }

    /**
     * @param array<string,mixed> $runtimeState
     * @return \Generator<int, array<string, mixed>>
     */
    private function iterateFormsBackfillEvents(array $runtimeState, string $windowStart): \Generator
    {
        foreach ($this->formAdapters() as $adapter) {
            yield from $this->iterateAdapterSubmissionEvents($adapter, $runtimeState, $windowStart);
        }
    }

    /**
     * @return \Generator<int, array<string, mixed>>
     */
    private function iterateAdapterSubmissionEvents(
        \burrow\Burrow\integrations\forms\FormIntegrationAdapter $adapter,
        array $runtimeState,
        string $windowStart
    ): \Generator {
        $offset = 0;
        while (true) {
            $page = $adapter->fetchBackfillPage($runtimeState, $windowStart, $offset, self::BACKFILL_QUERY_BATCH);
            foreach ($page['events'] as $event) {
                yield $event;
            }
            if ($page['exhausted']) {
                return;
            }
            $offset = $page['nextOffset'];
        }
    }

    private function countFormsBackfillEvents(array $runtimeState, string $windowStart): int
    {
        $total = 0;
        foreach ($this->formAdapters() as $adapter) {
            $total += $this->countAdapterBackfillSubmissions($adapter, $runtimeState, $windowStart);
        }

        return $total;
    }

    private function countAdapterBackfillSubmissions(
        \burrow\Burrow\integrations\forms\FormIntegrationAdapter $adapter,
        array $runtimeState,
        string $windowStart
    ): int {
        $ctx = $adapter->prepareBackfillContext($runtimeState);
        if ($ctx === null) {
            return 0;
        }
        /** @var class-string<\craft\base\ElementInterface> $submissionClass */
        $submissionClass = $ctx['submissionClass'];
        $formIds = array_map('intval', array_keys($ctx['enabledFormIdMap']));
        if ($formIds === []) {
            return 0;
        }
        try {
            return (int)$submissionClass::find()
                ->status(null)
                ->site('*')
                ->formId($formIds)
                ->dateCreated('>= ' . $windowStart)
                ->count();
        } catch (\Throwable) {
            return 0;
        }
    }

    /**
     * @return \burrow\Burrow\integrations\forms\FormIntegrationAdapter[]
     */
    private function formAdapters(): array
    {
        return \burrow\Burrow\Plugin::getInstance()->getIntegrations()->getFormIntegrations()->all();
    }

    private function getFormAdapterForPhase(string $phase): ?\burrow\Burrow\integrations\forms\FormIntegrationAdapter
    {
        return \burrow\Burrow\Plugin::getInstance()->getIntegrations()->getFormIntegration($phase);
    }

    /**
     * @param array<string,mixed> $integrationSettings
     */
    private function hasConfiguredFormsBackfill(array $integrationSettings): bool
    {
        foreach ($this->formAdapters() as $adapter) {
            if ($adapter->hasConfiguredTracking($integrationSettings)) {
                return true;
            }
        }

        return false;
    }


    private function normalizeTimestamp(string $value): string
    {
        $trimmed = trim($value);
        if ($trimmed === '') {
            return '';
        }
        try {
            return (new \DateTimeImmutable($trimmed))->setTimezone(new \DateTimeZone('UTC'))->format('c');
        } catch (\Throwable) {
            return '';
        }
    }

    /**
     * @param array<int,string> $keys
     * @param array<int,array<string,mixed>> $events
     * @param array<string,mixed> $sdkResult
     */
    private function applyBackfillDeliveryResults(
        \burrow\Burrow\Plugin $plugin,
        array $keys,
        array $events,
        array $sdkResult
    ): bool {
        $acceptedCount = (int)($sdkResult['acceptedCount'] ?? 0);
        $rejectedCount = (int)($sdkResult['rejectedCount'] ?? 0);
        /** @var list<array<string,mixed>> $acceptedRows */
        $acceptedRows = is_array($sdkResult['accepted'] ?? null)
            ? array_values(array_filter($sdkResult['accepted'], static fn (mixed $row): bool => is_array($row)))
            : [];
        /** @var list<array<string,mixed>> $rejectedRows */
        $rejectedRows = is_array($sdkResult['rejected'] ?? null)
            ? array_values(array_filter($sdkResult['rejected'], static fn (mixed $row): bool => is_array($row)))
            : [];

        $rejectedIndexes = $this->resolveBackfillOutcomeIndexes($events, $rejectedRows);
        $acceptedIndexes = $this->resolveBackfillOutcomeIndexes($events, $acceptedRows);
        $eventCount = count($events);

        if ($eventCount > 0 && $rejectedIndexes === [] && $acceptedIndexes === [] && ($acceptedCount > 0 || $rejectedCount > 0)) {
            if ($rejectedCount === 0 && $acceptedCount >= $eventCount) {
                $acceptedIndexes = range(0, $eventCount - 1);
            } elseif ($acceptedCount === 0 && $rejectedCount >= $eventCount) {
                $rejectedIndexes = range(0, $eventCount - 1);
            }
        }

        $defaultRejectReason = $this->summarizeBackfillRejections($rejectedRows, $sdkResult);

        foreach ($events as $index => $event) {
            if (!is_array($event)) {
                continue;
            }
            $eventKey = (string)($keys[$index] ?? $this->buildBackfillEventKey($event));
            $channel = (string)($event['channel'] ?? '');
            $eventName = (string)($event['event'] ?? '');

            if (in_array($index, $rejectedIndexes, true)) {
                $plugin->getQueue()->markFailed(
                    $eventKey,
                    $event,
                    $this->extractBackfillRejectionReasonForIndex($index, $events, $rejectedRows, $defaultRejectReason),
                    $channel,
                    $eventName,
                    false
                );
                continue;
            }

            if ($acceptedCount === 0 && $rejectedCount > 0) {
                $plugin->getQueue()->markFailed(
                    $eventKey,
                    $event,
                    $defaultRejectReason,
                    $channel,
                    $eventName,
                    false
                );
                continue;
            }

            if ($rejectedCount > 0) {
                if ($acceptedIndexes !== []) {
                    if (!in_array($index, $acceptedIndexes, true)) {
                        $plugin->getQueue()->markFailed(
                            $eventKey,
                            $event,
                            'Burrow did not accept this event in a partial backfill response.',
                            $channel,
                            $eventName,
                            false
                        );
                        continue;
                    }
                } elseif ($rejectedIndexes === []) {
                    $plugin->getQueue()->markFailed(
                        $eventKey,
                        $event,
                        'Ambiguous partial backfill response from Burrow.',
                        $channel,
                        $eventName,
                        false
                    );
                    continue;
                }
            }

            $plugin->getQueue()->markSent($eventKey, $event, $channel, $eventName);
        }

        return !($acceptedCount === 0 && $rejectedCount > 0);
    }

    /**
     * @param array<int,array<string,mixed>> $events
     * @param list<array<string,mixed>> $outcomeRows
     * @return list<int>
     */
    private function resolveBackfillOutcomeIndexes(array $events, array $outcomeRows): array
    {
        $indexes = [];
        foreach ($outcomeRows as $row) {
            if (isset($row['index']) && is_numeric($row['index'])) {
                $indexes[] = (int)$row['index'];
                continue;
            }
            $matched = $this->matchBackfillEventIndexByOutcomeRow($events, $row);
            if ($matched !== null) {
                $indexes[] = $matched;
            }
        }

        return array_values(array_unique($indexes));
    }

    /**
     * @param array<int,array<string,mixed>> $events
     * @param array<string,mixed> $row
     */
    private function matchBackfillEventIndexByOutcomeRow(array $events, array $row): ?int
    {
        $externalEventId = trim((string)($row['externalEventId'] ?? ''));
        if ($externalEventId !== '') {
            foreach ($events as $index => $event) {
                if (!is_array($event)) {
                    continue;
                }
                if (trim((string)($event['externalEventId'] ?? '')) === $externalEventId) {
                    return $index;
                }
            }
        }

        return null;
    }

    /**
     * @param array<int,array<string,mixed>> $events
     * @param list<array<string,mixed>> $rejectedRows
     */
    private function extractBackfillRejectionReasonForIndex(int $index, array $events, array $rejectedRows, string $fallback): string
    {
        foreach ($rejectedRows as $row) {
            if ((int)($row['index'] ?? -1) === $index) {
                return $this->formatBackfillRejectionReason($row);
            }
            if ($this->matchBackfillEventIndexByOutcomeRow($events, $row) === $index) {
                return $this->formatBackfillRejectionReason($row);
            }
        }

        return $fallback;
    }

    /**
     * @param array<string,mixed> $row
     */
    private function formatBackfillRejectionReason(array $row): string
    {
        $reason = trim((string)($row['reason'] ?? ''));
        $message = trim((string)($row['message'] ?? ''));
        if ($reason !== '' && $message !== '') {
            return $reason . ': ' . $message;
        }
        if ($reason !== '') {
            return $reason;
        }
        if ($message !== '') {
            return $message;
        }

        return 'Burrow rejected the backfill event.';
    }

    /**
     * @param list<array<string,mixed>> $rejectedRows
     * @param array<string,mixed> $sdkResult
     */
    private function summarizeBackfillRejections(array $rejectedRows, array $sdkResult): string
    {
        if ($rejectedRows !== []) {
            return $this->formatBackfillRejectionReason($rejectedRows[0]);
        }
        $validationRejectedCount = (int)($sdkResult['validationRejectedCount'] ?? 0);
        if ($validationRejectedCount > 0) {
            return 'Burrow SDK rejected ' . $validationRejectedCount . ' event(s) before submit (validation).';
        }

        return 'Burrow rejected the backfill event batch.';
    }

    /**
     * @param array<string,mixed> $event
     */
    private function buildBackfillEventKey(array $event): string
    {
        $normalized = $this->normalizeForHash($event);
        $channel = trim((string)($event['channel'] ?? 'unknown'));
        $eventName = trim((string)($event['event'] ?? 'event'));
        return 'backfill:' . $channel . ':' . $eventName . ':' . hash('sha256', json_encode($normalized));
    }

    /**
     * @param mixed $value
     * @return mixed
     */
    private function normalizeForHash($value)
    {
        if (!is_array($value)) {
            return $value;
        }

        if ($this->isAssoc($value)) {
            ksort($value);
        }

        foreach ($value as $key => $item) {
            $value[$key] = $this->normalizeForHash($item);
        }

        return $value;
    }

    /**
     * @param array<int|string,mixed> $value
     */
    private function isAssoc(array $value): bool
    {
        return array_keys($value) !== range(0, count($value) - 1);
    }

    /**
     * @param array<int,string> $keys
     */
    private function objectStringValue(object $source, array $keys): string
    {
        foreach ($keys as $key) {
            if (method_exists($source, 'get' . ucfirst($key))) {
                $value = $source->{'get' . ucfirst($key)}();
                $text = trim((string)$value);
                if ($text !== '') {
                    return $text;
                }
            }
            if (isset($source->{$key})) {
                $text = trim((string)$source->{$key});
                if ($text !== '') {
                    return $text;
                }
            }
        }
        return '';
    }

    /**
     * @param array<int,string> $keys
     */
    private function objectFloatValue(object $source, array $keys): float
    {
        $fallback = null;
        foreach ($keys as $key) {
            if (method_exists($source, 'get' . ucfirst($key))) {
                $value = $this->normalizeNumericValue($source->{'get' . ucfirst($key)}());
                if ($value !== 0.0) {
                    return $value;
                }
                if ($fallback === null) {
                    $fallback = $value;
                }
                continue;
            }
            if (isset($source->{$key})) {
                $value = $this->normalizeNumericValue($source->{$key});
                if ($value !== 0.0) {
                    return $value;
                }
                if ($fallback === null) {
                    $fallback = $value;
                }
            }
        }
        return $fallback ?? 0.0;
    }

    /**
     * @param array<int,string> $keys
     */
    private function objectDateValue(object $source, array $keys): string
    {
        foreach ($keys as $key) {
            $value = null;
            if (method_exists($source, 'get' . ucfirst($key))) {
                $value = $source->{'get' . ucfirst($key)}();
            } elseif (isset($source->{$key})) {
                $value = $source->{$key};
            }
            if ($value instanceof \DateTimeInterface) {
                return $value->format('c');
            }
            if (is_string($value) && trim($value) !== '') {
                return $value;
            }
        }
        return '';
    }

    /**
     * @return array<int,array<string,mixed>>
     */
    private function extractCommerceLineItemsFromOrderElement(object $order): array
    {
        $lineItems = [];
        $items = [];
        if (method_exists($order, 'getLineItems')) {
            $items = (array)$order->getLineItems();
        } elseif (property_exists($order, 'lineItems')) {
            $items = (array)$order->lineItems;
        }
        foreach ($items as $item) {
            if (!is_object($item)) {
                continue;
            }
            $lineItems[] = [
                'productId' => $this->objectStringValue($item, ['purchasableId', 'id']),
                'productName' => $this->objectStringValue($item, ['description', 'sku']) ?: 'Item',
                'quantity' => $this->objectFloatValue($item, ['qty', 'quantity']),
                'unitPrice' => $this->objectFloatValue($item, ['salePrice', 'price']),
                'lineTotal' => $this->objectFloatValue($item, ['subtotal', 'lineTotal', 'total']),
            ];
        }
        return $lineItems;
    }


    /**
     * @return array<string,mixed>
     */
    private function probeCommerceOrders(int $windowStartTs): array
    {
        $orderClass = '\craft\commerce\elements\Order';
        if (!class_exists($orderClass) || !method_exists($orderClass, 'find')) {
            return ['available' => false, 'scanned' => 0, 'inWindow' => 0, 'samples' => []];
        }
        try {
            $query = $orderClass::find()->status(null)->site('*');
            $this->applyPaidOnlyCommerceBackfillFilter($query);
            $orders = $query->orderBy(['dateCreated' => SORT_DESC])->limit(500)->all();
        } catch (\Throwable $e) {
            return ['available' => true, 'error' => $e->getMessage(), 'scanned' => 0, 'inWindow' => 0, 'samples' => []];
        }

        $inWindow = 0;
        $placedEligible = 0;
        $backfillEligible = 0;
        $statusSignalPresent = 0;
        /** @var array<string,int> $statusBreakdown */
        $statusBreakdown = [];
        /** @var array<string,float> $revenueByStatus */
        $revenueByStatus = [];
        $totalRevenue = 0.0;
        $samples = [];
        foreach ($orders as $order) {
            if (!is_object($order)) {
                continue;
            }
            $timestamp = $this->normalizeTimestamp($this->objectDateValue($order, ['dateOrdered', 'datePaid', 'dateAuthorized', 'dateCreated']));
            $ts = strtotime($timestamp) ?: 0;
            if ($ts < $windowStartTs) {
                continue;
            }
            $inWindow++;
            $statusHandle = $this->extractCommerceOrderStatusHandle($order);
            $statusLabel = $this->extractCommerceOrderStatusLabel($order);
            $rawStatus = strtolower(trim($this->objectStringValue($order, ['status', 'orderStatus'])));
            $normalizedStatus = preg_replace('/\s+/', '', $rawStatus) ?? $rawStatus;
            if ($normalizedStatus !== '' || $statusLabel !== '') {
                $statusSignalPresent++;
            }
            $placedEligible++;
            $backfillEligible++;

            $bucketKey = $statusHandle !== '' ? $statusHandle : ($statusLabel !== '' ? $statusLabel : 'unknown');
            $statusBreakdown[$bucketKey] = ($statusBreakdown[$bucketKey] ?? 0) + 1;
            $orderTotal = $this->extractCommerceOrderTotal($order);
            $revenueByStatus[$bucketKey] = ($revenueByStatus[$bucketKey] ?? 0.0) + $orderTotal;
            $totalRevenue += $orderTotal;

            if (count($samples) < 5) {
                $isCompleted = false;
                if (method_exists($order, 'getIsCompleted')) {
                    $isCompleted = (bool)$order->getIsCompleted();
                } elseif (isset($order->isCompleted)) {
                    $isCompleted = (bool)$order->isCompleted;
                }
                $samples[] = [
                    'id' => $this->objectStringValue($order, ['id']),
                    'number' => $this->objectStringValue($order, ['number']),
                    'statusLabel' => $statusLabel,
                    'statusHandle' => $statusHandle,
                    'statusRaw' => $rawStatus,
                    'statusNormalized' => $normalizedStatus,
                    'orderStatusId' => $this->objectStringValue($order, ['orderStatusId']),
                    'isCompleted' => $isCompleted ? 'yes' : 'no',
                    'placedSignal' => 'yes',
                    'statusSignalPresent' => ($normalizedStatus !== '' || $statusLabel !== '') ? 'yes' : 'no',
                    'backfillEligible' => 'yes',
                    'excludeReason' => '',
                    'timestamp' => $timestamp,
                    'total' => $orderTotal,
                    'itemCount' => count($this->extractCommerceLineItemsFromOrderElement($order)),
                    'totalPaid' => $this->objectFloatValue($order, ['totalPaid']),
                    'totalPrice' => $this->objectFloatValue($order, ['totalPrice']),
                    'totalRaw' => $this->objectFloatValue($order, ['total']),
                    'itemSubtotal' => $this->objectFloatValue($order, ['itemSubtotal', 'subtotal']),
                ];
            }
        }

        arsort($statusBreakdown);

        return [
            'available' => true,
            'scanned' => count($orders),
            'inWindow' => $inWindow,
            'placedEligible' => $placedEligible,
            'backfillEligible' => $backfillEligible,
            'statusSignalPresent' => $statusSignalPresent,
            'statusBreakdown' => $statusBreakdown,
            'revenueByStatus' => array_map(fn($v) => round($v, 2), $revenueByStatus),
            'totalRevenue' => round($totalRevenue, 2),
            'samples' => $samples,
        ];
    }

    /**
     * @param array<string,mixed> $runtimeState
     * @param array<int,string> $sources
     * @return array<string,mixed>
     */
    private function buildDiscoveryDebug(array $runtimeState, array $sources, string $windowStart): array
    {
        $sourceIds = is_array($runtimeState['sourceIds'] ?? null) ? $runtimeState['sourceIds'] : [];
        $integrationSettings = is_array($runtimeState['integrationSettings'] ?? null) ? $runtimeState['integrationSettings'] : [];
        $commerceSettings = is_array($integrationSettings['commerce'] ?? null) ? $integrationSettings['commerce'] : [];
        $selected = array_values(array_filter(array_map('strval', (array)($runtimeState['selectedIntegrations'] ?? []))));
        $windowStartTs = strtotime($windowStart) ?: 0;
        $commerceProbe = $this->probeCommerceOrders($windowStartTs);
        $formsEvents = 0;
        $ecommerceEvents = 0;
        if (in_array('forms', $sources, true)) {
            $formsEvents = $this->countFormsBackfillEvents($runtimeState, $windowStart);
        }
        if (in_array('ecommerce', $sources, true)) {
            $ecommerceEvents = $this->countEcommerceBackfillEvents($runtimeState, $windowStart);
        }
        return [
            'sources' => $sources,
            'selectedIntegrations' => $selected,
            'projectIdPresent' => trim((string)($runtimeState['projectId'] ?? '')) !== '',
            'formsSourcePresent' => trim((string)($sourceIds['forms'] ?? $runtimeState['projectSourceId'] ?? '')) !== '',
            'ecommerceSourcePresent' => trim((string)($sourceIds['ecommerce'] ?? $sourceIds['forms'] ?? $runtimeState['projectSourceId'] ?? '')) !== '',
            'commerceSelected' => in_array('commerce', $selected, true),
            'commerceMode' => trim((string)($commerceSettings['mode'] ?? 'track')),
            'commerceScanned' => (int)($commerceProbe['scanned'] ?? 0),
            'commerceInWindow' => (int)($commerceProbe['inWindow'] ?? 0),
            'formsEvents' => $formsEvents,
            'ecommerceEvents' => $ecommerceEvents,
        ];
    }

    /**
     * @return array{ok:bool,error:string,windowStart:string,windowEnd:string,sources:array<int,string>,requested:int,accepted:int,rejected:int,validationRejected:int,latestCursor:string,breakdown:array<string,int>}
     */
    private function errorResult(string $error): array
    {
        return [
            'ok' => false,
            'error' => $error,
            'windowStart' => '',
            'windowEnd' => '',
            'sources' => [],
            'requested' => 0,
            'accepted' => 0,
            'rejected' => 0,
            'validationRejected' => 0,
            'latestCursor' => '',
            'breakdown' => ['forms' => 0, 'ecommerce' => 0],
        ];
    }
}
