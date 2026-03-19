<?php
namespace burrow\Burrow\services;

use craft\base\Component;
use yii\base\Event;

class FormTrackingService extends Component
{
    public function handleFreeformSubmissionEvent(Event $event): void
    {
        $submission = null;
        $eventForm = null;
        if (method_exists($event, 'getSubmission')) {
            $submission = $event->getSubmission();
        } elseif (is_object($event->submission ?? null)) {
            $submission = $event->submission;
        }
        if (method_exists($event, 'getForm')) {
            $eventForm = $event->getForm();
        } elseif (is_object($event->form ?? null)) {
            $eventForm = $event->form;
        }
        if (!is_object($submission)) {
            return;
        }

        $plugin = \burrow\Burrow\Plugin::getInstance();
        $runtimeState = $plugin->getState()->getState();
        $configByFormId = $this->freeformConfigsByFormId($runtimeState);
        if ($configByFormId === []) {
            return;
        }

        $formId = $this->extractSubmissionFormId($submission);
        if ($formId <= 0 && is_object($eventForm)) {
            $formId = $this->extractFormObjectId($eventForm);
        }
        if ($formId <= 0 || !isset($configByFormId[$formId])) {
            return;
        }

        $timestamp = $this->normalizeTimestamp($this->objectDateValue($submission, ['dateCreated', 'dateUpdated'])) ?: gmdate('c');
        $submissionId = $this->objectStringValue($submission, ['id']);
        $config = $configByFormId[$formId];

        $tags = [
            'provider' => 'freeform',
            'formId' => (string)$formId,
        ];
        $properties = [
            'provider' => 'freeform',
            'formId' => (string)$formId,
            'formName' => (string)($config['formName'] ?? ('Form ' . $formId)),
            'submissionId' => $submissionId,
            'submittedAt' => $timestamp,
            'isBackfill' => false,
        ];

        $mode = trim((string)($config['mode'] ?? 'count_only'));
        if ($mode === 'custom_fields') {
            $submissionPayload = $this->extractSubmissionScalarValues($submission);
            if ($submissionPayload !== []) {
                $properties = array_merge($submissionPayload, $properties);
            }
            $mapped = $this->extractMappedSubmissionPayload($submission, is_array($config['fields'] ?? null) ? $config['fields'] : []);
            if (!empty($mapped['tags']) && is_array($mapped['tags'])) {
                $tags = array_merge($tags, $mapped['tags']);
            }
            if (!empty($mapped['properties']) && is_array($mapped['properties'])) {
                $properties = array_merge($properties, $mapped['properties']);
            }
        }

        $eventEnvelope = $plugin->getBurrowApi()->buildFormsSubmissionEvent($runtimeState, [
            'timestamp' => $timestamp,
            'source' => 'craft-freeform',
            'tags' => $tags,
            'properties' => $properties,
        ]);
        if ($eventEnvelope === []) {
            return;
        }
        $this->publishAndTrackRealtimeEvent($eventEnvelope, $runtimeState, 'freeform', [
            'formId' => (string)$formId,
            'submissionId' => $submissionId,
        ]);
    }

    public function handleFormieSubmissionEvent(Event $event): void
    {
        $success = $event->success ?? null;
        if ($success !== null && $success !== true) {
            return;
        }

        $submission = is_object($event->submission ?? null) ? $event->submission : null;
        if ($submission === null) {
            return;
        }

        $plugin = \burrow\Burrow\Plugin::getInstance();
        $runtimeState = $plugin->getState()->getState();
        $selectedFormMap = $this->formieSelectedFormMap($runtimeState);
        if ($selectedFormMap === []) {
            return;
        }

        $formId = $this->extractSubmissionFormId($submission);
        if ($formId <= 0 || !isset($selectedFormMap[$formId])) {
            return;
        }

        $timestamp = $this->normalizeTimestamp($this->objectDateValue($submission, ['dateCreated', 'dateUpdated'])) ?: gmdate('c');
        $submissionId = $this->objectStringValue($submission, ['id']);
        $formName = $this->extractSubmissionFormName($submission) ?: ('Formie ' . $formId);

        $eventEnvelope = $plugin->getBurrowApi()->buildFormsSubmissionEvent($runtimeState, [
            'timestamp' => $timestamp,
            'source' => 'craft-formie',
            'tags' => [
                'provider' => 'formie',
                'formId' => (string)$formId,
            ],
            'properties' => [
                'provider' => 'formie',
                'formId' => (string)$formId,
                'formName' => $formName,
                'submissionId' => $submissionId,
                'submittedAt' => $timestamp,
                'isBackfill' => false,
            ],
        ]);
        if ($eventEnvelope === []) {
            return;
        }
        $this->publishAndTrackRealtimeEvent($eventEnvelope, $runtimeState, 'formie', [
            'formId' => (string)$formId,
            'submissionId' => $submissionId,
        ]);
    }

    private function freeformConfigsByFormId(array $runtimeState): array
    {
        $integrationSettings = is_array($runtimeState['integrationSettings'] ?? null) ? $runtimeState['integrationSettings'] : [];
        $freeform = is_array($integrationSettings['freeform'] ?? null) ? $integrationSettings['freeform'] : [];
        $globalMode = trim((string)($freeform['mode'] ?? 'off'));
        $forms = is_array($freeform['forms'] ?? null) ? $freeform['forms'] : [];
        $byId = [];
        foreach ($forms as $key => $form) {
            if (!is_array($form)) {
                continue;
            }
            $formId = (int)($form['id'] ?? $key);
            if ($formId <= 0 || (($form['enabled'] ?? true) === false)) {
                continue;
            }
            $mode = trim((string)($form['mode'] ?? $globalMode));
            if (!in_array($mode, ['count_only', 'custom_fields'], true)) {
                continue;
            }
            $byId[$formId] = [
                'mode' => $mode,
                'formName' => trim((string)($form['name'] ?? ('Form ' . $formId))),
                'fields' => is_array($form['fields'] ?? null) ? $form['fields'] : [],
            ];
        }
        return $byId;
    }

    private function formieSelectedFormMap(array $runtimeState): array
    {
        $integrationSettings = is_array($runtimeState['integrationSettings'] ?? null) ? $runtimeState['integrationSettings'] : [];
        $formie = is_array($integrationSettings['formie'] ?? null) ? $integrationSettings['formie'] : [];
        $mode = trim((string)($formie['mode'] ?? 'off'));
        if (!in_array($mode, ['count_only'], true)) {
            return [];
        }
        $formIds = array_values(array_unique(array_filter(array_map('intval', (array)($formie['formIds'] ?? [])))));
        return $formIds === [] ? [] : array_fill_keys($formIds, true);
    }

    private function extractMappedSubmissionPayload(object $submission, array $mappedFields): array
    {
        $values = $this->extractSubmissionScalarValues($submission);
        if ($values === []) {
            return ['tags' => [], 'properties' => []];
        }

        $tags = [];
        $properties = [];
        foreach ($mappedFields as $fieldMap) {
            if (!is_array($fieldMap)) {
                continue;
            }
            $enabled = (bool)($fieldMap['enabled'] ?? true);
            if (!$enabled) {
                continue;
            }
            $handle = trim((string)($fieldMap['handle'] ?? ''));
            if ($handle === '' || !array_key_exists($handle, $values)) {
                continue;
            }
            $target = strtolower(trim((string)($fieldMap['target'] ?? 'tag')));
            $label = trim((string)($fieldMap['label'] ?? $handle));
            if ($label === '') {
                $label = $handle;
            }
            if ($target === 'property' || $target === 'properties') {
                $properties[$label] = $values[$handle];
            } else {
                $tags[$label] = $values[$handle];
            }
        }

        return [
            'tags' => $tags,
            'properties' => $properties,
        ];
    }

    private function extractSubmissionScalarValues(object $submission): array
    {
        $values = [];
        if (method_exists($submission, 'toArray')) {
            $raw = $submission->toArray();
            if (is_array($raw)) {
                foreach ($raw as $key => $value) {
                    if (!is_string($key) || $key === '') {
                        continue;
                    }
                    if (is_scalar($value)) {
                        $normalized = $this->stringifyValue($value);
                        if ($normalized !== null && $normalized !== '') {
                            $values[$key] = $normalized;
                        }
                    }
                }
            }
        }

        if (method_exists($submission, 'getFieldValues')) {
            $fieldValues = $submission->getFieldValues();
            if (is_array($fieldValues)) {
                foreach ($fieldValues as $handle => $value) {
                    if (!is_string($handle) || $handle === '') {
                        continue;
                    }
                    if (array_key_exists($handle, $values)) {
                        continue;
                    }
                    if (is_scalar($value)) {
                        $normalized = $this->stringifyValue($value);
                        if ($normalized !== null && $normalized !== '') {
                            $values[$handle] = $normalized;
                        }
                        continue;
                    }
                    if (is_array($value)) {
                        $flattened = [];
                        foreach ($value as $part) {
                            $partValue = $this->stringifyValue($part);
                            if ($partValue !== null && $partValue !== '') {
                                $flattened[] = $partValue;
                            }
                        }
                        if ($flattened !== []) {
                            $values[$handle] = implode(', ', $flattened);
                        }
                    }
                }
            }
        }

        return $values;
    }

    private function extractSubmissionFormId(object $submission): int
    {
        $raw = null;
        foreach (['getFormId', 'getFormID'] as $method) {
            if (method_exists($submission, $method)) {
                try {
                    $value = $submission->{$method}();
                    if (is_numeric($value)) {
                        $id = (int)$value;
                        if ($id > 0) {
                            return $id;
                        }
                    }
                } catch (\Throwable $e) {
                }
            }
        }
        foreach (['formId', 'form_id'] as $prop) {
            if (property_exists($submission, $prop)) {
                $raw = $submission->{$prop};
                break;
            }
            if (method_exists($submission, '__get')) {
                try {
                    $value = $submission->{$prop};
                    if ($value !== null) {
                        $raw = $value;
                        break;
                    }
                } catch (\Throwable $e) {
                }
            }
        }
        if (is_numeric($raw)) {
            $id = (int)$raw;
            if ($id > 0) {
                return $id;
            }
        }

        if (method_exists($submission, 'getForm')) {
            try {
                $form = $submission->getForm();
                if (is_object($form)) {
                    $id = $this->extractFormObjectId($form);
                    if ($id > 0) {
                        return $id;
                    }
                }
            } catch (\Throwable $e) {
            }
        }

        if (is_object($submission->form ?? null)) {
            $form = $submission->form;
            $id = $this->extractFormObjectId($form);
            if ($id > 0) {
                return $id;
            }
        }

        return 0;
    }

    private function extractSubmissionFormName(object $submission): string
    {
        if (method_exists($submission, 'getForm')) {
            try {
                $form = $submission->getForm();
                if (is_object($form)) {
                    foreach (['getName', 'getTitle', 'getHandle'] as $method) {
                        if (!method_exists($form, $method)) {
                            continue;
                        }
                        $value = trim((string)$form->{$method}());
                        if ($value !== '') {
                            return $value;
                        }
                    }
                    foreach (['name', 'title', 'handle'] as $key) {
                        $value = trim((string)($form->{$key} ?? ''));
                        if ($value !== '') {
                            return $value;
                        }
                    }
                }
            } catch (\Throwable $e) {
            }
        }
        return '';
    }

    private function extractFormObjectId(object $form): int
    {
        foreach (['getId', 'getFormId'] as $method) {
            if (!method_exists($form, $method)) {
                continue;
            }
            try {
                $value = $form->{$method}();
                if (is_numeric($value)) {
                    $id = (int)$value;
                    if ($id > 0) {
                        return $id;
                    }
                }
            } catch (\Throwable $e) {
            }
        }
        foreach (['id', 'formId'] as $key) {
            $value = $form->{$key} ?? null;
            if (is_numeric($value)) {
                $id = (int)$value;
                if ($id > 0) {
                    return $id;
                }
            }
        }
        return 0;
    }

    private function objectDateValue(object $object, array $keys): string
    {
        foreach ($keys as $key) {
            $value = $object->{$key} ?? null;
            if ($value instanceof \DateTimeInterface) {
                return $value->format(\DateTimeInterface::ATOM);
            }
            if (is_string($value) && trim($value) !== '') {
                return trim($value);
            }
        }
        return '';
    }

    private function objectStringValue(object $object, array $keys): string
    {
        foreach ($keys as $key) {
            $value = $object->{$key} ?? null;
            if ($value === null) {
                continue;
            }
            if (is_scalar($value)) {
                $string = trim((string)$value);
                if ($string !== '') {
                    return $string;
                }
            }
        }
        return '';
    }

    private function normalizeTimestamp(string $value): string
    {
        $trimmed = trim($value);
        if ($trimmed === '') {
            return '';
        }
        $timestamp = strtotime($trimmed);
        if ($timestamp === false) {
            return '';
        }
        return gmdate('c', $timestamp);
    }

    private function stringifyValue(mixed $value): ?string
    {
        if ($value === null) {
            return null;
        }
        if (is_bool($value)) {
            return $value ? 'true' : 'false';
        }
        if (is_int($value) || is_float($value) || is_string($value)) {
            return trim((string)$value);
        }
        return null;
    }

    /**
     * @param array<string,mixed> $eventEnvelope
     * @param array<string,mixed> $runtimeState
     * @param array<string,mixed> $identity
     */
    private function publishAndTrackRealtimeEvent(array $eventEnvelope, array $runtimeState, string $provider, array $identity): void
    {
        $plugin = \burrow\Burrow\Plugin::getInstance();
        $eventKey = $this->buildRealtimeEventKey($eventEnvelope, $provider, $identity);
        if ($plugin->getQueue()->wasSent($eventKey)) {
            return;
        }

        $settings = $plugin->getSettings();
        $result = $plugin->getBurrowApi()->publishEvents(
            $settings->baseUrl,
            $settings->apiKey,
            $runtimeState,
            [$eventEnvelope]
        );

        $channel = trim((string)($eventEnvelope['channel'] ?? ''));
        $eventName = trim((string)($eventEnvelope['event'] ?? ''));
        if ($result['ok']) {
            $plugin->getQueue()->markSent($eventKey, $eventEnvelope, $channel, $eventName);
            return;
        }

        $error = trim((string)($result['error'] ?? 'Realtime publish failed.'));
        $plugin->getQueue()->markFailed($eventKey, $eventEnvelope, $error, $channel, $eventName);
        $plugin->getLogs()->log('warning', ucfirst($provider) . ' realtime publish failed', $provider, 'forms', $eventKey, [
            'error' => $error,
            'event' => $eventName,
            'channel' => $channel,
        ] + $identity);
    }

    /**
     * @param array<string,mixed> $eventEnvelope
     * @param array<string,mixed> $identity
     */
    private function buildRealtimeEventKey(array $eventEnvelope, string $provider, array $identity): string
    {
        $seed = [
            'provider' => $provider,
            'channel' => trim((string)($eventEnvelope['channel'] ?? '')),
            'event' => trim((string)($eventEnvelope['event'] ?? '')),
            'timestamp' => trim((string)($eventEnvelope['timestamp'] ?? '')),
            'source' => trim((string)($eventEnvelope['source'] ?? '')),
            'identity' => $identity,
            'tags' => is_array($eventEnvelope['tags'] ?? null) ? $eventEnvelope['tags'] : [],
            'properties' => is_array($eventEnvelope['properties'] ?? null) ? $eventEnvelope['properties'] : [],
        ];

        return 'rt_' . hash('sha256', $this->stableJsonEncode($seed));
    }

    /**
     * @param array<string,mixed> $value
     */
    private function stableJsonEncode(array $value): string
    {
        $normalized = $this->normalizeForStableHash($value);
        $encoded = json_encode($normalized, JSON_UNESCAPED_SLASHES | JSON_UNESCAPED_UNICODE);
        return is_string($encoded) ? $encoded : '';
    }

    private function normalizeForStableHash(mixed $value): mixed
    {
        if (!is_array($value)) {
            return $value;
        }

        if (array_is_list($value)) {
            $normalized = [];
            foreach ($value as $item) {
                $normalized[] = $this->normalizeForStableHash($item);
            }
            return $normalized;
        }

        ksort($value);
        foreach ($value as $key => $item) {
            $value[$key] = $this->normalizeForStableHash($item);
        }
        return $value;
    }
}
