<?php
namespace burrow\Burrow\services;

use burrow\Burrow\integrations\forms\FormIntegrationAdapter;
use craft\base\Component;
use yii\base\Event;

class FormTrackingService extends Component
{
    public function handleAdapterSubmissionEvent(FormIntegrationAdapter $adapter, Event $event): void
    {
        $normalized = $adapter->normalizeSubmissionFromEvent($event);
        if ($normalized === null) {
            return;
        }

        $plugin = \burrow\Burrow\Plugin::getInstance();
        $runtimeState = $plugin->getState()->getState();
        $configByFormId = $adapter->trackingConfigsByFormId($runtimeState);
        if ($configByFormId === []) {
            return;
        }

        $submission = $normalized['submission'];
        $eventForm = $normalized['eventForm'] ?? null;
        $formId = $this->extractSubmissionFormId($submission);
        if ($formId <= 0 && is_object($eventForm)) {
            $formId = $this->extractFormObjectId($eventForm);
        }
        if ($formId <= 0 || !isset($configByFormId[$formId])) {
            return;
        }

        $config = $configByFormId[$formId];
        $submissionId = $this->objectStringValue($submission, ['id']);
        $eventEnvelope = $adapter->buildSubmissionEnvelope($submission, $formId, $config, $eventForm, $runtimeState);
        if ($eventEnvelope === []) {
            $sourceIds = is_array($runtimeState['sourceIds'] ?? null) ? $runtimeState['sourceIds'] : [];
            $resolvedFormsSource = trim((string)($sourceIds['forms'] ?? ''));
            if ($resolvedFormsSource === '') {
                $resolvedFormsSource = trim((string)($runtimeState['projectSourceId'] ?? ''));
            }
            $plugin->getLogs()->log(
                'warning',
                ucfirst($adapter->getId()) . ' submission not sent: forms event envelope could not be built (check project link and forms source id).',
                $adapter->getId(),
                'forms',
                null,
                [
                    'formId' => $formId,
                    'submissionId' => $submissionId,
                    'projectIdPresent' => trim((string)($runtimeState['projectId'] ?? '')) !== '',
                    'formsSourceIdPresent' => $resolvedFormsSource !== '',
                ]
            );

            return;
        }

        $this->publishAndTrackRealtimeEvent($eventEnvelope, $runtimeState, $adapter->getId(), [
            'formId' => (string)$formId,
            'submissionId' => $submissionId,
        ]);
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

        $result = $plugin->getBurrowApi()->publishEvents(
            $plugin->getBurrowBaseUrl(),
            $plugin->getBurrowApiKey(),
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

    private function extractSubmissionFormId(object $submission): int
    {
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
                } catch (\Throwable) {
                }
            }
        }

        foreach (['formId', 'form_id'] as $prop) {
            $raw = null;
            if (property_exists($submission, $prop)) {
                $raw = $submission->{$prop};
            } elseif (method_exists($submission, '__get')) {
                try {
                    $value = $submission->{$prop};
                    if ($value !== null) {
                        $raw = $value;
                    }
                } catch (\Throwable) {
                }
            }
            if (is_numeric($raw)) {
                $id = (int)$raw;
                if ($id > 0) {
                    return $id;
                }
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
            } catch (\Throwable) {
            }
        }

        if (is_object($submission->form ?? null)) {
            $id = $this->extractFormObjectId($submission->form);
            if ($id > 0) {
                return $id;
            }
        }

        return 0;
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
            } catch (\Throwable) {
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

    /**
     * @param array<int, string> $keys
     */
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
}
