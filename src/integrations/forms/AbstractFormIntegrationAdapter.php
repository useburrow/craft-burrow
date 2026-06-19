<?php
namespace burrow\Burrow\integrations\forms;

use Craft;

use burrow\Burrow\Plugin;

abstract class AbstractFormIntegrationAdapter implements FormIntegrationAdapter
{
    use SubmissionSupportTrait;

    public function isAvailable(): bool
    {
        $submissionClass = $this->getSubmissionElementClass();

        return $submissionClass !== null && class_exists($submissionClass);
    }

    /**
     * @return class-string|null
     */
    abstract protected function getSubmissionElementClass(): ?string;

    /**
     * When true, backfill only runs for forms explicitly enabled in settings.
     */
    abstract protected function backfillRequiresConfiguredForms(): bool;

    /**
     * @param array<string,mixed> $bodyParams
     * @return array{prefix: string, forms: array<string, array<string, mixed>>}
     */
    public function normalizeSettingsFromRequest(array $bodyParams, string $prefix): array
    {
        $payload = (array)($bodyParams[$this->getId()] ?? []);
        $formsPayload = is_array($payload['forms'] ?? null) ? (array)$payload['forms'] : [];
        $normalizedForms = [];
        foreach ($formsPayload as $formId => $formConfig) {
            if (!is_array($formConfig)) {
                continue;
            }
            $mode = trim((string)($formConfig['mode'] ?? 'off'));
            if (!in_array($mode, ['off', 'count_only', 'custom_fields'], true)) {
                $mode = 'off';
            }
            $fieldsPayload = is_array($formConfig['fields'] ?? null) ? (array)$formConfig['fields'] : [];
            $normalizedFields = [];
            $formLabel = trim((string)($formConfig['formName'] ?? '')) ?: ('Form ' . $formId);
            foreach ($fieldsPayload as $fieldId => $fieldConfig) {
                if (!is_array($fieldConfig) || empty($fieldConfig['include'])) {
                    continue;
                }
                $target = trim((string)($fieldConfig['target'] ?? ''));
                if (!in_array($target, ['properties', 'tags'], true)) {
                    throw new \InvalidArgumentException(Craft::t('burrow', '"{form}": choose a target (properties or tags) for each included field.', [
                        'form' => $formLabel,
                    ]));
                }
                $canonicalKey = trim((string)($fieldConfig['canonicalKey'] ?? ''));
                if ($canonicalKey === '') {
                    throw new \InvalidArgumentException(Craft::t('burrow', '"{form}": each included field needs a canonical key.', [
                        'form' => $formLabel,
                    ]));
                }
                $normalizedFields[(string)$fieldId] = [
                    'externalFieldId' => trim((string)($fieldConfig['externalFieldId'] ?? $fieldId)),
                    'sourceLabel' => trim((string)($fieldConfig['sourceLabel'] ?? '')),
                    'dataType' => trim((string)($fieldConfig['dataType'] ?? 'string')),
                    'canonicalKey' => $canonicalKey,
                    'target' => $target,
                    'displayLabelOverride' => trim((string)($fieldConfig['displayLabelOverride'] ?? '')),
                ];
            }
            if ($mode === 'custom_fields' && $normalizedFields === []) {
                throw new \InvalidArgumentException(Craft::t('burrow', '"{form}" is set to Custom fields. Include at least one field.', [
                    'form' => $formLabel,
                ]));
            }
            $normalizedForms[(string)$formId] = [
                'externalFormId' => trim((string)($formConfig['externalFormId'] ?? $formId)),
                'formName' => trim((string)($formConfig['formName'] ?? '')),
                'mode' => $mode,
                'fields' => $normalizedFields,
            ];
        }

        $normalizedPrefix = strtoupper(trim($prefix));
        if ($normalizedPrefix === '') {
            $normalizedPrefix = $this->getDefaultPrefix();
        }

        return [
            'prefix' => $normalizedPrefix,
            'forms' => $normalizedForms,
        ];
    }

    /**
     * @param array<string,mixed> $integrationConfig
     * @param array<string,mixed> $runtimeState
     * @return array<int, array<string, mixed>>
     */
    public function buildContracts(array $integrationConfig, array $runtimeState): array
    {
        $contracts = [];
        $knownForms = [];
        foreach ($this->discoverForms() as $form) {
            $id = (string)($form['id'] ?? '');
            if ($id === '') {
                continue;
            }
            $knownForms[$id] = $form;
        }

        $prefix = strtoupper(trim((string)($integrationConfig['prefix'] ?? ''))) ?: $this->getDefaultPrefix();
        $prefixLower = strtolower($prefix) . '_';
        $formsConfig = is_array($integrationConfig['forms'] ?? null) ? $integrationConfig['forms'] : [];

        foreach ($formsConfig as $formId => $config) {
            if (!is_array($config)) {
                continue;
            }
            $mode = trim((string)($config['mode'] ?? 'off'));
            if (!in_array($mode, ['off', 'count_only', 'custom_fields'], true) || $mode === 'off') {
                continue;
            }

            $mappings = $this->buildContractFieldMappings($config, $mode);
            $known = $knownForms[(string)$formId] ?? null;
            $externalId = trim((string)($config['externalFormId'] ?? $formId));
            $configFormName = trim((string)($config['formName'] ?? ''));
            $formName = $configFormName !== ''
                ? $configFormName
                : (trim((string)($known['name'] ?? '')) ?: ($this->getLabel() . ' ' . $formId));
            $formHandle = trim((string)($known['handle'] ?? ''));

            $contracts[] = [
                'provider' => $this->getId(),
                'externalFormId' => str_starts_with($externalId, $prefixLower) ? $externalId : ($prefixLower . $externalId),
                'formHandle' => $formHandle !== '' ? $formHandle : ($this->getId() . '-' . $formId),
                'formName' => $formName,
                'enabled' => true,
                'countOnly' => $mode !== 'custom_fields',
                'mode' => $mode,
                'fieldMappings' => $mappings,
                'icon' => null,
            ];
        }

        return $contracts;
    }

    /**
     * @param array<string,mixed> $config
     * @return array<int, array<string, mixed>>
     */
    protected function buildContractFieldMappings(array $config, string $mode): array
    {
        $mappings = [];
        if ($mode === 'custom_fields') {
            $fields = is_array($config['fields'] ?? null) ? $config['fields'] : [];
            foreach ($fields as $field) {
                if (!is_array($field)) {
                    continue;
                }
                $target = trim((string)($field['target'] ?? ''));
                $canonicalKey = trim((string)($field['canonicalKey'] ?? ''));
                if (!in_array($target, ['properties', 'tags'], true) || $canonicalKey === '') {
                    continue;
                }
                $mappings[] = [
                    'externalFieldId' => trim((string)($field['externalFieldId'] ?? '')),
                    'sourceLabel' => trim((string)($field['sourceLabel'] ?? '')),
                    'dataType' => trim((string)($field['dataType'] ?? 'string')) ?: 'string',
                    'canonicalKey' => trim((string)($field['canonicalKey'] ?? '')),
                    'target' => $target,
                    'reportable' => $target === 'tags',
                    'displayLabelOverride' => trim((string)($field['displayLabelOverride'] ?? '')),
                ];
            }
        }

        $mappings[] = [
            'externalFieldId' => 'submission_id',
            'sourceLabel' => 'Submission ID',
            'dataType' => 'string',
            'canonicalKey' => 'submissionId',
            'target' => 'properties',
            'reportable' => false,
            'displayLabelOverride' => '',
        ];
        $mappings[] = [
            'externalFieldId' => 'submitted_at',
            'sourceLabel' => 'Submitted At',
            'dataType' => 'string',
            'canonicalKey' => 'submittedAt',
            'target' => 'properties',
            'reportable' => false,
            'displayLabelOverride' => '',
        ];

        return $mappings;
    }

    /**
     * @param array<string,mixed> $runtimeState
     * @return array<int, array{mode: string, formName: string, fields: array<int|string, array<string, mixed>>}>
     */
    public function trackingConfigsByFormId(array $runtimeState): array
    {
        $integrationSettings = is_array($runtimeState['integrationSettings'] ?? null) ? $runtimeState['integrationSettings'] : [];
        $root = is_array($integrationSettings[$this->getId()] ?? null) ? $integrationSettings[$this->getId()] : [];
        $forms = is_array($root['forms'] ?? null) ? $root['forms'] : [];
        $byId = [];
        foreach ($forms as $key => $form) {
            if (!is_array($form)) {
                continue;
            }
            $formId = (int)($form['id'] ?? $form['externalFormId'] ?? $key);
            if ($formId <= 0 || (($form['enabled'] ?? true) === false)) {
                continue;
            }
            $mode = trim((string)($form['mode'] ?? 'off'));
            if (!in_array($mode, ['count_only', 'custom_fields'], true)) {
                continue;
            }
            $byId[$formId] = [
                'mode' => $mode,
                'formName' => trim((string)($form['formName'] ?? '')) ?: ('Form ' . $formId),
                'fields' => is_array($form['fields'] ?? null) ? $form['fields'] : [],
            ];
        }

        return $byId;
    }

    /**
     * @param array<string,mixed> $runtimeState
     * @return null|array{
     *     submissionClass: class-string,
     *     enabledFormIdMap: array<int, bool>,
     *     formNames: array<int, string>,
     *     formConfigsById: array<int, array<string, mixed>>
     * }
     */
    public function prepareBackfillContext(array $runtimeState): ?array
    {
        $submissionClass = $this->getSubmissionElementClass();
        if ($submissionClass === null || !class_exists($submissionClass) || !method_exists($submissionClass, 'find')) {
            return null;
        }

        $integrationSettings = is_array($runtimeState['integrationSettings'] ?? null) ? $runtimeState['integrationSettings'] : [];
        $root = is_array($integrationSettings[$this->getId()] ?? null) ? $integrationSettings[$this->getId()] : [];
        $config = is_array($root['forms'] ?? null) ? $root['forms'] : [];

        $liveFormNames = [];
        foreach ($this->discoverForms() as $form) {
            if (!is_array($form)) {
                continue;
            }
            $id = (int)($form['id'] ?? 0);
            if ($id <= 0) {
                continue;
            }
            $liveName = trim((string)($form['name'] ?? ''));
            if ($liveName !== '') {
                $liveFormNames[$id] = $liveName;
            }
        }

        $enabledFormIds = [];
        $formNames = [];
        $formConfigsById = [];
        foreach ($config as $formId => $formConfig) {
            if (!is_array($formConfig)) {
                continue;
            }
            $mode = trim((string)($formConfig['mode'] ?? 'off'));
            if (!in_array($mode, ['count_only', 'custom_fields'], true)) {
                continue;
            }
            $stringFormId = trim((string)$formId);
            if ($stringFormId === '') {
                continue;
            }
            $intFormId = (int)$stringFormId;
            $enabledFormIds[] = $intFormId;
            $configName = trim((string)($formConfig['formName'] ?? ''));
            $formNames[$intFormId] = $configName !== '' ? $configName : ($liveFormNames[$intFormId] ?? ('Form ' . $stringFormId));
            $formConfigsById[$intFormId] = $formConfig;
        }

        if ($enabledFormIds === [] && !$this->backfillRequiresConfiguredForms()) {
            foreach ($liveFormNames as $id => $name) {
                $enabledFormIds[] = $id;
                $formNames[$id] = $name;
            }
            $enabledFormIds = array_values(array_unique($enabledFormIds));
        }

        if ($enabledFormIds === []) {
            return null;
        }

        return [
            'submissionClass' => $submissionClass,
            'enabledFormIdMap' => array_fill_keys($enabledFormIds, true),
            'formNames' => $formNames,
            'formConfigsById' => $formConfigsById,
        ];
    }

    /**
     * @param array<string,mixed> $runtimeState
     * @return array{events: array<int, array<string, mixed>>, nextOffset: int, exhausted: bool}
     */
    public function fetchBackfillPage(array $runtimeState, string $windowStart, int $offset, int $batchSize): array
    {
        $events = [];
        $ctx = $this->prepareBackfillContext($runtimeState);
        if ($ctx === null) {
            return ['events' => [], 'nextOffset' => 0, 'exhausted' => true];
        }

        /** @var class-string<\craft\base\ElementInterface> $submissionClass */
        $submissionClass = $ctx['submissionClass'];
        $enabledFormIdMap = $ctx['enabledFormIdMap'];
        $formNames = $ctx['formNames'];
        $formConfigsById = $ctx['formConfigsById'];
        $windowStartTs = strtotime($windowStart) ?: 0;

        try {
            $rows = $submissionClass::find()
                ->status(null)
                ->site('*')
                ->orderBy(['dateCreated' => SORT_DESC])
                ->limit($batchSize)
                ->offset($offset)
                ->all();
        } catch (\Throwable) {
            return ['events' => [], 'nextOffset' => $offset, 'exhausted' => true];
        }
        if ($rows === []) {
            return ['events' => [], 'nextOffset' => $offset, 'exhausted' => true];
        }

        $oldestTsInBatch = \PHP_INT_MAX;
        foreach ($rows as $row) {
            if (!is_object($row)) {
                continue;
            }
            $formId = $this->extractSubmissionFormId($row);
            if ($formId <= 0 || !isset($enabledFormIdMap[$formId])) {
                continue;
            }
            $timestamp = $this->normalizeSubmissionTimestamp($this->objectDateValue($row, ['dateCreated', 'dateUpdated']));
            if ($timestamp === '') {
                continue;
            }
            $submittedTs = strtotime($timestamp) ?: 0;
            $oldestTsInBatch = min($oldestTsInBatch, $submittedTs);
            if ($submittedTs < $windowStartTs) {
                continue;
            }

            $formConfig = is_array($formConfigsById[$formId] ?? null) ? $formConfigsById[$formId] : [
                'mode' => 'count_only',
                'formName' => (string)($formNames[$formId] ?? ('Form ' . $formId)),
                'fields' => [],
            ];
            $event = $this->buildSubmissionEnvelope($row, $formId, $formConfig, null, $runtimeState);
            if ($event !== []) {
                $events[] = $event;
            }
        }
        unset($rows);

        return [
            'events' => $events,
            'nextOffset' => $offset + $batchSize,
            'exhausted' => $oldestTsInBatch < $windowStartTs,
        ];
    }

    /**
     * @return array<string, mixed>
     */
    public function probeSubmissions(int $windowStartTs, int $sampleLimit): array
    {
        $submissionClass = $this->getSubmissionElementClass();
        if ($submissionClass === null || !class_exists($submissionClass) || !method_exists($submissionClass, 'find')) {
            return ['available' => false, 'scanned' => 0, 'inWindow' => 0, 'samples' => []];
        }

        try {
            $rows = $submissionClass::find()
                ->status(null)
                ->site('*')
                ->orderBy(['dateCreated' => SORT_DESC])
                ->limit(200)
                ->all();
        } catch (\Throwable $e) {
            return ['available' => true, 'error' => $e->getMessage(), 'scanned' => 0, 'inWindow' => 0, 'samples' => []];
        }

        $inWindow = 0;
        $samples = [];
        foreach ($rows as $row) {
            if (!is_object($row)) {
                continue;
            }
            $timestamp = $this->normalizeSubmissionTimestamp($this->objectDateValue($row, ['dateCreated', 'dateUpdated']));
            $ts = strtotime($timestamp) ?: 0;
            if ($ts >= $windowStartTs) {
                $inWindow++;
            }
            if (count($samples) < $sampleLimit) {
                $samples[] = [
                    'id' => $this->objectStringValue($row, ['id']),
                    'formId' => (string)$this->extractSubmissionFormId($row),
                    'timestamp' => $timestamp,
                ];
            }
        }

        return [
            'available' => true,
            'scanned' => count($rows),
            'inWindow' => $inWindow,
            'samples' => $samples,
        ];
    }

    /**
     * @param array<string,mixed> $formConfig
     * @param array<string,mixed> $runtimeState
     * @return array<string, mixed>
     */
    public function buildSubmissionEnvelope(
        object $submission,
        int $formId,
        array $formConfig,
        ?object $eventForm,
        array $runtimeState
    ): array {
        $timestamp = $this->normalizeSubmissionTimestamp($this->objectDateValue($submission, ['dateCreated', 'dateUpdated'])) ?: gmdate('c');
        $submissionId = $this->objectStringValue($submission, ['id']);

        $configFormName = trim((string)($formConfig['formName'] ?? ''));
        $liveFormName = $this->extractSubmissionFormName($submission);
        if (is_object($eventForm) && $liveFormName === '') {
            foreach (['name', 'title', 'handle'] as $key) {
                $value = trim((string)($eventForm->{$key} ?? ''));
                if ($value !== '') {
                    $liveFormName = $value;
                    break;
                }
            }
        }
        $formName = $liveFormName !== ''
            ? $liveFormName
            : ($configFormName !== '' ? $configFormName : ($this->getLabel() . ' ' . $formId));

        $tags = [
            'formId' => $this->getFormIdTagPrefix() . $formId,
        ];
        $properties = [
            'formName' => $formName,
            'submissionId' => $submissionId,
            'submittedAt' => $timestamp,
        ];

        $mode = trim((string)($formConfig['mode'] ?? 'count_only'));
        if ($mode === 'custom_fields') {
            $mapped = $this->extractMappedSubmissionPayload($submission, is_array($formConfig['fields'] ?? null) ? $formConfig['fields'] : []);
            if (!empty($mapped['tags']) && is_array($mapped['tags'])) {
                $tags = array_merge($tags, $mapped['tags']);
            }
            if (!empty($mapped['properties']) && is_array($mapped['properties'])) {
                $properties = array_merge($properties, $mapped['properties']);
            }
        }

        return Plugin::getInstance()->getBurrowApi()->buildFormsSubmissionEvent($runtimeState, [
            'timestamp' => $timestamp,
            'source' => $this->getSource(),
            'tags' => $tags,
            'properties' => $properties,
        ]);
    }

    /**
     * @param array<string,mixed> $integrationSettings
     */
    public function hasConfiguredTracking(array $integrationSettings): bool
    {
        $root = is_array($integrationSettings[$this->getId()] ?? null) ? $integrationSettings[$this->getId()] : [];
        $forms = is_array($root['forms'] ?? null) ? $root['forms'] : [];
        foreach ($forms as $config) {
            if (!is_array($config)) {
                continue;
            }
            $mode = trim((string)($config['mode'] ?? 'off'));
            if (in_array($mode, ['count_only', 'custom_fields'], true)) {
                return true;
            }
        }

        return false;
    }
}
