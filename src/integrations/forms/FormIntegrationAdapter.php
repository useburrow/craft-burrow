<?php
namespace burrow\Burrow\integrations\forms;

use yii\base\Event;

/**
 * Translates a Craft form plugin into Burrow's canonical forms channel events.
 */
interface FormIntegrationAdapter
{
    public function getId(): string;

    public function getLabel(): string;

    public function getCraftPluginHandle(): string;

    /** Burrow event source, e.g. craft-freeform. */
    public function getSource(): string;

    public function getDefaultPrefix(): string;

    /** Tag prefix for formId, e.g. ff_ or frm_. */
    public function getFormIdTagPrefix(): string;

    public function isAvailable(): bool;

    /**
     * Register Craft event hooks and invoke $handler on each qualifying submission.
     *
     * @param callable(Event):void $handler
     */
    public function registerEventHooks(callable $handler): void;

    /**
     * @return null|array{submission: object, eventForm: ?object}
     */
    public function normalizeSubmissionFromEvent(Event $event): ?array;

    /**
     * @return array<int, array{id: string, name: string, handle: string}>
     */
    public function discoverForms(): array;

    /**
     * @return array<int, array{externalFieldId: string, sourceLabel: string, dataType: string, canonicalKey: string}>
     */
    public function discoverFields(string $formId): array;

    /**
     * @param array<string,mixed> $bodyParams
     * @return array{prefix: string, forms: array<string, array<string, mixed>>}
     */
    public function normalizeSettingsFromRequest(array $bodyParams, string $prefix): array;

    /**
     * @param array<string,mixed> $integrationConfig
     * @param array<string,mixed> $runtimeState
     * @return array<int, array<string, mixed>>
     */
    public function buildContracts(array $integrationConfig, array $runtimeState): array;

    /**
     * @param array<string,mixed> $runtimeState
     * @return array<int, array{mode: string, formName: string, fields: array<int|string, array<string, mixed>>}>
     */
    public function trackingConfigsByFormId(array $runtimeState): array;

    /**
     * @param array<string,mixed> $runtimeState
     * @return null|array{
     *     submissionClass: class-string,
     *     enabledFormIdMap: array<int, bool>,
     *     formNames: array<int, string>,
     *     formConfigsById: array<int, array<string, mixed>>
     * }
     */
    public function prepareBackfillContext(array $runtimeState): ?array;

    /**
     * @param array<string,mixed> $runtimeState
     * @return array{events: array<int, array<string, mixed>>, nextOffset: int, exhausted: bool}
     */
    public function fetchBackfillPage(array $runtimeState, string $windowStart, int $offset, int $batchSize): array;

    /**
     * @return array<string, mixed>
     */
    public function probeSubmissions(int $windowStartTs, int $sampleLimit): array;

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
    ): array;
}
