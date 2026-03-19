<?php
namespace burrow\Burrow\services;

use Craft;
use craft\base\Component;

class IntegrationsService extends Component
{
    /**
     * @return string[]
     */
    public function integrationOrder(): array
    {
        return ['freeform', 'formie', 'commerce'];
    }

    /**
     * @return array<string,string>
     */
    public function integrationLabels(): array
    {
        return [
            'freeform' => 'Freeform',
            'formie' => 'Formie',
            'commerce' => 'Craft Commerce',
        ];
    }

    /**
     * @param string[] $selected
     * @return array<string,string>
     */
    public function buildWizardSteps(array $selected): array
    {
        $steps = [
            'connection' => 'Connection',
            'project' => 'Project',
            'integrations' => 'Integrations',
        ];

        $labels = $this->integrationLabels();
        foreach ($this->integrationOrder() as $integration) {
            if (!in_array($integration, $selected, true)) {
                continue;
            }
            $steps[$integration] = (string)($labels[$integration] ?? $integration);
        }

        $steps['review'] = 'Review';
        $steps['finish'] = 'Finish';

        return $steps;
    }

    /**
     * @param string[] $selected
     */
    public function nextWizardStep(string $fromStep, array $selected): string
    {
        $keys = array_keys($this->buildWizardSteps($selected));
        $index = array_search($fromStep, $keys, true);
        if ($index === false) {
            return 'review';
        }

        return (string)($keys[$index + 1] ?? 'review');
    }

    /**
     * @param string[] $selected
     */
    public function previousWizardStep(string $fromStep, array $selected): string
    {
        $keys = array_keys($this->buildWizardSteps($selected));
        $index = array_search($fromStep, $keys, true);
        if ($index === false || $index === 0) {
            return '';
        }

        return (string)$keys[$index - 1];
    }

    public function isIntegrationStep(string $step): bool
    {
        return in_array($step, $this->integrationOrder(), true);
    }

    /**
     * @return array<string,array<string,mixed>>
     */
    public function getAvailableIntegrations(): array
    {
        $labels = $this->integrationLabels();

        return [
            'freeform' => $this->pluginStatus('freeform', (string)($labels['freeform'] ?? 'Freeform')),
            'formie' => $this->pluginStatus('formie', (string)($labels['formie'] ?? 'Formie')),
            'commerce' => $this->pluginStatus('commerce', (string)($labels['commerce'] ?? 'Craft Commerce')),
        ];
    }

    /**
     * @return array<int,array<string,string>>
     */
    public function getFreeformForms(): array
    {
        $forms = [];
        if (class_exists('\Solspace\Freeform\Freeform')) {
            try {
                $freeformForms = \Solspace\Freeform\Freeform::getInstance()->forms->getAllForms(true);
                foreach ($freeformForms as $freeformForm) {
                    $id = (string)($freeformForm->getId() ?? '');
                    if ($id === '') {
                        continue;
                    }
                    $forms[] = [
                        'id' => $id,
                        'name' => trim((string)($freeformForm->getName() ?? '')) ?: ('Form ' . $id),
                        'handle' => (string)($freeformForm->getHandle() ?? ''),
                    ];
                }

                return $forms;
            } catch (\Throwable) {
                return [];
            }
        }

        return [];
    }

    /**
     * @return array<int,array<string,string>>
     */
    public function getFreeformFields(string $formId): array
    {
        $fields = [];
        if ($formId === '') {
            return $fields;
        }

        if (class_exists('\Solspace\Freeform\Freeform')) {
            try {
                $form = \Solspace\Freeform\Freeform::getInstance()->forms->getFormById((int)$formId);
                if ($form !== null) {
                    $freeformFields = \Solspace\Freeform\Freeform::getInstance()->fields->getFields($form);
                    foreach ($freeformFields as $freeformField) {
                        if (!$freeformField instanceof \Solspace\Freeform\Fields\FieldInterface) {
                            continue;
                        }
                        $fieldId = trim((string)($freeformField->getId() ?? ''));
                        if ($fieldId === '') {
                            continue;
                        }
                        $sourceLabel = trim((string)($freeformField->getLabel() ?? $freeformField->getHandle() ?? ''));
                        $sourceLabel = $sourceLabel !== '' ? $sourceLabel : ('Field ' . $fieldId);
                        $fieldType = trim((string)($freeformField->getType() ?? 'string'));
                        $fields[] = [
                            'externalFieldId' => $fieldId,
                            'sourceLabel' => $sourceLabel,
                            'dataType' => $fieldType !== '' ? $fieldType : 'string',
                            'canonicalKey' => $this->labelToCanonicalKey($sourceLabel),
                        ];
                    }

                    return $fields;
                }
            } catch (\Throwable) {
                return [];
            }
        }

        return [];
    }

    /**
     * @return array<int,array<string,string>>
     */
    public function getFormieForms(): array
    {
        $formClass = '\verbb\formie\elements\Form';
        if (!class_exists($formClass) || !method_exists($formClass, 'find')) {
            return [];
        }
        try {
            $rows = $formClass::find()
                ->orderBy(['title' => SORT_ASC, 'dateCreated' => SORT_ASC])
                ->all();
        } catch (\Throwable) {
            return [];
        }

        $forms = [];
        foreach ($rows as $row) {
            if (!is_object($row)) {
                continue;
            }
            $id = $this->objectStringValue($row, ['id']);
            if ($id === '') {
                continue;
            }
            $name = $this->objectStringValue($row, ['title', 'name']);
            $handle = $this->objectStringValue($row, ['handle']);
            $forms[] = [
                'id' => $id,
                'name' => $name !== '' ? $name : ($handle !== '' ? $handle : 'Untitled Form'),
                'handle' => $handle,
            ];
        }

        return $forms;
    }

    /**
     * @param string[] $selected
     * @return array<string,mixed>
     */
    public function buildCapabilities(array $selected): array
    {
        $forms = [];
        if (in_array('freeform', $selected, true)) {
            $forms[] = 'freeform';
        }
        if (in_array('formie', $selected, true)) {
            $forms[] = 'formie';
        }

        return [
            'forms' => $forms,
            'ecommerce' => in_array('commerce', $selected, true) ? ['craft-commerce'] : [],
            'ecommerce_funnel' => in_array('commerce', $selected, true),
        ];
    }

    /**
     * @param array<string,mixed> $runtimeState
     * @return array<int,array<string,mixed>>
     */
    public function buildFormsContracts(array $runtimeState): array
    {
        $contracts = [];
        $integrationSettings = is_array($runtimeState['integrationSettings'] ?? null)
            ? $runtimeState['integrationSettings']
            : [];

        $freeformForms = [];
        foreach ($this->getFreeformForms() as $form) {
            $id = (string)($form['id'] ?? '');
            if ($id === '') {
                continue;
            }
            $freeformForms[$id] = $form;
        }
        $formieForms = [];
        foreach ($this->getFormieForms() as $form) {
            $id = (string)($form['id'] ?? '');
            if ($id === '') {
                continue;
            }
            $formieForms[$id] = $form;
        }

        $freeformConfig = is_array($integrationSettings['freeform'] ?? null) ? $integrationSettings['freeform'] : [];
        $freeformPrefix = strtoupper(trim((string)($freeformConfig['prefix'] ?? ''))) ?: 'FF';
        $freeformFormsConfig = is_array($freeformConfig['forms'] ?? null) ? $freeformConfig['forms'] : [];
        foreach ($freeformFormsConfig as $formId => $config) {
            if (!is_array($config)) {
                continue;
            }
            $mode = trim((string)($config['mode'] ?? 'off'));
            if (!in_array($mode, ['off', 'count_only', 'custom_fields'], true) || $mode === 'off') {
                continue;
            }
            $fields = is_array($config['fields'] ?? null) ? $config['fields'] : [];
            $mappings = [];
            if ($mode === 'custom_fields') {
                foreach ($fields as $field) {
                    if (!is_array($field)) {
                        continue;
                    }
                    $target = trim((string)($field['target'] ?? ''));
                    if (!in_array($target, ['properties', 'tags'], true)) {
                        continue;
                    }
                    $mappings[] = [
                        'externalFieldId' => trim((string)($field['externalFieldId'] ?? '')),
                        'sourceLabel' => trim((string)($field['sourceLabel'] ?? '')),
                        'dataType' => trim((string)($field['dataType'] ?? 'string')) ?: 'string',
                        'canonicalKey' => trim((string)($field['canonicalKey'] ?? '')),
                        'target' => $target,
                        'displayLabelOverride' => trim((string)($field['displayLabelOverride'] ?? '')),
                    ];
                }
            }
            $known = $freeformForms[(string)$formId] ?? null;
            $externalId = trim((string)($config['externalFormId'] ?? $formId));
            $formName = trim((string)($config['formName'] ?? ($known['name'] ?? ('Freeform ' . $formId))));
            $formHandle = trim((string)($known['handle'] ?? ''));
            $prefixLower = strtolower($freeformPrefix) . '_';
            $contracts[] = [
                'provider' => 'freeform',
                'externalFormId' => str_starts_with($externalId, $prefixLower) ? $externalId : ($prefixLower . $externalId),
                'formHandle' => $formHandle !== '' ? $formHandle : ('freeform-' . $formId),
                'formName' => $formName,
                'enabled' => true,
                'countOnly' => $mode !== 'custom_fields',
                'mode' => $mode,
                'fieldMappings' => $mappings,
                'icon' => null,
            ];
        }

        $formieConfig = is_array($integrationSettings['formie'] ?? null) ? $integrationSettings['formie'] : [];
        $formiePrefix = strtoupper(trim((string)($formieConfig['prefix'] ?? ''))) ?: 'FRM';
        $formieMode = trim((string)($formieConfig['mode'] ?? 'off'));
        $normalizedFormieMode = $formieMode === 'custom_fields' ? 'count_only' : $formieMode;
        $formieIds = array_values(array_filter(array_map('strval', (array)($formieConfig['formIds'] ?? []))));
        if (in_array($normalizedFormieMode, ['count_only'], true)) {
            $formiePrefixLower = strtolower($formiePrefix) . '_';
            foreach ($formieIds as $formId) {
                $known = $formieForms[$formId] ?? null;
                $formName = trim((string)($known['name'] ?? ('Formie ' . $formId)));
                $formHandle = trim((string)($known['handle'] ?? ''));
                $contracts[] = [
                    'provider' => 'formie',
                    'externalFormId' => str_starts_with($formId, $formiePrefixLower) ? $formId : ($formiePrefixLower . $formId),
                    'formHandle' => $formHandle !== '' ? $formHandle : ('formie-' . $formId),
                    'formName' => $formName,
                    'enabled' => true,
                    'countOnly' => true,
                    'mode' => $normalizedFormieMode,
                    'fieldMappings' => [],
                    'icon' => null,
                ];
            }
        }

        return $contracts;
    }

    /**
     * @param array<string,mixed> $runtimeState
     * @return array<int,array{name:string,status:string}>
     */
    public function buildIntegrationReadinessRows(array $runtimeState): array
    {
        $selected = array_values(array_filter(array_map('strval', (array)($runtimeState['selectedIntegrations'] ?? []))));
        $contracts = $this->buildFormsContracts($runtimeState);
        $countsByProvider = [];
        foreach ($contracts as $contract) {
            $provider = trim((string)($contract['provider'] ?? ''));
            if ($provider === '') {
                continue;
            }
            $countsByProvider[$provider] = (int)($countsByProvider[$provider] ?? 0) + 1;
        }

        $labels = $this->integrationLabels();
        $integrationSettings = is_array($runtimeState['integrationSettings'] ?? null)
            ? $runtimeState['integrationSettings']
            : [];
        $rows = [];
        foreach ($selected as $integration) {
            $status = 'Configured';
            if (in_array($integration, ['freeform', 'formie'], true)) {
                $status = !empty($countsByProvider[$integration]) ? 'Configured' : 'Needs setup';
            } elseif ($integration === 'commerce') {
                $commerce = is_array($integrationSettings['commerce'] ?? null) ? $integrationSettings['commerce'] : [];
                $status = isset($commerce['mode']) ? 'Configured' : 'Needs setup';
            }
            $rows[] = [
                'name' => (string)($labels[$integration] ?? $integration),
                'status' => $status,
            ];
        }

        return $rows;
    }

    /**
     * @return array<int,array<string,mixed>>
     */
    public function collectPluginVersionSnapshot(): array
    {
        $pluginsService = Craft::$app->getPlugins();
        $updates = Craft::$app->getUpdates()->getUpdates(true);
        $updateMap = is_array($updates->plugins ?? null) ? $updates->plugins : [];

        $snapshot = [];
        foreach ($pluginsService->getAllPlugins() as $plugin) {
            $handle = $plugin->id;
            $update = $updateMap[$handle] ?? null;
            $latest = $update?->getLatest()?->version ?? $plugin->getVersion();
            $snapshot[] = [
                'handle' => $handle,
                'name' => $plugin->name,
                'version' => $plugin->getVersion(),
                'latest' => $latest,
                'updateAvailable' => version_compare($latest, $plugin->getVersion(), '>'),
            ];
        }

        return $snapshot;
    }

    /**
     * @return array<string,mixed>
     */
    private function pluginStatus(string $handle, string $label): array
    {
        $pluginsService = Craft::$app->getPlugins();
        $plugin = $pluginsService->getPlugin($handle);
        $installed = $pluginsService->isPluginInstalled($handle);
        $enabled = $installed && $pluginsService->isPluginEnabled($handle);

        if ($plugin === null) {
            return [
                'handle' => $handle,
                'label' => $label,
                'installed' => false,
                'enabled' => false,
                'version' => '',
                'iconDataUri' => '',
            ];
        }

        $iconDataUri = '';
        foreach (['icon.svg', 'icon-mask.svg'] as $iconFile) {
            $iconPath = rtrim((string)$plugin->getBasePath(), DIRECTORY_SEPARATOR) . DIRECTORY_SEPARATOR . $iconFile;
            if (!is_file($iconPath) || !is_readable($iconPath)) {
                continue;
            }
            $svg = @file_get_contents($iconPath);
            if (!is_string($svg) || trim($svg) === '') {
                continue;
            }
            $iconDataUri = 'data:image/svg+xml;base64,' . base64_encode($svg);
            break;
        }

        return [
            'handle' => $handle,
            'label' => $label,
            'installed' => $installed,
            'enabled' => $enabled,
            'version' => $plugin->getVersion(),
            'iconDataUri' => $iconDataUri,
        ];
    }

    private function labelToCanonicalKey(string $label): string
    {
        $normalized = preg_replace('/[^a-z0-9]+/i', ' ', trim($label)) ?? '';
        $words = preg_split('/\s+/', strtolower(trim($normalized))) ?: [];
        $words = array_values(array_filter($words, static fn(string $word): bool => $word !== ''));
        if ($words === []) {
            return 'field';
        }

        $first = array_shift($words);
        if ($first === null) {
            return 'field';
        }

        $result = $first;
        foreach ($words as $word) {
            $result .= ucfirst($word);
        }

        return $result;
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
}
