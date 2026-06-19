<?php
namespace burrow\Burrow\integrations\forms;

use yii\base\Event;

class FormieFormAdapter extends AbstractFormIntegrationAdapter
{
    public function getId(): string
    {
        return 'formie';
    }

    public function getLabel(): string
    {
        return 'Formie';
    }

    public function getCraftPluginHandle(): string
    {
        return 'formie';
    }

    public function getSource(): string
    {
        return 'craft-formie';
    }

    public function getDefaultPrefix(): string
    {
        return 'FRM';
    }

    public function getFormIdTagPrefix(): string
    {
        return 'frm_';
    }

    protected function getSubmissionElementClass(): ?string
    {
        return '\verbb\formie\elements\Submission';
    }

    protected function backfillRequiresConfiguredForms(): bool
    {
        return true;
    }

    /**
     * @param callable(Event):void $handler
     */
    public function registerEventHooks(callable $handler): void
    {
        $serviceClass = '\verbb\formie\services\Submissions';
        $eventConst = $serviceClass . '::EVENT_AFTER_SUBMISSION';
        if (!class_exists($serviceClass) || !defined($eventConst)) {
            return;
        }

        /** @var string $eventName */
        $eventName = constant($eventConst);
        Event::on(
            $serviceClass,
            $eventName,
            static function (Event $event) use ($handler): void {
                $handler($event);
            }
        );
    }

    /**
     * @return null|array{submission: object, eventForm: ?object}
     */
    public function normalizeSubmissionFromEvent(Event $event): ?array
    {
        $success = $event->success ?? null;
        if ($success !== null && $success !== true) {
            return null;
        }

        $submission = is_object($event->submission ?? null) ? $event->submission : null;
        if ($submission === null) {
            return null;
        }

        return [
            'submission' => $submission,
            'eventForm' => null,
        ];
    }

    /**
     * @return array<int, array{id: string, name: string, handle: string}>
     */
    public function discoverForms(): array
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
     * @return array<int, array{externalFieldId: string, sourceLabel: string, dataType: string, canonicalKey: string}>
     */
    public function discoverFields(string $formId): array
    {
        $fields = [];
        if ($formId === '') {
            return $fields;
        }

        $formClass = '\verbb\formie\elements\Form';
        if (!class_exists($formClass) || !method_exists($formClass, 'find')) {
            return $fields;
        }

        try {
            $form = $formClass::find()->id((int)$formId)->status(null)->one();
            if ($form === null) {
                return $fields;
            }

            $candidates = [];
            if (method_exists($form, 'getCustomFields')) {
                $raw = $form->getCustomFields();
                if (is_array($raw)) {
                    $candidates = $raw;
                }
            }
            if ($candidates === [] && method_exists($form, 'getFieldLayout')) {
                $layout = $form->getFieldLayout();
                if ($layout !== null && method_exists($layout, 'getCustomFields')) {
                    $custom = $layout->getCustomFields();
                    if (is_array($custom)) {
                        $candidates = $custom;
                    }
                }
            }
            if ($candidates === [] && method_exists($form, 'getPages')) {
                foreach ($form->getPages() as $page) {
                    if (!is_object($page) || !method_exists($page, 'getRows')) {
                        continue;
                    }
                    foreach ($page->getRows() as $row) {
                        if (!is_object($row) || !method_exists($row, 'getFields')) {
                            continue;
                        }
                        foreach ($row->getFields() as $formField) {
                            $candidates[] = $formField;
                        }
                    }
                }
            }

            foreach ($candidates as $unionField) {
                if (!is_object($unionField)) {
                    continue;
                }
                $inner = $unionField;
                if (method_exists($unionField, 'getField')) {
                    try {
                        $maybe = $unionField->getField();
                        if (is_object($maybe)) {
                            $inner = $maybe;
                        }
                    } catch (\Throwable) {
                    }
                }
                $handle = '';
                if (method_exists($inner, 'getHandle')) {
                    $handle = trim((string)$inner->getHandle());
                } elseif (isset($inner->handle)) {
                    $handle = trim((string)$inner->handle);
                }
                if ($handle === '') {
                    continue;
                }
                $label = $handle;
                if (method_exists($inner, 'name') && is_string($inner->name) && trim($inner->name) !== '') {
                    $label = trim($inner->name);
                } elseif (method_exists($inner, 'getName')) {
                    $label = trim((string)$inner->getName()) ?: $handle;
                } elseif (method_exists($unionField, 'getLabel')) {
                    $label = trim((string)$unionField->getLabel()) ?: $handle;
                }
                $classBase = basename(str_replace('\\', '/', get_class($inner)));
                $dataType = strtolower((string)preg_replace('/Field$/', '', $classBase));
                if ($dataType === '') {
                    $dataType = 'string';
                }
                $fields[] = [
                    'externalFieldId' => $handle,
                    'sourceLabel' => $label !== '' ? $label : $handle,
                    'dataType' => $dataType,
                    'canonicalKey' => $this->labelToCanonicalKey($label !== '' ? $label : $handle),
                ];
            }
        } catch (\Throwable) {
            return [];
        }

        return $fields;
    }
}
