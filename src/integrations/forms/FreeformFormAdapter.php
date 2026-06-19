<?php
namespace burrow\Burrow\integrations\forms;

use yii\base\Event;

class FreeformFormAdapter extends AbstractFormIntegrationAdapter
{
    public function getId(): string
    {
        return 'freeform';
    }

    public function getLabel(): string
    {
        return 'Freeform';
    }

    public function getCraftPluginHandle(): string
    {
        return 'freeform';
    }

    public function getSource(): string
    {
        return 'craft-freeform';
    }

    public function getDefaultPrefix(): string
    {
        return 'FF';
    }

    public function getFormIdTagPrefix(): string
    {
        return 'ff_';
    }

    protected function getSubmissionElementClass(): ?string
    {
        return '\Solspace\Freeform\Elements\Submission';
    }

    protected function backfillRequiresConfiguredForms(): bool
    {
        return false;
    }

    /**
     * @param callable(Event):void $handler
     */
    public function registerEventHooks(callable $handler): void
    {
        $freeformFormClass = '\Solspace\Freeform\Form\Form';
        $eventConst = $freeformFormClass . '::EVENT_AFTER_SUBMIT';
        if (!class_exists($freeformFormClass) || !defined($eventConst)) {
            return;
        }

        /** @var string $eventName */
        $eventName = constant($eventConst);
        Event::on(
            $freeformFormClass,
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
        $eventForm = null;
        if (method_exists($event, 'getForm')) {
            $eventForm = $event->getForm();
        } elseif (is_object($event->form ?? null)) {
            $eventForm = $event->form;
        }
        if (is_object($eventForm)) {
            if (method_exists($eventForm, 'hasErrors') && $eventForm->hasErrors()) {
                return null;
            }
            if (method_exists($eventForm, 'isMarkedAsSpam') && $eventForm->isMarkedAsSpam()) {
                return null;
            }
        }

        $submission = null;
        if (method_exists($event, 'getSubmission')) {
            $submission = $event->getSubmission();
        } elseif (is_object($event->submission ?? null)) {
            $submission = $event->submission;
        }
        if (!is_object($submission)) {
            return null;
        }

        return [
            'submission' => $submission,
            'eventForm' => is_object($eventForm) ? $eventForm : null,
        ];
    }

    /**
     * @return array<int, array{id: string, name: string, handle: string}>
     */
    public function discoverForms(): array
    {
        $forms = [];
        if (!class_exists('\Solspace\Freeform\Freeform')) {
            return $forms;
        }

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
        } catch (\Throwable) {
            return [];
        }

        return $forms;
    }

    /**
     * @return array<int, array{externalFieldId: string, sourceLabel: string, dataType: string, canonicalKey: string}>
     */
    public function discoverFields(string $formId): array
    {
        $fields = [];
        if ($formId === '' || !class_exists('\Solspace\Freeform\Freeform')) {
            return $fields;
        }

        try {
            $formModel = \Solspace\Freeform\Freeform::getInstance()->forms->getFormById((int)$formId);
            if ($formModel === null) {
                return $fields;
            }

            $layout = method_exists($formModel, 'getLayout')
                ? $formModel->getLayout()
                : (method_exists($formModel, 'getForm') ? $formModel->getForm()->getLayout() : null);
            if ($layout === null || !method_exists($layout, 'getFields')) {
                return $fields;
            }

            foreach ($layout->getFields() as $freeformField) {
                if (!is_object($freeformField)) {
                    continue;
                }
                if (method_exists($freeformField, 'canStoreValues') && !$freeformField->canStoreValues()) {
                    continue;
                }

                $handle = trim((string)($freeformField->getHandle() ?? ''));
                if ($handle === '') {
                    continue;
                }

                $sourceLabel = trim((string)($freeformField->getLabel() ?? $handle));
                $sourceLabel = $sourceLabel !== '' ? $sourceLabel : $handle;
                $fieldType = method_exists($freeformField, 'getType')
                    ? trim((string)($freeformField->getType() ?? 'string'))
                    : 'string';
                $fields[] = [
                    'externalFieldId' => $handle,
                    'sourceLabel' => $sourceLabel,
                    'dataType' => $fieldType !== '' ? $fieldType : 'string',
                    'canonicalKey' => $this->labelToCanonicalKey($sourceLabel),
                ];
            }
        } catch (\Throwable) {
            return [];
        }

        return $fields;
    }
}
