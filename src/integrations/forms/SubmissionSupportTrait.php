<?php
namespace burrow\Burrow\integrations\forms;

trait SubmissionSupportTrait
{
    /**
     * @param array<string,mixed> $mappedFields
     * @return array{tags: array<string, mixed>, properties: array<string, mixed>}
     */
    protected function extractMappedSubmissionPayload(object $submission, array $mappedFields): array
    {
        $values = $this->extractSubmissionScalarValues($submission);
        if ($values === []) {
            return ['tags' => [], 'properties' => []];
        }
        $normalizedValues = [];
        foreach ($values as $key => $value) {
            $normalizedValues[$this->normalizeFieldKey($key)] = $value;
        }

        $tags = [];
        $properties = [];
        foreach ($mappedFields as $fieldConfig) {
            if (!is_array($fieldConfig)) {
                continue;
            }
            $target = trim((string)($fieldConfig['target'] ?? ''));
            if (!in_array($target, ['tags', 'properties'], true)) {
                continue;
            }
            $canonicalKey = trim((string)($fieldConfig['canonicalKey'] ?? ''));
            if ($canonicalKey === '') {
                continue;
            }
            $candidates = array_values(array_filter([
                trim((string)($fieldConfig['externalFieldId'] ?? '')),
                trim((string)($fieldConfig['sourceLabel'] ?? '')),
                $canonicalKey,
            ], static fn(string $value): bool => $value !== ''));

            $value = $this->findSubmissionValue($values, $normalizedValues, $candidates);
            if ($value === null || $value === '') {
                continue;
            }
            if ($target === 'tags') {
                $tags[$canonicalKey] = $value;
            } else {
                $properties[$canonicalKey] = $value;
            }
        }

        return [
            'tags' => $tags,
            'properties' => $properties,
        ];
    }

    /**
     * @param array<string,mixed> $values
     * @param array<string,mixed> $normalizedValues
     * @param list<string> $candidates
     */
    protected function findSubmissionValue(array $values, array $normalizedValues, array $candidates): mixed
    {
        foreach ($candidates as $candidate) {
            if (array_key_exists($candidate, $values)) {
                return $values[$candidate];
            }
            $normalized = $this->normalizeFieldKey($candidate);
            if ($normalized !== '' && array_key_exists($normalized, $normalizedValues)) {
                return $normalizedValues[$normalized];
            }
        }

        return null;
    }

    protected function normalizeFieldKey(string $key): string
    {
        return preg_replace('/[^a-z0-9]+/i', '', strtolower(trim($key))) ?? '';
    }

    /**
     * @return array<string, string>
     */
    protected function extractSubmissionScalarValues(object $submission): array
    {
        $values = [];

        if (method_exists($submission, 'getFormFieldValues')) {
            try {
                $fieldValues = $submission->getFormFieldValues();
                if (is_array($fieldValues)) {
                    foreach ($fieldValues as $handle => $value) {
                        if (!is_string($handle) || $handle === '') {
                            continue;
                        }
                        $normalized = $this->stringifySubmissionFieldValue($value);
                        if ($normalized !== null && $normalized !== '') {
                            $values[$handle] = $normalized;
                        }
                    }
                }
            } catch (\Throwable) {
            }
        }

        if (method_exists($submission, 'getValuesAsString')) {
            try {
                $fieldValues = $submission->getValuesAsString();
                if (is_array($fieldValues)) {
                    foreach ($fieldValues as $handle => $value) {
                        if (!is_string($handle) || $handle === '' || array_key_exists($handle, $values)) {
                            continue;
                        }
                        $normalized = $this->stringifySubmissionFieldValue($value);
                        if ($normalized !== null && $normalized !== '') {
                            $values[$handle] = $normalized;
                        }
                    }
                }
            } catch (\Throwable) {
            }
        }

        if (method_exists($submission, 'toArray')) {
            $raw = $submission->toArray();
            if (is_array($raw)) {
                foreach ($raw as $key => $value) {
                    if (!is_string($key) || $key === '' || array_key_exists($key, $values)) {
                        continue;
                    }
                    if (is_scalar($value)) {
                        $normalized = $this->stringifySubmissionValue($value);
                        if ($normalized !== null && $normalized !== '') {
                            $values[$key] = $normalized;
                        }
                        continue;
                    }
                    $normalized = $this->stringifySubmissionFieldValue($value);
                    if ($normalized !== null && $normalized !== '') {
                        $values[$key] = $normalized;
                    }
                }
            }
        }

        if (method_exists($submission, 'getFieldValues')) {
            $fieldValues = $submission->getFieldValues();
            if (is_array($fieldValues)) {
                foreach ($fieldValues as $handle => $value) {
                    if (!is_string($handle) || $handle === '' || array_key_exists($handle, $values)) {
                        continue;
                    }
                    if (is_scalar($value)) {
                        $normalized = $this->stringifySubmissionValue($value);
                        if ($normalized !== null && $normalized !== '') {
                            $values[$handle] = $normalized;
                        }
                        continue;
                    }
                    if (is_array($value)) {
                        $flattened = [];
                        foreach ($value as $part) {
                            $partValue = $this->stringifySubmissionFieldValue($part);
                            if ($partValue !== null && $partValue !== '') {
                                $flattened[] = $partValue;
                            }
                        }
                        if ($flattened !== []) {
                            $values[$handle] = implode(', ', $flattened);
                        }
                        continue;
                    }
                    $normalized = $this->stringifySubmissionFieldValue($value);
                    if ($normalized !== null && $normalized !== '') {
                        $values[$handle] = $normalized;
                    }
                }
            }
        }

        if ($values === [] && is_iterable($submission)) {
            try {
                foreach ($submission as $field) {
                    if (!is_object($field)) {
                        continue;
                    }
                    $handle = method_exists($field, 'getHandle') ? trim((string)$field->getHandle()) : '';
                    if ($handle === '' || array_key_exists($handle, $values)) {
                        continue;
                    }
                    $fieldValue = method_exists($field, 'getValue') ? $field->getValue() : null;
                    $normalized = $this->stringifySubmissionFieldValue($fieldValue);
                    if ($normalized !== null && $normalized !== '') {
                        $values[$handle] = $normalized;
                    }
                }
            } catch (\Throwable) {
            }
        }

        return $values;
    }

    protected function extractSubmissionFormId(object $submission): int
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

        $raw = null;
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
                } catch (\Throwable) {
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

    protected function extractSubmissionFormName(object $submission): string
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
            } catch (\Throwable) {
            }
        }

        return '';
    }

    protected function extractFormObjectId(object $form): int
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
    protected function objectDateValue(object $object, array $keys): string
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

    /**
     * @param array<int, string> $keys
     */
    protected function objectStringValue(object $object, array $keys): string
    {
        foreach ($keys as $key) {
            if (method_exists($object, 'get' . ucfirst($key))) {
                $value = $object->{'get' . ucfirst($key)}();
                $text = trim((string)$value);
                if ($text !== '') {
                    return $text;
                }
            }
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

    protected function normalizeSubmissionTimestamp(string $value): string
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

    protected function stringifySubmissionValue(mixed $value): ?string
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

    protected function stringifySubmissionFieldValue(mixed $value): ?string
    {
        if ($value === null) {
            return null;
        }
        if (is_object($value)) {
            if (method_exists($value, 'getValueAsString')) {
                try {
                    $asString = $value->getValueAsString();
                    if (is_string($asString)) {
                        $trimmed = trim($asString);
                        if ($trimmed !== '') {
                            return $trimmed;
                        }
                    }
                } catch (\Throwable) {
                }
            }
            if (method_exists($value, 'getValue')) {
                try {
                    return $this->stringifySubmissionFieldValue($value->getValue());
                } catch (\Throwable) {
                }
            }

            return null;
        }
        if (is_array($value)) {
            $flattened = [];
            foreach ($value as $part) {
                $partValue = $this->stringifySubmissionFieldValue($part);
                if ($partValue !== null && $partValue !== '') {
                    $flattened[] = $partValue;
                }
            }

            return $flattened !== [] ? implode(', ', $flattened) : null;
        }

        return $this->stringifySubmissionValue($value);
    }

    protected function labelToCanonicalKey(string $label): string
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
}
