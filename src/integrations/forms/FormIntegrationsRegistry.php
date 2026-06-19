<?php
namespace burrow\Burrow\integrations\forms;

class FormIntegrationsRegistry
{
    /** @var array<string, FormIntegrationAdapter> */
    private array $_adapters = [];

    public function __construct()
    {
        $this->register(new FreeformFormAdapter());
        $this->register(new FormieFormAdapter());
    }

    public function register(FormIntegrationAdapter $adapter): void
    {
        $this->_adapters[$adapter->getId()] = $adapter;
    }

    /**
     * @return FormIntegrationAdapter[]
     */
    public function all(): array
    {
        return array_values($this->_adapters);
    }

    /**
     * @return string[]
     */
    public function ids(): array
    {
        return array_keys($this->_adapters);
    }

    public function get(string $id): ?FormIntegrationAdapter
    {
        return $this->_adapters[$id] ?? null;
    }

    public function has(string $id): bool
    {
        return isset($this->_adapters[$id]);
    }
}
