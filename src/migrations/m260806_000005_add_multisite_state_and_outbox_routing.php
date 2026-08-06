<?php
namespace burrow\Burrow\migrations;

use craft\db\Migration;

/**
 * Adds per-site runtime state and outbox routing columns for multi-site installs.
 *
 * @author Burrow Analytics, LLC
 * @since 5.4.0
 */
class m260806_000005_add_multisite_state_and_outbox_routing extends Migration
{
    // =========================================================================
    // Public Methods
    // =========================================================================

    /**
     * @inheritdoc
     */
    public function safeUp(): bool
    {
        if (!$this->db->columnExists('{{%burrow_runtime_state}}', 'siteStates')) {
            $this->addColumn('{{%burrow_runtime_state}}', 'siteStates', $this->json());
        }

        if (!$this->db->columnExists('{{%burrow_outbox}}', 'project_id')) {
            $this->addColumn('{{%burrow_outbox}}', 'project_id', $this->string());
            $this->createIndex('idx_burrow_outbox_project_id', '{{%burrow_outbox}}', ['project_id'], false);
        }
        if (!$this->db->columnExists('{{%burrow_outbox}}', 'site_id')) {
            $this->addColumn('{{%burrow_outbox}}', 'site_id', $this->integer());
            $this->createIndex('idx_burrow_outbox_site_id', '{{%burrow_outbox}}', ['site_id'], false);
        }

        return true;
    }

    /**
     * @inheritdoc
     */
    public function safeDown(): bool
    {
        if ($this->db->columnExists('{{%burrow_outbox}}', 'site_id')) {
            $this->dropIndex('idx_burrow_outbox_site_id', '{{%burrow_outbox}}');
            $this->dropColumn('{{%burrow_outbox}}', 'site_id');
        }
        if ($this->db->columnExists('{{%burrow_outbox}}', 'project_id')) {
            $this->dropIndex('idx_burrow_outbox_project_id', '{{%burrow_outbox}}');
            $this->dropColumn('{{%burrow_outbox}}', 'project_id');
        }

        if ($this->db->columnExists('{{%burrow_runtime_state}}', 'siteStates')) {
            $this->dropColumn('{{%burrow_runtime_state}}', 'siteStates');
        }

        return true;
    }
}
