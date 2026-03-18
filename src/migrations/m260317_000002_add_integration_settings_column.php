<?php
namespace amici\Burrow\migrations;

use craft\db\Migration;

class m260317_000002_add_integration_settings_column extends Migration
{
    public function safeUp(): bool
    {
        $table = '{{%burrow_runtime_state}}';
        if ($this->db->tableExists($table) && !$this->db->columnExists($table, 'integrationSettings')) {
            $this->addColumn($table, 'integrationSettings', $this->json()->after('capabilities'));
        }

        return true;
    }

    public function safeDown(): bool
    {
        $table = '{{%burrow_runtime_state}}';
        if ($this->db->tableExists($table) && $this->db->columnExists($table, 'integrationSettings')) {
            $this->dropColumn($table, 'integrationSettings');
        }

        return true;
    }
}
