<?php
namespace burrow\Burrow\migrations;

use craft\db\Migration;

class m260325_000004_add_connection_credentials_to_runtime_state extends Migration
{
    public function safeUp(): bool
    {
        if (!$this->db->columnExists('{{%burrow_runtime_state}}', 'connectionBaseUrl')) {
            $this->addColumn('{{%burrow_runtime_state}}', 'connectionBaseUrl', $this->text());
        }
        if (!$this->db->columnExists('{{%burrow_runtime_state}}', 'connectionApiKey')) {
            $this->addColumn('{{%burrow_runtime_state}}', 'connectionApiKey', $this->text());
        }

        return true;
    }

    public function safeDown(): bool
    {
        if ($this->db->columnExists('{{%burrow_runtime_state}}', 'connectionApiKey')) {
            $this->dropColumn('{{%burrow_runtime_state}}', 'connectionApiKey');
        }
        if ($this->db->columnExists('{{%burrow_runtime_state}}', 'connectionBaseUrl')) {
            $this->dropColumn('{{%burrow_runtime_state}}', 'connectionBaseUrl');
        }

        return true;
    }
}
