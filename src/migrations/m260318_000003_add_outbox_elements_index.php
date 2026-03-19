<?php
namespace burrow\Burrow\migrations;

use craft\db\Migration;

class m260318_000003_add_outbox_elements_index extends Migration
{
    public function safeUp(): bool
    {
        if (!$this->db->tableExists('{{%burrow_outbox_elements}}')) {
            $this->createTable('{{%burrow_outbox_elements}}', [
                'id' => $this->primaryKey(),
                'outboxId' => $this->char(32)->notNull(),
                'eventKey' => $this->string()->notNull(),
                'channel' => $this->string(),
                'eventName' => $this->string(),
                'outboxStatus' => $this->string(20)->notNull()->defaultValue('pending'),
                'attemptCount' => $this->integer()->notNull()->defaultValue(0),
                'maxAttempts' => $this->integer()->notNull()->defaultValue(1),
                'lastError' => $this->text(),
                'nextAttemptAt' => $this->dateTime(),
                'sentAt' => $this->dateTime(),
                'outboxCreatedAt' => $this->dateTime()->notNull(),
                'outboxUpdatedAt' => $this->dateTime()->notNull(),
            ]);

            $this->createIndex(null, '{{%burrow_outbox_elements}}', ['outboxId'], true);
            $this->createIndex(null, '{{%burrow_outbox_elements}}', ['outboxStatus'], false);
            $this->createIndex(null, '{{%burrow_outbox_elements}}', ['eventKey'], false);
            $this->createIndex(null, '{{%burrow_outbox_elements}}', ['channel'], false);
            $this->createIndex(null, '{{%burrow_outbox_elements}}', ['eventName'], false);
            $this->createIndex(null, '{{%burrow_outbox_elements}}', ['outboxCreatedAt'], false);

            $this->addForeignKey(
                null,
                '{{%burrow_outbox_elements}}',
                ['id'],
                '{{%elements}}',
                ['id'],
                'CASCADE',
                'CASCADE'
            );
        }

        return true;
    }

    public function safeDown(): bool
    {
        if ($this->db->tableExists('{{%burrow_outbox_elements}}')) {
            $this->dropTable('{{%burrow_outbox_elements}}');
        }

        return true;
    }
}
