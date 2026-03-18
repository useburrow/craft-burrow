<?php
namespace amici\Burrow\migrations;

use craft\db\Migration;

class Install extends Migration
{
    public function safeUp(): bool
    {
        if (!$this->db->tableExists('{{%burrow_runtime_state}}')) {
            $this->createTable('{{%burrow_runtime_state}}', [
                'id' => $this->primaryKey(),
                'projectId' => $this->string(),
                'clientId' => $this->string(),
                'organizationId' => $this->string(),
                'projectSourceId' => $this->string(),
                'sourceIds' => $this->json(),
                'sdkState' => $this->json(),
                'ingestionKey' => $this->json(),
                'burrowProject' => $this->json(),
                'selectedIntegrations' => $this->json(),
                'capabilities' => $this->json(),
                'integrationSettings' => $this->json(),
                'lastSnapshot' => $this->json(),
                'onboardingStep' => $this->string()->defaultValue('connection'),
                'onboardingCompleted' => $this->boolean()->defaultValue(false),
                'dateCreated' => $this->dateTime()->notNull(),
                'dateUpdated' => $this->dateTime()->notNull(),
                'uid' => $this->uid(),
            ]);
        }

        if (!$this->db->tableExists('{{%burrow_outbox}}')) {
            $this->createTable('{{%burrow_outbox}}', [
                'id' => $this->string(32)->notNull(),
                'event_key' => $this->string()->notNull(),
                'channel' => $this->string(),
                'event_name' => $this->string(),
                'status' => $this->string(16)->notNull()->defaultValue('pending'),
                'attempt_count' => $this->integer()->notNull()->defaultValue(0),
                'max_attempts' => $this->integer()->notNull()->defaultValue(6),
                'payload' => $this->json()->notNull(),
                'last_error' => $this->text(),
                'next_attempt_at' => $this->dateTime(),
                'sent_at' => $this->dateTime(),
                'created_at' => $this->dateTime()->notNull(),
                'updated_at' => $this->dateTime()->notNull(),
            ]);
            $this->addPrimaryKey('pk_burrow_outbox', '{{%burrow_outbox}}', ['id']);
            $this->createIndex('idx_burrow_outbox_event_key', '{{%burrow_outbox}}', ['event_key'], true);
            $this->createIndex('idx_burrow_outbox_status', '{{%burrow_outbox}}', ['status'], false);
            $this->createIndex('idx_burrow_outbox_next_attempt', '{{%burrow_outbox}}', ['next_attempt_at'], false);
        }

        if (!$this->db->tableExists('{{%burrow_outbox_sent}}')) {
            $this->createTable('{{%burrow_outbox_sent}}', [
                'event_key' => $this->string()->notNull(),
                'sent_at' => $this->dateTime()->notNull(),
            ]);
            $this->addPrimaryKey('pk_burrow_outbox_sent', '{{%burrow_outbox_sent}}', ['event_key']);
        }

        if (!$this->db->tableExists('{{%burrow_event_logs}}')) {
            $this->createTable('{{%burrow_event_logs}}', [
                'id' => $this->primaryKey(),
                'type' => $this->string(32)->notNull()->defaultValue('info'),
                'provider' => $this->string(),
                'channel' => $this->string(),
                'eventKey' => $this->string(),
                'message' => $this->text(),
                'context' => $this->json(),
                'dateCreated' => $this->dateTime()->notNull(),
                'dateUpdated' => $this->dateTime()->notNull(),
                'uid' => $this->uid(),
            ]);
            $this->createIndex('idx_burrow_event_logs_type', '{{%burrow_event_logs}}', ['type'], false);
            $this->createIndex('idx_burrow_event_logs_provider', '{{%burrow_event_logs}}', ['provider'], false);
            $this->createIndex('idx_burrow_event_logs_channel', '{{%burrow_event_logs}}', ['channel'], false);
        }

        return true;
    }

    public function safeDown(): bool
    {
        $this->dropTableIfExists('{{%burrow_event_logs}}');
        $this->dropTableIfExists('{{%burrow_outbox_sent}}');
        $this->dropTableIfExists('{{%burrow_outbox}}');
        $this->dropTableIfExists('{{%burrow_runtime_state}}');

        return true;
    }
}
