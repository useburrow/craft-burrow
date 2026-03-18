# Burrow for Craft CMS 5

`useburrow/craft-burrow` is a Craft CMS 5 plugin that connects a Craft site to Burrow through the Burrow PHP SDK. It provides a guided Control Panel onboarding flow, integration setup for supported Craft plugins, contract sync, system snapshot publishing, and operational screens for backfill, logs, and outbox visibility.

## Current Release

Version `5.0.0` includes:

- 5-step onboarding wizard in the Craft Control Panel
- Burrow project discovery and linking through the SDK
- Integration setup for `Freeform`, `Formie`, and `Craft Commerce`
- Forms contract generation and sync to Burrow
- System snapshot capture and publish flow
- Dashboard for connection, integrations, sync state, and recent logs
- Outbox screen with retry and delete actions
- Manual historical backfill for forms and ecommerce events
- Runtime state, event log, and queue persistence in plugin tables

## Supported Mapping

- WordPress form plugin flows map to Craft form integrations through `Freeform` and `Formie`
- WooCommerce-style commerce tracking maps to `Craft Commerce`
- Stack health and update reporting maps to Craft + installed plugin snapshot data

## Control Panel Flow

The plugin setup is implemented as a 5-step wizard:

1. **Connection**
   - Save Burrow `baseUrl` and `apiKey`
   - Run SDK discovery to load available projects
2. **Project**
   - Select the Burrow project/client to link
   - Persist routing metadata, ingestion key, project metadata, and SDK state
3. **Integrations**
   - Choose supported integrations
   - Generate capabilities from enabled providers
   - Configure provider-specific settings
4. **Review**
   - Review integration summary and contracts
   - Refresh the current system snapshot
   - Sync form contracts to Burrow
5. **Finish**
   - Mark onboarding complete
   - Publish the latest system snapshot

After onboarding, the plugin exposes:

- `Dashboard`: linked project, integrations, contract sync state, snapshot status, logs, and backfill controls
- `Outbox`: recent queue records with retry/delete actions
- `Setup`: the onboarding wizard for later updates

## Integrations

### Freeform

- Discovers available forms and fields
- Supports per-form modes:
  - `off`
  - `count_only`
  - `custom_fields`
- Supports field mapping metadata for custom field sync contracts

### Formie

- Supports selecting one or more forms
- Supports modes:
  - `off`
  - `count_only`
  - `custom_fields`

### Craft Commerce

- Supports ecommerce event tracking mode
- Optional ecommerce funnel capability flag
- Supports historical order/item backfill into Burrow

## Backfill and Operations

The plugin includes a manual backfill workflow for replaying historical data into Burrow.

- Forms backfill from Freeform and Formie submissions
- Ecommerce backfill from Craft Commerce orders and line items
- Time windows:
  - last 7 days
  - last 30 days
  - last 90 days
  - past year
  - two years
  - all time

Operational visibility includes:

- queue status counters
- recent event logs
- recent outbox rows
- retry/delete controls for outbox entries

## Installation

### Requirements

- PHP `^8.2`
- Craft CMS `^5.0`
- Burrow PHP SDK `^0.9.2`

### Composer

Install the plugin and SDK in your Craft project:

```json
{
  "require": {
    "useburrow/craft-burrow": "^5.0",
    "useburrow/sdk-php": "^0.9.2"
  }
}
```

If you are developing locally with path repositories, point Composer at this plugin and the Burrow SDK repository as needed.

If the SDK is not installed, the Control Panel setup UI will still load, but Burrow discovery/link/sync actions will fail gracefully with a warning.

## Persistence Model

### Plugin Settings / Project Config

Minimal plugin settings:

- `baseUrl`
- `apiKey`
- `pluginName`

Avoid committing live Burrow credentials in project config or environment-specific config files.

### Database Tables

- `burrow_runtime_state`
  - linked org/client/project identifiers
  - channel source IDs
  - SDK state
  - ingestion key metadata
  - selected integrations and capabilities
  - integration settings
  - onboarding progress/completion
  - latest system snapshot
- `burrow_outbox`
  - queued event payloads
  - attempt counts and retry metadata
  - failure and sent state
- `burrow_outbox_sent`
  - sent event key ledger for dedupe support
- `burrow_event_logs`
  - operational logs for onboarding, sync, snapshot, and backfill actions

## Key Files

- `src/Plugin.php`: plugin bootstrap, CP navigation, and route registration
- `src/controllers/SettingsController.php`: onboarding, dashboard, outbox, contract sync, snapshot, and backfill actions
- `src/services/BurrowApiService.php`: Burrow SDK integration layer
- `src/services/IntegrationsService.php`: provider discovery, capabilities, and contract building
- `src/services/SystemSnapshotService.php`: Craft/system/plugin snapshot collector
- `src/services/BackfillService.php`: historical event extraction and submission
- `src/services/QueueService.php`: outbox operations and queue statistics
- `src/services/EventLogService.php`: operational event logging
- `src/services/StateService.php`: runtime state persistence
- `src/templates/settings/index.twig`: onboarding wizard UI
- `src/templates/dashboard/index.twig`: operational dashboard
- `src/templates/outbox/index.twig`: outbox management UI
- `src/migrations/Install.php`: initial plugin table install

## Notes

- The plugin is designed as a Craft-side bridge for Burrow onboarding and operational sync workflows.
- Runtime state is stored in plugin tables rather than expanding project config beyond the minimal plugin settings.
- This release focuses on onboarding, sync metadata, visibility, and backfill tooling for supported integrations.
