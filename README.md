# Burrow for Craft CMS 5

Install a plugin. See everything.

[Burrow](https://useburrow.com) is the orchestration layer for agency operations. This plugin connects your Craft site to Burrow — detecting your installed integrations, syncing event data from forms and ecommerce, publishing system snapshots, and giving your team operational visibility across every client project.

No webhook configurations in form plugins. No CSV imports. One plugin, total visibility.

## Why agencies install Burrow

- **Install. Detect. Done.** — Drop the plugin into Craft and it scans your stack, detects Freeform, Formie, and Commerce, and starts syncing automatically.
- **Event tracking for forms and ecommerce** — Form submissions and Commerce orders flow into Burrow as normalized events, scoped by organization, client, and project.
- **System snapshots** — Publish your Craft version, PHP version, and installed plugin inventory so Burrow always has current environment context.
- **Backfill your history** — Replay up to two years of form submissions and Commerce orders into Burrow with one click.
- **Operational confidence** — Dashboard, outbox, event logs, retry controls, and queue visibility right in the Control Panel.

## Quick links

- Package: `useburrow/craft-burrow`
- Documentation: https://useburrow.com/docs
- Issues: https://github.com/useburrow/craft-burrow/issues
- Changelog: [CHANGELOG.md](CHANGELOG.md)
- Source: https://github.com/useburrow/craft-burrow

## Supported integrations

| Integration | What Burrow tracks |
|---|---|
| **Freeform** | Form submissions, field mapping, per-form sync modes |
| **Formie** | Form submissions, field mapping, per-form sync modes |
| **Craft Commerce** | Orders, line items, ecommerce funnel events |
| **Craft CMS** | System snapshots — Craft version, PHP version, installed plugins |

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

```bash
composer require useburrow/craft-burrow
```

Then install the plugin from the Craft Control Panel or run:

```bash
php craft plugin/install burrow
```

### Requirements

- Craft CMS **5.0+**
- PHP **8.2+**

## Configuration

The plugin stores three settings in project config:

| Setting | Description |
|---|---|
| `baseUrl` | Burrow API endpoint |
| `apiKey` | Burrow API key |
| `pluginName` | Display name in the Control Panel |

All runtime state (linked project, integrations, sync metadata, snapshots) is stored in plugin tables — not project config.

> Avoid committing live Burrow credentials in project config. Use environment variables where possible.

## About Burrow

Burrow is the event-driven orchestration layer for agency operations. It ingests and normalizes signals from your entire stack — code, analytics, ecommerce, forms, monitoring — into a single event model scoped by organization, client, and project.

- First-class plugins for **WordPress**, **Craft CMS**, **Statamic**, **ExpressionEngine**, and more.
- PHP and TypeScript SDKs for custom integrations
- Client portals, reporting, and AI-powered automation

Learn more at [useburrow.com](https://useburrow.com).
