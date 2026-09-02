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
| **Shopify (Headless)** | Cart funnel events (`ecommerce.cart.added` / `ecommerce.cart.removed`) on `craftcms/shopify` frontends |
| **Craft CMS** | System snapshots — Craft version, PHP version, installed plugins |

## Control Panel Flow

### Initial setup (`Setup`)

First-time connection uses a step-by-step wizard at **Burrow → Setup** (`burrow/setup`):

1. **Connection** — Save Burrow `baseUrl` and `apiKey`, run SDK discovery (auto-skipped when `BURROW_API_KEY` resolves)
2. **Sites** — Multi-site installs only: choose which Craft sites to link
3. **Project** — Select the Burrow project/client to link; persist routing metadata and ingestion key
4. **Integrations** — Choose supported integrations and configure provider-specific settings
5. **Review** — Review contracts and sync to Burrow (including Commerce-only installs)
6. **Finish** — Mark onboarding complete

### After onboarding (`Settings`)

Once connected, **Burrow → Settings** (`burrow/settings`) provides ongoing configuration without repeating the wizard:

- **Overview** — Linked project, last sync metadata, active contracts, manual sync
- **Integrations** — Enable or disable providers (auto-syncs capabilities to Burrow)
- **Freeform / Formie / Commerce** — Edit tracking modes, field mappings, and funnel options (auto-syncs on save)
- **Connection** — View connection details; re-link via the setup wizard when needed

Changes saved in Settings are pushed to Burrow automatically (contracts, capability re-link when needed, and system snapshot).

### Operations

- **Dashboard** — Linked project summary, contract sync state, backfill, and outbox retention
- **Outbox** — Recent queue records with retry/delete actions
- **Setup** — Available again from the Connection section or before onboarding completes

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

### Shopify (Headless)

For Craft frontends that use [`craftcms/shopify`](https://github.com/craftcms/shopify) for catalog sync with cart and checkout on Shopify's side. Auto-suggested during setup when `craftcms/shopify` is installed and Craft Commerce is not.

- A small collector script is injected into the site frontend (only when funnel capture is enabled) that:
  - automatically captures forms posting to Shopify's `cart/add` endpoint — no site-code changes needed
  - exposes `window.burrow.track({ type: 'cart.added' | 'cart.removed', productId, productName, variantName, quantity, unitPrice, currency, ... })` for custom Storefront-API carts
- Events relay through a CSRF-protected, rate-limited same-origin endpoint (`/actions/burrow/collect`); the plugin validates and forwards them to Burrow server-side with the project ingestion key, so no Burrow credential is exposed to the page. Payloads carry no PII.
- Events are tagged with `provider: shopify` and your shop domain (auto-detected from the Shopify plugin settings, overridable in settings).
- Checkout, order, and refund events are captured by the Shopify checkout pixel and Admin API integration configured directly in Burrow (Integrations → Shopify) against the same project — the plugin never emits them in headless mode, so nothing is double counted.
- Optional data attributes enrich auto-captured cart events: `data-product-name`, `data-variant-name`, `data-price`, `data-currency` (or `data-burrow-*` variants) on or above the cart form.

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
| `baseUrl` | Burrow API endpoint (literal or `$BURROW_BASE_URL`) |
| `apiKey` | Organization API key (literal or `$BURROW_API_KEY`) |
| `pluginName` | Display name in the Control Panel |

All runtime state (linked project, integrations, sync metadata, snapshots) is stored in plugin tables — not project config.

### Environment variables (recommended)

`baseUrl` and `apiKey` are [Craft environmental settings](https://craftcms.com/docs/5.x/extend/environmental-settings.html). In **Setup → Connection**, enter an env reference instead of the secret:

```bash
BURROW_BASE_URL=https://app.useburrow.com
BURROW_API_KEY=your_org_api_key
```

Then in the Control Panel set:

- Base URL → `$BURROW_BASE_URL`
- API Key → `$BURROW_API_KEY`

Project config and the database store only the `$…` aliases. The secret stays in the environment.

You can also set `BURROW_BASE_URL` / `BURROW_API_KEY` as direct process overrides (they win over stored settings). That is useful in production when `allowAdminChanges` is false.

**What is stored after setup**

- The **organization API key** is used for discover/link (and linking additional Craft sites). Prefer `$BURROW_API_KEY` so the raw secret is never persisted.
- After a project is linked, **event delivery** uses a project-scoped **ingestion key** (encrypted in the plugin DB). That key is write-oriented for Burrow ingestion — not a general Burrow account password — but treat it as a secret.

> Avoid committing live Burrow credentials in project config. Use `$BURROW_API_KEY` (or the direct `BURROW_API_KEY` env override) instead.

### Local vs production

A Burrow project can be registered to **one** Craft site URL at a time. Linking from a non-production Craft environment (`CRAFT_ENVIRONMENT` anything other than `production` / `prod`) binds that environment’s site URL and will block production until you Disconnect in Burrow or confirm a site URL change.

**Recommended agency path**

1. Set `BURROW_API_KEY` (and optionally `BURROW_BASE_URL`) in every environment’s `.env` / hosting variables.
2. Finish full Setup (project link + integrations) only in **production**.
3. On local/dev, either stop after install + env vars, or link a **sandbox** Burrow project created in the Burrow app (the plugin cannot create projects).

**If you already linked a live project from local**

1. In Burrow: Project Settings → Integrations → **Disconnect** Craft.
2. In production Craft: run Setup and link the live project again.

When Craft’s environment is not `production` / `prod`, Setup shows a warning on the Project step explaining this.

## About Burrow

Burrow is the event-driven orchestration layer for agency operations. It ingests and normalizes signals from your entire stack — code, analytics, ecommerce, forms, monitoring — into a single event model scoped by organization, client, and project.

- First-class Craft CMS integration for agency operations workflows.
- PHP and TypeScript SDKs for custom integrations
- Client portals, reporting, and AI-powered automation

Learn more at [useburrow.com](https://useburrow.com).
