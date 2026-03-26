# Changelog

All notable changes to `useburrow/craft-burrow` will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

## [5.3.1] - 2026-03-26

### Fixed

- SDK-built ecommerce envelopes (cart abandonment, cart/checkout funnel, order placed, line items, etc.) now set `source` to `craft-plugin` after canonical builders run, so payloads and Burrow UI no longer incorrectly show `wordpress-plugin` for Craft Commerce.

### Changed

- Historical backfill loads submissions and orders in fixed batches, streams events to the API in submit chunks, and avoids building a single giant in-memory event list (reduces PHP memory exhaustion on large datasets).
- Backfill probe uses lightweight DB counts for form submission totals instead of materializing full event lists; ecommerce event counts still follow the same eligibility rules as backfill.

## [5.3.0] - 2026-03-25

### Added

- Runtime state columns `connectionBaseUrl` and `connectionApiKey` (migration) so Control Panel connection can be saved when `allowAdminChanges` is `false`.
- `Plugin::getBurrowBaseUrl()`, `getBurrowApiKey()`, `getConnectionSettingsForDisplay()`, `canDispatchToBurrow()`, `runtimeStateHasIngestionKey()`, and `clearAccountApiKeyFromProjectConfigIfAllowed()` for consistent credential resolution across CP, queue jobs, and the snapshot API.
- `helpers/CredentialCrypto` — encrypts `ingestionKey.key` and `connectionApiKey` at rest via `Craft::$app->getSecurity()->encryptByKey()` / `decryptByKey()` with distinct HKDF info strings; legacy plaintext values are read until the next save re-seals them.

### Changed

- Connection step persists credentials to the database first; project-config plugin settings are updated only when admin changes are permitted.
- After a successful project link, the **account-level** API key is cleared from runtime state (short-lived bootstrap); project **ingestion** key remains for ongoing API use. When `allowAdminChanges` is `true`, the plugin `apiKey` setting is also cleared after link.
- `getBurrowApiKey()` no longer falls back to project-config `apiKey` when an ingestion key exists, so stale YAML cannot override project-scoped auth.
- Forms contract submission uses ingestion-first SDK client auth (same pattern as event dispatch).
- Snapshot, heartbeat, abandoned-cart jobs, and the stack-snapshot API gate on `canDispatchToBurrow()` so ingestion-only installs work without a stored account key.

### Fixed

- Control Panel connection save no longer hard-depends on `allowAdminChanges`; production environments with project config frozen can complete onboarding using DB-backed credentials.

## [5.2.0] - 2026-03-19

### Added

- Ecommerce funnel event: `ecommerce.cart.abandoned` — queue job scans for idle Commerce carts past a configurable threshold (default 120 min), emitted as a lifecycle event with deduplication on `externalEntityId`.
- Ecommerce funnel event: `ecommerce.payment.failed` — hooks Commerce `EVENT_AFTER_PROCESS_PAYMENT` for unsuccessful transactions with gateway-provided failure reasons.
- Ecommerce funnel event: `ecommerce.checkout.started` — detects checkout initiation when an email is first populated on an incomplete order with line items, deduped per cart.
- Ecommerce funnel event: `ecommerce.cart.recovered` — emitted at order completion when a prior cart or checkout abandonment signal exists for the customer.
- Four new envelope builders in `BurrowApiService` with SDK canonical builder pass-through and manual fallback.
- `DetectAbandonedCartsJob` scheduled alongside existing system jobs (30-minute cadence), gated by `ecommerce_funnel` capability.

## [5.1.0] - 2026-03-19

### Added

- Realtime event tracking for Freeform and Formie form submissions.
- Realtime commerce hooks for completed orders and cart removal events.
- Durable Outbox pipeline — all realtime events now route through the Outbox for delivery guarantees.
- Native Craft Element Index UI for the Outbox page with condition rules, channel/event name query filters, and status badges.
- Outbox slideout detail view with configurable provider prefix for form IDs.
- Public API endpoint for remote snapshot refresh.
- CP breadcrumbs across all control panel pages.

### Changed

- Aligned backfill and event assembly around SDK boundaries.
- Tightened forms backfill filtering and normalized Formie mode handling.
- Hardened Freeform realtime submission matching and field mapping.
- Populated full commerce order tags to match SDK and WordPress parity.
- Cleaned up dashboard layout and persisted project name from SDK link result.
- Refined onboarding defaults and plugin branding.
- Simplified CP page titles to avoid redundant plugin name prefix.

### Fixed

- Ecommerce order envelope parity with SDK contracts.
- Outbox element index virtual attribute errors.
- Outbox slideout click handling and detail view polish.

## [5.0.0] - 2026-03-18

### Added

- Initial Craft CMS 5 release of the Burrow bridge plugin.
- 5-step Control Panel onboarding flow for connection, project linking, integration setup, review, and finish.
- Burrow SDK-based discovery and link workflow for connecting a Craft site to a Burrow project.
- Integration support for `Freeform`, `Formie`, and `Craft Commerce`.
- Forms contract generation and contract sync to Burrow.
- System snapshot collection and publish flow for Craft and installed plugin version visibility.
- Dashboard view for linked project status, integrations, sync state, snapshot health, logs, and backfill actions.
- Outbox management screen with queue statistics plus retry and delete actions.
- Manual historical backfill workflow for form submissions and ecommerce orders/items.
- Plugin database tables for runtime state, outbox records, sent-event dedupe tracking, and operational event logs.
