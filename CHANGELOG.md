# Changelog

All notable changes to `useburrow/craft-burrow` will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

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
