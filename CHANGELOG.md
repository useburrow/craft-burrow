# Changelog

All notable changes to `useburrow/craft-burrow` will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

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
