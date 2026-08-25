# Executor testnet artifact runbook

This directory is a deployment template only. The current task does not authorize testnet startup, credentials, orders, or network calls. The systemd unit requires an external approval marker and the Windows service installs as Manual without starting.

A future human-gated task must provision a dedicated testnet subaccount, one-way position mode, unique token/certificate identity, approved release ID, exact code SHA, local SQLite, NTP, and WireGuard. It must run the full preflight before creating the approval marker.

Never reuse production-paper data, credentials, consumer ID, ownership ID, or certificates. Mainnet remains disabled and is not represented by this artifact.
