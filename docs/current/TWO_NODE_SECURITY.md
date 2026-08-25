# Two-node security

Production traffic uses a WireGuard/private network only. The control API binds to loopback behind a controlled reverse proxy; public 0.0.0.0 binding is rejected unless the trusted-proxy setting is explicit.

TLS verifies the server CA. mTLS verifies each executor client certificate. Every executor has a unique bearer token, consumer ID, and certificate common-name mapping; those identities must agree. Production startup rejects an empty token, missing certificate/key/CA, non-HTTPS endpoint, or mismatched certificate identity.

The reverse proxy injects the verified client certificate identity. Clients cannot make identity trustworthy by sending that header directly over an untrusted path. Authentication failure logs only consumer ID and outcome, never bearer token or private key.

Certificate rotation:

1. Issue a new certificate under the executor CA with the same approved consumer identity.
2. Add the new certificate and a new unique token during an overlap window.
3. Deploy to the executor and verify capabilities, time, and ownership handshake.
4. Revoke the old certificate and remove the old token.
5. Confirm authentication-failure logs contain no secret material.

Data security:

- no shared SQLite, SMB, NFS, prediction database, execution database, or model/PIT directory;
- backups are local snapshots copied through an approved encrypted channel;
- restores require stopped ownership service, an explicit maintenance marker, checksum, and quick_check;
- paper is the only authorized execution mode; mainnet_allowed is false and live_count is zero.
