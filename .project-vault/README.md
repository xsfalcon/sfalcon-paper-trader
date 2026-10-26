# Project Vault — TRD-002

Central index: https://docs.google.com/spreadsheets/d/1FhnuvIyYnwl1nl3MMCX2RsWIl70x7PPHtHdFKCbkZTQ/edit
Repository: https://github.com/xsfalcon/sfalcon-paper-trader
Project ID: TRD-002

## Source archive

Workflow: [.github/workflows/project-vault.yml](../.github/workflows/project-vault.yml)
Archive format: `<repository>-vault-<12-character-commit>-source.zip`
Primary destination: private/public-as-repo GitHub Releases with manifest.json and SHA-256.
Secondary destination: **private** Cloudflare R2 when explicitly enabled and credentials configured.

The GitHub archive stores tracked source only. A complete disaster recovery requires separately backed-up environment configuration, databases, untracked assets, custom domains, provider accounts and access procedures. Never put credentials, patients' records, or expiring signed URLs into this index, a repository, or release ZIP.

## Variables/secrets required for secondary archive

Repository variables: `PROJECT_VAULT_R2_ACCOUNT_ID`, `PROJECT_VAULT_R2_BUCKET`.
Repository secrets: `PROJECT_VAULT_R2_ACCESS_KEY_ID`, `PROJECT_VAULT_R2_SECRET_ACCESS_KEY`.

Do not confuse a successful GitHub release with a verified R2 backup or a tested restoration.
No automatic Google Sheet synchronization is configured. Update verified URLs and release manifests in the central index.

## Recovery sequence

1. Identify the approved commit and corresponding exact GitHub Release.
2. Download the source ZIP and `.sha256` from that release.
3. Run `sha256sum -c <zip>.sha256` in a safe local workspace.
4. Follow repository README and dependency lockfiles to install/build.
5. Restore necessary **separate** encrypted database and media snapshots in an isolated environment.
6. Retrieve secrets from the authorised secret manager, never from this Vault.
7. Verify tests and only then restore a deployment using approved rollout procedures.
8. Record the restore test, outcome and evidence in the central spreadsheet.

**Status as of 2026-10-26:** workflow proposed on archive branch; not verified active in default branch. R2 account not yet enabled; historical ZIP uploads not confirmed.
