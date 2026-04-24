# newdb Branch Protection Baseline

This baseline defines the minimum repository protection profile for production readiness.

## Protected Branches

- `main`
- `master` (if still active)

## Pull Request Requirements

- Require pull request before merging.
- Require at least 1 approving review.
- Dismiss stale approvals when new commits are pushed.
- Require conversation resolution before merging.

## Required Status Checks

Mark the checks below as required:

- `observability-schema-gate`
- `telemetry-schema-gate`
- `c-api-abi-gate`
- `linux-gcc`
- `linux-clang`
- `windows-clean-room`
- `newdb-gui`
- `dependency-review`
- `codeql`
- `codeql-severity-gate`
- `sbom`

## Additional Safeguards

- Require branches to be up to date before merging.
- Restrict who can push to matching branches.
- Disable force pushes and branch deletion on protected branches.

## Operational Notes

- If workflow job names are renamed, update required checks accordingly.
- If only one default branch remains, remove the inactive branch from protection scope.
