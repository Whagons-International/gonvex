# Runtime error attribution and triage

Runtime failures enter the same error store as browser reports. Scheduled internal mutations preserve the job's tenant, job ID, and sanitized arguments even when dispatch fails before the function runs.

New runtime log entries record `release` as `runtime@<runtimeBuildVersion()>` and `runtimeInstance` as the process hostname. The version identifies the backend runtime deployment, not the requesting web or mobile app. The instance appears in error execution context. It is not a client device and does not increase the dashboard's machine count. Scheduled jobs normally have no end user or client device.

Release identity is recorded before log persistence. Replaying historical logs does not assign the current deployment to old failures. Existing events with missing context remain incomplete.

Mark a confirmed fix resolved to remove it from the unresolved inbox while retaining history. A new accepted occurrence reopens the group and marks it as a regression, including occurrences on the same release or without a release. Duplicate event IDs do not reopen groups. Ignored groups remain ignored.

Do not bulk resolve active failures just to empty the inbox. Verify the fix is deployed and inspect last-seen times. Error group listings return at most 500 groups, so counts from a single response are not necessarily totals for the project.
