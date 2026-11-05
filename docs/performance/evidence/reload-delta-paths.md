# Exact saved 130-open reload trace

Representative session `t0-u0-admin`, first reload iteration. CPU columns are process CPU microseconds for `open_replica`, excluding the separately measured page/JSON/deflate codec. Each phase total in the results JSON includes the codec model and tracing overhead. Individual samples are attribution, not stable per-path speedup estimates. Arguments are preserved by the normalized capture; this table identifies its unique paths.

89 requests carry cursors; 41 do not. `cache validation` reevaluates the full cached view; `SQL validation` takes the existing authoritative SQL path; `metadata/log` validates the server anchor and retained log without a whole-list scan. Cursorless requests still produce authoritative snapshots from the current cached view.

| Path | Cursor | Before | After | Before CPU µs | After CPU µs |
| --- | --- | --- | --- | ---: | ---: |
| `__local.advancedVoiceSessions` | no | cached snapshot: no cursor | cached snapshot: no cursor | 119 | 122 |
| `__local.bugReports` | no | cached snapshot: no cursor | cached snapshot: no cursor | 107 | 113 |
| `__local.centers` | no | cached snapshot: no cursor | cached snapshot: no cursor | 93 | 107 |
| `__local.cleaningReportDeliveries` | no | cached snapshot: no cursor | cached snapshot: no cursor | 95 | 111 |
| `__local.cleaningReportSchedules` | no | cached snapshot: no cursor | cached snapshot: no cursor | 92 | 116 |
| `__local.cleaningResetRules` | no | cached snapshot: no cursor | cached snapshot: no cursor | 92 | 109 |
| `__local.clockInEvents` | no | cached snapshot: no cursor | cached snapshot: no cursor | 95 | 114 |
| `__local.clockInSessions` | no | cached snapshot: no cursor | cached snapshot: no cursor | 95 | 108 |
| `__local.deepResearchAssignments` | no | cached snapshot: no cursor | cached snapshot: no cursor | 94 | 107 |
| `__local.deepResearchSettings` | no | cached snapshot: no cursor | cached snapshot: no cursor | 99 | 108 |
| `__local.fcmTokens` | no | cached snapshot: no cursor | cached snapshot: no cursor | 90 | 110 |
| `__local.githubConnections` | no | cached snapshot: no cursor | cached snapshot: no cursor | 93 | 111 |
| `__local.journeySyncConfigurations` | no | cached snapshot: no cursor | cached snapshot: no cursor | 93 | 108 |
| `__local.learnProgress` | no | cached snapshot: no cursor | cached snapshot: no cursor | 91 | 107 |
| `__local.marketProjects` | no | cached snapshot: no cursor | cached snapshot: no cursor | 101 | 119 |
| `__local.memberAuthStates` | no | cached snapshot: no cursor | cached snapshot: no cursor | 91 | 107 |
| `__local.nfcTapLogs` | no | cached snapshot: no cursor | cached snapshot: no cursor | 92 | 108 |
| `__local.opsManagerConfigs` | no | cached snapshot: no cursor | cached snapshot: no cursor | 93 | 109 |
| `__local.opsManagerReports` | no | cached snapshot: no cursor | cached snapshot: no cursor | 92 | 224 |
| `__local.peopleFlowTemplates` | no | cached snapshot: no cursor | cached snapshot: no cursor | 103 | 115 |
| `__local.peopleLinks` | no | cached snapshot: no cursor | cached snapshot: no cursor | 102 | 218 |
| `__local.phoneContacts` | no | cached snapshot: no cursor | cached snapshot: no cursor | 411 | 258 |
| `__local.phoneScripts` | no | cached snapshot: no cursor | cached snapshot: no cursor | 91 | 109 |
| `__local.plugins` | no | cached snapshot: no cursor | cached snapshot: no cursor | 121 | 112 |
| `__local.pmsConnections` | no | cached snapshot: no cursor | cached snapshot: no cursor | 96 | 116 |
| `__local.purchasingDocumentCounters` | no | cached snapshot: no cursor | cached snapshot: no cursor | 95 | 111 |
| `__local.relayCallPolicies` | no | cached snapshot: no cursor | cached snapshot: no cursor | 94 | 107 |
| `__local.relayNumberAssignments` | no | cached snapshot: no cursor | cached snapshot: no cursor | 92 | 107 |
| `__local.relayPhoneNumbers` | no | cached snapshot: no cursor | cached snapshot: no cursor | 90 | 108 |
| `__local.reportDocuments` | no | cached snapshot: no cursor | cached snapshot: no cursor | 99 | 112 |
| `__local.reportTemplates` | no | cached snapshot: no cursor | cached snapshot: no cursor | 97 | 102 |
| `__local.researchCompanies` | no | cached snapshot: no cursor | cached snapshot: no cursor | 91 | 88 |
| `__local.researchRuns` | no | cached snapshot: no cursor | cached snapshot: no cursor | 92 | 98 |
| `__local.researchSettings` | no | cached snapshot: no cursor | cached snapshot: no cursor | 145 | 100 |
| `__local.taskNumberCounters` | no | cached snapshot: no cursor | cached snapshot: no cursor | 171 | 242 |
| `__local.tenantBilling` | no | cached snapshot: no cursor | cached snapshot: no cursor | 82 | 117 |
| `__local.tenantBranding` | no | cached snapshot: no cursor | cached snapshot: no cursor | 96 | 96 |
| `__local.webhooks` | no | cached snapshot: no cursor | cached snapshot: no cursor | 102 | 91 |
| `__local.workspaceViewPublicShares` | no | cached snapshot: no cursor | cached snapshot: no cursor | 98 | 98 |
| `__local.workspaceViewWorkspaces` | no | cached snapshot: no cursor | cached snapshot: no cursor | 96 | 2325 |
| `members.listMembers` | no | cached snapshot: no cursor | cached snapshot: no cursor | 1094 | 1046 |
| `tasks.recentWorkspaceTasks` | yes | cache validation | metadata/log | 3583 | 206 |
| `memberAuthStates.mine` | yes | cache validation | metadata/log | 101 | 104 |
| `billing.current` | yes | cache validation | metadata/log | 96 | 201 |
| `tenantBranding.getPublic` | yes | cache validation | metadata/log | 97 | 97 |
| `settingsReplicas.globalSettings` | yes | cache validation | metadata/log | 96 | 103 |
| `properties.properties` | yes | cache validation | metadata/log | 88 | 94 |
| `centers.centers` | yes | cache validation | metadata/log | 93 | 302 |
| `centers.jobPositionCenters` | yes | cache validation | metadata/log | 95 | 99 |
| `centers.preferences` | yes | cache validation | metadata/log | 93 | 102 |
| `settings.plugins` | yes | cache validation | metadata/log | 86 | 95 |
| `workspacePreferences.workspacePreferences` | yes | cache validation | metadata/log | 89 | 734 |
| `chat.conversationParticipants` | yes | cache validation | metadata/log | 102 | 101 |
| `chat.directMessages` | yes | cache validation | metadata/log | 108 | 99 |
| `notifications.listNotifications` | yes | cache validation | metadata/log | 86 | 100 |
| `taskResources.listSharedToMe` | yes | cache validation | metadata/log | 391 | 264 |
| `members.listMemberTeams` | yes | SQL validation | metadata/log | 1186 | 100 |
| `taskWorkspaceContexts.contexts` | yes | cache validation | metadata/log | 388 | 242 |
| `taskResources.recentTaskAcknowledgments` | yes | cache validation | metadata/log | 222 | 223 |
| `taskResources.recentTaskAcknowledgmentReads` | yes | cache validation | metadata/log | 223 | 224 |
| `categories.categories` | yes | cache validation | metadata/log | 164 | 165 |
| `workspaceGroups.workspaceGroups` | yes | cache validation | metadata/log | 105 | 87 |
| `statuses.statuses` | yes | cache validation | metadata/log | 91 | 97 |
| `learn.progress` | yes | cache validation | metadata/log | 97 | 100 |
| `learn.positionFits` | yes | cache validation | metadata/log | 94 | 85 |
| `jobPositions.jobPositions` | yes | cache validation | metadata/log | 93 | 92 |
| `roles.list` | yes | cache validation | metadata/log | 93 | 97 |
| `roles.listPermissions` | yes | cache validation | metadata/log | 93 | 88 |
| `roles.rolePermissions` | yes | cache validation | metadata/log | 91 | 93 |
| `roles.memberTeams` | yes | SQL validation | metadata/log | 949 | 98 |
| `broadcasts.listAllAcknowledgments` | yes | cache validation | metadata/log | 103 | 2297 |
| `workspaces.list` | yes | cache validation | metadata/log | 196 | 265 |
| `teams.teams` | yes | cache validation | metadata/log | 93 | 116 |
| `boards.list` | yes | cache validation | metadata/log | 117 | 121 |
| `gamification.pointActionSettings` | yes | cache validation | metadata/log | 94 | 222 |
| `gamification.userPoints` | yes | cache validation | metadata/log | 99 | 225 |
| `gamification.pointTransactions` | yes | cache validation | metadata/log | 98 | 132 |
| `gamification.userActionStats` | yes | cache validation | metadata/log | 91 | 127 |
| `gamification.userBadges` | yes | cache validation | metadata/log | 93 | 117 |
| `gamification.badges` | yes | cache validation | metadata/log | 90 | 113 |
| `gamification.levels` | yes | cache validation | metadata/log | 90 | 111 |
| `schedulingReplicas.scheduleBoards` | yes | cache validation | metadata/log | 99 | 122 |
| `complianceReplicas.complianceStandards` | yes | cache validation | metadata/log | 143 | 113 |
| `complianceReplicas.complianceRequirements` | yes | cache validation | metadata/log | 85 | 109 |
| `complianceReplicas.complianceMappings` | yes | cache validation | metadata/log | 83 | 108 |
| `complianceReplicas.complianceAudits` | yes | cache validation | metadata/log | 108 | 112 |
| `assistant.listThreads` | yes | cache validation | metadata/log | 103 | 111 |
| `workplans.workplans` | yes | cache validation | metadata/log | 103 | 122 |
| `taskResources.taskUsers` | yes | cache validation | metadata/log | 311 | 312 |
| `priorities.list` | yes | cache validation | metadata/log | 95 | 115 |
| `spots.list` | yes | cache validation | metadata/log | 194 | 226 |
| `tags.tags` | yes | cache validation | metadata/log | 96 | 108 |
| `customFields.customFields` | yes | cache validation | metadata/log | 103 | 109 |
| `workspaceViews.listAvailable` | yes | cache validation | metadata/log | 119 | 126 |
| `workspaces.workspaceTeams` | yes | cache validation | metadata/log | 181 | 206 |
| `workplans.listWorkplans` | yes | cache validation | metadata/log | 89 | 118 |
| `templates.list` | yes | cache validation | metadata/log | 105 | 116 |
| `statusTransitions.statusTransitions` | yes | cache validation | metadata/log | 92 | 107 |
| `statusTransitions.statusTransitionGroups` | yes | cache validation | metadata/log | 90 | 103 |
| `approvals.approvals` | yes | cache validation | metadata/log | 91 | 107 |
| `approvals.approvalApprovers` | yes | cache validation | metadata/log | 90 | 103 |
| `approvals.taskApprovalInstances` | yes | cache validation | metadata/log | 225 | 252 |
| `workflowRuntime.workflows` | yes | cache validation | metadata/log | 99 | 106 |
| `sla.slas` | yes | cache validation | metadata/log | 91 | 106 |
| `customFields.categoryCustomFields` | yes | cache validation | metadata/log | 87 | 107 |
| `assets.assetItems` | yes | cache validation | metadata/log | 97 | 107 |
| `analytics.listTaskTagsForAnalytics` | yes | cache validation | metadata/log | 222 | 235 |
| `analytics.listKpiCards` | yes | cache validation | metadata/log | 94 | 107 |
| `customFields.taskValues` | yes | cache validation | metadata/log | 235 | 232 |
| `chat.workspaceChat` | yes | cache validation | metadata/log | 111 | 110 |
| `calls.listForUser` | yes | cache validation | metadata/log | 88 | 91 |
| `taskResources.listTaskRelations` | yes | cache validation | metadata/log | 238 | 245 |
| `taskFindings.routingLinks` | yes | cache validation | metadata/log | 219 | 246 |
| `cleaningStatuses.cleaningStatuses` | yes | cache validation | metadata/log | 82 | 89 |
| `reminderRules.reminderRules` | yes | cache validation | metadata/log | 102 | 93 |
| `forms.list` | yes | cache validation | metadata/log | 97 | 107 |
| `notificationSoundRules.notificationSoundRules` | yes | cache validation | metadata/log | 90 | 88 |
| `audiences.list` | yes | cache validation | metadata/log | 89 | 92 |
| `emailTemplates.emailTemplates` | yes | cache validation | metadata/log | 89 | 97 |
| `taskResources.noteCountsByTenant` | yes | cache validation | metadata/log | 226 | 214 |
| `taskFindings.countsByTenant` | yes | cache validation | metadata/log | 208 | 215 |
| `taskFindings.linkedTasksByTenant` | yes | cache validation | metadata/log | 212 | 214 |
| `taskFindings.routesByTenant` | yes | cache validation | metadata/log | 278 | 213 |
| `taskFindings.list` | yes | cache validation | metadata/log | 206 | 203 |
| `forms.listVersions` | yes | cache validation | metadata/log | 96 | 2977 |
| `forms.listTaskForms` | yes | cache validation | metadata/log | 210 | 393 |
| `taskResources.noteSummariesByTenant` | yes | cache validation | metadata/log | 207 | 318 |
| `taskResources.listTaskIdsWithSignatures` | yes | cache validation | metadata/log | 210 | 286 |
| `documents.listEntityIdsWithDocuments` | yes | cache validation | metadata/log | 210 | 258 |
| `taskResources.listTaskIdsWithAttachments` | yes | cache validation | metadata/log | 206 | 245 |
