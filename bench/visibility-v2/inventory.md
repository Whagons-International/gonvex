Plans: 340; legacy: 340; with data-table dependencies: 96; collections over flagged tables: 263; live queries: 27

Dependency table | legacy plans reading it
--- | ---
`tasks` | 40
`taskApprovalInstances` | 39
`taskWorkspaceContexts` | 39
`taskShares` | 37
`taskUsers` | 37
`workspaceViews` | 10
`boards` | 6
`relayCalls` | 6
`workbenchFiles` | 6
`goals` | 5
`scheduleBoards` | 5
`purchaseApprovalInstances` | 4
`purchaseOrders` | 4
`purchaseRequests` | 4
`assistantThreads` | 3
`peoplePosts` | 3
`workplans` | 3
`boardMessages` | 2
`directMessages` | 2
`documentAssociations` | 2
`timeOffApprovalInstances` | 2
`timeOffRequests` | 2
`workspaceChat` | 2
`advancedVoiceSessions` | 1
`centers` | 1
`cleaningHistory` | 1
`clockInSessions` | 1
`documents` | 1
`formSubmissions` | 1
`goodsReceipts` | 1
`scheduleAvailabilitySubmissions` | 1
`talentEmailTemplates` | 1
`taskFindings` | 1
`taskNotes` | 1
`workbenchFolders` | 1
`workplanItems` | 1
`workspaceViewWorkspaces` | 1

Table | v1 data-table dependencies | collections | live queries | v2 set kinds / inherits
--- | --- | --- | --- | ---
`advancedVoiceContextSummaries` | `assistantThreads` | 2 | 0 | grant
`advancedVoiceTurns` | `advancedVoiceSessions` | 2 | 0 | grant
`assistantMessages` | `assistantThreads` | 3 | 1 | grant
`assistantPendingActions` | `assistantThreads` | 3 | 0 | grant
`boardAttachments` | `boardMessages`, `boards` | 3 | 0 | - / inherits boardMessages
`boardBirthdayImages` | `boards` | 2 | 0 | grant,principal
`boardComments` | `boards` | 2 | 0 | grant,principal
`boardMembers` | `boards` | 3 | 0 | grant
`boardMessages` | `boards` | 3 | 0 | grant,principal
`categories` | `taskApprovalInstances`, `taskShares`, `taskUsers`, `taskWorkspaceContexts`, `tasks` | 3 | 1 | derived,grant,principal
`centers` | `centers` | 2 | 0 | -
`cleaningHistory` | `cleaningHistory`, `taskApprovalInstances`, `taskShares`, `taskUsers`, `taskWorkspaceContexts`, `tasks` | 2 | 0 | grant,principal / inherits tasks
`clockInEvents` | `clockInSessions` | 2 | 0 | grant
`crmActivities` | `taskApprovalInstances`, `taskShares`, `taskUsers`, `taskWorkspaceContexts`, `tasks` | 2 | 0 | principal / inherits tasks
`crmDeals` | `taskApprovalInstances`, `taskShares`, `taskUsers`, `taskWorkspaceContexts`, `tasks` | 2 | 0 | principal / inherits tasks
`crmFollowUps` | `taskApprovalInstances`, `taskShares`, `taskUsers`, `taskWorkspaceContexts`, `tasks` | 2 | 0 | principal / inherits tasks
`documentAssociations` | `documentAssociations`, `documents`, `taskApprovalInstances`, `taskShares`, `taskUsers`, `taskWorkspaceContexts`, `tasks` | 5 | 0 | grant / inherits tasks
`documents` | `documentAssociations`, `taskApprovalInstances`, `taskShares`, `taskUsers`, `taskWorkspaceContexts`, `tasks` | 3 | 1 | derived,grant,principal / inherits tasks
`findingNotes` | `taskApprovalInstances`, `taskFindings`, `taskShares`, `taskUsers`, `taskWorkspaceContexts`, `tasks` | 2 | 0 | - / inherits taskFindings
`formSubmissions` | `formSubmissions`, `tasks` | 2 | 0 | grant,principal
`formTaskLinks` | `taskApprovalInstances`, `taskShares`, `taskUsers`, `taskWorkspaceContexts`, `tasks` | 3 | 0 | grant / inherits tasks
`goalMilestones` | `goals` | 2 | 0 | principal / inherits goals
`goalParticipants` | `goals` | 2 | 0 | principal / inherits goals
`goalPeriods` | `goals` | 2 | 0 | principal / inherits goals
`goalUpdates` | `goals` | 2 | 0 | principal / inherits goals
`goals` | `goals` | 3 | 0 | principal
`goodsReceiptItems` | `goodsReceipts` | 2 | 0 | grant
`goodsReceipts` | `purchaseOrders` | 3 | 0 | grant
`linkPreviews` | `directMessages`, `workspaceChat` | 3 | 0 | - / inherits directMessages,workspaceChat
`messageReactions` | `boardMessages`, `boards`, `directMessages`, `taskApprovalInstances`, `taskNotes`, `taskShares`, `taskUsers`, `taskWorkspaceContexts`, `tasks`, `workspaceChat` | 3 | 0 | derived,grant,principal / inherits boardMessages,tasks,workspaceChat
`peoplePostComments` | `peoplePosts` | 2 | 0 | principal / inherits peoplePosts
`peoplePostReactions` | `peoplePosts` | 2 | 0 | principal / inherits peoplePosts
`peoplePosts` | `peoplePosts` | 2 | 0 | principal
`phoneCallEvents` | `taskApprovalInstances`, `taskShares`, `taskUsers`, `taskWorkspaceContexts`, `tasks` | 2 | 0 | - / inherits tasks
`phoneCalls` | `taskApprovalInstances`, `taskShares`, `taskUsers`, `taskWorkspaceContexts`, `tasks` | 2 | 0 | - / inherits tasks
`phoneContacts` | `taskApprovalInstances`, `taskShares`, `taskUsers`, `taskWorkspaceContexts`, `tasks` | 2 | 0 | - / inherits tasks
`purchaseApprovalInstances` | `purchaseOrders`, `purchaseRequests` | 3 | 0 | grant
`purchaseOrderItems` | `purchaseApprovalInstances`, `purchaseOrders`, `purchaseRequests` | 2 | 0 | grant / inherits purchaseOrders
`purchaseOrders` | `purchaseApprovalInstances`, `purchaseOrders`, `purchaseRequests` | 3 | 0 | grant
`purchaseRequestItems` | `purchaseApprovalInstances`, `purchaseRequests` | 2 | 0 | grant
`purchaseRequests` | `purchaseApprovalInstances` | 3 | 0 | grant,principal
`relayActionProposals` | `relayCalls` | 2 | 0 | grant
`relayAiAnalyses` | `relayCalls` | 2 | 0 | grant
`relayCallEvents` | `relayCalls` | 2 | 0 | grant
`relayPolicyDecisions` | `relayCalls` | 2 | 0 | grant
`relayTranscriptSegments` | `relayCalls` | 2 | 1 | grant
`relayTranscripts` | `relayCalls` | 2 | 1 | grant
`scheduleAvailabilityItems` | `scheduleAvailabilitySubmissions`, `scheduleBoards` | 2 | 0 | - / inherits scheduleAvailabilitySubmissions
`scheduleAvailabilitySubmissions` | `scheduleBoards` | 3 | 0 | grant
`scheduleRosterEntries` | `scheduleBoards` | 2 | 0 | grant
`scheduleShiftTemplates` | `scheduleBoards` | 3 | 0 | grant
`scheduleWeeks` | `scheduleBoards` | 2 | 0 | grant
`spots` | `taskApprovalInstances`, `taskShares`, `taskUsers`, `taskWorkspaceContexts`, `tasks` | 4 | 1 | derived,grant,principal
`statusTransitionLogs` | `taskApprovalInstances`, `taskShares`, `taskUsers`, `taskWorkspaceContexts`, `tasks` | 2 | 0 | grant / inherits tasks
`talentEmailTemplates` | `talentEmailTemplates` | 2 | 0 | principal
`taskAckReads` | `taskApprovalInstances`, `taskShares`, `taskUsers`, `taskWorkspaceContexts`, `tasks` | 3 | 0 | grant / inherits tasks
`taskAcks` | `taskApprovalInstances`, `taskShares`, `taskUsers`, `taskWorkspaceContexts`, `tasks` | 3 | 0 | grant / inherits tasks
`taskApprovalInstances` | `taskApprovalInstances`, `taskShares`, `taskUsers`, `taskWorkspaceContexts`, `tasks` | 3 | 0 | grant / inherits tasks
`taskCustomFieldValues` | `taskApprovalInstances`, `taskShares`, `taskUsers`, `taskWorkspaceContexts`, `tasks` | 3 | 0 | grant / inherits tasks
`taskFindings` | `taskApprovalInstances`, `taskShares`, `taskUsers`, `taskWorkspaceContexts`, `tasks` | 7 | 0 | grant / inherits tasks
`taskForms` | `taskApprovalInstances`, `taskShares`, `taskUsers`, `taskWorkspaceContexts`, `tasks` | 2 | 0 | grant / inherits tasks
`taskLogs` | `taskApprovalInstances`, `taskShares`, `taskUsers`, `taskWorkspaceContexts`, `tasks` | 5 | 0 | grant / inherits tasks
`taskNotes` | `taskApprovalInstances`, `taskShares`, `taskUsers`, `taskWorkspaceContexts`, `tasks` | 7 | 2 | grant / inherits tasks
`taskPublicShares` | `taskApprovalInstances`, `taskShares`, `taskUsers`, `taskWorkspaceContexts`, `tasks` | 3 | 0 | grant / inherits tasks
`taskPullRequests` | `taskApprovalInstances`, `taskShares`, `taskUsers`, `taskWorkspaceContexts`, `tasks` | 2 | 0 | grant / inherits tasks
`taskRelations` | `taskApprovalInstances`, `taskShares`, `taskUsers`, `taskWorkspaceContexts`, `tasks` | 4 | 0 | grant / inherits tasks
`taskReminders` | `taskApprovalInstances`, `taskShares`, `taskUsers`, `taskWorkspaceContexts`, `tasks` | 2 | 0 | grant / inherits tasks
`taskShareAcknowledgments` | `taskApprovalInstances`, `taskShares`, `taskUsers`, `taskWorkspaceContexts`, `tasks` | 2 | 0 | grant / inherits tasks
`taskShares` | `taskApprovalInstances`, `taskShares`, `taskUsers`, `taskWorkspaceContexts`, `tasks` | 5 | 0 | grant / inherits tasks
`taskSignatures` | `taskApprovalInstances`, `taskShares`, `taskUsers`, `taskWorkspaceContexts`, `tasks` | 3 | 0 | grant / inherits tasks
`taskSlaStates` | `taskApprovalInstances`, `taskShares`, `taskUsers`, `taskWorkspaceContexts`, `tasks` | 3 | 0 | grant / inherits tasks
`taskSupplies` | `taskApprovalInstances`, `taskShares`, `taskUsers`, `taskWorkspaceContexts`, `tasks` | 2 | 0 | grant / inherits tasks
`taskTags` | `taskApprovalInstances`, `taskShares`, `taskUsers`, `taskWorkspaceContexts`, `tasks` | 4 | 0 | grant / inherits tasks
`taskUsers` | `taskApprovalInstances`, `taskShares`, `taskUsers`, `taskWorkspaceContexts`, `tasks` | 6 | 1 | grant / inherits tasks
`taskViews` | `taskApprovalInstances`, `taskShares`, `taskUsers`, `taskWorkspaceContexts`, `tasks` | 2 | 0 | grant / inherits tasks
`taskWorkspaceContexts` | `taskApprovalInstances`, `taskShares`, `taskUsers`, `taskWorkspaceContexts`, `tasks` | 6 | 0 | grant / inherits tasks
`tasks` | `taskApprovalInstances`, `taskShares`, `taskUsers`, `taskWorkspaceContexts`, `tasks` | 9 | 14 | grant,principal
`timeOffApprovalDecisions` | `timeOffRequests` | 2 | 0 | grant
`timeOffApprovalInstances` | `timeOffApprovalInstances`, `timeOffRequests` | 2 | 0 | grant
`timeOffRequests` | `timeOffApprovalInstances` | 2 | 0 | grant,principal
`workbenchComments` | `workbenchFiles`, `workspaceViews` | 2 | 0 | - / inherits workbenchFiles
`workbenchDocumentSteps` | `workbenchFiles`, `workspaceViews` | 2 | 0 | - / inherits workbenchFiles
`workbenchFiles` | `workbenchFiles`, `workspaceViews` | 2 | 2 | grant,principal
`workbenchFolders` | `workbenchFolders`, `workspaceViews` | 2 | 0 | grant
`workbenchPresence` | `workbenchFiles`, `workspaceViews` | 2 | 0 | - / inherits workbenchFiles
`workbenchRevisions` | `workbenchFiles`, `workspaceViews` | 2 | 0 | - / inherits workbenchFiles
`workbenchShares` | `workbenchFiles`, `workspaceViews` | 2 | 0 | derived,grant
`workplanInstances` | `workplans` | 4 | 0 | grant
`workplanItems` | `workplans` | 3 | 0 | grant,principal
`workplanOccurrences` | `workplanItems`, `workplans` | 2 | 0 | - / inherits workplanItems
`workspaceTeams` | `taskApprovalInstances`, `taskWorkspaceContexts`, `tasks` | 2 | 0 | derived,grant,principal
`workspaceViewPublicShares` | `workspaceViews` | 2 | 0 | grant
`workspaceViewShares` | `workspaceViews` | 2 | 0 | grant
`workspaceViewWorkspaces` | `workspaceViews` | 2 | 0 | grant
`workspaceViews` | `workspaceViewWorkspaces` | 3 | 0 | grant,principal
`workspaces` | `taskApprovalInstances`, `taskWorkspaceContexts`, `tasks` | 5 | 2 | derived,grant,principal
