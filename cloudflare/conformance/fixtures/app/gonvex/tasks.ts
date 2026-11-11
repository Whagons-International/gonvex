import { liveQuery, query, reducer, schema, visibility } from "@gonvex/module-sdk";

// Live Queries (K08): Whagons' task grid. `grid` below is `tasks.grid` from
// whagons5-client `2457bb65f` (gonvex/tasks/gridQueries.ts), copied verbatim
// except its stable-name option. The masks are Whagons' own (legacy plans),
// reduced to the sets this fixture has tables for. Conformance Members are
// seeded as `admin` with `{"e2e":true}`, so no branch here admits by `admin`.

export const tasksVisibility = visibility({
  table: "tasks",
  key: "_id",
  sets: {
    memberTeams: { table: "memberTeams", select: "teamId", joins: [], where: [{ table: "memberTeams", column: "memberId", context: "member.id" }, { table: "memberTeams", column: "deletedAt", value: { literal: null } }] },
    assignedTasks: { table: "taskUsers", select: "taskId", joins: [], where: [{ table: "taskUsers", column: "userId", context: "member.id" }, { table: "taskUsers", column: "deletedAt", value: { literal: null } }] },
    contextUserTasks: { table: "taskWorkspaceContexts", select: "taskId", joins: [], where: [{ table: "taskWorkspaceContexts", column: "targetUserId", context: "member.id" }, { table: "taskWorkspaceContexts", column: "revokedAt", value: { literal: null } }] },
    contextTeamTasks: { table: "taskWorkspaceContexts", select: "taskId", joins: [{ table: "memberTeams", leftColumn: "targetTeamId", rightColumn: "teamId" }], where: [{ table: "memberTeams", column: "memberId", context: "member.id" }, { table: "memberTeams", column: "deletedAt", value: { literal: null } }, { table: "taskWorkspaceContexts", column: "revokedAt", value: { literal: null } }] },
  },
  where: {
    operator: "or",
    children: [
      { operator: "permission", value: "tasks.viewAll" },
      { operator: "role", value: "system_admin" },
      { operator: "role", value: "owner" },
      { operator: "eqContext", column: "createdBy", context: "member.id" },
      { operator: "eqContext", column: "reportedByUserId", context: "member.id" },
      { operator: "inSet", column: "reportedByTeamId", set: "memberTeams" },
      { operator: "inSet", column: "_id", set: "assignedTasks" },
      { operator: "inSet", column: "_id", set: "contextUserTasks" },
      { operator: "inSet", column: "_id", set: "contextTeamTasks" },
    ],
  },
});

export const memberTeamsVisibility = visibility({
  table: "memberTeams",
  key: "_id",
  sets: {},
  where: {
    operator: "or",
    children: [
      { operator: "permission", value: "view-users" },
      { operator: "permission", value: "manage-broadcasts" },
      { operator: "eqContext", column: "memberId", context: "member.id" },
    ],
  },
});

export const taskWorkspaceContextsVisibility = visibility({
  table: "taskWorkspaceContexts",
  key: "_id",
  sets: {
    assignedTasks: { table: "taskUsers", select: "taskId", joins: [], where: [{ table: "taskUsers", column: "userId", context: "member.id" }, { table: "taskUsers", column: "deletedAt", value: { literal: null } }] },
    ownedTasks: { table: "tasks", select: "_id", joins: [], where: [{ table: "tasks", column: "createdBy", context: "member.id" }] },
  },
  where: {
    operator: "or",
    children: [
      { operator: "permission", value: "tasks.viewAll" },
      { operator: "eqContext", column: "targetUserId", context: "member.id" },
      { operator: "inSet", column: "taskId", set: "assignedTasks" },
      { operator: "inSet", column: "taskId", set: "ownedTasks" },
      { operator: "not", children: [{ operator: "eq", column: "targetTeamId", value: { literal: null } }] },
    ],
  },
});

export const taskFindingsVisibility = visibility({ table: "taskFindings", key: "_id", sets: {}, where: { operator: "public" } });
export const taskUsersVisibility = visibility({ table: "taskUsers", key: "_id", sets: {}, where: { operator: "eqContext", column: "userId", context: "member.id" } });

/** Upserts one row of a task-grid table by its key (the scenario drives every table through this). */
export const putRow = reducer({
  args: schema.object({ table: schema.string(), row: schema.record(schema.any()) }),
  result: schema.null(),
  offline: { mode: "onlineOnly", reason: "conformance fixture" },
  nonOptimisticReason: "conformance fixture checks server results",
  run: async ({ db }, args) => {
    const keys: Record<string, string> = { tasks: "_id", taskGridRows: "taskId", taskRelatedSearchDocuments: "taskId", taskWorkspaceContexts: "_id", memberTeams: "_id", taskUsers: "_id", taskFindings: "_id", statuses: "_id", priorities: "_id" };
    const key = keys[args.table];
    if (!key) throw new Error(`table ${args.table} is not a task grid table`);
    const id = String(args.row[key]);
    const [found] = await db.query<{ present: number }>(`SELECT 1 AS present FROM "${args.table}" WHERE "${key}" = ?`, [id]);
    if (found) {
      const patch = { ...args.row };
      delete patch[key];
      await db.update(args.table, id, patch);
    } else {
      await db.insert(args.table, args.row);
    }
    return null;
  },
});

/** Deletes one row of a task-grid table by its key. */
export const removeRow = reducer({
  args: schema.object({ table: schema.string(), id: schema.string() }),
  result: schema.null(),
  offline: { mode: "onlineOnly", reason: "conformance fixture" },
  nonOptimisticReason: "conformance fixture checks server results",
  run: async ({ db }, args) => {
    await db.delete(args.table, args.id);
    return null;
  },
});

/** A one-shot structured plan (Whagons has 35): tasks by id list, which binds one parameter per id. */
export const byIds = query({
  args: schema.object({ ids: schema.array(schema.string()), offset: schema.optional(schema.integer()), limit: schema.optional(schema.integer()) }),
  result: schema.array(schema.record(schema.any())),
  liveQueryPlan: {
    table: "tasks",
    key: "_id",
    columns: ["_id", "id", "name", "statusId", "createdBy", "requiresSignature", "audienceRules", "latitude"],
    where: { operator: "in", column: "_id", value: { argument: "ids" } },
    sort: { defaultColumn: "id", defaultDirection: "asc", allowedColumns: ["id"] },
    window: { offsetArgument: "offset", limitArgument: "limit", defaultLimit: 500, maxLimit: 500 },
  },
});

export const grid = liveQuery({
  interactive: true,
  description: "Grid.",
  agent: { tags: ["tasks"], confirmation: "none" },
  args: schema.object({
    workspaceId: schema.string(),
    search: schema.optional(schema.string()),
    filters: schema.optional(schema.array(schema.object({
      column: schema.string(),
      operator: schema.string(),
      value: schema.string(),
      valueTo: schema.optional(schema.string()),
    }))),
    sortColumn: schema.optional(schema.string()),
    sortDirection: schema.optional(schema.string()),
    offset: schema.integer(),
    limit: schema.integer(),
  }),
  // The runtime returns the rows at resultPath: { page, total?, offset?, limit? }.
  result: schema.object({ page: schema.array(schema.record(schema.any())), total: schema.optional(schema.number()), offset: schema.optional(schema.number()), limit: schema.optional(schema.number()) }),
  liveQueryPlan: {
    table: "tasks",
    key: "_id",
    resultPath: ["page"],
    columns: ["_id", "_creationTime", "id", "name", "description", "workspaceId", "statusId", "priorityId", "categoryId", "spotId", "teamId", "templateId", "createdBy", "attributedCreatorId", "createdVia", "createdByWorkflowId", "createdByWorkflowRunId", "workflowTriggeredByUserId", "approvalId", "slaId", "formId", "formVersionId", "requiresSignature", "assetId", "fleetVehicleId", "fleetLeaderId", "fleetCompanionId", "requiresAcknowledgment", "audienceRules", "reportedByGuest", "guestId", "guestVip", "guestReportSeenAt", "reportedByTeamId", "reportedByUserId", "firstViewedAt", "dueDate", "startDate", "latitude", "longitude", "completedAt", "createdAt", "updatedAt", "flagColor", "rowHash", "recurrenceId", "workplanId", "workplanItemId", "workplanInstanceId", "occurrenceDate", "deletedAt", "deletedBy"],
    where: {
      "operator": "or",
      "children": [
        {
          "operator": "eq",
          "column": "workspaceId",
          "value": {
            "argument": "workspaceId"
          }
        },
        {
          "operator": "inRelation",
          "column": "_id",
          "relation": {
            "table": "taskWorkspaceContexts",
            "column": "taskId",
            "where": {
              "operator": "and",
              "children": [
                {
                  "operator": "eq",
                  "column": "workspaceId",
                  "value": {
                    "argument": "workspaceId"
                  }
                },
                {
                  "operator": "eq",
                  "column": "revokedAt",
                  "value": {
                    "literal": null
                  }
                },
                {
                  "operator": "or",
                  "children": [
                    {
                      "operator": "arrayContains",
                      "column": "recipientUserIds",
                      "value": {
                        "context": "member.id"
                      }
                    },
                    {
                      "operator": "and",
                      "children": [
                        {
                          "operator": "or",
                          "children": [
                            {
                              "operator": "eq",
                              "column": "recipientUserIds",
                              "value": {
                                "literal": null
                              }
                            },
                            {
                              "operator": "eq",
                              "column": "recipientUserIds",
                              "value": {
                                "literal": []
                              }
                            }
                          ]
                        },
                        {
                          "operator": "or",
                          "children": [
                            {
                              "operator": "eq",
                              "column": "targetUserId",
                              "value": {
                                "context": "member.id"
                              }
                            },
                            {
                              "operator": "and",
                              "children": [
                                {
                                  "operator": "eq",
                                  "column": "targetUserId",
                                  "value": {
                                    "literal": null
                                  }
                                },
                                {
                                  "operator": "or",
                                  "children": [
                                    {
                                      "operator": "eq",
                                      "column": "targetTeamId",
                                      "value": {
                                        "literal": null
                                      }
                                    },
                                    {
                                      "operator": "inRelation",
                                      "column": "targetTeamId",
                                      "relation": {
                                        "table": "memberTeams",
                                        "column": "teamId",
                                        "where": {
                                          "operator": "and",
                                          "children": [
                                            {
                                              "operator": "eq",
                                              "column": "memberId",
                                              "value": {
                                                "context": "member.id"
                                              }
                                            },
                                            {
                                              "operator": "eq",
                                              "column": "deletedAt",
                                              "value": {
                                                "literal": null
                                              }
                                            }
                                          ]
                                        }
                                      }
                                    }
                                  ]
                                }
                              ]
                            }
                          ]
                        }
                      ]
                    }
                  ]
                },
                {
                  "operator": "or",
                  "children": [
                    {
                      "operator": "neq",
                      "column": "kind",
                      "value": {
                        "literal": "finding"
                      }
                    },
                    {
                      "operator": "inRelation",
                      "column": "taskId",
                      "relation": {
                        "table": "taskFindings",
                        "column": "linkedTaskId",
                        "where": {
                          "operator": "eq",
                          "column": "deletedAt",
                          "value": {
                            "literal": null
                          }
                        }
                      }
                    }
                  ]
                }
              ]
            }
          }
        }
      ]
    },
    index: {
  table: 'taskGridRows',
  key: 'taskId',
  columns: ['assetId', 'fleetVehicleId', 'fleetLeaderId', 'fleetCompanionId', 'reportedByGuest', 'guestVip', 'id', 'workspaceId', 'name', 'description', 'statusId', 'priorityId', 'categoryId', 'spotId', 'teamId', 'templateId', 'createdBy', 'dueDate', 'startDate', 'completedAt', 'createdAt', 'updatedAt', '_creationTime', 'flagColor', 'deletedAt'],
  sortColumns: {
    statusId: ['statusOrder', 'statusName', 'updatedAt'],
    priorityId: ['priorityOrder', 'priorityName', 'updatedAt'],
  },
  dependencies: ['statuses', 'priorities'],
  referenceFields: {
    statusOrder: { table: 'statuses', foreignKey: 'statusId', column: 'order' },
    statusName: { table: 'statuses', foreignKey: 'statusId', column: 'name' },
    priorityOrder: { table: 'priorities', foreignKey: 'priorityId', column: 'order' },
    priorityName: { table: 'priorities', foreignKey: 'priorityId', column: 'name' },
  },
},
    search: {
  argument: 'search',
  columns: [],
  booleanTerms: true,
  offlineColumns: ['id', 'name', 'description'],
  sources: [
    { table: 'taskGridRows', key: 'taskId', column: 'searchText', dependencies: ['statuses', 'priorities'] },
    { table: 'taskRelatedSearchDocuments', key: 'taskId', column: 'searchText', dependencies: ['taskTags', 'tags', 'taskCustomFieldValues'] },
  ],
},
    filters: {
      argument: "filters",
      allowedColumns: [
        "id", "name", "description", "workspaceId", "statusId", "priorityId", "categoryId", "spotId", "teamId",
        "templateId", "createdBy", "reportedByGuest", "guestVip", "reportedByTeamId", "reportedByUserId", "dueDate", "startDate",
        "completedAt", "createdAt", "updatedAt", "deletedAt", "flagColor", "workplanId", "assetId", "fleetVehicleId", "fleetLeaderId", "fleetCompanionId",
      ],
      allowedOperators: [
        "contains", "notContains", "equals", "notEquals", "startsWith", "endsWith", "empty", "notEmpty",
        "oneOf", "lessThan", "lessThanOrEqual", "greaterThan", "greaterThanOrEqual", "inRange",
      ],
      columnTypes: {
        id: "number",
        dueDate: "number",
        startDate: "number",
        completedAt: "number",
        createdAt: "number",
        updatedAt: "number",
        deletedAt: "number",
      },
    },
    sort: {
      columnArgument: "sortColumn",
      directionArgument: "sortDirection",
      defaultColumn: "id",
      defaultDirection: "desc",
      allowedColumns: [
        "id", "name", "statusId", "priorityId", "categoryId", "spotId", "teamId", "dueDate", "startDate",
        "completedAt", "updatedAt", "createdAt", "_creationTime", "assetId", "fleetVehicleId", "fleetLeaderId", "fleetCompanionId",
      ],
    },
    window: {
      offsetArgument: "offset",
      limitArgument: "limit",
      defaultLimit: 100,
      maxLimit: 250,
      count: "exact",
    },
  },
});
