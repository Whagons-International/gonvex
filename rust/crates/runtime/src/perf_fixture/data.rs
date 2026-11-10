//! Deterministic Whagons-shaped tenant content (seeded; no `rand` so the
//! bytes never change with a dependency upgrade).

use serde_json::{json, Value};

/// SplitMix64: tiny, fast and stable across platforms and versions.
#[derive(Clone)]
pub struct Rng(u64);

impl Rng {
    pub fn new(seed: u64) -> Self {
        Self(seed ^ 0x9e37_79b9_7f4a_7c15)
    }
    pub fn next_u64(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9e37_79b9_7f4a_7c15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        z ^ (z >> 31)
    }
    /// Uniform in `0..n` (n > 0).
    pub fn below(&mut self, n: usize) -> usize {
        (self.next_u64() % n as u64) as usize
    }
    pub fn chance(&mut self, p: f64) -> bool {
        (self.next_u64() >> 11) as f64 / (1u64 << 53) as f64 <= p
    }
    pub fn pick<'a, T>(&mut self, items: &'a [T]) -> &'a T {
        &items[self.below(items.len())]
    }
    /// A UUID-shaped id (Whagons keys are UUID strings).
    pub fn uuid(&mut self) -> String {
        let a = self.next_u64();
        let b = self.next_u64();
        format!(
            "{:08x}-{:04x}-4{:03x}-{:04x}-{:012x}",
            a >> 32,
            (a >> 16) & 0xffff,
            a & 0xfff,
            ((b >> 48) & 0x3fff) | 0x8000,
            b & 0xffff_ffff_ffff
        )
    }
}

/// A bound SQL value.
#[derive(Clone, Debug)]
pub enum SqlValue {
    Null,
    Int(i64),
    Real(f64),
    Text(String),
}

impl From<&str> for SqlValue {
    fn from(value: &str) -> Self {
        Self::Text(value.to_owned())
    }
}
impl From<String> for SqlValue {
    fn from(value: String) -> Self {
        Self::Text(value)
    }
}
impl From<i64> for SqlValue {
    fn from(value: i64) -> Self {
        Self::Int(value)
    }
}
impl From<bool> for SqlValue {
    fn from(value: bool) -> Self {
        Self::Int(value as i64)
    }
}
impl From<f64> for SqlValue {
    fn from(value: f64) -> Self {
        Self::Real(value)
    }
}
impl From<Option<String>> for SqlValue {
    fn from(value: Option<String>) -> Self {
        value.map(Self::Text).unwrap_or(Self::Null)
    }
}
impl From<Value> for SqlValue {
    /// JSON columns are stored as JSON text.
    fn from(value: Value) -> Self {
        match value {
            Value::Null => Self::Null,
            value => Self::Text(value.to_string()),
        }
    }
}

/// Rows of one table, all with the same columns.
pub struct TableRows {
    pub table: &'static str,
    pub columns: Vec<&'static str>,
    pub rows: Vec<Vec<SqlValue>>,
}

impl TableRows {
    fn new(table: &'static str, columns: &[&'static str]) -> Self {
        Self {
            table,
            columns: columns.to_vec(),
            rows: Vec::new(),
        }
    }
    fn push(&mut self, row: Vec<SqlValue>) {
        debug_assert_eq!(row.len(), self.columns.len(), "{}", self.table);
        self.rows.push(row);
    }
}

/// How a member gets to see tasks (mirrors the Whagons task visibility plan).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum MemberKind {
    /// role `admin` with `tasks.viewAll` and `view-users`: sees every task.
    Admin,
    /// `view-users` (sees all members / member teams), teams, `see_all_spots`.
    Manager,
    /// Team member with `see_all_spots`: every task of its teams' workspaces.
    TeamAllSpots,
    /// Team member with `memberSpots`: tasks at or below its spots, plus
    /// spotless tasks of its teams' workspaces.
    TeamSpots,
    /// No team: only assigned (`taskUsers`) and shared (`taskShares`) tasks.
    GrantOnly,
}

#[derive(Clone, Debug)]
pub struct FixtureMember {
    pub id: String,
    pub account_id: String,
    pub email: String,
    pub name: String,
    pub role: String,
    pub permissions: Value,
    pub kind: MemberKind,
    pub teams: Vec<String>,
    pub spots: Vec<String>,
}

#[derive(Clone, Debug)]
pub struct FixtureTask {
    pub id: String,
    pub number: i64,
    pub workspace_id: String,
    pub spot_id: Option<String>,
    pub status_id: String,
}

/// One generated tenant.
pub struct TenantData {
    pub members: Vec<FixtureMember>,
    pub tasks: Vec<FixtureTask>,
    pub tables: Vec<TableRows>,
}

const BASE_MS: i64 = 1_760_000_000_000;
const WORDS: &[&str] = &[
    "inspect",
    "room",
    "lobby",
    "replace",
    "filter",
    "guest",
    "request",
    "towel",
    "minibar",
    "check",
    "leak",
    "bathroom",
    "air",
    "conditioning",
    "noise",
    "complaint",
    "elevator",
    "corridor",
    "light",
    "bulb",
    "repair",
    "urgent",
    "after",
    "checkout",
    "housekeeping",
    "maintenance",
    "pool",
    "deck",
    "chair",
    "broken",
    "window",
    "lock",
    "battery",
    "door",
    "card",
    "reader",
    "kitchen",
    "fridge",
    "temperature",
    "log",
    "laundry",
    "linen",
    "stain",
    "carpet",
    "vacuum",
    "front",
    "desk",
    "note",
    "please",
    "confirm",
    "photo",
    "attached",
];

fn sentence(rng: &mut Rng, target: usize) -> String {
    let mut text = String::with_capacity(target + 16);
    while text.len() < target {
        if !text.is_empty() {
            text.push(' ');
        }
        text.push_str(rng.pick::<&str>(WORDS));
    }
    text
}

fn count(total: usize, fraction: f64) -> usize {
    ((total as f64 * fraction).round() as usize).max(1)
}

/// Generates one tenant: `tasks` task rows (~`description_bytes` each) and
/// `members` members of mixed visibility.
pub fn generate(seed: u64, tasks: usize, members: usize, description_bytes: usize) -> TenantData {
    assert!(members >= 5, "a tenant needs one member of every kind");
    let mut rng = Rng::new(seed);
    let mut tables = Vec::new();
    let t = |i: usize| BASE_MS + i as i64 * 1000;

    // Teams, workspaces (each linked to one or two teams), categories.
    let team_count = (members / 10).clamp(2, 12);
    let teams: Vec<String> = (0..team_count).map(|_| rng.uuid()).collect();
    let mut team_rows = TableRows::new(
        "teams",
        &[
            "_id",
            "_creationTime",
            "name",
            "color",
            "isActive",
            "slug",
            "updatedAt",
            "allowMultitasking",
        ],
    );
    for (i, id) in teams.iter().enumerate() {
        team_rows.push(vec![
            id.as_str().into(),
            t(i).into(),
            format!("Team {i}").into(),
            "#3366ff".into(),
            true.into(),
            format!("team-{i}").into(),
            t(i).into(),
            false.into(),
        ]);
    }
    tables.push(team_rows);

    let workspace_count = team_count + 4;
    let mut workspaces = Vec::new();
    let mut workspace_teams: Vec<Vec<String>> = Vec::new();
    for i in 0..workspace_count {
        workspaces.push(rng.uuid());
        let mut linked = vec![teams[i % team_count].clone()];
        if i >= team_count {
            let other = teams[rng.below(team_count)].clone();
            if !linked.contains(&other) {
                linked.push(other);
            }
        }
        workspace_teams.push(linked);
    }
    let categories: Vec<String> = (0..workspace_count).map(|_| rng.uuid()).collect();

    // Spot tree: roots > children > leaves.
    let (roots, children, leaves) = if tasks >= 2000 { (4, 5, 4) } else { (2, 3, 2) };
    let mut spot_rows = TableRows::new(
        "spots",
        &[
            "_id",
            "name",
            "parentId",
            "order",
            "isBranch",
            "updatedAt",
            "tagIds",
        ],
    );
    let mut spot_levels: [Vec<String>; 3] = Default::default();
    let add_spot = |rows: &mut TableRows,
                    rng: &mut Rng,
                    parent: Option<&String>,
                    level: usize,
                    order: usize| {
        let id = rng.uuid();
        rows.push(vec![
            id.as_str().into(),
            format!("Spot {level}.{order}").into(),
            parent.cloned().into(),
            (order as i64).into(),
            (level < 2).into(),
            t(order).into(),
            json!([]).into(),
        ]);
        id
    };
    for r in 0..roots {
        let root = add_spot(&mut spot_rows, &mut rng, None, 0, r);
        spot_levels[0].push(root.clone());
        for c in 0..children {
            let child = add_spot(&mut spot_rows, &mut rng, Some(&root), 1, r * children + c);
            spot_levels[1].push(child.clone());
            for l in 0..leaves {
                let leaf = add_spot(
                    &mut spot_rows,
                    &mut rng,
                    Some(&child),
                    2,
                    (r * children + c) * leaves + l,
                );
                spot_levels[2].push(leaf);
            }
        }
    }
    tables.push(spot_rows);

    let mut workspace_rows = TableRows::new(
        "workspaces",
        &[
            "_id",
            "_creationTime",
            "name",
            "description",
            "color",
            "icon",
            "teams",
            "preferences",
            "type",
            "categoryId",
            "spots",
            "allowAdHocTasks",
            "createdBy",
            "allowedCategoryIds",
            "viewModes",
            "updatedAt",
            "deletedAt",
        ],
    );
    let mut workspace_team_rows = TableRows::new(
        "workspaceTeams",
        &[
            "_id",
            "_creationTime",
            "workspaceId",
            "teamId",
            "createdAt",
            "updatedAt",
            "deletedAt",
        ],
    );
    let mut category_rows = TableRows::new(
        "categories",
        &[
            "_id",
            "_creationTime",
            "name",
            "color",
            "enabled",
            "teamId",
            "workspaceId",
            "updatedAt",
            "deletedAt",
            "defaultTagIds",
            "defaultUserIds",
            "reportingTeamIds",
        ],
    );
    for (i, id) in workspaces.iter().enumerate() {
        workspace_rows.push(vec![
            id.as_str().into(),
            t(i).into(),
            format!("Workspace {i}").into(),
            sentence(&mut rng, 60).into(),
            "#22aa66".into(),
            "fas fa-building".into(),
            json!(workspace_teams[i]).into(),
            json!({"defaultView": "grid", "showCompleted": false}).into(),
            "DEFAULT".into(),
            categories[i].as_str().into(),
            json!(spot_levels[0]).into(),
            true.into(),
            SqlValue::Null,
            json!([categories[i]]).into(),
            json!(["grid", "board", "calendar"]).into(),
            t(i).into(),
            SqlValue::Null,
        ]);
        for team in &workspace_teams[i] {
            workspace_team_rows.push(vec![
                rng.uuid().into(),
                t(i).into(),
                id.as_str().into(),
                team.as_str().into(),
                t(i).into(),
                t(i).into(),
                SqlValue::Null,
            ]);
        }
        category_rows.push(vec![
            categories[i].as_str().into(),
            t(i).into(),
            format!("Category {i}").into(),
            "#aa3366".into(),
            true.into(),
            workspace_teams[i][0].as_str().into(),
            id.as_str().into(),
            t(i).into(),
            SqlValue::Null,
            json!([]).into(),
            json!([]).into(),
            json!([]).into(),
        ]);
    }
    tables.push(workspace_rows);
    tables.push(workspace_team_rows);
    tables.push(category_rows);

    // Statuses, priorities, tags (tenant-wide catalogs).
    let status_names = [
        "Pending",
        "Accepted",
        "In progress",
        "Paused",
        "Waiting",
        "Review",
        "Done",
        "Cancelled",
    ];
    let statuses: Vec<String> = status_names.iter().map(|_| rng.uuid()).collect();
    let mut status_rows = TableRows::new(
        "statuses",
        &[
            "_id",
            "categoryId",
            "name",
            "action",
            "color",
            "icon",
            "system",
            "initial",
            "enabled",
            "final",
            "order",
            "workingAnimation",
            "updatedAt",
            "deletedAt",
        ],
    );
    for (i, name) in status_names.iter().enumerate() {
        status_rows.push(vec![
            statuses[i].as_str().into(),
            SqlValue::Null,
            (*name).into(),
            (if i == 2 {
                "WORKING"
            } else if i >= 6 {
                "FINISHED"
            } else {
                "NONE"
            })
            .into(),
            "#888888".into(),
            "fas fa-circle".into(),
            true.into(),
            (i == 0).into(),
            true.into(),
            (i >= 6).into(),
            (i as i64).into(),
            SqlValue::Null,
            t(i).into(),
            SqlValue::Null,
        ]);
    }
    tables.push(status_rows);
    let priorities: Vec<String> = (0..4).map(|_| rng.uuid()).collect();
    let mut priority_rows = TableRows::new(
        "priorities",
        &["_id", "name", "color", "order", "updatedAt"],
    );
    for (i, id) in priorities.iter().enumerate() {
        priority_rows.push(vec![
            id.as_str().into(),
            ["Low", "Normal", "High", "Urgent"][i].into(),
            "#ff8800".into(),
            (i as i64).into(),
            t(i).into(),
        ]);
    }
    tables.push(priority_rows);
    let tags: Vec<String> = (0..(tasks / 225).clamp(10, 60))
        .map(|_| rng.uuid())
        .collect();
    let mut tag_rows = TableRows::new(
        "tags",
        &[
            "_id",
            "name",
            "color",
            "icon",
            "categoryId",
            "updatedAt",
            "deletedAt",
        ],
    );
    for (i, id) in tags.iter().enumerate() {
        tag_rows.push(vec![
            id.as_str().into(),
            format!("tag-{i}").into(),
            "#00aaff".into(),
            "fas fa-tag".into(),
            SqlValue::Null,
            t(i).into(),
            SqlValue::Null,
        ]);
    }
    tables.push(tag_rows);
    let mut settings = TableRows::new(
        "globalSettings",
        &[
            "_id",
            "_creationTime",
            "group",
            "key",
            "updatedAt",
            "value",
            "allSpotsMemberStatus",
        ],
    );
    settings.push(vec![
        rng.uuid().into(),
        t(0).into(),
        "tasks".into(),
        "taskVisibility".into(),
        t(0).into(),
        json!({"mode": "spots"}).into(),
        SqlValue::Null,
    ]);
    tables.push(settings);

    // Members: the kind mix decides how each one reaches tasks.
    let admins = count(members, 0.05);
    let managers = count(members, 0.08);
    let all_spots = count(members, 0.25);
    let grant_only = count(members, 0.27);
    let spot_members = members - admins - managers - all_spots - grant_only;
    assert!(spot_members >= 1);
    let kinds = std::iter::repeat_n(MemberKind::Admin, admins)
        .chain(std::iter::repeat_n(MemberKind::Manager, managers))
        .chain(std::iter::repeat_n(MemberKind::TeamAllSpots, all_spots))
        .chain(std::iter::repeat_n(MemberKind::TeamSpots, spot_members))
        .chain(std::iter::repeat_n(MemberKind::GrantOnly, grant_only));
    let mut fixture_members = Vec::with_capacity(members);
    for (i, kind) in kinds.enumerate() {
        let id = rng.uuid();
        let account_id = rng.uuid();
        let (role, permissions) = match kind {
            MemberKind::Admin => ("admin", json!({"tasks.viewAll": true, "view-users": true})),
            MemberKind::Manager => ("member", json!({"view-users": true})),
            _ => ("member", json!({})),
        };
        let member_teams = match kind {
            MemberKind::GrantOnly => Vec::new(),
            MemberKind::Admin => vec![teams[i % team_count].clone()],
            _ => {
                let mut picked = vec![teams[rng.below(team_count)].clone()];
                if rng.chance(0.4) {
                    let other = teams[rng.below(team_count)].clone();
                    if !picked.contains(&other) {
                        picked.push(other);
                    }
                }
                picked
            }
        };
        let member_spots = if kind == MemberKind::TeamSpots {
            // One to three spots at mixed depths (root, area or leaf).
            (0..1 + rng.below(3))
                .map(|_| {
                    let level = [0, 1, 1, 2, 2][rng.below(5)];
                    rng.pick(&spot_levels[level]).clone()
                })
                .collect::<std::collections::BTreeSet<_>>()
                .into_iter()
                .collect()
        } else {
            Vec::new()
        };
        fixture_members.push(FixtureMember {
            email: format!("member-{i}@example.invalid"),
            name: format!("Member {i}"),
            id,
            account_id,
            role: role.to_owned(),
            permissions,
            kind,
            teams: member_teams,
            spots: member_spots,
        });
    }
    let mut member_rows = TableRows::new(
        "members",
        &[
            "id",
            "account_id",
            "status",
            "display_name",
            "avatar_url",
            "role",
            "permissions",
            "membership_revision",
            "created_at",
            "updated_at",
            "email",
            "settings",
            "nickname",
            "show_nickname",
            "color",
            "see_all_spots",
            "availability_status",
            "global_role_names",
            "initialization_stage",
            "employment_status",
        ],
    );
    let mut member_team_rows = TableRows::new(
        "memberTeams",
        &[
            "_id",
            "_creationTime",
            "memberId",
            "userId",
            "teamId",
            "roleId",
            "roleKey",
            "createdAt",
            "updatedAt",
            "deletedAt",
        ],
    );
    let mut member_spot_rows = TableRows::new(
        "memberSpots",
        &[
            "_id",
            "_creationTime",
            "memberId",
            "userId",
            "spotId",
            "createdAt",
            "updatedAt",
            "deletedAt",
        ],
    );
    let mut presence_rows = TableRows::new(
        "userPresence",
        &["_id", "_creationTime", "userId", "lastSeenAt", "state"],
    );
    let mut preference_rows = TableRows::new(
        "workspacePreferences",
        &[
            "_id",
            "_creationTime",
            "userId",
            "memberId",
            "workspaceId",
            "preferences",
            "createdAt",
            "updatedAt",
        ],
    );
    for (i, member) in fixture_members.iter().enumerate() {
        let see_all = matches!(member.kind, MemberKind::Manager | MemberKind::TeamAllSpots);
        member_rows.push(vec![
            member.id.as_str().into(),
            member.account_id.as_str().into(),
            "active".into(),
            member.name.as_str().into(),
            "".into(),
            member.role.as_str().into(),
            member.permissions.clone().into(),
            1i64.into(),
            t(i).into(),
            t(i).into(),
            member.email.as_str().into(),
            json!({"locale": "en", "timezone": "America/Mexico_City", "notifications": {"push": true, "email": false}}).into(),
            SqlValue::Null,
            false.into(),
            "#6655ee".into(),
            see_all.into(),
            "available".into(),
            json!([]).into(),
            2i64.into(),
            "active".into(),
        ]);
        for team in &member.teams {
            member_team_rows.push(vec![
                rng.uuid().into(),
                t(i).into(),
                member.id.as_str().into(),
                member.id.as_str().into(),
                team.as_str().into(),
                SqlValue::Null,
                "member".into(),
                t(i).into(),
                t(i).into(),
                SqlValue::Null,
            ]);
        }
        for spot in &member.spots {
            member_spot_rows.push(vec![
                rng.uuid().into(),
                t(i).into(),
                member.id.as_str().into(),
                member.id.as_str().into(),
                spot.as_str().into(),
                t(i).into(),
                t(i).into(),
                SqlValue::Null,
            ]);
        }
        presence_rows.push(vec![
            rng.uuid().into(),
            t(i).into(),
            member.id.as_str().into(),
            t(i).into(),
            "active".into(),
        ]);
        for _ in 0..2 {
            preference_rows.push(vec![
                rng.uuid().into(),
                t(i).into(),
                member.id.as_str().into(),
                member.id.as_str().into(),
                rng.pick(&workspaces).as_str().into(),
                json!({"view": "grid", "sort": [{"colId": "updatedAt", "sort": "desc"}],
                       "hiddenColumns": ["latitude", "longitude"], "pageSize": 100})
                .into(),
                t(i).into(),
                t(i).into(),
            ]);
        }
    }
    tables.push(member_rows);
    tables.push(member_team_rows);
    tables.push(member_spot_rows);
    tables.push(presence_rows);
    tables.push(preference_rows);

    // Tasks.
    let spot_choices: Vec<&String> = spot_levels[2]
        .iter()
        .chain(spot_levels[2].iter())
        .chain(spot_levels[1].iter())
        .chain(spot_levels[0].iter().take(1))
        .collect();
    let mut task_rows = TableRows::new(
        "tasks",
        &[
            "_id",
            "_creationTime",
            "id",
            "name",
            "description",
            "workspaceId",
            "statusId",
            "priorityId",
            "categoryId",
            "spotId",
            "teamId",
            "createdBy",
            "createdVia",
            "requiresSignature",
            "requiresAcknowledgment",
            "audienceRules",
            "reportedByGuest",
            "guestVip",
            "dueDate",
            "startDate",
            "latitude",
            "longitude",
            "completedAt",
            "createdAt",
            "updatedAt",
            "flagColor",
            "rowHash",
            "deletedAt",
        ],
    );
    let mut fixture_tasks = Vec::with_capacity(tasks);
    for number in 1..=tasks {
        let id = rng.uuid();
        let w = rng.below(workspace_count);
        let spot = match rng.below(100) {
            0..=14 => None,
            15..=19 => Some(String::new()),
            _ => Some((*rng.pick(&spot_choices)).clone()),
        };
        let status = rng.below(statuses.len());
        let created = BASE_MS + number as i64 * 60_000;
        let creator = &rng.pick(&fixture_members).id;
        fixture_tasks.push(FixtureTask {
            id: id.clone(),
            number: number as i64,
            workspace_id: workspaces[w].clone(),
            spot_id: spot.clone(),
            status_id: statuses[status].clone(),
        });
        let audience = if rng.chance(0.3) {
            json!({"mode": "teams", "teamIds": [workspace_teams[w][0]], "roles": []})
        } else {
            Value::Null
        };
        task_rows.push(vec![
            id.into(),
            created.into(),
            (number as i64).into(),
            format!("Task {number}: {}", sentence(&mut rng, 24)).into(),
            sentence(&mut rng, description_bytes).into(),
            workspaces[w].as_str().into(),
            statuses[status].as_str().into(),
            rng.pick(&priorities).as_str().into(),
            categories[w].as_str().into(),
            spot.into(),
            workspace_teams[w][0].as_str().into(),
            creator.as_str().into(),
            "app".into(),
            rng.chance(0.1).into(),
            false.into(),
            audience.into(),
            false.into(),
            false.into(),
            (created + 3 * 86_400_000).into(),
            created.into(),
            if rng.chance(0.2) {
                SqlValue::Real(19.4326 + rng.below(1000) as f64 / 1e4)
            } else {
                SqlValue::Null
            },
            if rng.chance(0.2) {
                SqlValue::Real(-99.1332 - rng.below(1000) as f64 / 1e4)
            } else {
                SqlValue::Null
            },
            if status >= 6 {
                SqlValue::Int(created + 86_400_000)
            } else {
                SqlValue::Null
            },
            created.into(),
            (created + rng.below(86_400_000) as i64).into(),
            SqlValue::Null,
            format!("{:016x}", rng.next_u64()).into(),
            SqlValue::Null,
        ]);
    }
    tables.push(task_rows);

    // Assignments: grant-only members get ~40 tasks each; other tasks get a
    // random team member. Shares and approvals add the other grant paths.
    let mut task_users = TableRows::new(
        "taskUsers",
        &[
            "_id",
            "taskId",
            "userId",
            "workspaceId",
            "role",
            "createdAt",
            "updatedAt",
            "deletedAt",
        ],
    );
    let assignable: Vec<&FixtureMember> = fixture_members
        .iter()
        .filter(|m| m.kind != MemberKind::GrantOnly)
        .collect();
    let mut assigned = std::collections::HashSet::new();
    let mut assign = |rows: &mut TableRows, rng: &mut Rng, task: &FixtureTask, member: &str| {
        if !assigned.insert((task.id.clone(), member.to_owned())) {
            return;
        }
        rows.push(vec![
            rng.uuid().into(),
            task.id.as_str().into(),
            member.into(),
            task.workspace_id.as_str().into(),
            "assignee".into(),
            (BASE_MS + task.number * 60_000).into(),
            (BASE_MS + task.number * 60_000).into(),
            SqlValue::Null,
        ]);
    };
    for task in &fixture_tasks {
        if rng.chance(0.75) {
            let member = rng.pick(&assignable).id.clone();
            assign(&mut task_users, &mut rng, task, &member);
        }
    }
    let grant_members: Vec<&FixtureMember> = fixture_members
        .iter()
        .filter(|m| m.kind == MemberKind::GrantOnly)
        .collect();
    let per_grant = (tasks / 225).clamp(5, 40);
    for member in &grant_members {
        for _ in 0..per_grant {
            let task = rng.pick(&fixture_tasks).clone();
            assign(&mut task_users, &mut rng, &task, &member.id);
        }
    }
    tables.push(task_users);
    let mut task_shares = TableRows::new(
        "taskShares",
        &[
            "_id",
            "taskId",
            "sharedByUserId",
            "sharedToUserId",
            "sharedToTeamId",
            "permission",
            "status",
            "createdAt",
            "updatedAt",
            "revokedAt",
            "deletedAt",
        ],
    );
    for member in &grant_members {
        for _ in 0..5 {
            let task = rng.pick(&fixture_tasks);
            task_shares.push(vec![
                rng.uuid().into(),
                task.id.as_str().into(),
                rng.pick(&assignable).id.as_str().into(),
                member.id.as_str().into(),
                SqlValue::Null,
                "view".into(),
                "active".into(),
                (BASE_MS + task.number * 60_000).into(),
                (BASE_MS + task.number * 60_000).into(),
                SqlValue::Null,
                SqlValue::Null,
            ]);
        }
    }
    for _ in 0..count(tasks, 0.002) {
        let task = rng.pick(&fixture_tasks);
        task_shares.push(vec![
            rng.uuid().into(),
            task.id.as_str().into(),
            rng.pick(&assignable).id.as_str().into(),
            SqlValue::Null,
            rng.pick(&teams).as_str().into(),
            "view".into(),
            "active".into(),
            (BASE_MS + task.number * 60_000).into(),
            (BASE_MS + task.number * 60_000).into(),
            SqlValue::Null,
            SqlValue::Null,
        ]);
    }
    tables.push(task_shares);
    let approvers: Vec<&FixtureMember> = fixture_members
        .iter()
        .filter(|m| m.kind == MemberKind::Manager)
        .collect();
    let mut approvals = TableRows::new(
        "taskApprovalInstances",
        &[
            "_id",
            "_creationTime",
            "taskId",
            "approverUserId",
            "approverName",
            "status",
            "isRequired",
            "orderIndex",
            "createdAt",
            "updatedAt",
            "deletedAt",
        ],
    );
    for _ in 0..count(tasks, 0.01) {
        let task = rng.pick(&fixture_tasks);
        let approver = rng.pick(&approvers);
        approvals.push(vec![
            rng.uuid().into(),
            (BASE_MS + task.number * 60_000).into(),
            task.id.as_str().into(),
            approver.id.as_str().into(),
            approver.name.as_str().into(),
            "pending".into(),
            true.into(),
            0i64.into(),
            (BASE_MS + task.number * 60_000).into(),
            (BASE_MS + task.number * 60_000).into(),
            SqlValue::Null,
        ]);
    }
    tables.push(approvals);
    let mut task_tags = TableRows::new(
        "taskTags",
        &["_id", "_creationTime", "taskId", "tagId", "userId"],
    );
    for task in &fixture_tasks {
        if rng.chance(0.4) {
            let first = rng.below(tags.len());
            for k in 0..1 + rng.below(2) {
                task_tags.push(vec![
                    rng.uuid().into(),
                    (BASE_MS + task.number * 60_000).into(),
                    task.id.as_str().into(),
                    tags[(first + k) % tags.len()].as_str().into(),
                    rng.pick(&assignable).id.as_str().into(),
                ]);
            }
        }
    }
    tables.push(task_tags);

    TenantData {
        members: fixture_members,
        tasks: fixture_tasks,
        tables,
    }
}
