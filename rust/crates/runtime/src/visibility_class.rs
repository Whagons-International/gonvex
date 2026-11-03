//! Compiled visibility programs and shared visibility classes.
//!
//! Pure and synchronous: no database handle, no async runtime (V1 in
//! `docs/visibility-masks.md`). The interpreted evaluator in
//! `visibility_mask.rs` stays the reference semantics; this module must agree
//! with it (and with the SQL compiler) on every row and principal. The shared
//! corpus in `visibility_corpus.rs` checks all three.
//!
//! # Model
//!
//! A version 2 plan is compiled once per plan (cached on the plan's shared
//! caches, so every clone of a module's plan uses the same program) into a flat
//! node array over column slots. Atoms fall into three groups:
//!
//! * **class atoms** depend on the principal's shared tokens only: `role`,
//!   `permission`, `nonEmpty` (a bit per class) and `inSet` over principal or
//!   principal-derived sets (`InClass`, tokens interned to `u32`, sorted);
//! * **member atoms** depend on one member: `eqContext` and `inSet` over grant
//!   sets, grant-derived sets and member-owned principal sets (a principal set
//!   that selects the key of rows anchored on `member.id`, such as the member's
//!   own personal workspaces);
//! * **row atoms**: `eq` literals.
//!
//! Members whose class atoms resolve identically share one
//! `Arc<VisibilityClass>`. For a row, a member is *hit* when one of its member
//! atoms is true on that row (directly, or on an inherited parent row). A
//! member that is not hit sees the row exactly when its class's *default*
//! evaluation (member atoms false) admits it, so the default is evaluated once
//! per class per row; only hit members are evaluated individually.
//!
//! A class is *gated* when its default is false for every row whose tokens hit
//! none of the class's sets (Kleene evaluation with class tokens false and row
//! literals unknown). `ClassScope::candidates` uses a token → class index plus
//! the list of ungated classes and a token → member index to find, for one
//! changed row, every class and member that could see its old or new value.
//!
//! # API (feed path and workstream 5)
//!
//! * `member_visibility(plan, resolved, scope) -> MemberHandle`: compile (once)
//!   and bind a member's principal. `MemberHandle::Fallback` means the plan has
//!   a shape this module does not compile (`inheritsAny`, an unknown
//!   operator); the caller must use `visibility::row_matches` and count it.
//! * `MemberVisibility::class() -> &Arc<VisibilityClass>`;
//!   `MemberVisibility::visible(RowView) -> bool`;
//!   `MemberVisibility::visible_memo(&mut EvalMemo, &Value) -> bool` for change
//!   rows that live for the whole feed event (one class evaluation per row).
//! * `VisibilityClass::visible_default(&AclRow) -> bool`.
//! * `ClassScope::decode(RowView) -> AclRow`, `ClassScope::register(&handle)`
//!   (token index for `candidates`), `ClassScope::candidates(old, new)`.

use std::borrow::Cow;
use std::collections::HashMap;
use std::hash::{BuildHasherDefault, Hasher};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock, Weak};

use serde::de::{DeserializeSeed, Deserializer, IgnoredAny, MapAccess, Visitor};
use serde::Deserialize;
use serde_json::{Number, Value};

use crate::visibility::{
    literal_constraints_hold, ResolvedVisibility, VisibilityConstraint, VisibilityExpression,
    VisibilityPlan,
};
use crate::visibility_index::ParentAclIndex;

// ---------------------------------------------------------------------------
// Hashing (no extra dependency): FxHash-style multiply/rotate.

#[derive(Default, Clone, Copy)]
pub(crate) struct FxHasher(u64);

const FX_SEED: u64 = 0x51_7c_c1_b7_27_22_0a_95;

impl FxHasher {
    #[inline]
    fn add(&mut self, word: u64) {
        self.0 = (self.0.rotate_left(5) ^ word).wrapping_mul(FX_SEED);
    }
}

impl Hasher for FxHasher {
    #[inline]
    fn write(&mut self, bytes: &[u8]) {
        let mut chunks = bytes.chunks_exact(8);
        for chunk in &mut chunks {
            self.add(u64::from_le_bytes(chunk.try_into().unwrap()));
        }
        let rest = chunks.remainder();
        if !rest.is_empty() {
            let mut word = [0u8; 8];
            word[..rest.len()].copy_from_slice(rest);
            self.add(u64::from_le_bytes(word) ^ ((rest.len() as u64) << 59));
        }
    }
    #[inline]
    fn write_u8(&mut self, value: u8) {
        self.add(value as u64);
    }
    #[inline]
    fn write_u16(&mut self, value: u16) {
        self.add(value as u64);
    }
    #[inline]
    fn write_u32(&mut self, value: u32) {
        self.add(value as u64);
    }
    #[inline]
    fn write_u64(&mut self, value: u64) {
        self.add(value);
    }
    #[inline]
    fn write_usize(&mut self, value: usize) {
        self.add(value as u64);
    }
    #[inline]
    fn finish(&self) -> u64 {
        self.0
    }
}

pub(crate) type FxMap<K, V> = HashMap<K, V, BuildHasherDefault<FxHasher>>;

// ---------------------------------------------------------------------------
// Compiled plan.

type Slot = u16;

#[derive(Clone, Debug)]
enum Node {
    Const(bool),
    /// Principal-only atom: `role`, `permission`, `nonEmpty`; one bit per class.
    Bit(u16),
    Eq { column: Slot, literal: Value },
    InClass { column: Slot, set: u16 },
    InMember { column: Slot, set: u16 },
    Context { column: Slot, context: u16 },
    Inherits(u16),
    And(u32, u32),
    Or(u32, u32),
    Not(u32),
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum BitAtom {
    Role(String),
    Permission(String),
    NonEmpty(String),
}

#[derive(Clone, Copy, Debug)]
enum MemberAtom {
    InSet { column: Slot, set: u16 },
    Context { column: Slot, context: u16 },
}

#[derive(Debug)]
struct InheritsSpec {
    column: Slot,
    table: String,
    parent_where: Vec<VisibilityConstraint>,
    /// Child-principal predicate on the parent row, over `override_columns`.
    override_root: Option<u32>,
    override_columns: Vec<String>,
    override_member_atoms: Vec<MemberAtom>,
    on_missing: Option<u32>,
}

/// One plan's program. Built once per plan and shared by every clone of it.
pub struct CompiledPlan {
    table: String,
    /// Content hash of the plan (stable across processes and restarts).
    fingerprint: [u8; 32],
    columns: Vec<String>,
    nodes: Vec<Node>,
    kids: Vec<u32>,
    root: u32,
    bits: Vec<BitAtom>,
    class_sets: Vec<String>,
    member_sets: Vec<String>,
    contexts: Vec<String>,
    inherits: Vec<InheritsSpec>,
    /// Distinct (column, class set) atoms of the row's own domain.
    class_atoms: Vec<(Slot, u16)>,
    /// Member atoms of the row's own domain (including `onMissing`).
    member_atoms: Vec<MemberAtom>,
    unsupported: Option<String>,
    scopes: Mutex<FxMap<String, Arc<ClassScope>>>,
    /// Per bit pattern: which class sets can still change a row decision.
    relevant: Mutex<FxMap<Box<[bool]>, Residual>>,
}

/// What a class's bits leave of the program: the residual shape (fixed
/// subtrees and their values) and the class sets it still consults. Members
/// with equal residuals decide every row identically whatever their bits.
#[derive(Clone)]
struct Residual {
    shape: Arc<[u32]>,
    sets: Arc<[bool]>,
}

impl std::fmt::Debug for CompiledPlan {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("CompiledPlan")
            .field("table", &self.table)
            .field("columns", &self.columns)
            .field("nodes", &self.nodes.len())
            .field("unsupported", &self.unsupported)
            .finish()
    }
}

/// Whether a set's tokens are member-specific (grant, grant-derived, or a
/// member-owned principal set) rather than shared by a class of members.
fn member_set(plan: &VisibilityPlan, name: &str) -> bool {
    let mut current = name;
    for _ in 0..8 {
        let Some(set) = plan.sets.get(current) else {
            return false;
        };
        match set.kind.as_str() {
            "grant" => return true,
            "derived" => current = set.from.as_str(),
            _ => {
                // Rows of the set's own table, selected by key and anchored on
                // this member (e.g. workspaces the member created): unique per
                // member, so keeping them in the class key would split classes.
                let base = if set.alias.is_empty() {
                    set.table.as_str()
                } else {
                    set.alias.as_str()
                };
                return set.joins.is_empty()
                    && (set.select_from.is_empty() || set.select_from == base)
                    && matches!(set.select.as_str(), "_id" | "id")
                    && set.constraints.iter().any(|constraint| {
                        !constraint.context.is_empty()
                            && (constraint.table.is_empty()
                                || constraint.table == base
                                || constraint.table == set.table)
                    });
            }
        }
    }
    false
}

struct Compiler<'p> {
    plan: &'p VisibilityPlan,
    nodes: Vec<Node>,
    kids: Vec<u32>,
    bits: Vec<BitAtom>,
    class_sets: Vec<String>,
    member_sets: Vec<String>,
    contexts: Vec<String>,
    inherits: Vec<InheritsSpec>,
    unsupported: Option<String>,
}

/// Where compiled atoms read their row columns and record member atoms.
struct Domain<'a> {
    columns: &'a mut Vec<String>,
    member_atoms: &'a mut Vec<MemberAtom>,
    class_atoms: Option<&'a mut Vec<(Slot, u16)>>,
    allow_inherits: bool,
}

fn slot(columns: &mut Vec<String>, column: &str) -> Slot {
    if let Some(index) = columns.iter().position(|existing| existing == column) {
        return index as Slot;
    }
    columns.push(column.to_owned());
    (columns.len() - 1) as Slot
}

fn intern_name(names: &mut Vec<String>, name: &str) -> u16 {
    if let Some(index) = names.iter().position(|existing| existing == name) {
        return index as u16;
    }
    names.push(name.to_owned());
    (names.len() - 1) as u16
}

impl Compiler<'_> {
    fn push(&mut self, node: Node) -> u32 {
        self.nodes.push(node);
        (self.nodes.len() - 1) as u32
    }

    fn unsupported(&mut self, reason: String) -> u32 {
        if self.unsupported.is_none() {
            self.unsupported = Some(reason);
        }
        self.push(Node::Const(false))
    }

    fn bit(&mut self, atom: BitAtom) -> u32 {
        let index = match self.bits.iter().position(|existing| *existing == atom) {
            Some(index) => index,
            None => {
                self.bits.push(atom);
                self.bits.len() - 1
            }
        };
        self.push(Node::Bit(index as u16))
    }

    fn compile(&mut self, expression: &VisibilityExpression, domain: &mut Domain<'_>) -> u32 {
        match expression.operator.as_str() {
            "public" => self.push(Node::Const(true)),
            "nonEmpty" => self.bit(BitAtom::NonEmpty(expression.set.clone())),
            "permission" => match expression.value.as_ref().and_then(Value::as_str) {
                Some(permission) => self.bit(BitAtom::Permission(permission.to_owned())),
                None => self.push(Node::Const(false)),
            },
            "role" => match expression.value.as_ref().and_then(Value::as_str) {
                Some(role) => self.bit(BitAtom::Role(role.to_owned())),
                None => self.push(Node::Const(false)),
            },
            "eq" => match expression.value.as_ref().and_then(|value| value.get("literal")) {
                Some(literal) => {
                    let column = slot(domain.columns, &expression.column);
                    self.push(Node::Eq {
                        column,
                        literal: literal.clone(),
                    })
                }
                None => self.push(Node::Const(false)),
            },
            "eqContext" => {
                let column = slot(domain.columns, &expression.column);
                let context = intern_name(&mut self.contexts, &expression.context);
                domain
                    .member_atoms
                    .push(MemberAtom::Context { column, context });
                self.push(Node::Context { column, context })
            }
            "inSet" => {
                let column = slot(domain.columns, &expression.column);
                if member_set(self.plan, &expression.set) {
                    let set = intern_name(&mut self.member_sets, &expression.set);
                    domain.member_atoms.push(MemberAtom::InSet { column, set });
                    self.push(Node::InMember { column, set })
                } else {
                    let set = intern_name(&mut self.class_sets, &expression.set);
                    if let Some(atoms) = domain.class_atoms.as_deref_mut() {
                        if !atoms.contains(&(column, set)) {
                            atoms.push((column, set));
                        }
                    }
                    self.push(Node::InClass { column, set })
                }
            }
            "inherits" => {
                if !domain.allow_inherits {
                    return self.unsupported("inherits inside a parent predicate".to_owned());
                }
                let column = slot(domain.columns, &expression.column);
                let mut override_columns = Vec::new();
                let mut override_member_atoms = Vec::new();
                let override_root = expression.predicate.as_ref().map(|predicate| {
                    let mut inner = Domain {
                        columns: &mut override_columns,
                        member_atoms: &mut override_member_atoms,
                        class_atoms: None,
                        allow_inherits: false,
                    };
                    self.compile(predicate, &mut inner)
                });
                let on_missing = expression
                    .on_missing
                    .as_ref()
                    .map(|fallback| self.compile(fallback, domain));
                self.inherits.push(InheritsSpec {
                    column,
                    table: expression.table.clone(),
                    parent_where: expression.parent_where.clone(),
                    override_root,
                    override_columns,
                    override_member_atoms,
                    on_missing,
                });
                self.push(Node::Inherits((self.inherits.len() - 1) as u16))
            }
            "and" | "or" => {
                let children = expression
                    .children
                    .iter()
                    .map(|child| self.compile(child, domain))
                    .collect::<Vec<_>>();
                let start = self.kids.len() as u32;
                self.kids.extend(children.iter().copied());
                let length = children.len() as u32;
                if expression.operator == "and" {
                    self.push(Node::And(start, length))
                } else {
                    self.push(Node::Or(start, length))
                }
            }
            "not" => match expression.children.first() {
                Some(child) if expression.children.len() == 1 => {
                    let child = self.compile(child, domain);
                    self.push(Node::Not(child))
                }
                _ => self.unsupported("not without exactly one child".to_owned()),
            },
            other => self.unsupported(format!("operator {other}")),
        }
    }
}

/// Compile a plan. Never fails: unsupported shapes set `unsupported`.
pub fn compile(plan: &VisibilityPlan) -> CompiledPlan {
    let mut compiler = Compiler {
        plan,
        nodes: Vec::new(),
        kids: Vec::new(),
        bits: Vec::new(),
        class_sets: Vec::new(),
        member_sets: Vec::new(),
        contexts: Vec::new(),
        inherits: Vec::new(),
        unsupported: None,
    };
    if plan.version != Some(2) {
        compiler.unsupported = Some("legacy (unversioned) plan".to_owned());
    }
    let mut columns = Vec::new();
    let mut member_atoms = Vec::new();
    let mut class_atoms = Vec::new();
    let root = {
        let mut domain = Domain {
            columns: &mut columns,
            member_atoms: &mut member_atoms,
            class_atoms: Some(&mut class_atoms),
            allow_inherits: true,
        };
        compiler.compile(&plan.predicate, &mut domain)
    };
    if columns.len() > Slot::MAX as usize || compiler.nodes.len() > u32::MAX as usize / 2 {
        compiler.unsupported = Some("plan too large".to_owned());
    }
    use sha2::{Digest, Sha256};
    let fingerprint: [u8; 32] = Sha256::digest(
        serde_json::to_vec(&(&plan.table, &plan.key, &plan.sets, &plan.predicate)).unwrap_or_default(),
    )
    .into();
    CompiledPlan {
        fingerprint,
        table: plan.table.clone(),
        columns,
        nodes: compiler.nodes,
        kids: compiler.kids,
        root,
        bits: compiler.bits,
        class_sets: compiler.class_sets,
        member_sets: compiler.member_sets,
        contexts: compiler.contexts,
        inherits: compiler.inherits,
        class_atoms,
        member_atoms,
        unsupported: compiler.unsupported,
        scopes: Mutex::new(FxMap::default()),
        relevant: Mutex::new(FxMap::default()),
    }
}

impl CompiledPlan {
    pub fn table(&self) -> &str {
        &self.table
    }

    /// The row columns this program reads (its ACL columns).
    pub fn columns(&self) -> &[String] {
        &self.columns
    }

    /// Why this plan falls back to the interpreted evaluator, if it does.
    pub fn unsupported(&self) -> Option<&str> {
        self.unsupported.as_deref()
    }

    /// The class registry of one tenant database (`scope`, the database url).
    pub fn scope(self: &Arc<Self>, scope: &str) -> Arc<ClassScope> {
        let mut scopes = self.scopes.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(existing) = scopes.get(scope) {
            return existing.clone();
        }
        // A scope only the map still holds has no classes left: drop it.
        scopes.retain(|_, existing| Arc::strong_count(existing) > 1);
        let created = Arc::new(ClassScope {
            plan: Arc::downgrade(self),
            interner: RwLock::new(Interner::default()),
            generation: AtomicU64::new(0),
            classes: Mutex::new(FxMap::default()),
            index: RwLock::new(ClassIndex::default()),
        });
        scopes.insert(scope.to_owned(), created.clone());
        created
    }
}

// ---------------------------------------------------------------------------
// Rows.

/// A changed row, as a parsed value or as the raw JSON object bytes the
/// change feed / SQLite produced. Only ACL columns are decoded.
#[derive(Clone, Copy, Debug)]
pub enum RowView<'a> {
    Value(&'a Value),
    Json(&'a [u8]),
}

/// One decoded ACL column.
#[derive(Clone, Debug, PartialEq)]
pub enum AclValue<'a> {
    Missing,
    Null,
    Bool(bool),
    Number(Number),
    Str(Cow<'a, str>),
    /// Arrays and objects (never tokens; compared to literals only).
    Json(Cow<'a, Value>),
}

impl<'a> AclValue<'a> {
    fn from_value(value: Option<&'a Value>) -> Self {
        match value {
            None => AclValue::Missing,
            Some(Value::Null) => AclValue::Null,
            Some(Value::Bool(value)) => AclValue::Bool(*value),
            Some(Value::Number(value)) => AclValue::Number(value.clone()),
            Some(Value::String(value)) => AclValue::Str(Cow::Borrowed(value)),
            Some(value) => AclValue::Json(Cow::Borrowed(value)),
        }
    }

    fn into_owned(self) -> AclValue<'static> {
        match self {
            AclValue::Missing => AclValue::Missing,
            AclValue::Null => AclValue::Null,
            AclValue::Bool(value) => AclValue::Bool(value),
            AclValue::Number(value) => AclValue::Number(value),
            AclValue::Str(value) => AclValue::Str(Cow::Owned(value.into_owned())),
            AclValue::Json(value) => AclValue::Json(Cow::Owned(value.into_owned())),
        }
    }

    /// `visibility_mask::scalar`: the token text of a value.
    fn scalar(&self) -> Cow<'_, str> {
        match self {
            AclValue::Missing | AclValue::Null => Cow::Borrowed("null"),
            AclValue::Str(value) => Cow::Borrowed(value),
            AclValue::Bool(true) => Cow::Borrowed("true"),
            AclValue::Bool(false) => Cow::Borrowed("false"),
            AclValue::Number(value) => {
                Cow::Owned(serde_json::to_string(&Value::Number(value.clone())).unwrap_or_default())
            }
            AclValue::Json(value) => Cow::Owned(serde_json::to_string(value).unwrap_or_default()),
        }
    }

    /// `row.get(column) == Some(literal)`.
    fn equals(&self, literal: &Value) -> bool {
        match (self, literal) {
            (AclValue::Missing, _) => false,
            (AclValue::Null, Value::Null) => true,
            (AclValue::Bool(left), Value::Bool(right)) => left == right,
            (AclValue::Number(left), Value::Number(right)) => left == right,
            (AclValue::Str(left), Value::String(right)) => left.as_ref() == right,
            (AclValue::Json(left), right) => left.as_ref() == right,
            _ => false,
        }
    }

    fn is_absent(&self) -> bool {
        matches!(self, AclValue::Missing | AclValue::Null)
    }
}

/// The ACL columns of one row, with interned token ids per column.
#[derive(Clone, Debug)]
pub struct AclRow<'a> {
    object: bool,
    values: Vec<AclValue<'a>>,
    tokens: Vec<Option<u32>>,
    generation: u64,
}

impl<'a> AclRow<'a> {
    fn absent() -> Self {
        AclRow {
            object: false,
            values: Vec::new(),
            tokens: Vec::new(),
            generation: 0,
        }
    }

    fn from_value(value: &'a Value, columns: &[String]) -> Self {
        let Some(object) = value.as_object() else {
            return Self::absent();
        };
        AclRow {
            object: true,
            values: columns
                .iter()
                .map(|column| AclValue::from_value(object.get(column)))
                .collect(),
            tokens: Vec::new(),
            generation: 0,
        }
    }

    fn from_json(bytes: &'a [u8], columns: &[String]) -> Self {
        let mut values = vec![AclValue::Missing; columns.len()];
        let mut deserializer = serde_json::Deserializer::from_slice(bytes);
        match (AclObject {
            columns,
            values: &mut values,
        })
        .deserialize(&mut deserializer)
        {
            Ok(true) => AclRow {
                object: true,
                values,
                tokens: Vec::new(),
                generation: 0,
            },
            _ => Self::absent(),
        }
    }

    fn into_owned(self) -> AclRow<'static> {
        AclRow {
            object: self.object,
            values: self.values.into_iter().map(AclValue::into_owned).collect(),
            tokens: self.tokens,
            generation: self.generation,
        }
    }

    /// Whether the row is a JSON object at all (null rows are never visible).
    pub fn is_row(&self) -> bool {
        self.object
    }

    fn value(&self, column: Slot) -> &AclValue<'a> {
        self.values
            .get(column as usize)
            .unwrap_or(&AclValue::Missing)
    }

    #[inline]
    fn token(&self, column: Slot) -> Option<u32> {
        self.tokens.get(column as usize).copied().flatten()
    }

    fn resolve_tokens(&mut self, scope: &ClassScope) {
        let interner = scope.interner.read().unwrap_or_else(|e| e.into_inner());
        self.generation = scope.generation.load(Ordering::Acquire);
        self.tokens.clear();
        self.tokens.extend(self.values.iter().map(|value| match value {
            AclValue::Missing | AclValue::Null => None,
            AclValue::Str(text) => interner.get(text),
            other => interner.get(&other.scalar()),
        }));
    }

    fn current(&mut self, scope: &ClassScope) {
        if self.tokens.len() != self.values.len()
            || self.generation != scope.generation.load(Ordering::Acquire)
        {
            self.resolve_tokens(scope);
        }
    }
}

/// Deserializes a JSON object, keeping only the requested columns.
struct AclObject<'c, 'v, 'a> {
    columns: &'c [String],
    values: &'v mut Vec<AclValue<'a>>,
}

impl<'de: 'a, 'a> DeserializeSeed<'de> for AclObject<'_, '_, 'a> {
    type Value = bool;
    fn deserialize<D: Deserializer<'de>>(self, deserializer: D) -> Result<bool, D::Error> {
        deserializer.deserialize_any(self)
    }
}

impl<'de: 'a, 'a> Visitor<'de> for AclObject<'_, '_, 'a> {
    type Value = bool;
    fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
        formatter.write_str("a JSON row")
    }
    fn visit_map<M: MapAccess<'de>>(self, mut map: M) -> Result<bool, M::Error> {
        while let Some(KeyStr(key)) = map.next_key::<KeyStr<'de>>()? {
            match self.columns.iter().position(|column| *column == *key) {
                Some(index) => self.values[index] = map.next_value::<AclValueSeed<'a>>()?.0,
                None => {
                    map.next_value::<IgnoredAny>()?;
                }
            }
        }
        Ok(true)
    }
    fn visit_unit<E>(self) -> Result<bool, E> {
        Ok(false)
    }
    fn visit_bool<E>(self, _: bool) -> Result<bool, E> {
        Ok(false)
    }
    fn visit_i64<E>(self, _: i64) -> Result<bool, E> {
        Ok(false)
    }
    fn visit_u64<E>(self, _: u64) -> Result<bool, E> {
        Ok(false)
    }
    fn visit_f64<E>(self, _: f64) -> Result<bool, E> {
        Ok(false)
    }
    fn visit_str<E>(self, _: &str) -> Result<bool, E> {
        Ok(false)
    }
    fn visit_seq<S: serde::de::SeqAccess<'de>>(self, mut seq: S) -> Result<bool, S::Error> {
        while seq.next_element::<IgnoredAny>()?.is_some() {}
        Ok(false)
    }
}

/// An object key, borrowed from the input unless it contains escapes.
struct KeyStr<'de>(Cow<'de, str>);

impl<'de> Deserialize<'de> for KeyStr<'de> {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct KeyVisitor;
        impl<'de> Visitor<'de> for KeyVisitor {
            type Value = KeyStr<'de>;
            fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
                formatter.write_str("an object key")
            }
            fn visit_borrowed_str<E>(self, value: &'de str) -> Result<Self::Value, E> {
                Ok(KeyStr(Cow::Borrowed(value)))
            }
            fn visit_str<E>(self, value: &str) -> Result<Self::Value, E> {
                Ok(KeyStr(Cow::Owned(value.to_owned())))
            }
            fn visit_string<E>(self, value: String) -> Result<Self::Value, E> {
                Ok(KeyStr(Cow::Owned(value)))
            }
        }
        deserializer.deserialize_str(KeyVisitor)
    }
}

struct AclValueSeed<'a>(AclValue<'a>);

impl<'de: 'a, 'a> serde::Deserialize<'de> for AclValueSeed<'a> {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct ValueVisitor;
        impl<'de> Visitor<'de> for ValueVisitor {
            type Value = AclValue<'de>;
            fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
                formatter.write_str("a JSON value")
            }
            fn visit_unit<E>(self) -> Result<Self::Value, E> {
                Ok(AclValue::Null)
            }
            fn visit_none<E>(self) -> Result<Self::Value, E> {
                Ok(AclValue::Null)
            }
            fn visit_bool<E>(self, value: bool) -> Result<Self::Value, E> {
                Ok(AclValue::Bool(value))
            }
            fn visit_i64<E>(self, value: i64) -> Result<Self::Value, E> {
                Ok(AclValue::Number(value.into()))
            }
            fn visit_u64<E>(self, value: u64) -> Result<Self::Value, E> {
                Ok(AclValue::Number(value.into()))
            }
            fn visit_f64<E>(self, value: f64) -> Result<Self::Value, E> {
                Ok(Number::from_f64(value).map_or(AclValue::Null, AclValue::Number))
            }
            fn visit_borrowed_str<E>(self, value: &'de str) -> Result<Self::Value, E> {
                Ok(AclValue::Str(Cow::Borrowed(value)))
            }
            fn visit_str<E>(self, value: &str) -> Result<Self::Value, E> {
                Ok(AclValue::Str(Cow::Owned(value.to_owned())))
            }
            fn visit_string<E>(self, value: String) -> Result<Self::Value, E> {
                Ok(AclValue::Str(Cow::Owned(value)))
            }
            fn visit_seq<S: serde::de::SeqAccess<'de>>(
                self,
                seq: S,
            ) -> Result<Self::Value, S::Error> {
                let value =
                    Value::deserialize(serde::de::value::SeqAccessDeserializer::new(seq))?;
                Ok(AclValue::Json(Cow::Owned(value)))
            }
            fn visit_map<M: MapAccess<'de>>(self, map: M) -> Result<Self::Value, M::Error> {
                let value = Value::deserialize(serde::de::value::MapAccessDeserializer::new(map))?;
                Ok(AclValue::Json(Cow::Owned(value)))
            }
        }
        deserializer.deserialize_any(ValueVisitor).map(AclValueSeed)
    }
}

// ---------------------------------------------------------------------------
// Tokens.

#[derive(Default)]
struct Interner {
    ids: FxMap<Box<str>, u32>,
}

impl Interner {
    #[inline]
    fn get(&self, token: &str) -> Option<u32> {
        self.ids.get(token).copied()
    }

    fn intern(&mut self, token: &str, grew: &mut bool) -> u32 {
        if let Some(id) = self.ids.get(token) {
            return *id;
        }
        let id = self.ids.len() as u32;
        self.ids.insert(token.into(), id);
        *grew = true;
        id
    }

    fn bytes(&self) -> usize {
        self.ids
            .keys()
            .map(|key| key.len() + std::mem::size_of::<(Box<str>, u32)>() + 8)
            .sum()
    }
}

/// A sorted, deduplicated token set.
pub type TokenSet = Arc<[u32]>;

#[inline]
fn contains(set: &[u32], token: u32) -> bool {
    match set.len() {
        0 => false,
        1..=8 => set.contains(&token),
        _ => set.binary_search(&token).is_ok(),
    }
}

// ---------------------------------------------------------------------------
// Classes and members.

#[derive(Clone, PartialEq, Eq, Hash)]
struct ClassKey {
    /// The residual shape of the class bits (see `Residual`), not the bits.
    shape: Arc<[u32]>,
    sets: Box<[TokenSet]>,
    /// Per inherits spec: (parent index identity, parent class id).
    parents: Box<[Option<(usize, u64)>]>,
}

struct ParentClass {
    index: Arc<RwLock<ParentAclIndex>>,
    /// The parent plan's class for members of this class; `None` when the
    /// spec evaluates its own predicate override.
    class: Option<Arc<VisibilityClass>>,
}

/// Members whose class atoms resolve identically. Immutable once built.
pub struct VisibilityClass {
    id: u64,
    stable_id: u64,
    plan: Arc<CompiledPlan>,
    scope: Arc<ClassScope>,
    bits: Box<[bool]>,
    sets: Box<[TokenSet]>,
    parents: Box<[Option<ParentClass>]>,
    gated: bool,
    key: ClassKey,
}

impl std::fmt::Debug for VisibilityClass {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("VisibilityClass")
            .field("id", &self.id)
            .field("table", &self.plan.table)
            .field("gated", &self.gated)
            .finish()
    }
}

impl Drop for VisibilityClass {
    fn drop(&mut self) {
        let mut classes = self.scope.classes.lock().unwrap_or_else(|e| e.into_inner());
        if classes
            .get(&self.key)
            .is_some_and(|existing| existing.strong_count() == 0)
        {
            classes.remove(&self.key);
        }
        drop(classes);
        self.scope
            .index
            .write()
            .unwrap_or_else(|e| e.into_inner())
            .remove(self);
    }
}

/// One member's binding of one plan: its shared class plus member atoms.
pub struct MemberVisibility {
    class: Arc<VisibilityClass>,
    member_sets: Box<[TokenSet]>,
    contexts: Box<[Option<Box<str>>]>,
    parents: Box<[Option<Arc<MemberVisibility>>]>,
    /// Whether any member atom exists (here or on an inherited parent).
    has_member_atoms: bool,
}

impl std::fmt::Debug for MemberVisibility {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("MemberVisibility")
            .field("class", &self.class)
            .finish()
    }
}

/// A member's compiled visibility, or an explicit fallback.
#[derive(Clone, Debug)]
pub enum MemberHandle {
    Compiled(Arc<MemberVisibility>),
    /// The plan (or an inherited parent plan) is not compiled; use
    /// `visibility::row_matches` and count it as a fallback check.
    Fallback,
}

fn truthy(value: Option<&Value>) -> bool {
    match value {
        Some(Value::Bool(value)) => *value,
        Some(Value::String(value)) => value == "true" || value == "1",
        Some(Value::Number(value)) => value.as_f64().unwrap_or_default() != 0.0,
        _ => false,
    }
}

/// The compiled program of a plan, built on first use and shared by every
/// clone of the plan (module install clones plans per subscription).
pub fn compiled(plan: &VisibilityPlan) -> Arc<CompiledPlan> {
    plan.active_sets_cache
        .0
        .compiled
        .get_or_init(|| Arc::new(compile(plan)))
        .clone()
}

/// Bind one member's principal for `plan` in a tenant database (`scope`).
pub fn member_visibility(
    plan: &VisibilityPlan,
    resolved: &ResolvedVisibility,
    scope: &str,
) -> MemberHandle {
    let compiled = compiled(plan);
    if compiled.unsupported.is_some() {
        return MemberHandle::Fallback;
    }
    let scope_handle = compiled.scope(scope);
    match bind(&compiled, &scope_handle, scope, resolved, 0) {
        Some(member) => MemberHandle::Compiled(member),
        None => MemberHandle::Fallback,
    }
}

fn bind(
    plan: &Arc<CompiledPlan>,
    scope: &Arc<ClassScope>,
    scope_name: &str,
    resolved: &ResolvedVisibility,
    depth: usize,
) -> Option<Arc<MemberVisibility>> {
    if depth > 8 {
        return None;
    }
    // Parents first: a child class is keyed by its parents' classes.
    let mut parent_classes = Vec::with_capacity(plan.inherits.len());
    let mut parent_members = Vec::with_capacity(plan.inherits.len());
    for spec in &plan.inherits {
        let Some(parent) = resolved.inherited.get(&spec.table) else {
            parent_classes.push(None);
            parent_members.push(None);
            continue;
        };
        if spec.override_root.is_some() {
            parent_classes.push(Some(ParentClass {
                index: parent.index.clone(),
                class: None,
            }));
            parent_members.push(None);
            continue;
        }
        let parent_plan = compiled(&parent.plan);
        if parent_plan.unsupported.is_some() {
            return None;
        }
        let parent_scope = parent_plan.scope(scope_name);
        let member = bind(&parent_plan, &parent_scope, scope_name, &parent.resolved, depth + 1)?;
        parent_classes.push(Some(ParentClass {
            index: parent.index.clone(),
            class: Some(member.class.clone()),
        }));
        parent_members.push(Some(member));
    }

    let bits = plan
        .bits
        .iter()
        .map(|atom| match atom {
            BitAtom::Role(role) => resolved.role == *role,
            BitAtom::Permission(permission) => truthy(resolved.permissions.get(permission)),
            BitAtom::NonEmpty(set) => resolved.sets.get(set).is_some_and(|set| !set.is_empty()),
        })
        .collect::<Box<[bool]>>();
    let residual = plan.residual(&bits);
    let relevant = residual.sets.clone();
    let (sets, member_sets) = {
        let mut grew = false;
        let mut interner = scope.interner.write().unwrap_or_else(|e| e.into_inner());
        let mut tokens = |names: &[String]| {
            let mut built: Vec<TokenSet> = Vec::with_capacity(names.len());
            for name in names {
                let mut ids = resolved
                    .sets
                    .get(name)
                    .map(|set| {
                        set.iter()
                            .map(|token| interner.intern(token, &mut grew))
                            .collect::<Vec<_>>()
                    })
                    .unwrap_or_default();
                ids.sort_unstable();
                ids.dedup();
                // Identical sets (Whagons repeats one team→workspace set under
                // several names) share one allocation.
                match built.iter().find(|existing| ***existing == *ids) {
                    Some(existing) => built.push(existing.clone()),
                    None => built.push(ids.into()),
                }
            }
            built.into_boxed_slice()
        };
        let mut sets = tokens(&plan.class_sets);
        let empty: TokenSet = Arc::from(Vec::new());
        for (set, keep) in sets.iter_mut().zip(relevant.iter()) {
            if !keep {
                *set = empty.clone();
            }
        }
        let member_sets = tokens(&plan.member_sets);
        drop(interner);
        if grew {
            scope.generation.fetch_add(1, Ordering::AcqRel);
        }
        (sets, member_sets)
    };
    let key = ClassKey {
        shape: residual.shape,
        sets,
        parents: parent_classes
            .iter()
            .map(|parent| {
                parent.as_ref().map(|parent| {
                    (
                        Arc::as_ptr(&parent.index) as usize,
                        parent.class.as_ref().map_or(0, |class| class.id),
                    )
                })
            })
            .collect(),
    };
    // Stable id: plan content, class bits, the class sets' token text and the
    // parents' stable ids. Equal in every process that sees the same principal.
    let stable_id = {
        use sha2::{Digest, Sha256};
        let mut hasher = Sha256::new();
        hasher.update(plan.fingerprint);
        for word in key.shape.iter() {
            hasher.update(word.to_le_bytes());
        }
        for (name, keep) in plan.class_sets.iter().zip(relevant.iter()) {
            hasher.update([0xfe]);
            if !keep {
                continue;
            }
            for token in resolved.sets.get(name).into_iter().flatten() {
                hasher.update(token.as_bytes());
                hasher.update([0]);
            }
        }
        for parent in &parent_classes {
            hasher.update([0xfd]);
            if let Some(class) = parent.as_ref().and_then(|parent| parent.class.as_ref()) {
                hasher.update(class.stable_id.to_le_bytes());
            }
        }
        u64::from_le_bytes(hasher.finalize()[..8].try_into().unwrap())
    };
    let class = scope.class_for(plan, key, bits, parent_classes, stable_id);
    let contexts = plan
        .contexts
        .iter()
        .map(|context| resolved.direct.get(context).map(|value| value.as_str().into()))
        .collect::<Box<[_]>>();
    let has_member_atoms = !plan.member_atoms.is_empty()
        || plan
            .inherits
            .iter()
            .any(|spec| !spec.override_member_atoms.is_empty())
        || parent_members
            .iter()
            .flatten()
            .any(|parent| parent.has_member_atoms);
    Some(Arc::new(MemberVisibility {
        class,
        member_sets,
        contexts,
        parents: parent_members.into_boxed_slice(),
        has_member_atoms,
    }))
}

/// Kleene truth value for class-level analysis.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Tri {
    False,
    True,
    Unknown,
}

impl CompiledPlan {
    /// Row-independent value of a node given the class bits, if any.
    fn fixed(&self, node: u32, bits: &[bool]) -> Option<bool> {
        match &self.nodes[node as usize] {
            Node::Const(value) => Some(*value),
            Node::Bit(index) => Some(bits[*index as usize]),
            Node::And(start, length) => {
                let mut all = true;
                for child in self.children(*start, *length) {
                    match self.fixed(*child, bits) {
                        Some(false) => return Some(false),
                        Some(true) => {}
                        None => all = false,
                    }
                }
                all.then_some(true)
            }
            Node::Or(start, length) => {
                let mut none = true;
                for child in self.children(*start, *length) {
                    match self.fixed(*child, bits) {
                        Some(true) => return Some(true),
                        Some(false) => {}
                        None => none = false,
                    }
                }
                none.then_some(false)
            }
            Node::Not(child) => self.fixed(*child, bits).map(|value| !value),
            _ => None,
        }
    }

    /// The residual program for these bits: class sets an evaluation can
    /// consult in a subtree whose value is not already fixed, and the shape
    /// of fixed subtrees. Other sets and bits never change a decision, so
    /// they stay out of the class key (an admin with tasks.viewAll shares a
    /// class whatever its teams and flags are).
    fn residual(&self, bits: &[bool]) -> Residual {
        let mut cache = self.relevant.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(found) = cache.get(bits) {
            return found.clone();
        }
        fn mark(
            plan: &CompiledPlan,
            node: u32,
            bits: &[bool],
            sets: &mut [bool],
            shape: &mut Vec<u32>,
        ) {
            if let Some(value) = plan.fixed(node, bits) {
                shape.push(node << 2 | 2 | value as u32);
                return;
            }
            shape.push(node << 2);
            match &plan.nodes[node as usize] {
                Node::InClass { set, .. } => sets[*set as usize] = true,
                Node::And(start, length) | Node::Or(start, length) => {
                    for child in plan.children(*start, *length) {
                        mark(plan, *child, bits, sets, shape);
                    }
                }
                Node::Not(child) => mark(plan, *child, bits, sets, shape),
                Node::Inherits(spec) => {
                    let spec = &plan.inherits[*spec as usize];
                    for root in spec.override_root.into_iter().chain(spec.on_missing) {
                        mark(plan, root, bits, sets, shape);
                    }
                }
                _ => {}
            }
        }
        let mut sets = vec![false; self.class_sets.len()];
        let mut shape = Vec::new();
        mark(self, self.root, bits, &mut sets, &mut shape);
        let residual = Residual {
            shape: shape.into(),
            sets: sets.into(),
        };
        cache.insert(bits.into(), residual.clone());
        residual
    }

    fn children(&self, start: u32, length: u32) -> &[u32] {
        &self.kids[start as usize..(start + length) as usize]
    }

    /// Default evaluation with class tokens false: is the class gated?
    fn tri(&self, node: u32, bits: &[bool]) -> Tri {
        match &self.nodes[node as usize] {
            Node::Const(value) => {
                if *value {
                    Tri::True
                } else {
                    Tri::False
                }
            }
            Node::Bit(index) => {
                if bits[*index as usize] {
                    Tri::True
                } else {
                    Tri::False
                }
            }
            Node::Eq { .. } | Node::Inherits(_) => Tri::Unknown,
            Node::InClass { .. } | Node::InMember { .. } | Node::Context { .. } => Tri::False,
            Node::And(start, length) => {
                let mut result = Tri::True;
                for child in self.children(*start, *length) {
                    match self.tri(*child, bits) {
                        Tri::False => return Tri::False,
                        Tri::Unknown => result = Tri::Unknown,
                        Tri::True => {}
                    }
                }
                result
            }
            Node::Or(start, length) => {
                let mut result = Tri::False;
                for child in self.children(*start, *length) {
                    match self.tri(*child, bits) {
                        Tri::True => return Tri::True,
                        Tri::Unknown => result = Tri::Unknown,
                        Tri::False => {}
                    }
                }
                result
            }
            Node::Not(child) => match self.tri(*child, bits) {
                Tri::True => Tri::False,
                Tri::False => Tri::True,
                Tri::Unknown => Tri::Unknown,
            },
        }
    }

    fn eval(
        &self,
        node: u32,
        row: &AclRow<'_>,
        class: &VisibilityClass,
        member: Option<&MemberVisibility>,
    ) -> bool {
        match &self.nodes[node as usize] {
            Node::Const(value) => *value,
            Node::Bit(index) => class.bits[*index as usize],
            Node::Eq { column, literal } => row.value(*column).equals(literal),
            Node::InClass { column, set } => row
                .token(*column)
                .is_some_and(|token| contains(&class.sets[*set as usize], token)),
            Node::InMember { column, set } => member.is_some_and(|member| {
                row.token(*column)
                    .is_some_and(|token| contains(&member.member_sets[*set as usize], token))
            }),
            Node::Context { column, context } => member.is_some_and(|member| {
                member.contexts[*context as usize]
                    .as_deref()
                    .is_some_and(|expected| row.value(*column).scalar() == expected)
            }),
            Node::Inherits(spec) => self.eval_inherits(*spec, row, class, member),
            Node::And(start, length) => self
                .children(*start, *length)
                .iter()
                .all(|child| self.eval(*child, row, class, member)),
            Node::Or(start, length) => self
                .children(*start, *length)
                .iter()
                .any(|child| self.eval(*child, row, class, member)),
            Node::Not(child) => !self.eval(*child, row, class, member),
        }
    }

    fn eval_inherits(
        &self,
        spec_index: u16,
        row: &AclRow<'_>,
        class: &VisibilityClass,
        member: Option<&MemberVisibility>,
    ) -> bool {
        let spec = &self.inherits[spec_index as usize];
        let Some(parent) = &class.parents[spec_index as usize] else {
            return false;
        };
        let key = row.value(spec.column);
        if key.is_absent() {
            return false;
        }
        let parent_row = parent
            .index
            .read()
            .ok()
            .and_then(|index| index.get(&key.scalar()));
        match parent_row {
            Some(parent_row) => {
                literal_constraints_hold(&spec.parent_where, &parent_row)
                    && parent_row.is_object()
                    && match (spec.override_root, &parent.class) {
                        (Some(root), _) => {
                            let mut decoded = AclRow::from_value(&parent_row, &spec.override_columns);
                            decoded.resolve_tokens(&class.scope);
                            self.eval(root, &decoded, class, member)
                        }
                        (None, Some(parent_class)) => {
                            let parent_member =
                                member.and_then(|member| member.parents[spec_index as usize].as_deref());
                            parent_class.visible_value(&parent_row, parent_member)
                        }
                        (None, None) => false,
                    }
            }
            None => spec
                .on_missing
                .is_some_and(|fallback| self.eval(fallback, row, class, member)),
        }
    }
}

impl VisibilityClass {
    /// Process-local id (unique for the life of the process; memo keys).
    pub fn id(&self) -> u64 {
        self.id
    }

    /// Content-derived id, identical across processes and restarts for the
    /// same plan and shared principal (tenant-scoped by the caller): the class
    /// id of a per-class change frame (tenant, class, per-class revision).
    pub fn stable_id(&self) -> u64 {
        self.stable_id
    }

    pub fn plan(&self) -> &Arc<CompiledPlan> {
        &self.plan
    }

    /// Whether the class can see a row only through one of its tokens.
    pub fn gated(&self) -> bool {
        self.gated
    }

    /// Visibility for a member of this class with no member atom true on the
    /// row (`MemberVisibility::hit` is false). `row` must come from
    /// `ClassScope::decode` of this class's scope.
    pub fn visible_default(&self, row: &AclRow<'_>) -> bool {
        row.object && self.plan.eval(self.plan.root, row, self, None)
    }

    /// Approximate heap bytes of this class (sets shared within the class
    /// counted once).
    pub fn heap_bytes(&self) -> usize {
        let mut seen: Vec<*const [u32]> = Vec::new();
        let mut bytes = std::mem::size_of::<Self>() + self.bits.len();
        bytes += self.sets.len() * std::mem::size_of::<TokenSet>() * 2;
        for set in self.sets.iter() {
            let pointer = Arc::as_ptr(set);
            if !seen.contains(&pointer) {
                seen.push(pointer);
                bytes += 16 + set.len() * 4;
            }
        }
        bytes + self.parents.len() * std::mem::size_of::<Option<ParentClass>>()
    }

    fn visible_value(&self, row: &Value, member: Option<&MemberVisibility>) -> bool {
        let mut decoded = AclRow::from_value(row, &self.plan.columns);
        if !decoded.object {
            return false;
        }
        decoded.resolve_tokens(&self.scope);
        self.plan.eval(self.plan.root, &decoded, self, member)
    }
}

impl MemberVisibility {
    pub fn class(&self) -> &Arc<VisibilityClass> {
        &self.class
    }

    pub fn scope(&self) -> &Arc<ClassScope> {
        &self.class.scope
    }

    /// Approximate heap bytes owned by this member binding (not its class).
    pub fn heap_bytes(&self) -> usize {
        let mut bytes = std::mem::size_of::<Self>() + 16;
        for set in self.member_sets.iter() {
            bytes += std::mem::size_of::<TokenSet>() + 16 + set.len() * 4;
        }
        for context in self.contexts.iter().flatten() {
            bytes += context.len() + 16;
        }
        bytes
            + self
                .parents
                .iter()
                .flatten()
                .map(|parent| parent.heap_bytes())
                .sum::<usize>()
    }

    /// Is some member atom true on this row (directly or on a parent row)?
    /// When false, the member sees exactly what its class's default sees.
    pub fn hit(&self, row: &AclRow<'_>) -> bool {
        if !self.has_member_atoms || !row.object {
            return false;
        }
        let plan = &self.class.plan;
        let atom_true = |atom: &MemberAtom, row: &AclRow<'_>| match *atom {
            MemberAtom::InSet { column, set } => row
                .token(column)
                .is_some_and(|token| contains(&self.member_sets[set as usize], token)),
            MemberAtom::Context { column, context } => self.contexts[context as usize]
                .as_deref()
                .is_some_and(|expected| row.value(column).scalar() == expected),
        };
        if plan.member_atoms.iter().any(|atom| atom_true(atom, row)) {
            return true;
        }
        for (index, spec) in plan.inherits.iter().enumerate() {
            let parent_member = self.parents[index].as_deref();
            if spec.override_member_atoms.is_empty()
                && !parent_member.is_some_and(|parent| parent.has_member_atoms)
            {
                continue;
            }
            let Some(parent) = &self.class.parents[index] else {
                continue;
            };
            let key = row.value(spec.column);
            if key.is_absent() {
                continue;
            }
            let Some(parent_row) = parent
                .index
                .read()
                .ok()
                .and_then(|index| index.get(&key.scalar()))
            else {
                continue;
            };
            if !spec.override_member_atoms.is_empty() {
                let mut decoded = AclRow::from_value(&parent_row, &spec.override_columns);
                decoded.resolve_tokens(&self.class.scope);
                if spec
                    .override_member_atoms
                    .iter()
                    .any(|atom| atom_true(atom, &decoded))
                {
                    return true;
                }
            }
            if let Some(parent_member) = parent_member {
                let mut decoded = AclRow::from_value(&parent_row, &parent_member.class.plan.columns);
                decoded.resolve_tokens(&parent_member.class.scope);
                if parent_member.hit(&decoded) {
                    return true;
                }
            }
        }
        false
    }

    /// Full evaluation for this member. `row` comes from `scope().decode`.
    pub fn visible_row(&self, row: &AclRow<'_>) -> bool {
        if !row.object {
            return false;
        }
        if self.hit(row) {
            self.class.plan.eval(self.class.plan.root, row, &self.class, Some(self))
        } else {
            self.class.visible_default(row)
        }
    }

    /// Decode and evaluate one row.
    pub fn visible(&self, row: RowView<'_>) -> bool {
        self.visible_row(&self.class.scope.decode(row))
    }

    /// Evaluate a change row that stays alive (and unmodified) for the whole
    /// memo: its decode and its class default are computed once per class.
    pub fn visible_memo(&self, memo: &mut EvalMemo, row: &Value) -> bool {
        memo.stats.compiled_checks += 1;
        let scope = &self.class.scope;
        let row_key = row as *const Value as usize;
        let decoded = memo
            .rows
            .entry((Arc::as_ptr(scope) as usize, row_key))
            .or_insert_with(|| AclRow::from_value(row, &self.class.plan.columns).into_owned());
        decoded.current(scope);
        if !decoded.object {
            return false;
        }
        if self.hit(decoded) {
            memo.stats.member_evaluations += 1;
            return self
                .class
                .plan
                .eval(self.class.plan.root, decoded, &self.class, Some(self));
        }
        let class = &self.class;
        match memo.defaults.get(&(class.id, row_key)) {
            Some(visible) => *visible,
            None => {
                memo.stats.class_evaluations += 1;
                let visible = class.visible_default(decoded);
                memo.defaults.insert((class.id, row_key), visible);
                visible
            }
        }
    }
}

/// Per-feed-event memo of decoded change rows and class defaults. Keys are
/// row addresses: only pass rows that outlive the memo unchanged.
#[derive(Default)]
pub struct EvalMemo {
    rows: FxMap<(usize, usize), AclRow<'static>>,
    defaults: FxMap<(u64, usize), bool>,
    pub stats: EvalStats,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct EvalStats {
    /// Row checks answered by a compiled program.
    pub compiled_checks: u64,
    /// Class default evaluations (one per class per row).
    pub class_evaluations: u64,
    /// Members evaluated individually because a member atom hit the row.
    pub member_evaluations: u64,
    /// Row checks answered by the interpreted fallback evaluator.
    pub fallback_checks: u64,
}

// ---------------------------------------------------------------------------
// Scope: interner, class registry, candidate index.

/// Classes of one plan in one tenant database.
pub struct ClassScope {
    plan: Weak<CompiledPlan>,
    interner: RwLock<Interner>,
    generation: AtomicU64,
    classes: Mutex<FxMap<ClassKey, Weak<VisibilityClass>>>,
    index: RwLock<ClassIndex>,
}

static NEXT_CLASS: AtomicU64 = AtomicU64::new(1);

#[derive(Default)]
struct ClassIndex {
    /// (class set slot, token) → classes holding the token in that set.
    by_token: FxMap<(u16, u32), Vec<(u64, Weak<VisibilityClass>)>>,
    ungated: Vec<(u64, Weak<VisibilityClass>)>,
    /// (member set slot, token) → registered members holding it.
    members_by_token: FxMap<(u16, u32), Vec<Weak<MemberVisibility>>>,
    /// context value → registered members with that context value.
    members_by_context: FxMap<Box<str>, Vec<Weak<MemberVisibility>>>,
    /// Registered members of a plan whose hits can come from parent rows.
    members_always: Vec<Weak<MemberVisibility>>,
}

impl ClassIndex {
    fn remove(&mut self, class: &VisibilityClass) {
        let id = class.id;
        if class.gated {
            let class_sets = class
                .plan
                .class_atoms
                .iter()
                .map(|(_, set)| *set)
                .collect::<Vec<_>>();
            for set in class_sets {
                for token in class.sets[set as usize].iter() {
                    if let Some(entries) = self.by_token.get_mut(&(set, *token)) {
                        entries.retain(|(entry, _)| *entry != id);
                        if entries.is_empty() {
                            self.by_token.remove(&(set, *token));
                        }
                    }
                }
            }
        } else {
            self.ungated.retain(|(entry, _)| *entry != id);
        }
    }
}

/// Classes and members that may see one changed row (old or new value).
#[derive(Debug, Default)]
pub struct Candidates {
    /// Evaluate `visible_default` once per class; it applies to every
    /// member of the class that is not in `members`.
    pub classes: Vec<Arc<VisibilityClass>>,
    /// Registered members hit by a member atom: evaluate individually.
    pub members: Vec<Arc<MemberVisibility>>,
}

impl ClassScope {
    /// Decode a row's ACL columns and resolve its tokens.
    pub fn decode<'a>(&self, row: RowView<'a>) -> AclRow<'a> {
        let Some(plan) = self.plan.upgrade() else {
            return AclRow::absent();
        };
        let mut decoded = match row {
            RowView::Value(value) => AclRow::from_value(value, &plan.columns),
            RowView::Json(bytes) => AclRow::from_json(bytes, &plan.columns),
        };
        decoded.resolve_tokens(self);
        decoded
    }

    /// Live classes and interned tokens, for metrics and benchmarks.
    pub fn stats(&self) -> ScopeStats {
        let classes = self
            .classes
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .values()
            .filter_map(Weak::upgrade)
            .collect::<Vec<_>>();
        let index = self.index.read().unwrap_or_else(|e| e.into_inner());
        ScopeStats {
            classes: classes.len(),
            gated_classes: classes.iter().filter(|class| class.gated).count(),
            class_bytes: classes.iter().map(|class| class.heap_bytes()).sum(),
            tokens: self.interner.read().unwrap_or_else(|e| e.into_inner()).ids.len(),
            interner_bytes: self.interner.read().unwrap_or_else(|e| e.into_inner()).bytes(),
            index_entries: index.by_token.values().map(Vec::len).sum::<usize>()
                + index.ungated.len(),
        }
    }

    fn class_for(
        self: &Arc<Self>,
        plan: &Arc<CompiledPlan>,
        key: ClassKey,
        bits: Box<[bool]>,
        parents: Vec<Option<ParentClass>>,
        stable_id: u64,
    ) -> Arc<VisibilityClass> {
        let mut classes = self.classes.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(existing) = classes.get(&key).and_then(Weak::upgrade) {
            return existing;
        }
        // Any member's bits with this residual decide identically.
        let gated = plan.tri(plan.root, &bits) == Tri::False;
        let class = Arc::new(VisibilityClass {
            // Process-unique: memo keys mix classes of different scopes.
            id: NEXT_CLASS.fetch_add(1, Ordering::Relaxed),
            stable_id,
            plan: plan.clone(),
            scope: self.clone(),
            bits,
            sets: key.sets.clone(),
            parents: parents.into_boxed_slice(),
            gated,
            key: key.clone(),
        });
        classes.insert(key, Arc::downgrade(&class));
        drop(classes);
        let mut index = self.index.write().unwrap_or_else(|e| e.into_inner());
        if gated {
            let mut done: Vec<u16> = Vec::new();
            for (_, set) in &plan.class_atoms {
                if done.contains(set) {
                    continue;
                }
                done.push(*set);
                for token in class.sets[*set as usize].iter() {
                    index
                        .by_token
                        .entry((*set, *token))
                        .or_default()
                        .push((class.id, Arc::downgrade(&class)));
                }
            }
        } else {
            index.ungated.push((class.id, Arc::downgrade(&class)));
        }
        class
    }

    /// Index a member's member atoms for `candidates`. Dropping the member
    /// handle unregisters it lazily (dead entries are skipped and swept).
    pub fn register(&self, member: &Arc<MemberVisibility>) {
        let Some(plan) = self.plan.upgrade() else {
            return;
        };
        let mut index = self.index.write().unwrap_or_else(|e| e.into_inner());
        let weak = Arc::downgrade(member);
        if !plan.inherits.is_empty() && member.has_member_atoms {
            index.members_always.push(weak.clone());
        }
        let mut sets_done: Vec<u16> = Vec::new();
        let mut contexts_done: Vec<u16> = Vec::new();
        for atom in &plan.member_atoms {
            match *atom {
                MemberAtom::InSet { set, .. } => {
                    if sets_done.contains(&set) {
                        continue;
                    }
                    sets_done.push(set);
                    for token in member.member_sets[set as usize].iter() {
                        index
                            .members_by_token
                            .entry((set, *token))
                            .or_default()
                            .push(weak.clone());
                    }
                }
                MemberAtom::Context { context, .. } => {
                    if contexts_done.contains(&context) {
                        continue;
                    }
                    contexts_done.push(context);
                    if let Some(value) = &member.contexts[context as usize] {
                        index
                            .members_by_context
                            .entry(value.clone())
                            .or_default()
                            .push(weak.clone());
                    }
                }
            }
        }
    }

    /// Drop index entries of members that no longer exist.
    pub fn sweep(&self) {
        let mut index = self.index.write().unwrap_or_else(|e| e.into_inner());
        index.members_by_token.retain(|_, members| {
            members.retain(|member| member.strong_count() > 0);
            !members.is_empty()
        });
        index.members_by_context.retain(|_, members| {
            members.retain(|member| member.strong_count() > 0);
            !members.is_empty()
        });
        index
            .members_always
            .retain(|member| member.strong_count() > 0);
    }

    /// Every class whose default may admit `old` or `new`, and every
    /// registered member a member atom hits on either. A member in neither
    /// (its class absent from `classes`, itself absent from `members`) sees
    /// neither row. Rows come from `decode` of this scope.
    pub fn candidates(&self, old: Option<&AclRow<'_>>, new: Option<&AclRow<'_>>) -> Candidates {
        let Some(plan) = self.plan.upgrade() else {
            return Candidates::default();
        };
        let index = self.index.read().unwrap_or_else(|e| e.into_inner());
        let mut class_ids: Vec<u64> = Vec::new();
        let mut output = Candidates::default();
        let mut add_class = |id: u64, weak: &Weak<VisibilityClass>, output: &mut Candidates| {
            if !class_ids.contains(&id) {
                if let Some(class) = weak.upgrade() {
                    class_ids.push(id);
                    output.classes.push(class);
                }
            }
        };
        for (id, weak) in &index.ungated {
            add_class(*id, weak, &mut output);
        }
        let mut member_ptrs: Vec<usize> = Vec::new();
        // Check identity before upgrading: dropping a just-upgraded last
        // reference here would run its Drop under this index lock.
        let mut add_member = |weak: &Weak<MemberVisibility>, output: &mut Candidates| {
            let pointer = Weak::as_ptr(weak) as usize;
            if !member_ptrs.contains(&pointer) {
                if let Some(member) = weak.upgrade() {
                    member_ptrs.push(pointer);
                    output.members.push(member);
                }
            }
        };
        for row in [old, new].into_iter().flatten() {
            if !row.object {
                continue;
            }
            for (column, set) in &plan.class_atoms {
                if let Some(token) = row.token(*column) {
                    if let Some(entries) = index.by_token.get(&(*set, token)) {
                        for (id, weak) in entries {
                            add_class(*id, weak, &mut output);
                        }
                    }
                }
            }
            for atom in &plan.member_atoms {
                match *atom {
                    MemberAtom::InSet { column, set } => {
                        if let Some(token) = row.token(column) {
                            for weak in index
                                .members_by_token
                                .get(&(set, token))
                                .into_iter()
                                .flatten()
                            {
                                add_member(weak, &mut output);
                            }
                        }
                    }
                    MemberAtom::Context { column, .. } => {
                        for weak in index
                            .members_by_context
                            .get(row.value(column).scalar().as_ref())
                            .into_iter()
                            .flatten()
                        {
                            add_member(weak, &mut output);
                        }
                    }
                }
            }
        }
        for weak in &index.members_always {
            add_member(weak, &mut output);
        }
        output
    }
}

#[derive(Clone, Copy, Debug, Default)]
pub struct ScopeStats {
    pub classes: usize,
    pub gated_classes: usize,
    pub class_bytes: usize,
    pub tokens: usize,
    pub interner_bytes: usize,
    pub index_entries: usize,
}

/// A subscription's cached member binding. Clear it whenever the
/// subscription's `ResolvedVisibility` (or a parent's) changes.
#[derive(Clone, Debug, Default)]
pub struct HandleCache(Option<MemberHandle>);

impl HandleCache {
    pub fn get(
        &mut self,
        plan: &VisibilityPlan,
        resolved: &ResolvedVisibility,
        scope: &str,
    ) -> MemberHandle {
        self.0
            .get_or_insert_with(|| member_visibility(plan, resolved, scope))
            .clone()
    }

    pub fn clear(&mut self) {
        self.0 = None;
    }
}
