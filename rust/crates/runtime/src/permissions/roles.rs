//! Explicit scoped-role lowering to group membership and provenance grants.
//! No role name implies an app permission, admin bypass or global authority.
use super::model::{Atom, InvalidPrimitive};
use gonvex_store::permission_indexes::{GrantFact, MembershipFact, SourceChange, Subject};

pub(super) enum RoleScope {
    Tenant,
    Group { namespace: String, id: String },
    Row { table: String, key: String },
}
pub(super) struct ScopedRole {
    pub id: String,
    pub scope: RoleScope,
}

impl ScopedRole {
    fn group(&self) -> Result<String, InvalidPrimitive> {
        fn field(bytes: &mut Vec<u8>, value: &str) -> Result<(), InvalidPrimitive> {
            if value.is_empty() {
                return Err(InvalidPrimitive::EmptyIdentity);
            }
            bytes.extend_from_slice(&(value.len() as u64).to_be_bytes());
            bytes.extend_from_slice(value.as_bytes());
            Ok(())
        }
        let mut bytes = b"gonvex-scoped-role-v1".to_vec();
        field(&mut bytes, &self.id)?;
        match &self.scope {
            RoleScope::Tenant => bytes.push(0),
            RoleScope::Group { namespace, id } => {
                bytes.push(1);
                field(&mut bytes, namespace)?;
                field(&mut bytes, id)?;
            }
            RoleScope::Row { table, key } => {
                bytes.push(2);
                field(&mut bytes, table)?;
                field(&mut bytes, key)?;
            }
        }
        // Exact encoding, not a digest. The trusted normalized binder reserves
        // this namespace; unnormalized application group labels are not inputs.
        // Different typed role scopes cannot collapse to the same role group.
        Ok(format!(
            "scoped-role:{}",
            bytes.iter().map(|b| format!("{b:02x}")).collect::<String>()
        ))
    }

    pub fn definition(
        &self,
        source: String,
        atoms: &[Atom],
    ) -> Result<SourceChange, InvalidPrimitive> {
        if source.is_empty() {
            return Err(InvalidPrimitive::EmptyIdentity);
        }
        let group = self.group()?;
        let facts = atoms
            .iter()
            .map(|atom| {
                Ok(GrantFact {
                    target: Subject::Group(group.clone()),
                    atom: atom.canonical()?,
                })
            })
            .collect::<Result<Vec<_>, InvalidPrimitive>>()?;
        Ok(SourceChange::Grants { source, facts })
    }

    pub fn assignments(
        &self,
        source: String,
        subjects: Vec<Subject>,
    ) -> Result<SourceChange, InvalidPrimitive> {
        if source.is_empty() {
            return Err(InvalidPrimitive::EmptyIdentity);
        }
        let group = self.group()?;
        Ok(SourceChange::Memberships {
            source,
            facts: subjects
                .into_iter()
                .map(|member| MembershipFact {
                    group: group.clone(),
                    member,
                })
                .collect(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn role_identity_does_not_flatten_tenant_team_or_row_scope() {
        let tenant = ScopedRole {
            id: "admin".into(),
            scope: RoleScope::Tenant,
        };
        let team = ScopedRole {
            id: "admin".into(),
            scope: RoleScope::Group {
                namespace: "team".into(),
                id: "t1".into(),
            },
        };
        let row = ScopedRole {
            id: "admin".into(),
            scope: RoleScope::Row {
                table: "tasks".into(),
                key: "t1".into(),
            },
        };
        assert_ne!(tenant.group().unwrap(), team.group().unwrap());
        assert_ne!(team.group().unwrap(), row.group().unwrap());
    }
}
