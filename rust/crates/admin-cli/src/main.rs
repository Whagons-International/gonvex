use std::collections::BTreeMap;
use std::path::PathBuf;

use gonvex_admin::identity::{self, MigrationPlan, Result, VerificationResult, MIGRATION_SCOPE};
use serde::Serialize;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Mode {
    Plan,
    Apply,
    Verify,
}

#[derive(Debug)]
struct Options {
    mode: Mode,
    control_plane_url: String,
    source: String,
    run_id: String,
    input: String,
    plan_file: PathBuf,
    allow_unresolved_collisions: bool,
    json: bool,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct Output<'a> {
    operation: &'a str,
    run_id: &'a str,
    source: &'a str,
    plan_file: String,
    scope: &'static str,
    summary: Summary<'a>,
    #[serde(skip_serializing_if = "Option::is_none")]
    verification: Option<&'a VerificationResult>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct Summary<'a> {
    legacy_rows: usize,
    unique_accounts: usize,
    provider_matches: usize,
    verified_email_matches: usize,
    new_accounts: usize,
    ambiguous_collisions: usize,
    plan_checksum: &'a str,
}

#[tokio::main]
async fn main() {
    if let Err(error) = run(std::env::args().skip(1).collect()).await {
        eprintln!("{error}");
        std::process::exit(1);
    }
}

async fn run(args: Vec<String>) -> Result<()> {
    if args.is_empty() || matches!(args[0].as_str(), "help" | "--help" | "-h") {
        print_help();
        return Ok(());
    }
    if args.first().map(String::as_str) != Some("migrate")
        || args.get(1).map(String::as_str) != Some("identity-v2")
    {
        print_help();
        return Err("expected `gonvex-admin migrate identity-v2`".into());
    }
    let options = parse_options(&args[2..])?;
    let control = identity::connect(&options.control_plane_url).await?;
    match options.mode {
        Mode::Plan => {
            if options.source.is_empty() || options.run_id.is_empty() || options.input.is_empty() {
                return Err("--plan requires --source, --run-id, and --input".into());
            }
            let records = identity::load_inventory(&options.input, &options.source)?;
            let existing = identity::load_existing_accounts(&control).await?;
            let plan = identity::plan_identity_migration(
                &options.run_id,
                &options.source,
                &records,
                &existing,
            )?;
            identity::inspect_runtime_migration(&control, &plan).await?;
            identity::write_plan(&options.plan_file, &plan)?;
            print_result(&options, "plan", &plan, None)?;
        }
        Mode::Apply => {
            let plan = identity::read_plan(&options.plan_file)?;
            match_plan_flags(&options, &plan)?;
            identity::inspect_runtime_migration(&control, &plan).await?;
            identity::install_identity_schema(&control).await?;
            identity::apply_identity_migration(
                &control,
                &plan,
                options.allow_unresolved_collisions,
            )
            .await?;
            identity::apply_runtime_migration(&control, &plan).await?;
            print_result(&options, "apply", &plan, None)?;
        }
        Mode::Verify => {
            let plan = identity::read_plan(&options.plan_file)?;
            match_plan_flags(&options, &plan)?;
            let verification = identity::verify_identity_migration(&control, &plan).await?;
            identity::verify_runtime_migration(&control, &plan).await?;
            print_result(&options, "verify", &plan, Some(&verification))?;
            if !verification.findings.is_empty() {
                return Err(format!(
                    "identity-v2 verification found {} issue(s)",
                    verification.findings.len()
                )
                .into());
            }
        }
    }
    Ok(())
}

fn parse_options(args: &[String]) -> Result<Options> {
    let mut values = BTreeMap::new();
    let mut plan = false;
    let mut apply = false;
    let mut verify = false;
    let mut allow_unresolved_collisions = false;
    let mut json = false;
    let mut index = 0;
    while index < args.len() {
        match args[index].as_str() {
            "--plan" => plan = true,
            "--apply" => apply = true,
            "--verify" => verify = true,
            "--allow-unresolved-collisions" => allow_unresolved_collisions = true,
            "--json" => json = true,
            flag
            @ ("--control-plane-url" | "--source" | "--run-id" | "--input" | "--plan-file") => {
                index += 1;
                let value = args
                    .get(index)
                    .ok_or_else(|| format!("{flag} requires a value"))?;
                if values.insert(flag, value.trim().to_owned()).is_some() {
                    return Err(format!("{flag} may be specified only once").into());
                }
            }
            flag if flag.starts_with('-') => return Err(format!("unknown option {flag}").into()),
            value => return Err(format!("unexpected argument {value:?}").into()),
        }
        index += 1;
    }
    let modes = usize::from(plan) + usize::from(apply) + usize::from(verify);
    if modes != 1 {
        return Err("exactly one of --plan, --apply, or --verify is required".into());
    }
    if allow_unresolved_collisions && !apply {
        return Err("--allow-unresolved-collisions is valid only with --apply".into());
    }
    let control_plane_url = values
        .remove("--control-plane-url")
        .or_else(|| std::env::var("GONVEX_CONTROL_PLANE_DATABASE_URL").ok())
        .unwrap_or_default();
    if control_plane_url.trim().is_empty() {
        return Err(
            "--control-plane-url is required, or set GONVEX_CONTROL_PLANE_DATABASE_URL".into(),
        );
    }
    Ok(Options {
        mode: if plan {
            Mode::Plan
        } else if apply {
            Mode::Apply
        } else {
            Mode::Verify
        },
        control_plane_url,
        source: values.remove("--source").unwrap_or_default(),
        run_id: values.remove("--run-id").unwrap_or_default(),
        input: values.remove("--input").unwrap_or_default(),
        plan_file: PathBuf::from(
            values
                .remove("--plan-file")
                .unwrap_or_else(|| "identity-v2-plan.json".to_owned()),
        ),
        allow_unresolved_collisions,
        json,
    })
}

fn match_plan_flags(options: &Options, plan: &MigrationPlan) -> Result<()> {
    if !options.run_id.is_empty() && options.run_id != plan.run_id {
        return Err(format!(
            "--run-id {:?} does not match plan runId {:?}",
            options.run_id, plan.run_id
        )
        .into());
    }
    if !options.source.is_empty() && options.source != plan.source {
        return Err(format!(
            "--source {:?} does not match plan source {:?}",
            options.source, plan.source
        )
        .into());
    }
    Ok(())
}

fn print_result(
    options: &Options,
    operation: &str,
    plan: &MigrationPlan,
    verification: Option<&VerificationResult>,
) -> Result<()> {
    let output = Output {
        operation,
        run_id: &plan.run_id,
        source: &plan.source,
        plan_file: options.plan_file.display().to_string(),
        scope: MIGRATION_SCOPE,
        summary: Summary {
            legacy_rows: plan.legacy_rows,
            unique_accounts: plan.unique_accounts,
            provider_matches: plan.provider_matches,
            verified_email_matches: plan.email_matches,
            new_accounts: plan.new_accounts,
            ambiguous_collisions: plan.ambiguous_collisions,
            plan_checksum: &plan.checksum,
        },
        verification,
    };
    if options.json {
        println!("{}", serde_json::to_string_pretty(&output)?);
        return Ok(());
    }
    println!(
        "identity-v2 {operation}: run={} source={}",
        plan.run_id, plan.source
    );
    println!(
        "legacy rows={} unique accounts={} provider matches={} verified-email matches={} new accounts={} collisions={}",
        plan.legacy_rows,
        plan.unique_accounts,
        plan.provider_matches,
        plan.email_matches,
        plan.new_accounts,
        plan.ambiguous_collisions
    );
    println!(
        "plan={} checksum={}",
        options.plan_file.display(),
        plan.checksum
    );
    println!("scope: {MIGRATION_SCOPE}");
    if let Some(verification) = verification {
        println!("verification findings={}", verification.findings.len());
        for finding in &verification.findings {
            println!("- {} [{}]: {}", finding.code, finding.scope, finding.detail);
        }
    }
    Ok(())
}

fn print_help() {
    println!("Gonvex administration CLI");
    println!("  gonvex-admin migrate identity-v2 (--plan | --apply | --verify) [options]");
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parser_requires_one_mode() {
        let error = parse_options(&[
            "--control-plane-url".to_owned(),
            "postgres://example".to_owned(),
        ])
        .unwrap_err();
        assert!(error.to_string().contains("exactly one"));
    }

    #[test]
    fn parser_rejects_unknown_options() {
        let error = parse_options(&["--plan".to_owned(), "--database".to_owned()]).unwrap_err();
        assert!(error.to_string().contains("unknown option"));
    }
}
