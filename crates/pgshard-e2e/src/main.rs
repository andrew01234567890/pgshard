//! Dispatches end-to-end scenarios against explicitly selected clusters.

use std::collections::BTreeSet;
use std::env;
use std::ffi::OsString;
use std::path::{Path, PathBuf};
use std::process::{Command, ExitCode};

const OPERATOR_API_SAFETY_SCENARIO: &str = "operator-api-safety";
const OPERATOR_API_SAFETY_TESTS: [&str; 4] = [
    "TestKINDCRDRejectsUnsafeSpecTransitionsWithoutWebhooks",
    "TestKINDDeletionWaitsForOwnedResourcesBeforeSameNameRecreate",
    "TestKINDGarbageCollectorDeletesLatePostgreSQLCreationFence",
    "TestKINDServerSideApplyPrunesAndIsolatesScaleOwnership",
];

#[derive(Debug, Eq, PartialEq)]
enum Request {
    Help,
    Run { scenario: String },
}

fn parse_args(args: impl IntoIterator<Item = OsString>) -> Result<Request, String> {
    let mut args = args.into_iter();
    let mut scenario = None;
    while let Some(argument) = args.next() {
        match argument.to_str() {
            Some("-h" | "--help") => return Ok(Request::Help),
            Some("--scenario") => {
                if scenario.is_some() {
                    return Err("--scenario may be provided only once".to_owned());
                }
                let value = args
                    .next()
                    .ok_or_else(|| "--scenario requires a value".to_owned())?;
                let value = value
                    .into_string()
                    .map_err(|_| "--scenario must be valid UTF-8".to_owned())?;
                scenario = Some(value);
            }
            Some(value) => return Err(format!("unsupported argument {value:?}")),
            None => return Err("arguments must be valid UTF-8".to_owned()),
        }
    }
    Ok(Request::Run {
        scenario: scenario.ok_or_else(|| "--scenario is required".to_owned())?,
    })
}

fn repository_root() -> Result<PathBuf, String> {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(Path::parent)
        .map(Path::to_path_buf)
        .ok_or_else(|| "end-to-end package is not inside the repository test root".to_owned())
}

fn required_environment(name: &str) -> Result<OsString, String> {
    env::var_os(name)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| format!("{name} is required; refusing to select an implicit cluster"))
}

fn command_output(command: &mut Command, description: &str) -> Result<String, String> {
    let output = command
        .output()
        .map_err(|error| format!("{description}: {error}"))?;
    if !output.status.success() {
        return Err(format!(
            "{description} failed with {}: {}",
            output.status,
            String::from_utf8_lossy(&output.stderr).trim()
        ));
    }
    String::from_utf8(output.stdout)
        .map(|value| value.trim().to_owned())
        .map_err(|error| format!("{description} returned non-UTF-8 output: {error}"))
}

fn go_test_selector() -> String {
    format!("^({})$", OPERATOR_API_SAFETY_TESTS.join("|"))
}

fn listed_go_tests(output: &str) -> BTreeSet<&str> {
    output
        .lines()
        .filter(|line| line.starts_with("Test"))
        .collect()
}

fn run_scenario(scenario: &str) -> Result<(), String> {
    if scenario != OPERATOR_API_SAFETY_SCENARIO {
        return Err(format!(
            "unsupported scenario {scenario:?}; executable scenarios: {OPERATOR_API_SAFETY_SCENARIO}"
        ));
    }

    let expected_context = required_environment("PGSHARD_E2E_KUBE_CONTEXT")?
        .into_string()
        .map_err(|_| "PGSHARD_E2E_KUBE_CONTEXT must be valid UTF-8".to_owned())?;
    let actual_context = command_output(
        Command::new("kubectl").args(["config", "current-context"]),
        "read current Kubernetes context",
    )?;
    if actual_context != expected_context {
        return Err(format!(
            "current Kubernetes context is {actual_context:?}, expected {expected_context:?}"
        ));
    }
    command_output(
        Command::new("kubectl").args([
            "--context",
            &expected_context,
            "get",
            "customresourcedefinition",
            "pgshardclusters.pgshard.io",
            "--output=name",
        ]),
        "verify the pgshard CRD fixture",
    )?;

    let operator_root = repository_root()?.join("operator");
    let selector = go_test_selector();
    let listed = command_output(
        Command::new("go")
            .current_dir(&operator_root)
            .env("PGSHARD_KIND_E2E", "true")
            .args(["test", "-list", &selector, "./internal/controller"]),
        "list operator API safety tests",
    )?;
    let listed = listed_go_tests(&listed);
    let expected = OPERATOR_API_SAFETY_TESTS
        .into_iter()
        .collect::<BTreeSet<_>>();
    if listed != expected {
        return Err(format!(
            "operator API safety selector matched {listed:?}, expected exactly {expected:?}"
        ));
    }

    let status = Command::new("go")
        .current_dir(operator_root)
        .env("PGSHARD_KIND_E2E", "true")
        .args([
            "test",
            "-race",
            "-count=1",
            "-timeout=10m",
            "-run",
            &selector,
            "./internal/controller",
        ])
        .status()
        .map_err(|error| format!("start operator API safety scenario: {error}"))?;
    if !status.success() {
        return Err(format!("operator API safety scenario failed with {status}"));
    }
    Ok(())
}

fn print_help() {
    println!(
        "Usage: pgshard-e2e --scenario {OPERATOR_API_SAFETY_SCENARIO}\n\
         \n\
         Requires PGSHARD_E2E_KUBE_CONTEXT naming the current disposable KIND\n\
         context with the pgshard CRD already installed."
    );
}

fn main() -> ExitCode {
    let request = parse_args(env::args_os().skip(1));
    let result = match request {
        Ok(Request::Help) => {
            print_help();
            return ExitCode::SUCCESS;
        }
        Ok(Request::Run { scenario }) => run_scenario(&scenario),
        Err(error) => Err(error),
    };
    match result {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("pgshard end-to-end runner: {error}");
            ExitCode::FAILURE
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{
        OPERATOR_API_SAFETY_TESTS, Request, go_test_selector, listed_go_tests, parse_args,
        run_scenario,
    };
    use std::ffi::OsString;

    fn args(values: &[&str]) -> Vec<OsString> {
        values.iter().map(OsString::from).collect()
    }

    #[test]
    fn requires_an_explicit_scenario() {
        assert_eq!(
            parse_args(args(&[])),
            Err("--scenario is required".to_owned())
        );
    }

    #[test]
    fn accepts_the_registered_scenario_shape() {
        assert_eq!(
            parse_args(args(&["--scenario", "operator-api-safety"])),
            Ok(Request::Run {
                scenario: "operator-api-safety".to_owned()
            })
        );
    }

    #[test]
    fn rejects_unknown_arguments() {
        assert_eq!(
            parse_args(args(&["operator-api-safety"])),
            Err("unsupported argument \"operator-api-safety\"".to_owned())
        );
    }

    #[test]
    fn rejects_unknown_scenarios_before_touching_any_cluster() {
        assert_eq!(
            run_scenario("nonexistent"),
            Err(
                "unsupported scenario \"nonexistent\"; executable scenarios: operator-api-safety"
                    .to_owned()
            )
        );
    }

    #[test]
    fn selector_and_listing_cover_every_registered_test_exactly() {
        let output = format!("{}\nok\tpackage\n", OPERATOR_API_SAFETY_TESTS.join("\n"));
        let listed = listed_go_tests(&output);
        assert_eq!(listed.len(), OPERATOR_API_SAFETY_TESTS.len());
        assert!(
            OPERATOR_API_SAFETY_TESTS
                .into_iter()
                .all(|name| listed.contains(name))
        );
        assert!(go_test_selector().starts_with("^("));
        assert!(go_test_selector().ends_with(")$"));
    }
}
