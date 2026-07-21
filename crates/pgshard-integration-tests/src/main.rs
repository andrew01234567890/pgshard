//! Dispatches integration suites that require real external services.

use std::env;
use std::ffi::OsString;
use std::path::{Path, PathBuf};
use std::process::{Command, ExitCode};

const POSTGRES18_WIRE_SUITE: &str = "postgres18-wire";
const POSTGRES18_WIRE_TEST: &str =
    "postgres18_wire_and_persistent_slot_controls_decode_from_real_bytes";

#[derive(Debug, Eq, PartialEq)]
enum Request {
    Help,
    Run { suite: String },
}

fn parse_args(args: impl IntoIterator<Item = OsString>) -> Result<Request, String> {
    let mut args = args.into_iter();
    let mut suite = None;
    while let Some(argument) = args.next() {
        match argument.to_str() {
            Some("-h" | "--help") => return Ok(Request::Help),
            Some("--suite") => {
                if suite.is_some() {
                    return Err("--suite may be provided only once".to_owned());
                }
                let value = args
                    .next()
                    .ok_or_else(|| "--suite requires a value".to_owned())?;
                let value = value
                    .into_string()
                    .map_err(|_| "--suite must be valid UTF-8".to_owned())?;
                suite = Some(value);
            }
            Some(value) => return Err(format!("unsupported argument {value:?}")),
            None => return Err("arguments must be valid UTF-8".to_owned()),
        }
    }
    Ok(Request::Run {
        suite: suite.ok_or_else(|| "--suite is required".to_owned())?,
    })
}

fn repository_root() -> Result<PathBuf, String> {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(Path::parent)
        .map(Path::to_path_buf)
        .ok_or_else(|| "integration package is not inside the repository test root".to_owned())
}

fn required_environment(name: &str) -> Result<OsString, String> {
    env::var_os(name)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| format!("{name} is required; refusing to skip the live fixture"))
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
        .map_err(|error| format!("{description} returned non-UTF-8 output: {error}"))
}

fn listed_rust_tests(output: &str) -> Vec<&str> {
    output
        .lines()
        .filter_map(|line| line.strip_suffix(": test"))
        .collect()
}

fn run_suite(suite: &str) -> Result<(), String> {
    if suite != POSTGRES18_WIRE_SUITE {
        return Err(format!(
            "unsupported suite {suite:?}; executable suites: {POSTGRES18_WIRE_SUITE}"
        ));
    }

    let address = required_environment("PGSHARD_PGWIRE_TEST_ADDRESS")?;
    let cargo = env::var_os("CARGO").unwrap_or_else(|| OsString::from("cargo"));
    let root = repository_root()?;
    let listed = command_output(
        Command::new(&cargo).current_dir(&root).args([
            "test",
            "--locked",
            "-p",
            "pgshard-pgwire",
            "--test",
            "postgres18",
            POSTGRES18_WIRE_TEST,
            "--",
            "--ignored",
            "--exact",
            "--list",
        ]),
        "list live PostgreSQL integration test",
    )?;
    let listed = listed_rust_tests(&listed);
    if listed != [POSTGRES18_WIRE_TEST] {
        return Err(format!(
            "live PostgreSQL integration selector matched {listed:?}, expected exactly [{POSTGRES18_WIRE_TEST:?}]"
        ));
    }

    let status = Command::new(&cargo)
        .current_dir(root)
        .env("PGSHARD_PGWIRE_TEST_ADDRESS", address)
        .args([
            "test",
            "--locked",
            "-p",
            "pgshard-pgwire",
            "--test",
            "postgres18",
            POSTGRES18_WIRE_TEST,
            "--",
            "--ignored",
            "--exact",
            "--test-threads=1",
        ])
        .status()
        .map_err(|error| format!("start live PostgreSQL integration test: {error}"))?;
    if !status.success() {
        return Err(format!(
            "live PostgreSQL integration test failed with {status}"
        ));
    }
    Ok(())
}

fn print_help() {
    println!(
        "Usage: pgshard-integration-tests --suite {POSTGRES18_WIRE_SUITE}\n\
         \n\
         Requires PGSHARD_PGWIRE_TEST_ADDRESS pointing to a disposable PostgreSQL 18\n\
         server configured for logical replication and trust authentication."
    );
}

fn main() -> ExitCode {
    let request = parse_args(env::args_os().skip(1));
    let result = match request {
        Ok(Request::Help) => {
            print_help();
            return ExitCode::SUCCESS;
        }
        Ok(Request::Run { suite }) => run_suite(&suite),
        Err(error) => Err(error),
    };
    match result {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("pgshard integration runner: {error}");
            ExitCode::FAILURE
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{POSTGRES18_WIRE_TEST, Request, listed_rust_tests, parse_args, run_suite};
    use std::ffi::OsString;

    fn args(values: &[&str]) -> Vec<OsString> {
        values.iter().map(OsString::from).collect()
    }

    #[test]
    fn requires_an_explicit_suite() {
        assert_eq!(parse_args(args(&[])), Err("--suite is required".to_owned()));
    }

    #[test]
    fn accepts_the_registered_suite_shape() {
        assert_eq!(
            parse_args(args(&["--suite", "postgres18-wire"])),
            Ok(Request::Run {
                suite: "postgres18-wire".to_owned()
            })
        );
    }

    #[test]
    fn rejects_unknown_arguments() {
        assert_eq!(
            parse_args(args(&["postgres18-wire"])),
            Err("unsupported argument \"postgres18-wire\"".to_owned())
        );
    }

    #[test]
    fn rejects_unknown_suites_before_touching_any_fixture() {
        assert_eq!(
            run_suite("nonexistent"),
            Err("unsupported suite \"nonexistent\"; executable suites: postgres18-wire".to_owned())
        );
    }

    #[test]
    fn extracts_only_libtest_test_entries() {
        let output = format!("{POSTGRES18_WIRE_TEST}: test\n\n1 test, 0 benchmarks\n");
        assert_eq!(listed_rust_tests(&output), [POSTGRES18_WIRE_TEST]);
        assert!(listed_rust_tests("0 tests, 0 benchmarks\n").is_empty());
    }
}
