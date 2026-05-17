use std::{
    collections::BTreeMap,
    io::Write as _,
    path::{Path, PathBuf},
    process::{Command as StdCommand, Stdio},
    sync::{Arc, Mutex, OnceLock},
};

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio::task;

use crate::AutomationsError;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ProviderInvocation {
    pub slug: String,
    #[serde(default)]
    pub config: Value,
    #[serde(default)]
    pub input: Value,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum ProviderInvocationResult {
    Ok { output: Value },
    Err { message: String },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AutomationBundleManifest {
    pub name: String,
    pub artifact: PathBuf,
    #[serde(default)]
    pub trigger_subscriptions: Vec<String>,
    #[serde(default, rename = "build")]
    pub build_steps: Vec<BundleBuildStep>,
    #[serde(default)]
    pub tools: Vec<CapabilityBinding>,
    #[serde(default)]
    pub inference: Vec<CapabilityBinding>,
}

impl AutomationBundleManifest {
    /// Load a bundle manifest from disk.
    ///
    /// # Errors
    ///
    /// Returns an error when the manifest cannot be read or parsed.
    pub fn load(path: &Path) -> Result<Self, AutomationsError> {
        let raw = std::fs::read_to_string(path)
            .map_err(|error| AutomationsError::Io(error.to_string()))?;
        let manifest = toml::from_str::<Self>(&raw)
            .map_err(|error| AutomationsError::Io(error.to_string()))?;
        Ok(manifest.resolved_against(path.parent().unwrap_or_else(|| Path::new("."))))
    }

    #[must_use]
    pub fn resolved_against(&self, base_dir: &Path) -> Self {
        let mut resolved = self.clone();
        if resolved.artifact.is_relative() {
            resolved.artifact = base_dir.join(&resolved.artifact);
        }
        resolved.tools = resolved
            .tools
            .iter()
            .map(|binding| binding.resolved_against(base_dir))
            .collect();
        resolved.inference = resolved
            .inference
            .iter()
            .map(|binding| binding.resolved_against(base_dir))
            .collect();
        resolved
    }

    /// Ensure that any manifest-declared build steps have been executed.
    ///
    /// # Errors
    ///
    /// Returns an error if a build step fails.
    pub fn ensure_artifacts_built(&self, workspace_root: &Path) -> Result<(), AutomationsError> {
        ensure_bundle_build_steps(workspace_root, &self.build_steps)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum BundleBuildStep {
    Cargo {
        package: String,
        #[serde(default)]
        lib: bool,
        #[serde(default)]
        bin: Option<String>,
        #[serde(default)]
        target: Option<String>,
        #[serde(default)]
        release: bool,
        #[serde(default)]
        features: Vec<String>,
    },
}

impl BundleBuildStep {
    fn command_key(&self) -> String {
        self.command_args().join("\u{0}")
    }

    fn command_args(&self) -> Vec<String> {
        match self {
            Self::Cargo {
                package,
                lib,
                bin,
                target,
                release,
                features,
            } => {
                let mut args = vec![
                    "build".to_string(),
                    "--package".to_string(),
                    package.clone(),
                ];
                if *lib {
                    args.push("--lib".to_string());
                }
                if let Some(bin) = bin {
                    args.push("--bin".to_string());
                    args.push(bin.clone());
                }
                if let Some(target) = target {
                    args.push("--target".to_string());
                    args.push(target.clone());
                }
                if *release {
                    args.push("--release".to_string());
                }
                if !features.is_empty() {
                    args.push("--features".to_string());
                    args.push(features.join(","));
                }
                args
            }
        }
    }
}

/// Ensure that the provided manifest-declared build steps have been executed.
///
/// # Errors
///
/// Returns an error if any build step fails.
pub fn ensure_bundle_build_steps(
    workspace_root: &Path,
    build_steps: &[BundleBuildStep],
) -> Result<(), AutomationsError> {
    static BUILD_LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    let _guard = BUILD_LOCK
        .get_or_init(|| Mutex::new(()))
        .lock()
        .map_err(|_| AutomationsError::State("bundle build lock was poisoned".to_string()))?;

    let mut seen = std::collections::BTreeSet::new();
    for step in build_steps {
        if !seen.insert(step.command_key()) {
            continue;
        }
        run_bundle_build_step(workspace_root, step)?;
    }
    Ok(())
}

fn run_bundle_build_step(
    workspace_root: &Path,
    step: &BundleBuildStep,
) -> Result<(), AutomationsError> {
    match step {
        BundleBuildStep::Cargo { .. } => {
            let args = step.command_args();
            let status = StdCommand::new("cargo")
                .current_dir(workspace_root)
                .args(&args)
                .status()
                .map_err(|error| AutomationsError::Provider(error.to_string()))?;
            if status.success() {
                Ok(())
            } else {
                Err(AutomationsError::Provider(format!(
                    "cargo {} exited with status {status}",
                    args.join(" ")
                )))
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CapabilityBinding {
    pub slug: String,
    #[serde(default)]
    pub config: Value,
    pub transport: CapabilityTransport,
}

impl CapabilityBinding {
    #[must_use]
    pub fn resolved_against(&self, base_dir: &Path) -> Self {
        let mut resolved = self.clone();
        resolved.transport = self.transport.resolved_against(base_dir);
        resolved
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum CapabilityTransport {
    Command {
        program: PathBuf,
        #[serde(default)]
        args: Vec<String>,
        #[serde(default)]
        env: BTreeMap<String, String>,
    },
}

impl CapabilityTransport {
    #[must_use]
    pub fn resolved_against(&self, base_dir: &Path) -> Self {
        match self {
            Self::Command { program, args, env } => Self::Command {
                program: if program.is_relative() {
                    base_dir.join(program)
                } else {
                    program.clone()
                },
                args: args.clone(),
                env: env.clone(),
            },
        }
    }
}

#[async_trait]
pub trait JsonCapabilityProvider: Send + Sync {
    /// Invoke a dynamically registered capability.
    ///
    /// # Errors
    ///
    /// Returns an error when the provider cannot complete the invocation.
    async fn invoke(&self, invocation: ProviderInvocation) -> Result<Value, AutomationsError>;
}

#[derive(Clone, Default)]
pub struct CapabilityRegistry {
    bindings: Arc<Mutex<BTreeMap<String, RegisteredCapability>>>,
}

#[derive(Clone)]
struct RegisteredCapability {
    config: Value,
    provider: Arc<dyn JsonCapabilityProvider>,
}

impl std::fmt::Debug for CapabilityRegistry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let slugs = self
            .bindings
            .lock()
            .map(|bindings| bindings.keys().cloned().collect::<Vec<_>>())
            .unwrap_or_default();
        f.debug_struct("CapabilityRegistry")
            .field("slugs", &slugs)
            .finish()
    }
}

impl CapabilityRegistry {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Register a capability binding in the in-process registry.
    ///
    /// # Panics
    ///
    /// Panics if the registry mutex is poisoned.
    pub fn register(
        &self,
        slug: impl Into<String>,
        config: Value,
        provider: Arc<dyn JsonCapabilityProvider>,
    ) {
        self.bindings
            .lock()
            .expect("capability registry lock")
            .insert(slug.into(), RegisteredCapability { config, provider });
    }

    /// Register a command-backed capability binding.
    pub fn register_binding(&self, binding: &CapabilityBinding) {
        let provider: Arc<dyn JsonCapabilityProvider> = match &binding.transport {
            CapabilityTransport::Command { program, args, env } => {
                Arc::new(CommandJsonCapabilityProvider {
                    program: program.clone(),
                    args: args.clone(),
                    env: env.clone(),
                })
            }
        };
        self.register(binding.slug.clone(), binding.config.clone(), provider);
    }

    pub fn register_bindings(&self, bindings: &[CapabilityBinding]) {
        for binding in bindings {
            self.register_binding(binding);
        }
    }

    /// Invoke a registered capability by slug.
    ///
    /// # Errors
    ///
    /// Returns an error when the slug is not registered or the provider fails.
    ///
    /// # Panics
    ///
    /// Panics if the registry mutex is poisoned.
    pub async fn invoke(&self, slug: &str, input: Value) -> Result<Value, AutomationsError> {
        let (config, provider) = {
            let bindings = self.bindings.lock().expect("capability registry lock");
            let binding = bindings.get(slug).ok_or_else(|| {
                AutomationsError::Provider(format!("no provider registered for slug `{slug}`"))
            })?;
            (binding.config.clone(), Arc::clone(&binding.provider))
        };

        provider
            .invoke(ProviderInvocation {
                slug: slug.to_string(),
                config,
                input,
            })
            .await
    }
}

#[derive(Debug, Clone)]
pub struct CommandJsonCapabilityProvider {
    program: PathBuf,
    args: Vec<String>,
    env: BTreeMap<String, String>,
}

#[async_trait]
impl JsonCapabilityProvider for CommandJsonCapabilityProvider {
    async fn invoke(&self, invocation: ProviderInvocation) -> Result<Value, AutomationsError> {
        // TODO: This is an intentionally simple first transport. We should
        // revisit persistent providers, arena allocators, memory mapping,
        // crossbeam-style channels, and how this overlaps with the existing
        // in-memory message bus once multi-automation fan-out solidifies.
        let program = self.program.clone();
        let args = self.args.clone();
        let env = self.env.clone();
        let output =
            task::spawn_blocking(move || -> Result<std::process::Output, AutomationsError> {
                let mut child = StdCommand::new(&program)
                    .args(&args)
                    .stdin(Stdio::piped())
                    .stdout(Stdio::piped())
                    .stderr(Stdio::piped())
                    .envs(&env)
                    .spawn()
                    .map_err(|error| AutomationsError::Provider(error.to_string()))?;

                let request = serde_json::to_vec(&invocation)
                    .map_err(|error| AutomationsError::Provider(error.to_string()))?;
                let mut stdin = child.stdin.take().ok_or_else(|| {
                    AutomationsError::Provider("provider stdin unavailable".to_string())
                })?;
                stdin
                    .write_all(&request)
                    .map_err(|error| AutomationsError::Provider(error.to_string()))?;
                drop(stdin);

                child
                    .wait_with_output()
                    .map_err(|error| AutomationsError::Provider(error.to_string()))
            })
            .await
            .map_err(|error| AutomationsError::Provider(error.to_string()))??;
        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(AutomationsError::Provider(format!(
                "{} exited with status {}: {stderr}",
                self.program.display(),
                output.status
            )));
        }

        let response = serde_json::from_slice::<ProviderInvocationResult>(&output.stdout).map_err(
            |error| {
                AutomationsError::Provider(format!(
                    "failed to decode provider response from {}: {}; stdout=`{}` stderr=`{}`",
                    self.program.display(),
                    error,
                    render_output_snippet(&output.stdout),
                    render_output_snippet(&output.stderr)
                ))
            },
        )?;
        match response {
            ProviderInvocationResult::Ok { output } => Ok(output),
            ProviderInvocationResult::Err { message } => Err(AutomationsError::Provider(message)),
        }
    }
}

fn render_output_snippet(bytes: &[u8]) -> String {
    const LIMIT: usize = 240;

    if bytes.is_empty() {
        return "<empty>".to_string();
    }
    let rendered = String::from_utf8_lossy(bytes).replace('\n', "\\n");
    if rendered.len() <= LIMIT {
        rendered
    } else {
        format!("{}...", &rendered[..LIMIT])
    }
}

pub type ToolRegistry = CapabilityRegistry;
pub type InferenceRegistry = CapabilityRegistry;

#[cfg(test)]
mod tests {
    use std::{collections::BTreeMap, fs, os::unix::fs::PermissionsExt};

    use serde_json::json;
    use tempfile::tempdir;

    use super::{CommandJsonCapabilityProvider, JsonCapabilityProvider, ProviderInvocation};

    #[tokio::test]
    async fn command_transport_closes_stdin_for_stdio_providers() {
        let tempdir = tempdir().expect("tempdir");
        let script_path = tempdir.path().join("provider.sh");
        fs::write(
            &script_path,
            "#!/bin/sh\npayload=$(cat)\nprintf '{\"status\":\"ok\",\"output\":{\"payload\":%s}}' \"$payload\"\n",
        )
        .expect("script");
        let mut permissions = fs::metadata(&script_path).expect("metadata").permissions();
        permissions.set_mode(0o755);
        fs::set_permissions(&script_path, permissions).expect("chmod");

        let provider = CommandJsonCapabilityProvider {
            program: script_path,
            args: Vec::new(),
            env: BTreeMap::default(),
        };

        let output = provider
            .invoke(ProviderInvocation {
                slug: "demo".to_string(),
                config: json!(null),
                input: json!({ "kind": "ping" }),
            })
            .await
            .expect("provider should respond");

        assert_eq!(
            output,
            json!({
                "payload": {
                    "slug": "demo",
                    "config": null,
                    "input": { "kind": "ping" },
                }
            })
        );
    }
}
