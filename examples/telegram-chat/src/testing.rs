use std::{
    future::Future,
    path::{Path, PathBuf},
    pin::Pin,
    sync::Arc,
    time::Duration,
};

use example_support::{
    BoxFuture, ExampleBridge, ExampleRuntime, ExampleSubstrate, ExampleSurface, next_event,
    publish, session_stream, session_subscription,
};
use mango_core::agent::{
    AgentRuntime, AgentSchema, BusWorker, Completion, ControlEvent, EventBus, EventPayload,
    ExecutionEvent, InferenceEvent, InteractionEvent, Worker,
};
use mango_telegram::{
    DisplayTelegramTextMapper, TelegramClient, TelegramEgress, TelegramIngress, TelegramSurface,
    TestTelegramActor, TestTelegramDriver, TestTelegramError, telegram_inbox, telegram_test_client,
};
use serde_json::json;
use tempfile::TempDir;
use tokio::{task::JoinHandle, time::sleep};

use crate::{
    BundleAutomationDispatcher, ChatAppError, ChatBus, ChatDirective, ChatSchema, ChatSession,
    ChatSubscription, ConversationControl, SessionSentinel, SharedAutomationDispatcher,
    UsernameWhitelist, automation::default_bundle_manifest_paths, chat_session,
    handle_immediate_response, publish_inference_started,
};

// Bundle-backed BDD flows cold-start a Wasm guest and external providers, so a
// slightly larger default wait keeps the public harness stable under full test
// suite load.
const DEFAULT_WAIT: Duration = Duration::from_secs(30);
const SCRIPTED_ENGINE_ID: &str = "scripted-chat";

type ScriptedResponder =
    Arc<dyn Fn(String) -> Pin<Box<dyn Future<Output = String> + Send>> + Send + Sync + 'static>;

#[derive(Clone)]
pub struct ScriptedConversationBackend {
    responder: ScriptedResponder,
}

impl std::fmt::Debug for ScriptedConversationBackend {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ScriptedConversationBackend").finish()
    }
}

impl ScriptedConversationBackend {
    pub fn new<F, Fut>(responder: F) -> Self
    where
        F: Fn(String) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = String> + Send + 'static,
    {
        Self {
            responder: Arc::new(move |prompt| Box::pin(responder(prompt))),
        }
    }

    async fn respond(&self, prompt: String) -> String {
        (self.responder)(prompt).await
    }
}

impl Default for ScriptedConversationBackend {
    fn default() -> Self {
        Self::new(|prompt| async move { format!("baseline chat: {prompt}") })
    }
}

#[derive(Debug, Clone)]
struct ScriptedConversationInference {
    worker_id: example_support::WorkerId,
    session: ChatSession,
    backend: ScriptedConversationBackend,
}

impl ScriptedConversationInference {
    fn new(session: ChatSession, backend: ScriptedConversationBackend) -> Self {
        Self {
            worker_id: example_support::WorkerId::from("telegram-chat-scripted-inference"),
            session,
            backend,
        }
    }
}

impl Worker for ScriptedConversationInference {
    type WorkerId = example_support::WorkerId;
    type Subscription = ChatSubscription;

    fn worker_id(&self) -> Self::WorkerId {
        self.worker_id.clone()
    }

    fn subscription(&self) -> Self::Subscription {
        session_subscription::<ChatSchema>(&self.session)
    }
}

impl BusWorker<ChatSchema, ChatBus> for ScriptedConversationInference {
    type Error = ChatAppError;
    type Run = BoxFuture<'static, Self::Error>;

    fn run(self, bus: ChatBus) -> Self::Run {
        Box::pin(async move {
            let bus = &bus;
            let mut events = bus.subscribe(self.subscription())?;

            while let Some(event) = next_event(&mut events).await? {
                match event.payload {
                    EventPayload::Execution(ExecutionEvent::Control(ControlEvent::Requested {
                        request_id,
                        session_id,
                        thread_id,
                        directive,
                        ..
                    })) if session_id == self.session.session_id => {
                        let request = super::RequestedTurn {
                            request: request_id,
                            session: session_id,
                            thread: thread_id,
                        };

                        match directive {
                            ChatDirective::ConversationTurn { prompt } => {
                                let run_id = ChatSchema::next_inference_run_id();
                                publish_inference_started(
                                    bus,
                                    &self.session,
                                    run_id,
                                    request,
                                    ChatDirective::ConversationTurn {
                                        prompt: prompt.clone(),
                                    },
                                    SCRIPTED_ENGINE_ID,
                                )
                                .await?;
                                let response = self.backend.respond(prompt).await;
                                publish::<ChatSchema, _>(
                                    bus,
                                    session_stream::<ChatSchema>(&self.session),
                                    mango_core::agent::EventVisibility::Both,
                                    EventPayload::Execution(ExecutionEvent::Inference(
                                        InferenceEvent::Output {
                                            run_id,
                                            sequence: 0,
                                            output: response,
                                        },
                                    )),
                                )
                                .await?;
                                publish::<ChatSchema, _>(
                                    bus,
                                    session_stream::<ChatSchema>(&self.session),
                                    mango_core::agent::EventVisibility::Internal,
                                    EventPayload::Execution(ExecutionEvent::Inference(
                                        InferenceEvent::Completed {
                                            run_id,
                                            result: Completion::Completed,
                                        },
                                    )),
                                )
                                .await?;
                            }
                            ChatDirective::AutomationHandled { response } => {
                                handle_immediate_response(
                                    bus,
                                    &self.session,
                                    request,
                                    ChatDirective::AutomationHandled {
                                        response: response.clone(),
                                    },
                                    response,
                                    "automation-bundle",
                                )
                                .await?;
                            }
                            ChatDirective::RejectedByWhitelist { response } => {
                                handle_immediate_response(
                                    bus,
                                    &self.session,
                                    request,
                                    ChatDirective::RejectedByWhitelist {
                                        response: response.clone(),
                                    },
                                    response,
                                    "telegram-whitelist",
                                )
                                .await?;
                            }
                        }
                    }
                    EventPayload::Interaction(InteractionEvent::SessionClosed {
                        session_id,
                        ..
                    }) if session_id == self.session.session_id => {
                        break;
                    }
                    _ => {}
                }
            }

            Ok(())
        })
    }
}

pub struct TelegramChatHarness {
    _tempdir: TempDir,
    actor: TestTelegramActor,
    ingress_forwarder: JoinHandle<()>,
    runtime_handle: JoinHandle<()>,
    driver: TestTelegramDriver,
    state_root: PathBuf,
    bundle_dispatcher: Option<Arc<BundleAutomationDispatcher>>,
}

impl std::fmt::Debug for TelegramChatHarness {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TelegramChatHarness")
            .field("actor", &self.actor)
            .field("state_root", &self.state_root)
            .finish_non_exhaustive()
    }
}

impl TelegramChatHarness {
    /// Return the primary scripted actor for this harness.
    #[must_use]
    pub fn actor(&self) -> &TestTelegramActor {
        &self.actor
    }

    /// Send a text turn into the test Telegram session.
    ///
    /// # Errors
    ///
    /// Returns an error if the in-memory Telegram inbox is closed.
    pub async fn send_text(&self, text: impl Into<String>) -> Result<(), TestTelegramError> {
        self.send_text_from(&self.actor, text).await
    }

    /// Send a text turn from an arbitrary scripted actor into the same test
    /// Telegram session.
    ///
    /// # Errors
    ///
    /// Returns an error if the in-memory Telegram inbox is closed.
    pub async fn send_text_from(
        &self,
        actor: &TestTelegramActor,
        text: impl Into<String>,
    ) -> Result<(), TestTelegramError> {
        self.driver.send_text(actor, text).await
    }

    /// Send a photo turn into the test Telegram session.
    ///
    /// # Errors
    ///
    /// Returns an error if the in-memory Telegram inbox is closed.
    pub async fn send_photo(
        &self,
        local_path: impl Into<PathBuf>,
        caption: Option<String>,
    ) -> Result<(), TestTelegramError> {
        self.send_photo_from(&self.actor, local_path, caption).await
    }

    /// Send a photo turn from an arbitrary scripted actor into the same test
    /// Telegram session.
    ///
    /// # Errors
    ///
    /// Returns an error if the in-memory Telegram inbox is closed.
    pub async fn send_photo_from(
        &self,
        actor: &TestTelegramActor,
        local_path: impl Into<PathBuf>,
        caption: Option<String>,
    ) -> Result<(), TestTelegramError> {
        self.driver.send_photo(actor, local_path, caption).await
    }

    /// Wait for the next outbound reply emitted by the app.
    ///
    /// # Errors
    ///
    /// Returns an error if the outbound wait times out or the test client is
    /// already closed.
    pub async fn recv_reply(&self) -> Result<String, TestTelegramError> {
        self.recv_reply_with_timeout(DEFAULT_WAIT).await
    }

    /// Wait for the next outbound reply emitted by the app with an explicit
    /// timeout.
    ///
    /// # Errors
    ///
    /// Returns an error if the outbound wait times out or the test client is
    /// already closed.
    pub async fn recv_reply_with_timeout(
        &self,
        wait_for: Duration,
    ) -> Result<String, TestTelegramError> {
        self.driver
            .recv_outbound(wait_for)
            .await
            .map(|message| message.text)
    }

    pub async fn transcript(&self) -> Vec<String> {
        self.driver
            .transcript()
            .await
            .into_iter()
            .map(|message| message.text)
            .collect()
    }

    #[must_use]
    pub fn state_root(&self) -> &Path {
        &self.state_root
    }

    /// Return the markdown expense files currently persisted by the bundle.
    ///
    /// # Errors
    ///
    /// Returns an error if the expense-report directory cannot be listed.
    pub fn expense_markdown_files(&self) -> Result<Vec<PathBuf>, String> {
        let directory = self.state_root.join("expense-reports");
        if !directory.exists() {
            return Ok(Vec::new());
        }

        let mut files = std::fs::read_dir(directory)
            .map_err(|error| error.to_string())?
            .filter_map(|entry| entry.ok().map(|entry| entry.path()))
            .collect::<Vec<_>>();
        files.sort();
        Ok(files)
    }

    /// Read a persisted markdown file from the test state root.
    ///
    /// # Errors
    ///
    /// Returns an error if the file cannot be read.
    pub fn read_markdown(&self, path: &Path) -> Result<String, String> {
        std::fs::read_to_string(path).map_err(|error| error.to_string())
    }

    /// Write a local photo fixture under the test state root.
    ///
    /// # Errors
    ///
    /// Returns an error if the fixture cannot be written.
    pub fn write_photo_fixture(
        &self,
        name: impl AsRef<str>,
        contents: impl AsRef<[u8]>,
    ) -> Result<PathBuf, String> {
        let path = self.state_root.join(name.as_ref());
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).map_err(|error| error.to_string())?;
        }
        std::fs::write(&path, contents).map_err(|error| error.to_string())?;
        Ok(path)
    }

    /// Return automation control-plane traces captured during the test.
    ///
    /// # Errors
    ///
    /// Returns an error if the dispatcher cannot read control-plane state.
    pub fn traces(&self) -> Result<Vec<mango_automations::TraceRecord>, String> {
        self.bundle_dispatcher
            .as_ref()
            .map_or_else(|| Ok(Vec::new()), |dispatcher| dispatcher.traces())
    }
}

impl Drop for TelegramChatHarness {
    fn drop(&mut self) {
        self.ingress_forwarder.abort();
        self.runtime_handle.abort();
    }
}

#[derive(Clone)]
#[must_use]
pub struct TelegramChatHarnessBuilder {
    actor: TestTelegramActor,
    allowed_usernames: UsernameWhitelist,
    baseline_backend: ScriptedConversationBackend,
    automation_dispatcher: Option<SharedAutomationDispatcher>,
    bundle_manifest_paths: Vec<PathBuf>,
    automation_host_context: serde_json::Value,
    bus_capacity: usize,
    inbox_capacity: usize,
}

impl TelegramChatHarnessBuilder {
    pub fn new() -> Self {
        Self {
            actor: TestTelegramActor::new(
                mango_telegram::TelegramChatId(7),
                None,
                Some("trusted_customer".to_string()),
                "Trusted Customer",
            ),
            allowed_usernames: UsernameWhitelist::from_usernames(["trusted_customer"]),
            baseline_backend: ScriptedConversationBackend::default(),
            automation_dispatcher: None,
            bundle_manifest_paths: default_bundle_manifest_paths().to_vec(),
            automation_host_context: json!({}),
            bus_capacity: 256,
            inbox_capacity: 32,
        }
    }

    pub fn with_baseline_backend(mut self, backend: ScriptedConversationBackend) -> Self {
        self.baseline_backend = backend;
        self
    }

    pub fn with_allowed_usernames(mut self, allowed_usernames: UsernameWhitelist) -> Self {
        self.allowed_usernames = allowed_usernames;
        self
    }

    pub fn with_actor(mut self, actor: TestTelegramActor) -> Self {
        self.actor = actor;
        self
    }

    pub fn with_automation_dispatcher(
        mut self,
        automation_dispatcher: SharedAutomationDispatcher,
    ) -> Self {
        self.automation_dispatcher = Some(automation_dispatcher);
        self
    }

    pub fn with_bundle_manifests<I, P>(mut self, bundle_manifest_paths: I) -> Self
    where
        I: IntoIterator<Item = P>,
        P: Into<PathBuf>,
    {
        self.bundle_manifest_paths = bundle_manifest_paths.into_iter().map(Into::into).collect();
        self
    }

    pub fn with_automation_host_context(
        mut self,
        automation_host_context: serde_json::Value,
    ) -> Self {
        self.automation_host_context = automation_host_context;
        self
    }

    /// Build a full app-level Telegram chat harness with the generic bundle
    /// dispatcher wired in.
    ///
    /// # Errors
    ///
    /// Returns an error if the temporary state root, automation dispatcher, or
    /// in-memory runtime infrastructure cannot be initialized.
    pub async fn build(self) -> Result<TelegramChatHarness, String> {
        let tempdir = tempfile::tempdir().map_err(|error| error.to_string())?;
        let state_root = tempdir.path().join("state");
        std::fs::create_dir_all(&state_root).map_err(|error| error.to_string())?;
        let (automation_dispatcher, bundle_dispatcher) =
            if let Some(dispatcher) = self.automation_dispatcher {
                (dispatcher, None)
            } else {
                let host_context =
                    merge_automation_host_context(&self.automation_host_context, &state_root);
                let dispatcher = Arc::new(BundleAutomationDispatcher::from_bundle_manifests(
                    workspace_root()?,
                    tempdir.path().join("automation-control-plane.json"),
                    &self.bundle_manifest_paths,
                    &host_context,
                )?);
                (
                    dispatcher.clone() as SharedAutomationDispatcher,
                    Some(dispatcher),
                )
            };

        let (driver, client) = telegram_test_client(self.inbox_capacity);
        let (inbox_sender, inbox) = telegram_inbox(self.inbox_capacity);
        let ingress_client = client.clone();
        let ingress_forwarder = tokio::spawn(async move {
            while let Ok(Some(message)) = ingress_client.recv().await {
                if inbox_sender.send(message).await.is_err() {
                    break;
                }
            }
        });

        let surface = TelegramSurface {
            chat_id: self.actor.chat_id,
            thread_id: self.actor.thread_id,
            username: self.actor.username.clone(),
            display_name: self.actor.display_name.clone(),
        };
        let session = chat_session(surface);
        let runtime = ExampleRuntime::new(
            ExampleSubstrate::new(
                ChatBus::new(self.bus_capacity),
                ConversationControl::with_automation(
                    session.clone(),
                    self.allowed_usernames,
                    automation_dispatcher.clone(),
                ),
            ),
            ExampleSurface::new(
                TelegramIngress::new(
                    example_support::WorkerId::from("telegram-chat-ingress"),
                    inbox,
                    super::ChatTelegramInputMapper,
                ),
                TelegramEgress::new(
                    example_support::WorkerId::from("telegram-chat-egress"),
                    client,
                    DisplayTelegramTextMapper,
                ),
                SessionSentinel::new("telegram-chat-presentation", session.clone()),
            ),
            ExampleBridge::new(
                ScriptedConversationInference::new(session.clone(), self.baseline_backend),
                SessionSentinel::new("telegram-chat-tools", session.clone()),
            ),
        );

        let runtime_handle = tokio::spawn(async move {
            if let Err(error) = runtime.startup(session.clone()).await {
                tracing::error!("telegram-chat test runtime startup failed: {error}");
                return;
            }
            if let Err(error) = runtime.run_session(session).await {
                tracing::error!("telegram-chat test runtime failed: {error}");
            }
        });

        sleep(Duration::from_millis(50)).await;

        Ok(TelegramChatHarness {
            _tempdir: tempdir,
            actor: self.actor,
            ingress_forwarder,
            runtime_handle,
            driver,
            state_root,
            bundle_dispatcher,
        })
    }
}

impl Default for TelegramChatHarnessBuilder {
    fn default() -> Self {
        Self::new()
    }
}

fn workspace_root() -> Result<&'static Path, String> {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .ancestors()
        .nth(2)
        .map(Path::to_path_buf)
        .ok_or_else(|| "failed to resolve workspace root".to_string())?;
    Ok(Box::leak(Box::new(path)).as_path())
}

fn merge_automation_host_context(
    context: &serde_json::Value,
    state_root: &Path,
) -> serde_json::Value {
    let mut merged = match context {
        serde_json::Value::Object(object) => object.clone(),
        serde_json::Value::Null => serde_json::Map::new(),
        other => serde_json::Map::from_iter([("config".to_string(), other.clone())]),
    };
    merged.insert(
        "state_root".to_string(),
        json!(state_root.display().to_string()),
    );
    serde_json::Value::Object(merged)
}
