use std::{
    fs,
    path::{Path, PathBuf},
};

use async_trait::async_trait;
use mango_automations::AutomationsError;
use mango_automations_bdd::{AutomationsScenarioWorld, Scenario, ScenarioFailure};
use mango_telegram::{TelegramChatId, TelegramThreadId, TestTelegramActor};
use serde_json::json;
use serial_test::serial;
use telegram_chat::UsernameWhitelist;
use telegram_chat::testing::{
    ScriptedConversationBackend, TelegramChatHarness, TelegramChatHarnessBuilder,
};

#[derive(Debug)]
struct ExpenseWorld {
    harness: TelegramChatHarness,
    _provider_tempdir: Option<tempfile::TempDir>,
    markdown_root: PathBuf,
}

impl ExpenseWorld {
    async fn new() -> Result<Self, AutomationsError> {
        Self::from_builder(default_builder(), None, None).await
    }

    async fn with_builder(builder: TelegramChatHarnessBuilder) -> Result<Self, AutomationsError> {
        Self::from_builder(builder, None, None).await
    }

    async fn with_custom_ocr(stdout: &str) -> Result<Self, AutomationsError> {
        let provider_tempdir = tempfile::tempdir().map_err(|error| {
            AutomationsError::State(format!("failed to create provider tempdir: {error}"))
        })?;
        let ocr_script = provider_tempdir.path().join("fake-ocr.sh");
        write_executable(
            &ocr_script,
            &format!("#!/bin/sh\nprintf '{}'\n", shell_escape(stdout)),
        );

        Self::from_builder(
            default_builder().with_automation_host_context(json!({
                "ocr_executable": ocr_script.display().to_string(),
            })),
            None,
            Some(provider_tempdir),
        )
        .await
    }

    async fn from_builder(
        builder: TelegramChatHarnessBuilder,
        markdown_root: Option<PathBuf>,
        provider_tempdir: Option<tempfile::TempDir>,
    ) -> Result<Self, AutomationsError> {
        let harness = builder.build().await.map_err(AutomationsError::State)?;
        Ok(Self {
            markdown_root: markdown_root.unwrap_or_else(|| harness.state_root().to_path_buf()),
            harness,
            _provider_tempdir: provider_tempdir,
        })
    }

    async fn send_text(&self, text: impl Into<String>) -> Result<(), AutomationsError> {
        self.harness
            .send_text(text.into())
            .await
            .map_err(|error| AutomationsError::State(error.to_string()))
    }

    async fn send_text_from(
        &self,
        actor: &TestTelegramActor,
        text: impl Into<String>,
    ) -> Result<(), AutomationsError> {
        self.harness
            .send_text_from(actor, text.into())
            .await
            .map_err(|error| AutomationsError::State(error.to_string()))
    }

    async fn send_photo(
        &self,
        file_name: &str,
        caption: Option<&str>,
    ) -> Result<(), AutomationsError> {
        let photo = self
            .harness
            .write_photo_fixture(file_name, b"fixture-image")
            .map_err(AutomationsError::State)?;
        self.harness
            .send_photo(photo, caption.map(str::to_string))
            .await
            .map_err(|error| AutomationsError::State(error.to_string()))
    }

    async fn expect_reply_contains(&self, expected: &str) -> Result<(), AutomationsError> {
        let reply = match self.harness.recv_reply().await {
            Ok(reply) => reply,
            Err(error) => {
                let transcript = self.harness.transcript().await;
                let traces = self.harness.traces().map_err(AutomationsError::State)?;
                return Err(AutomationsError::State(format!(
                    "failed waiting for reply: {error}; transcript={transcript:?}; traces={traces:#?}"
                )));
            }
        };
        if reply.contains(expected) {
            Ok(())
        } else {
            let transcript = self.harness.transcript().await;
            Err(AutomationsError::State(format!(
                "expected reply containing {expected:?}, got {reply:?}; transcript={transcript:?}"
            )))
        }
    }

    fn markdown_files(&self) -> Result<Vec<std::path::PathBuf>, AutomationsError> {
        let directory = self.markdown_root.join("expense-reports");
        if !directory.exists() {
            return Ok(Vec::new());
        }

        let mut files = std::fs::read_dir(directory)
            .map_err(|error| AutomationsError::State(error.to_string()))?
            .filter_map(|entry| entry.ok().map(|entry| entry.path()))
            .collect::<Vec<_>>();
        files.sort();
        Ok(files)
    }

    fn only_markdown(&self) -> Result<String, AutomationsError> {
        let files = self.markdown_files()?;
        let path = files
            .first()
            .ok_or_else(|| AutomationsError::State("expected one markdown file".to_string()))?;
        self.harness
            .read_markdown(path)
            .map_err(AutomationsError::State)
    }
}

fn default_builder() -> TelegramChatHarnessBuilder {
    TelegramChatHarnessBuilder::new().with_baseline_backend(ScriptedConversationBackend::new(
        |prompt| async move { format!("baseline fallback: {prompt}") },
    ))
}

async fn create_expense(
    world: &ExpenseWorld,
    photo_name: &str,
    merchant: &str,
    amount: &str,
) -> Result<(), AutomationsError> {
    world.send_photo(photo_name, Some("receipt")).await?;
    world
        .expect_reply_contains("I think that is an expense")
        .await?;
    world.send_text(format!("{merchant} {amount}")).await?;
    world.expect_reply_contains("Saved expense").await
}

#[async_trait]
impl AutomationsScenarioWorld for ExpenseWorld {
    async fn traces(&mut self) -> Result<Vec<mango_automations::TraceRecord>, AutomationsError> {
        self.harness.traces().map_err(AutomationsError::State)
    }
}

#[tokio::test]
#[serial]
async fn receipt_photo_clarification_then_crud_flows_through_markdown_store()
-> Result<(), ScenarioFailure> {
    let mut scenario = Scenario::new(
        "receipt photos become markdown expenses and text turns can read update and delete them",
        ExpenseWorld::new().await.expect("world should initialize"),
    );

    scenario
        .when("a vague receipt photo is sent")
        .perform(|world| {
            Box::pin(async move {
                world
                    .send_photo("fixtures/receipt-a.jpg", Some("receipt"))
                    .await?;
                Ok(())
            })
        })
        .await?;

    scenario
        .then("the automation asks a clarification question instead of writing markdown")
        .perform(|world| {
            Box::pin(async move {
                world
                    .expect_reply_contains("I think that is an expense")
                    .await?;
                if !world.markdown_files()?.is_empty() {
                    return Err(AutomationsError::State(
                        "expected no markdown expenses yet".to_string(),
                    ));
                }
                Ok(())
            })
        })
        .await?;

    scenario
        .when("the user answers with the merchant and amount")
        .perform(|world| {
            Box::pin(async move {
                world.send_text("Acme Lunch 12.50").await?;
                Ok(())
            })
        })
        .await?;

    scenario
        .then("the automation saves a markdown expense")
        .perform(|world| {
            Box::pin(async move {
                world.expect_reply_contains("Saved expense").await?;
                let markdown = world.only_markdown()?;
                if !markdown.contains("Acme Lunch") || !markdown.contains("12.50") {
                    return Err(AutomationsError::State(format!(
                        "unexpected markdown contents: {markdown}"
                    )));
                }
                Ok(())
            })
        })
        .await?;

    scenario
        .when("the user asks to read the current expenses")
        .perform(|world| {
            Box::pin(async move {
                world.send_text("show my expenses").await?;
                Ok(())
            })
        })
        .await?;

    scenario
        .then("the automation responds from the markdown ledger")
        .perform(|world| {
            Box::pin(async move { world.expect_reply_contains("Current active expenses").await })
        })
        .await?;

    scenario
        .when("the user edits the expense in free-form text")
        .perform(|world| {
            Box::pin(async move {
                world.send_text("change Acme to 18.20").await?;
                Ok(())
            })
        })
        .await?;

    scenario
        .then("the automation updates the markdown file")
        .perform(|world| {
            Box::pin(async move {
                world.expect_reply_contains("Updated expense").await?;
                let markdown = world.only_markdown()?;
                if !markdown.contains("18.20") {
                    return Err(AutomationsError::State(format!(
                        "expected updated amount in markdown: {markdown}"
                    )));
                }
                Ok(())
            })
        })
        .await?;

    scenario
        .when("the user deletes the expense")
        .perform(|world| {
            Box::pin(async move {
                world.send_text("delete Acme").await?;
                Ok(())
            })
        })
        .await?;

    scenario
        .then("the expense is soft-deleted in markdown")
        .perform(|world| {
            Box::pin(async move {
                world.expect_reply_contains("Soft-deleted expense").await?;
                let markdown = world.only_markdown()?;
                if !markdown.contains("status = \"deleted\"") {
                    return Err(AutomationsError::State(format!(
                        "expected deleted status in markdown: {markdown}"
                    )));
                }
                Ok(())
            })
        })
        .await?;

    Ok(())
}

#[tokio::test]
#[serial]
async fn unrelated_text_falls_back_to_chat_while_a_clarification_stays_pending()
-> Result<(), ScenarioFailure> {
    let mut scenario = Scenario::new(
        "clarification state does not steal unrelated turns from baseline chat",
        ExpenseWorld::new().await.expect("world should initialize"),
    );

    scenario
        .when("a vague receipt photo opens a clarification")
        .perform(|world| {
            Box::pin(async move {
                world
                    .send_photo("fixtures/receipt-b.jpg", Some("receipt"))
                    .await?;
                Ok(())
            })
        })
        .await?;

    scenario
        .then("the automation asks for more detail")
        .perform(|world| {
            Box::pin(async move {
                world
                    .expect_reply_contains("I think that is an expense")
                    .await
            })
        })
        .await?;

    scenario
        .when("the user sends an unrelated chat message")
        .perform(|world| {
            Box::pin(async move {
                world.send_text("tell me a joke").await?;
                Ok(())
            })
        })
        .await?;

    scenario
        .then("baseline chat answers instead of the automation")
        .perform(|world| {
            Box::pin(async move {
                world
                    .expect_reply_contains("baseline fallback: tell me a joke")
                    .await?;
                if !world.markdown_files()?.is_empty() {
                    return Err(AutomationsError::State(
                        "expected no markdown writes while clarification is pending".to_string(),
                    ));
                }
                Ok(())
            })
        })
        .await?;

    scenario
        .when("the user later answers the clarification")
        .perform(|world| {
            Box::pin(async move {
                world.send_text("Orbit Coffee 9.99").await?;
                Ok(())
            })
        })
        .await?;

    scenario
        .then("the original clarification still resolves into a saved expense")
        .perform(|world| {
            Box::pin(async move {
                world.expect_reply_contains("Saved expense").await?;
                if world.markdown_files()?.len() != 1 {
                    return Err(AutomationsError::State(
                        "expected exactly one saved markdown expense".to_string(),
                    ));
                }
                Ok(())
            })
        })
        .await?;

    Ok(())
}

#[tokio::test]
#[serial]
async fn non_expense_photos_fall_through_to_baseline_chat() -> Result<(), ScenarioFailure> {
    let mut scenario = Scenario::new(
        "non-expense photos are ignored by the automation and answered by chat",
        ExpenseWorld::new().await.expect("world should initialize"),
    );

    scenario
        .when("a cat photo is sent")
        .perform(|world| {
            Box::pin(async move {
                world.send_photo("fixtures/cat.jpg", Some("cat")).await?;
                Ok(())
            })
        })
        .await?;

    scenario
        .then("baseline chat receives the turn")
        .perform(|world| {
            Box::pin(async move {
                world
                    .expect_reply_contains("baseline fallback: The user sent a photo")
                    .await?;
                if !world.markdown_files()?.is_empty() {
                    return Err(AutomationsError::State(
                        "expected no markdown expenses for a non-expense photo".to_string(),
                    ));
                }
                Ok(())
            })
        })
        .await?;

    Ok(())
}

#[tokio::test]
#[serial]
async fn explicit_expense_reads_are_not_misread_as_clarification_answers()
-> Result<(), ScenarioFailure> {
    let mut scenario = Scenario::new(
        "explicit expense commands still work while a receipt clarification is pending",
        ExpenseWorld::new().await.expect("world should initialize"),
    );

    scenario
        .when("a vague receipt photo opens a pending clarification")
        .perform(|world| {
            Box::pin(async move {
                world
                    .send_photo("fixtures/receipt-c.jpg", Some("receipt"))
                    .await?;
                Ok(())
            })
        })
        .await?;

    scenario
        .then("the automation asks for the missing expense details")
        .perform(|world| {
            Box::pin(async move {
                world
                    .expect_reply_contains("I think that is an expense")
                    .await
            })
        })
        .await?;

    scenario
        .when("the user asks to read expenses before answering the clarification")
        .perform(|world| {
            Box::pin(async move {
                world.send_text("show my expenses").await?;
                Ok(())
            })
        })
        .await?;

    scenario
        .then("the read intent is handled instead of being mistaken for a merchant answer")
        .perform(|world| {
            Box::pin(async move {
                world
                    .expect_reply_contains("You do not have any active expenses yet.")
                    .await?;
                if !world.markdown_files()?.is_empty() {
                    return Err(AutomationsError::State(
                        "expected no markdown writes from a read-only expense turn".to_string(),
                    ));
                }
                Ok(())
            })
        })
        .await?;

    scenario
        .when("the user then answers the original clarification")
        .perform(|world| {
            Box::pin(async move {
                world.send_text("Acme Lunch 12.50").await?;
                Ok(())
            })
        })
        .await?;

    scenario
        .then("the clarification is still pending and resolves into a saved expense")
        .perform(|world| {
            Box::pin(async move {
                world.expect_reply_contains("Saved expense").await?;
                if world.markdown_files()?.len() != 1 {
                    return Err(AutomationsError::State(
                        "expected exactly one markdown expense after answering the clarification"
                            .to_string(),
                    ));
                }
                Ok(())
            })
        })
        .await?;

    Ok(())
}

#[tokio::test]
#[serial]
async fn generic_what_questions_fall_through_to_baseline_chat() -> Result<(), ScenarioFailure> {
    let mut scenario = Scenario::new(
        "generic chat questions are not mistaken for expense reads",
        ExpenseWorld::new().await.expect("world should initialize"),
    );

    scenario
        .when("the user asks a normal conversational question")
        .perform(|world| {
            Box::pin(async move {
                world.send_text("what are you up to").await?;
                Ok(())
            })
        })
        .await?;

    scenario
        .then("baseline chat handles the turn")
        .perform(|world| {
            Box::pin(async move {
                world
                    .expect_reply_contains("baseline fallback: what are you up to")
                    .await?;
                if !world.markdown_files()?.is_empty() {
                    return Err(AutomationsError::State(
                        "expected no markdown writes for a general chat question".to_string(),
                    ));
                }
                Ok(())
            })
        })
        .await?;

    Ok(())
}

#[tokio::test]
#[serial]
async fn caption_only_photo_hints_require_confirmation_before_writing_markdown()
-> Result<(), ScenarioFailure> {
    let mut scenario = Scenario::new(
        "caption-only expense hints do not auto-save without actual receipt extraction",
        ExpenseWorld::new().await.expect("world should initialize"),
    );

    scenario
        .when("a non-receipt photo is sent with only caption-based expense hints")
        .perform(|world| {
            Box::pin(async move {
                world
                    .send_photo("fixtures/cat-lunch.jpg", Some("Lunch 12.50"))
                    .await?;
                Ok(())
            })
        })
        .await?;

    scenario
        .then("the automation asks for confirmation instead of saving from the caption alone")
        .perform(|world| {
            Box::pin(async move {
                world
                    .expect_reply_contains("I only have your caption")
                    .await?;
                if !world.markdown_files()?.is_empty() {
                    return Err(AutomationsError::State(
                        "expected no markdown writes from caption-only evidence".to_string(),
                    ));
                }
                Ok(())
            })
        })
        .await?;

    scenario
        .when("the user explicitly confirms the merchant and amount")
        .perform(|world| {
            Box::pin(async move {
                world.send_text("Acme Lunch 12.50").await?;
                Ok(())
            })
        })
        .await?;

    scenario
        .then("the automation saves the expense after the explicit confirmation")
        .perform(|world| {
            Box::pin(async move {
                world.expect_reply_contains("Saved expense").await?;
                let markdown = world.only_markdown()?;
                if !markdown.contains("Acme Lunch") || !markdown.contains("12.50") {
                    return Err(AutomationsError::State(format!(
                        "unexpected markdown contents after caption confirmation: {markdown}"
                    )));
                }
                Ok(())
            })
        })
        .await?;

    Ok(())
}

#[tokio::test]
#[serial]
async fn mixed_read_and_delete_requests_ask_for_one_action_without_mutating()
-> Result<(), ScenarioFailure> {
    let mut scenario = Scenario::new(
        "mixed destructive and read intents do not silently choose a mutating route",
        ExpenseWorld::new().await.expect("world should initialize"),
    );

    scenario
        .given("an expense already exists")
        .perform(|world| {
            Box::pin(async move {
                create_expense(world, "fixtures/mixed-acme.jpg", "Acme Lunch", "12.50").await
            })
        })
        .await?;

    scenario
        .when("the user asks to both show and delete the same expense in one message")
        .perform(|world| {
            Box::pin(async move {
                world.send_text("show and delete Acme Lunch").await?;
                Ok(())
            })
        })
        .await?;

    scenario
        .then("the automation asks for one action at a time and leaves the ledger unchanged")
        .perform(|world| {
            Box::pin(async move {
                world
                    .expect_reply_contains("one expense action at a time")
                    .await?;
                let markdown = world.only_markdown()?;
                if markdown.contains("status = \"deleted\"") {
                    return Err(AutomationsError::State(
                        "mixed intent request should not delete the expense".to_string(),
                    ));
                }
                Ok(())
            })
        })
        .await?;

    scenario
        .when("the user subsequently reads the ledger")
        .perform(|world| {
            Box::pin(async move {
                world.send_text("show my expenses").await?;
                Ok(())
            })
        })
        .await?;

    scenario
        .then("the expense is still present")
        .perform(|world| {
            Box::pin(async move {
                world.expect_reply_contains("Acme Lunch").await?;
                Ok(())
            })
        })
        .await?;

    Ok(())
}

#[tokio::test]
#[serial]
async fn merchant_names_that_look_like_commands_are_not_misrouted() -> Result<(), ScenarioFailure> {
    let mut scenario = Scenario::new(
        "merchant names containing command words remain readable without triggering mutations",
        ExpenseWorld::new().await.expect("world should initialize"),
    );

    scenario
        .given("an expense exists whose merchant name contains a command verb")
        .perform(|world| {
            Box::pin(async move {
                create_expense(world, "fixtures/delete-cafe.jpg", "Delete Cafe", "8.50").await
            })
        })
        .await?;

    scenario
        .when("the user asks to read that expense by merchant name")
        .perform(|world| {
            Box::pin(async move {
                world.send_text("show Delete Cafe").await?;
                Ok(())
            })
        })
        .await?;

    scenario
        .then("the automation reads the expense instead of deleting it")
        .perform(|world| {
            Box::pin(async move {
                world.expect_reply_contains("Delete Cafe").await?;
                let markdown = world.only_markdown()?;
                if markdown.contains("status = \"deleted\"") {
                    return Err(AutomationsError::State(
                        "reading a merchant named Delete Cafe should not soft-delete it"
                            .to_string(),
                    ));
                }
                Ok(())
            })
        })
        .await?;

    Ok(())
}

#[tokio::test]
#[serial]
async fn negated_delete_requests_do_not_mutate_the_ledger() -> Result<(), ScenarioFailure> {
    let mut scenario = Scenario::new(
        "negated destructive turns are handled safely without changing markdown",
        ExpenseWorld::new().await.expect("world should initialize"),
    );

    scenario
        .given("an expense already exists")
        .perform(|world| {
            Box::pin(async move {
                create_expense(world, "fixtures/negated-delete.jpg", "Acme Lunch", "12.50").await
            })
        })
        .await?;

    scenario
        .when("the user negates a delete request")
        .perform(|world| {
            Box::pin(async move {
                world.send_text("don't delete Acme Lunch").await?;
                Ok(())
            })
        })
        .await?;

    scenario
        .then("the automation refuses to mutate the ledger")
        .perform(|world| {
            Box::pin(async move {
                world
                    .expect_reply_contains("won't change the ledger")
                    .await?;
                let markdown = world.only_markdown()?;
                if markdown.contains("status = \"deleted\"") {
                    return Err(AutomationsError::State(
                        "negated delete request should not mutate markdown".to_string(),
                    ));
                }
                Ok(())
            })
        })
        .await?;

    scenario
        .when("the user reads the expenses afterward")
        .perform(|world| {
            Box::pin(async move {
                world.send_text("show my expenses").await?;
                Ok(())
            })
        })
        .await?;

    scenario
        .then("the expense is still active")
        .perform(|world| {
            Box::pin(async move {
                world.expect_reply_contains("Acme Lunch").await?;
                Ok(())
            })
        })
        .await?;

    Ok(())
}

#[tokio::test]
#[serial]
async fn question_form_amount_answers_do_not_complete_clarifications() -> Result<(), ScenarioFailure>
{
    let mut scenario = Scenario::new(
        "question-shaped clarification answers do not finalize an expense save",
        ExpenseWorld::with_custom_ocr("Acme Lunch\n")
            .await
            .expect("world should initialize"),
    );

    scenario
        .when("a receipt photo yields a merchant but still needs an amount")
        .perform(|world| {
            Box::pin(async move {
                world
                    .send_photo("fixtures/merchant-only.jpg", Some("receipt"))
                    .await?;
                Ok(())
            })
        })
        .await?;

    scenario
        .then("the automation asks for the missing amount")
        .perform(|world| {
            Box::pin(async move {
                world
                    .expect_reply_contains("I think that is an expense")
                    .await?;
                Ok(())
            })
        })
        .await?;

    scenario
        .when("the user answers with a question instead of a direct amount")
        .perform(|world| {
            Box::pin(async move {
                world.send_text("Is 12.50 okay?").await?;
                Ok(())
            })
        })
        .await?;

    scenario
        .then("the clarification stays open and nothing is written yet")
        .perform(|world| {
            Box::pin(async move {
                world.expect_reply_contains("direct amount answer").await?;
                if !world.markdown_files()?.is_empty() {
                    return Err(AutomationsError::State(
                        "question-form amount should not complete the save".to_string(),
                    ));
                }
                Ok(())
            })
        })
        .await?;

    scenario
        .when("the user later answers with a direct amount")
        .perform(|world| {
            Box::pin(async move {
                world.send_text("12.50").await?;
                Ok(())
            })
        })
        .await?;

    scenario
        .then("the original draft is saved exactly once")
        .perform(|world| {
            Box::pin(async move {
                world.expect_reply_contains("Saved expense").await?;
                let markdown = world.only_markdown()?;
                if !markdown.contains("Acme Lunch") || !markdown.contains("12.50") {
                    return Err(AutomationsError::State(format!(
                        "unexpected markdown contents after direct amount answer: {markdown}"
                    )));
                }
                Ok(())
            })
        })
        .await?;

    Ok(())
}

#[tokio::test]
#[serial]
async fn cancellation_answers_clear_pending_clarifications_without_saving()
-> Result<(), ScenarioFailure> {
    let mut scenario = Scenario::new(
        "cancellation text abandons a pending receipt draft instead of creating nonsense markdown",
        ExpenseWorld::with_custom_ocr("SGD 12.50\n")
            .await
            .expect("world should initialize"),
    );

    scenario
        .when("a receipt photo yields an amount but still needs a merchant")
        .perform(|world| {
            Box::pin(async move {
                world
                    .send_photo("fixtures/amount-only.jpg", Some("receipt"))
                    .await?;
                Ok(())
            })
        })
        .await?;

    scenario
        .then("the automation asks for the missing merchant")
        .perform(|world| {
            Box::pin(async move {
                world
                    .expect_reply_contains("I think that is an expense")
                    .await?;
                Ok(())
            })
        })
        .await?;

    scenario
        .when("the user explicitly cancels the draft")
        .perform(|world| {
            Box::pin(async move {
                world.send_text("not an expense").await?;
                Ok(())
            })
        })
        .await?;

    scenario
        .then("the automation clears the clarification and leaves markdown untouched")
        .perform(|world| {
            Box::pin(async move {
                world
                    .expect_reply_contains("won't save that expense")
                    .await?;
                if !world.markdown_files()?.is_empty() {
                    return Err(AutomationsError::State(
                        "cancellation should not write markdown".to_string(),
                    ));
                }
                Ok(())
            })
        })
        .await?;

    scenario
        .when(
            "the user later sends a text turn that would only have completed the old clarification",
        )
        .perform(|world| {
            Box::pin(async move {
                world.send_text("Acme Lunch 12.50").await?;
                Ok(())
            })
        })
        .await?;

    scenario
        .then("baseline chat handles it because the clarification is gone")
        .perform(|world| {
            Box::pin(async move {
                world
                    .expect_reply_contains("baseline fallback: Acme Lunch 12.50")
                    .await?;
                if !world.markdown_files()?.is_empty() {
                    return Err(AutomationsError::State(
                        "cleared clarification should not later save markdown".to_string(),
                    ));
                }
                Ok(())
            })
        })
        .await?;

    Ok(())
}

#[tokio::test]
#[serial]
async fn intruders_cannot_resolve_a_trusted_users_pending_clarification()
-> Result<(), ScenarioFailure> {
    let trusted = TestTelegramActor::new(
        TelegramChatId(77),
        Some(TelegramThreadId(11)),
        Some("trusted_customer".to_string()),
        "Trusted Customer",
    );
    let intruder = TestTelegramActor::new(
        trusted.chat_id,
        trusted.thread_id,
        Some("intruder".to_string()),
        "Intruder",
    );
    let mut scenario = Scenario::new(
        "unauthorized actors cannot complete another user's pending automation flow",
        ExpenseWorld::with_builder(
            default_builder()
                .with_actor(trusted.clone())
                .with_allowed_usernames(UsernameWhitelist::from_usernames(["trusted_customer"])),
        )
        .await
        .expect("world should initialize"),
    );

    scenario
        .when("the trusted user opens a receipt clarification")
        .perform(|world| {
            Box::pin(async move {
                world
                    .send_photo("fixtures/trusted-receipt.jpg", Some("receipt"))
                    .await?;
                Ok(())
            })
        })
        .await?;

    scenario
        .then("the automation asks the trusted user for more detail")
        .perform(|world| {
            Box::pin(async move {
                world
                    .expect_reply_contains("I think that is an expense")
                    .await
            })
        })
        .await?;

    scenario
        .when("an intruder in the same chat tries to answer the clarification")
        .perform(move |world| {
            let intruder = intruder.clone();
            Box::pin(async move {
                world.send_text_from(&intruder, "Acme Lunch 12.50").await?;
                Ok(())
            })
        })
        .await?;

    scenario
        .then("the intruder is rejected and the expense is not saved")
        .perform(|world| {
            Box::pin(async move {
                world
                    .expect_reply_contains("sorry, you're not my customer")
                    .await?;
                if !world.markdown_files()?.is_empty() {
                    return Err(AutomationsError::State(
                        "intruder should not be able to save markdown".to_string(),
                    ));
                }
                Ok(())
            })
        })
        .await?;

    scenario
        .when("the trusted user answers the original clarification")
        .perform(|world| {
            Box::pin(async move {
                world.send_text("Acme Lunch 12.50").await?;
                Ok(())
            })
        })
        .await?;

    scenario
        .then("the original draft is saved exactly once")
        .perform(|world| {
            Box::pin(async move {
                world.expect_reply_contains("Saved expense").await?;
                if world.markdown_files()?.len() != 1 {
                    return Err(AutomationsError::State(
                        "expected exactly one saved markdown expense".to_string(),
                    ));
                }
                Ok(())
            })
        })
        .await?;

    Ok(())
}

#[tokio::test]
#[serial]
async fn corrected_receipt_photo_can_update_an_existing_expense_after_confirmation() {
    let provider_tempdir = tempfile::tempdir().expect("tempdir");
    let ocr_script = provider_tempdir.path().join("fake-ocr.sh");
    write_executable(
        &ocr_script,
        "#!/bin/sh\nprintf 'Acme Lunch\\nSGD 12.50\\n'\n",
    );

    let harness = TelegramChatHarnessBuilder::new()
        .with_baseline_backend(ScriptedConversationBackend::new(|prompt| async move {
            format!("baseline fallback: {prompt}")
        }))
        .with_automation_host_context(json!({
            "ocr_executable": ocr_script.display().to_string(),
        }))
        .build()
        .await
        .expect("harness should build");

    let original_photo = harness
        .write_photo_fixture("fixtures/original-receipt.jpg", b"fixture-image")
        .expect("fixture should write");
    harness
        .send_photo(original_photo, Some("receipt".to_string()))
        .await
        .expect("photo should send");
    let initial_reply = harness.recv_reply().await.expect("reply should arrive");
    assert!(
        initial_reply.contains("Saved expense") && initial_reply.contains("12.50"),
        "expected initial expense save, got {initial_reply:?}"
    );

    write_executable(
        &ocr_script,
        "#!/bin/sh\nprintf 'Acme Lunch\\nSGD 14.20\\n'\n",
    );
    let corrected_photo = harness
        .write_photo_fixture("fixtures/corrected-receipt.jpg", b"fixture-image")
        .expect("fixture should write");
    harness
        .send_photo(corrected_photo, Some("receipt".to_string()))
        .await
        .expect("photo should send");
    let clarification = harness.recv_reply().await.expect("reply should arrive");
    assert!(
        clarification.contains("Should I update that expense or save this as a new one?"),
        "expected update clarification, got {clarification:?}"
    );

    harness
        .send_text("update it")
        .await
        .expect("clarification answer should send");
    let update_reply = harness.recv_reply().await.expect("reply should arrive");
    assert!(
        update_reply.contains("Updated expense") && update_reply.contains("14.20"),
        "expected updated expense reply, got {update_reply:?}"
    );

    harness
        .send_text("show my expenses")
        .await
        .expect("read should send");
    let read_reply = harness.recv_reply().await.expect("reply should arrive");
    assert!(
        read_reply.contains("14.20") && !read_reply.contains("12.50"),
        "expected updated amount in ledger reply, got {read_reply:?}"
    );
    assert_eq!(
        harness
            .expense_markdown_files()
            .expect("markdown files")
            .len(),
        1,
        "expected update path to keep a single markdown expense"
    );
}

#[tokio::test]
#[serial]
async fn photo_expense_flow_still_works_when_ocr_writes_to_stdout() {
    let provider_tempdir = tempfile::tempdir().expect("tempdir");
    let ocr_script = provider_tempdir.path().join("fake-ocr.sh");
    write_executable(
        &ocr_script,
        "#!/bin/sh\nprintf 'Tea Time, Singapore, SG\\nSGD 48.18\\n'\n",
    );

    let harness = TelegramChatHarnessBuilder::new()
        .with_baseline_backend(ScriptedConversationBackend::new(|prompt| async move {
            format!("baseline fallback: {prompt}")
        }))
        .with_automation_host_context(json!({
            "ocr_executable": ocr_script.display().to_string(),
        }))
        .build()
        .await
        .expect("harness should build");
    let photo = harness
        .write_photo_fixture("fixtures/wallet.jpg", b"fixture-image")
        .expect("fixture should write");

    harness
        .send_photo(photo, Some("had these expenses today".to_string()))
        .await
        .expect("photo should send");
    let reply = harness.recv_reply().await.expect("reply should arrive");

    assert!(
        reply.contains("Saved expense") && reply.contains("48.18"),
        "expected saved expense reply after OCR stdout capture, got {reply:?}"
    );
}

fn write_executable(path: &Path, contents: &str) {
    fs::write(path, contents).expect("script should write");
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;

        let mut permissions = fs::metadata(path).expect("metadata").permissions();
        permissions.set_mode(0o755);
        fs::set_permissions(path, permissions).expect("chmod");
    }
}

fn shell_escape(raw: &str) -> String {
    raw.replace('\'', "'\"'\"'")
}
