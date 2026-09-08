//! Per-thread outbound: everything that talks to a chat, off the core's task.
//!
//! The core is one `select!` loop, so anything it awaits, every other thread
//! waits for too. Channel I/O is the slow part — a request to
//! `api.telegram.org`, and on a 429 a wait measured in seconds — and awaiting
//! it inline meant one busy chat stalled every other chat on every channel,
//! including the ones that were not rate limited and the agent events that had
//! nothing to do with it.
//!
//! So each thread gets one of these: a task that owns the conversation with the
//! chat, and a queue the core drops work into without waiting. A thread that is
//! rate limited now delays only itself, which is the isolation the product
//! always claimed ("one turn per thread") but the transport did not have.
//!
//! It owns the message ids too, which is what makes that possible. "Grow the
//! turn's message" is a job, not a value the core has to hold and hand back, so
//! nothing in the core has to await a `send` to learn what to edit next.

use std::sync::Arc;

use anyhow::Result;
use tokio::sync::mpsc;

use crate::channel::{Channel, MessageId, RateLimited, ThreadKey};

/// How many jobs may queue for one thread.
///
/// Deep enough that a turn's flushes, its questions and its notes never fill
/// it; shallow enough that a thread whose channel is wedged does not grow an
/// unbounded backlog of text nobody will ever read.
const QUEUE_DEPTH: usize = 64;

/// How many times to wait out a rate limit before giving up on one message.
///
/// Telegram says exactly how long to wait, and waiting is the correct response
/// — the previous code waited and then threw the message away, which is the
/// cost without the benefit.
const RATE_LIMIT_ATTEMPTS: usize = 4;

/// One piece of outbound work for a thread.
pub enum OutJob {
    /// The turn's message as it should now read, in full.
    ///
    /// Idempotent by design: the worker compares it against what is already
    /// shown and edits, or posts the first one. A dropped job costs nothing
    /// because the next one carries the whole text again.
    Turn(String),

    /// Begin a new turn: stop growing the message the last one was using.
    NewTurn,

    /// A standalone note, outside any turn.
    Say(String),

    /// Ask for a decision, remembering the message so it can be settled.
    Ask { text: String, question: String },

    /// Retire the outstanding question with its outcome.
    Settle {
        verdict: String,
        /// The tap to acknowledge, when a button produced this.
        ack: Option<String>,
    },

    /// Acknowledge a tap that settled nothing, so the client stops spinning.
    Ack { ack: String, note: String },

    /// Show, or stop showing, that the agent is working.
    Typing { on: bool },
}

/// The core's handle on one thread's outbound task.
pub struct Outbox {
    jobs: mpsc::Sender<OutJob>,
}

impl Outbox {
    pub fn spawn(key: ThreadKey, channel: Arc<dyn Channel>) -> Self {
        let (jobs, rx) = mpsc::channel(QUEUE_DEPTH);
        tokio::spawn(run(key, channel, rx));
        Self { jobs }
    }

    /// Queue a job without waiting for it.
    ///
    /// Returns false if this thread's outbound is so far behind that the queue
    /// is full, which the caller may need to know: a [`OutJob::Turn`] can be
    /// dropped safely because the next one repeats the whole message, but text
    /// the core has already consumed cannot.
    pub fn queue(&self, job: OutJob) -> bool {
        match self.jobs.try_send(job) {
            Ok(()) => true,
            Err(e) => {
                tracing::warn!("outbound queue is full, dropping a message: {e}");
                false
            }
        }
    }
}

async fn run(key: ThreadKey, channel: Arc<dyn Channel>, mut jobs: mpsc::Receiver<OutJob>) {
    // The message this turn is growing, and what it currently says.
    let mut turn: Option<MessageId> = None;
    let mut shown = String::new();
    // The outstanding question's message, so it can be rewritten in place.
    let mut question: Option<MessageId> = None;

    while let Some(job) = jobs.recv().await {
        match job {
            OutJob::NewTurn => {
                turn = None;
                shown.clear();
            }

            OutJob::Turn(text) => {
                if text == shown {
                    continue;
                }
                match &turn {
                    Some(id) => {
                        if report(
                            &key,
                            "edit",
                            retrying(|| channel.edit(&key, id, &text)).await,
                        )
                        .is_some()
                        {
                            shown = text;
                        }
                    }
                    None => {
                        if let Some(id) =
                            report(&key, "send", retrying(|| channel.send(&key, &text)).await)
                        {
                            turn = Some(id);
                            shown = text;
                        }
                    }
                }
            }

            OutJob::Say(text) => {
                report(&key, "send", retrying(|| channel.send(&key, &text)).await);
            }

            OutJob::Ask {
                text,
                question: token,
            } => {
                question = report(
                    &key,
                    "ask_permission",
                    retrying(|| channel.ask_permission(&key, &text, &token)).await,
                );
            }

            OutJob::Settle { verdict, ack } => {
                if let Some(ack) = ack {
                    report(
                        &key,
                        "ack_decision",
                        retrying(|| channel.ack_decision(&ack, &verdict)).await,
                    );
                }

                // Where messages can be rewritten, retire the buttons and
                // record the outcome in place. Where they cannot, say it in a
                // new message — otherwise a tap looks like it did nothing.
                match (channel.can_edit(), question.take()) {
                    (true, Some(id)) => {
                        report(
                            &key,
                            "edit",
                            retrying(|| channel.edit(&key, &id, &verdict)).await,
                        );
                    }
                    _ => {
                        report(
                            &key,
                            "send",
                            retrying(|| channel.send(&key, &verdict)).await,
                        );
                    }
                }
            }

            OutJob::Ack { ack, note } => {
                report(
                    &key,
                    "ack_decision",
                    retrying(|| channel.ack_decision(&ack, &note)).await,
                );
            }

            // The one job that is not worth waiting out a rate limit for.
            // Retrying would park this task for the platform's whole retry
            // window with the turn's actual text queued behind an indicator
            // that will be stale by the time it lands — and another one is due
            // in a few seconds anyway.
            OutJob::Typing { on } => {
                report(&key, "typing", channel.typing(&key, on).await);
            }
        }
    }
}

/// Run one channel call, waiting out a rate limit rather than losing the
/// message.
///
/// The wait happens here, on this thread's task, which is the whole point: the
/// old code slept inside the shared core loop and then threw the message away
/// regardless, so a 429 was both blocking and lossy.
async fn retrying<F, Fut, T>(mut call: F) -> Result<T>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<T>>,
{
    let mut last = None;
    for attempt in 1..=RATE_LIMIT_ATTEMPTS {
        match call().await {
            Ok(value) => return Ok(value),
            Err(e) => match e.downcast_ref::<RateLimited>() {
                Some(limit) => {
                    let wait = limit.retry_after;
                    tracing::warn!(
                        "rate limited, waiting {}s (attempt {attempt} of {RATE_LIMIT_ATTEMPTS})",
                        wait.as_secs()
                    );
                    tokio::time::sleep(wait).await;
                    last = Some(e);
                }
                None => return Err(e),
            },
        }
    }
    Err(last.unwrap_or_else(|| anyhow::anyhow!("gave up after {RATE_LIMIT_ATTEMPTS} attempts")))
}

/// Log what failed, with the thread it belongs to.
///
/// Nothing above this can act on the error — the chat *is* the user interface,
/// so a failure to post is a failure to tell anyone anything. Journald is where
/// it can still be seen.
fn report<T>(key: &ThreadKey, what: &str, result: Result<T>) -> Option<T> {
    match result {
        Ok(value) => Some(value),
        Err(e) => {
            tracing::warn!(thread = %key, "{what} failed: {e:#}");
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Mutex;
    use std::time::Duration;

    use async_trait::async_trait;

    /// A channel that records what it was asked to do, and can be told to rate
    /// limit the first few calls the way Telegram does.
    struct FakeChannel {
        sent: Arc<Mutex<Vec<String>>>,
        edits: Arc<Mutex<Vec<(MessageId, String)>>>,
        rate_limits_left: AtomicUsize,
        retry_after: Duration,
        can_edit: bool,
    }

    impl FakeChannel {
        fn new() -> Arc<Self> {
            Arc::new(Self {
                sent: Arc::new(Mutex::new(Vec::new())),
                edits: Arc::new(Mutex::new(Vec::new())),
                rate_limits_left: AtomicUsize::new(0),
                retry_after: Duration::from_millis(0),
                can_edit: true,
            })
        }

        /// Refuse the first `times` calls with a rate limit, as Telegram would.
        fn rate_limiting(times: usize, retry_after: Duration) -> Arc<Self> {
            Arc::new(Self {
                sent: Arc::new(Mutex::new(Vec::new())),
                edits: Arc::new(Mutex::new(Vec::new())),
                rate_limits_left: AtomicUsize::new(times),
                retry_after,
                can_edit: true,
            })
        }

        fn rate_limit_if_owed(&self) -> Result<()> {
            if self
                .rate_limits_left
                .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |n| n.checked_sub(1))
                .is_ok()
            {
                return Err(RateLimited {
                    retry_after: self.retry_after,
                }
                .into());
            }
            Ok(())
        }

        fn sent(&self) -> Vec<String> {
            self.sent.lock().unwrap().clone()
        }
    }

    #[async_trait]
    impl Channel for FakeChannel {
        fn name(&self) -> &'static str {
            "fake"
        }

        fn can_edit(&self) -> bool {
            self.can_edit
        }

        async fn send(&self, _thread: &ThreadKey, text: &str) -> Result<MessageId> {
            self.rate_limit_if_owed()?;
            let mut sent = self.sent.lock().unwrap();
            sent.push(text.to_string());
            Ok(format!("m{}", sent.len()))
        }

        async fn edit(&self, _thread: &ThreadKey, id: &MessageId, text: &str) -> Result<()> {
            self.rate_limit_if_owed()?;
            self.edits
                .lock()
                .unwrap()
                .push((id.clone(), text.to_string()));
            Ok(())
        }

        async fn ask_permission(
            &self,
            thread: &ThreadKey,
            text: &str,
            _question: &str,
        ) -> Result<MessageId> {
            self.send(thread, text).await
        }
    }

    fn key(chat: &str) -> ThreadKey {
        ThreadKey {
            channel: "fake",
            chat_id: chat.to_string(),
            topic_id: None,
        }
    }

    /// Poll until `check` passes or the deadline expires.
    async fn eventually(deadline: Duration, mut check: impl FnMut() -> bool) -> bool {
        let start = std::time::Instant::now();
        while start.elapsed() < deadline {
            if check() {
                return true;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        check()
    }

    #[tokio::test]
    async fn a_turn_grows_one_message_instead_of_posting_many() {
        let channel = FakeChannel::new();
        let outbox = Outbox::spawn(key("1"), channel.clone());

        outbox.queue(OutJob::Turn("wor".into()));
        outbox.queue(OutJob::Turn("working".into()));
        outbox.queue(OutJob::Turn("working, done".into()));

        assert!(
            eventually(Duration::from_secs(2), || channel
                .edits
                .lock()
                .unwrap()
                .len()
                == 2)
            .await,
            "expected the first to post and the rest to edit it"
        );
        assert_eq!(channel.sent(), vec!["wor"]);
        let edits = channel.edits.lock().unwrap().clone();
        assert!(edits.iter().all(|(id, _)| id == "m1"), "all one message");
        assert_eq!(edits.last().unwrap().1, "working, done");
    }

    #[tokio::test]
    async fn a_rate_limited_message_is_waited_out_not_thrown_away() {
        // The old behaviour was to sleep and then bail, so the wait happened
        // and the message was lost anyway.
        let channel = FakeChannel::rate_limiting(2, Duration::from_millis(20));
        let outbox = Outbox::spawn(key("1"), channel.clone());

        outbox.queue(OutJob::Say("this must arrive".into()));

        assert!(
            eventually(Duration::from_secs(2), || !channel.sent().is_empty()).await,
            "the message should have been retried until it landed"
        );
        assert_eq!(channel.sent(), vec!["this must arrive"]);
    }

    #[tokio::test]
    async fn one_thread_waiting_out_a_rate_limit_does_not_delay_another() {
        // The finding this whole module exists for: the wait used to happen
        // inside the core's one loop, so a single rate-limited chat stopped
        // every other chat on every other channel.
        let slow = FakeChannel::rate_limiting(1, Duration::from_millis(600));
        let quick = FakeChannel::new();

        let stalled = Outbox::spawn(key("stalled"), slow.clone());
        let other = Outbox::spawn(key("other"), quick.clone());

        stalled.queue(OutJob::Say("held up behind a 429".into()));
        other.queue(OutJob::Say("not my problem".into()));

        // The unaffected thread gets through while the other is still waiting.
        assert!(
            eventually(Duration::from_millis(200), || !quick.sent().is_empty()).await,
            "an unrelated thread must not wait out someone else's rate limit"
        );
        assert!(
            slow.sent().is_empty(),
            "the rate-limited thread should still be waiting"
        );

        // And it does arrive, once its own wait is over.
        assert!(
            eventually(Duration::from_secs(2), || !slow.sent().is_empty()).await,
            "the delayed message still has to land"
        );
    }

    #[tokio::test]
    async fn a_new_turn_stops_growing_the_previous_message() {
        let channel = FakeChannel::new();
        let outbox = Outbox::spawn(key("1"), channel.clone());

        outbox.queue(OutJob::Turn("first turn".into()));
        assert!(eventually(Duration::from_secs(2), || channel.sent().len() == 1).await);

        outbox.queue(OutJob::NewTurn);
        outbox.queue(OutJob::Turn("second turn".into()));

        assert!(
            eventually(Duration::from_secs(2), || channel.sent().len() == 2).await,
            "a new turn posts its own message rather than rewriting the last"
        );
        assert_eq!(channel.sent(), vec!["first turn", "second turn"]);
        assert!(channel.edits.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn a_settled_question_is_rewritten_in_place_where_that_is_possible() {
        let channel = FakeChannel::new();
        let outbox = Outbox::spawn(key("1"), channel.clone());

        outbox.queue(OutJob::Ask {
            text: "Run Bash?".into(),
            question: "a1b2c3d4".into(),
        });
        assert!(eventually(Duration::from_secs(2), || channel.sent().len() == 1).await);

        outbox.queue(OutJob::Settle {
            verdict: "Allowed Bash".into(),
            ack: None,
        });

        assert!(
            eventually(Duration::from_secs(2), || !channel
                .edits
                .lock()
                .unwrap()
                .is_empty())
            .await,
            "the buttons should be retired on the question itself"
        );
        assert_eq!(channel.edits.lock().unwrap()[0].1, "Allowed Bash");
    }
}
