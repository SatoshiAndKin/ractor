//! Public delivery and supervision contracts for compact internal queues.
#![cfg(all(not(target_arch = "wasm32"), not(feature = "async-std")))]

use std::sync::{Arc, Barrier};
use std::time::Duration;

use ractor::{Actor, ActorProcessingErr, ActorRef, MessagingErr, SupervisionEvent};
use tokio::sync::mpsc;

#[derive(Debug)]
struct Payload {
    producer: usize,
    sequence: usize,
    owned: Arc<Vec<u8>>,
}
#[cfg(feature = "cluster")]
impl ractor::Message for Payload {}

struct Recorder;
#[cfg_attr(feature = "async-trait", ractor::async_trait)]
impl Actor for Recorder {
    type Msg = Payload;
    type State = mpsc::UnboundedSender<Payload>;
    type Arguments = Self::State;

    async fn pre_start(
        &self,
        _: ActorRef<Payload>,
        output: Self::State,
    ) -> Result<Self::State, ActorProcessingErr> {
        Ok(output)
    }

    async fn handle(
        &self,
        _: ActorRef<Payload>,
        msg: Payload,
        output: &mut Self::State,
    ) -> Result<(), ActorProcessingErr> {
        output.send(msg).unwrap();
        Ok(())
    }
}

struct Supervisor;
#[cfg_attr(feature = "async-trait", ractor::async_trait)]
impl Actor for Supervisor {
    type Msg = Payload;
    type State = mpsc::UnboundedSender<SupervisionEvent>;
    type Arguments = Self::State;

    async fn pre_start(
        &self,
        _: ActorRef<Payload>,
        output: Self::State,
    ) -> Result<Self::State, ActorProcessingErr> {
        Ok(output)
    }

    async fn handle_supervisor_evt(
        &self,
        _: ActorRef<Payload>,
        event: SupervisionEvent,
        output: &mut Self::State,
    ) -> Result<(), ActorProcessingErr> {
        output.send(event).unwrap();
        Ok(())
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn concurrent_sends_and_drain_preserve_every_accepted_message_in_producer_order() {
    let (tx, mut rx) = mpsc::unbounded_channel();
    let (actor, task) = Actor::spawn(None, Recorder, tx).await.unwrap();
    let owned = Arc::new(vec![19; 4096]);
    // Ensure the test also crosses several queue blocks before racing drain.
    for sequence in 0..96 {
        actor
            .cast(Payload {
                producer: 4,
                sequence,
                owned: owned.clone(),
            })
            .unwrap();
    }
    let barrier = Arc::new(Barrier::new(5));
    let senders: Vec<_> = (0..4)
        .map(|producer| {
            let actor = actor.clone();
            let barrier = barrier.clone();
            let owned = owned.clone();
            std::thread::spawn(move || {
                barrier.wait();
                let mut accepted = Vec::new();
                for sequence in 0..1024 {
                    match actor.cast(Payload {
                        producer,
                        sequence,
                        owned: owned.clone(),
                    }) {
                        Ok(()) => accepted.push(sequence),
                        Err(MessagingErr::SendErr(message)) => {
                            assert_eq!((message.producer, message.sequence), (producer, sequence));
                            assert!(Arc::ptr_eq(&owned, &message.owned));
                        }
                        Err(error) => panic!("Unexpected send error: {error}"),
                    }
                }
                accepted
            })
        })
        .collect();
    barrier.wait();
    actor
        .drain_and_wait(Some(Duration::from_secs(5)))
        .await
        .unwrap();
    task.await.unwrap();
    let mut expected: Vec<_> = senders
        .into_iter()
        .map(|sender| sender.join().unwrap())
        .collect();
    expected.push((0..96).collect());
    let mut observed = vec![Vec::new(); 5];
    while let Some(message) = rx.recv().await {
        assert!(Arc::ptr_eq(&owned, &message.owned));
        observed[message.producer].push(message.sequence);
    }
    assert_eq!(observed, expected);
    match actor.cast(Payload {
        producer: 8,
        sequence: 99,
        owned: owned.clone(),
    }) {
        Err(MessagingErr::SendErr(message)) => {
            assert_eq!((message.producer, message.sequence), (8, 99));
            assert!(Arc::ptr_eq(&owned, &message.owned));
        }
        result => panic!("Expected original message after shutdown: {result:?}"),
    }
}

#[tokio::test]
async fn linked_children_report_start_and_termination_once_for_drain_stop_and_kill() {
    let (events, mut rx) = mpsc::unbounded_channel();
    let (parent, parent_task) = Actor::spawn(None, Supervisor, events).await.unwrap();
    let mut ids = Vec::new();
    for mode in 0..3 {
        let (output, mut messages) = mpsc::unbounded_channel();
        let (child, task) = Actor::spawn_linked(None, Recorder, output, parent.get_cell())
            .await
            .unwrap();
        ids.push(child.get_id());
        // A processed message proves startup completed before each shutdown mode.
        child
            .cast(Payload {
                producer: mode,
                sequence: 17,
                owned: Arc::new(vec![3]),
            })
            .unwrap();
        let delivered = tokio::time::timeout(Duration::from_secs(5), messages.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!((delivered.producer, delivered.sequence), (mode, 17));
        match mode {
            0 => child
                .drain_and_wait(Some(Duration::from_secs(5)))
                .await
                .unwrap(),
            1 => child
                .stop_and_wait(Some("queue-test".into()), Some(Duration::from_secs(5)))
                .await
                .unwrap(),
            _ => child
                .kill_and_wait(Some(Duration::from_secs(5)))
                .await
                .unwrap(),
        }
        task.await.unwrap();
    }
    parent
        .drain_and_wait(Some(Duration::from_secs(5)))
        .await
        .unwrap();
    parent_task.await.unwrap();
    let mut started = Vec::new();
    let mut ended = Vec::new();
    while let Some(event) = rx.recv().await {
        match event {
            SupervisionEvent::ActorStarted(actor) => started.push(actor.get_id()),
            SupervisionEvent::ActorTerminated(actor, _, reason) => {
                if actor.get_id() == ids[1] {
                    assert_eq!(reason.as_deref(), Some("queue-test"));
                }
                ended.push(actor.get_id());
            }
            event => panic!("Unexpected supervision event: {event:?}"),
        }
    }
    assert_eq!(started, ids);
    assert_eq!(ended, ids);
}
