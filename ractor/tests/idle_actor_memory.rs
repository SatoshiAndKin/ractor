//! Measure retained allocations of real linked actors in an isolated test process.
#![cfg(all(not(target_arch = "wasm32"), not(feature = "async-std")))]

use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use ractor::{Actor, ActorProcessingErr, ActorRef, SupervisionEvent};
use tokio::sync::oneshot;

struct CountingSystem;
static LIVE_BYTES: AtomicUsize = AtomicUsize::new(0);

// SAFETY: Every allocation operation delegates to System with its original layout.
// The counter observes requested bytes; it never changes allocation ownership.
unsafe impl GlobalAlloc for CountingSystem {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let ptr = unsafe { System.alloc(layout) };
        if !ptr.is_null() {
            LIVE_BYTES.fetch_add(layout.size(), Ordering::Relaxed);
        }
        ptr
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { System.dealloc(ptr, layout) };
        LIVE_BYTES.fetch_sub(layout.size(), Ordering::Relaxed);
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, size: usize) -> *mut u8 {
        let new_ptr = unsafe { System.realloc(ptr, layout, size) };
        if !new_ptr.is_null() {
            LIVE_BYTES.fetch_add(size, Ordering::Relaxed);
            LIVE_BYTES.fetch_sub(layout.size(), Ordering::Relaxed);
        }
        new_ptr
    }
}

#[global_allocator]
static ALLOCATOR: CountingSystem = CountingSystem;

struct Idle;
struct Empty;
#[cfg(feature = "cluster")]
impl ractor::Message for Empty {}

#[cfg_attr(feature = "async-trait", ractor::async_trait)]
impl Actor for Idle {
    type Msg = Empty;
    type State = ();
    type Arguments = ();

    async fn pre_start(&self, _: ActorRef<Empty>, _: ()) -> Result<(), ActorProcessingErr> {
        Ok(())
    }

    async fn handle_supervisor_evt(
        &self,
        _: ActorRef<Empty>,
        _: SupervisionEvent,
        _: &mut (),
    ) -> Result<(), ActorProcessingErr> {
        Ok(())
    }
}

#[tokio::test]
async fn idle_linked_actors_fit_the_retained_heap_budget() {
    const ACTORS: usize = 1024;
    // Keep the per-actor infrastructure below 4 KiB, including both empty
    // queues, lifecycle ports, task, actor properties, and supervision links.
    // This measures allocations, not type sizes or process RSS.
    const BYTES_PER_ACTOR: usize = 4096;
    let (parent, parent_task) = Actor::spawn(None, Idle, ()).await.unwrap();
    let (warm, warm_task) = Actor::spawn_linked(None, Idle, (), parent.get_cell())
        .await
        .unwrap();
    warm.stop_and_wait(None, None).await.unwrap();
    warm_task.await.unwrap();
    drop(warm);
    let mut actors = Vec::with_capacity(ACTORS);
    let before = LIVE_BYTES.load(Ordering::Relaxed);
    for _ in 0..ACTORS {
        actors.push(
            Actor::spawn_linked(None, Idle, (), parent.get_cell())
                .await
                .unwrap(),
        );
    }
    let retained = LIVE_BYTES.load(Ordering::Relaxed).saturating_sub(before);
    eprintln!(
        "{ACTORS} linked actors retain {retained} requested bytes ({} per actor)",
        retained / ACTORS
    );
    for (actor, task) in actors {
        actor.stop_and_wait(None, None).await.unwrap();
        task.await.unwrap();
    }
    parent.stop_and_wait(None, None).await.unwrap();
    parent_task.await.unwrap();
    assert!(
        retained <= ACTORS * BYTES_PER_ACTOR,
        "idle actors retain {retained} bytes; budget is {}",
        ACTORS * BYTES_PER_ACTOR
    );
}

struct Resident;
struct ReadState(oneshot::Sender<(u64, u8)>);
#[cfg(feature = "cluster")]
impl ractor::Message for ReadState {}

#[repr(align(16))]
struct ResidentState {
    bytes: [u8; 448],
    replies: u64,
    stopped: Arc<AtomicUsize>,
}

#[cfg_attr(feature = "async-trait", ractor::async_trait)]
impl Actor for Resident {
    type Msg = ReadState;
    type State = ResidentState;
    type Arguments = (u8, Arc<AtomicUsize>);

    async fn pre_start(
        &self,
        _: ActorRef<ReadState>,
        (seed, stopped): Self::Arguments,
    ) -> Result<ResidentState, ActorProcessingErr> {
        Ok(ResidentState {
            bytes: [seed; 448],
            replies: 0,
            stopped,
        })
    }

    async fn handle(
        &self,
        _: ActorRef<ReadState>,
        ReadState(reply): ReadState,
        state: &mut ResidentState,
    ) -> Result<(), ActorProcessingErr> {
        state.replies += 1;
        assert!(state.bytes.iter().all(|byte| *byte == state.bytes[0]));
        reply.send((state.replies, state.bytes[0])).unwrap();
        Ok(())
    }

    async fn handle_supervisor_evt(
        &self,
        _: ActorRef<ReadState>,
        _: SupervisionEvent,
        _: &mut ResidentState,
    ) -> Result<(), ActorProcessingErr> {
        Ok(())
    }

    async fn post_stop(
        &self,
        _: ActorRef<ReadState>,
        state: &mut ResidentState,
    ) -> Result<(), ActorProcessingErr> {
        assert_eq!(state.replies, 2);
        assert!(state.bytes.iter().all(|byte| *byte == state.bytes[0]));
        state.stopped.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }
}

async fn read_state(actor: &ActorRef<ReadState>) -> (u64, u8) {
    let (send, receive) = oneshot::channel();
    actor.cast(ReadState(send)).unwrap();
    receive.await.unwrap()
}

#[tokio::test]
async fn started_stateful_actors_fit_the_retained_heap_budget() {
    const ACTORS: usize = 1024;
    // Include the real running loop and its state, ports, task, queues and
    // supervision links. A reply proves post_start and message handling ran.
    // Production's instrumented Tokio ports retain additional tracing state.
    // Cluster support also enlarges the message and supervision variants.
    // The non-cluster budgets fail on the pinned implementation; the cluster
    // configuration is a lifecycle and retained-allocation regression control.
    const BYTES_PER_ACTOR: usize = 4096
        + if cfg!(tokio_unstable) { 512 } else { 0 }
        + if cfg!(feature = "cluster") { 1024 } else { 0 };
    let stopped = Arc::new(AtomicUsize::new(0));
    let (parent, parent_task) = Actor::spawn(None, Resident, (0, stopped.clone()))
        .await
        .unwrap();
    assert_eq!(read_state(&parent).await, (1, 0));
    let (warm, warm_task) =
        Actor::spawn_linked(None, Resident, (255, stopped.clone()), parent.get_cell())
            .await
            .unwrap();
    assert_eq!(read_state(&warm).await, (1, 255));
    assert_eq!(read_state(&warm).await, (2, 255));
    warm.stop_and_wait(None, None).await.unwrap();
    warm_task.await.unwrap();
    drop(warm);
    let mut actors = Vec::with_capacity(ACTORS);
    let before = LIVE_BYTES.load(Ordering::Relaxed);
    for index in 0..ACTORS {
        let seed = (index % 251) as u8;
        let (actor, task) =
            Actor::spawn_linked(None, Resident, (seed, stopped.clone()), parent.get_cell())
                .await
                .unwrap();
        assert_eq!(read_state(&actor).await, (1, seed));
        actors.push((actor, task));
    }
    // Supervision has priority over ordinary messages, so this also drains
    // queued ActorStarted notifications before taking the allocation snapshot.
    assert_eq!(read_state(&parent).await, (2, 0));
    let retained = LIVE_BYTES.load(Ordering::Relaxed).saturating_sub(before);
    eprintln!("{ACTORS} running stateful actors retain {retained} requested bytes");
    for (index, (actor, task)) in actors.into_iter().enumerate() {
        assert_eq!(read_state(&actor).await, (2, (index % 251) as u8));
        actor.stop_and_wait(None, None).await.unwrap();
        task.await.unwrap();
    }
    parent.stop_and_wait(None, None).await.unwrap();
    parent_task.await.unwrap();
    assert_eq!(stopped.load(Ordering::Relaxed), ACTORS + 2);
    assert!(
        retained <= ACTORS * BYTES_PER_ACTOR,
        "running stateful actors retain {retained} bytes; budget is {}",
        ACTORS * BYTES_PER_ACTOR
    );
}
