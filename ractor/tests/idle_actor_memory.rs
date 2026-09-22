//! Measure retained allocations of real linked actors in an isolated test process.
#![cfg(all(not(target_arch = "wasm32"), not(feature = "async-std")))]

use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicUsize, Ordering};

use ractor::{Actor, ActorProcessingErr, ActorRef, SupervisionEvent};

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
