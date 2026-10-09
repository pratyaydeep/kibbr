//! The threads a disk scan chooses a day's rows on beside the thread that asked.
//!
//! **A scan reads on the thread it runs on, always, and hands its rows over there.** What
//! it shares out is what lies between: choosing a row group's rows (`disk::scan::pick`)
//! and decoding the one column a fetch is about to ask with. Both need the row group and
//! the question and nothing else, and leave what they made inside the row group. So
//! whether a run of row groups was shared out cannot be seen in an answer or in a counter,
//! and the two reasons for not sharing one out may depend on what else the server is doing.
//!
//! **`Turn::each` blocks the thread that calls it until the work is done.** It is reached
//! from `disk::scan::day` and from nowhere else, and a scan runs on a thread that may
//! block - the `spawn_blocking` thread a query is answered on - and never on a runtime
//! worker, where it would stall every request in flight. The pool itself is started by the
//! first scan that asks for it, so that is on such a thread too.
//!
//! **rayon's code runs in three places, and the third is not a call anyone writes**: the
//! build, in `Fan::pool`; the scope, in `Turn::each`; and the pool's own `Drop`, when the
//! last `Arc<Fan>` goes. The `Drop` tells each thread to end and waits for none. A
//! server keeps a `Fan` of its own until its runtime is gone (`serve::run`), so that the
//! last one is never let go on a runtime thread or under an `async fn`.
//!
//! This is the only file that names rayon, and it uses a pool of its own and a scope on
//! it: not the global pool, and not `par_iter`, whose caller sleeps while the pool works.
//! Here the thread that asked takes row groups too.

use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, OnceLock};

use super::lock;

/// The scan threads of one disk store, and what says whether a scan may use them now.
pub struct Fan {
    /// The pool's threads: one fewer than a scan uses, the thread that asked being one.
    helpers: usize,
    /// A run of row groups holding fewer rows than this, by the index, is chosen in turn.
    least_rows: u64,
    /// Started by the first scan that takes a turn. `None` inside: it could not start.
    pool: OnceLock<Option<rayon::ThreadPool>>,
    /// One scan holds the pool.
    taken: AtomicBool,
    /// Ingest bodies in their task.
    bodies: AtomicUsize,
    /// Runs of row groups shared out so far: for `/stats`, and for a test to know that
    /// what it compared was shared out.
    fanned: AtomicU64,
    /// The threads `each` shared work out from: for the test that holds a scan to a
    /// thread that may block.
    #[cfg(test)]
    entered: Mutex<Vec<std::thread::ThreadId>>,
}

/// The pool, held by one scan for one run of row groups and given back when this goes.
pub(crate) struct Turn<'f>(&'f Fan);

impl Drop for Turn<'_> {
    fn drop(&mut self) {
        self.0.taken.store(false, Ordering::Release);
    }
}

/// An ingest body in its task, from before it is inflated until its answer is ready.
/// While one is held no scan is given the pool: the ingest path keeps its cores.
pub struct Body<'f>(&'f Fan);

impl Drop for Body<'_> {
    fn drop(&mut self) {
        self.0.bodies.fetch_sub(1, Ordering::Relaxed);
    }
}

impl Fan {
    /// The threads one scan may choose rows on, the asking thread among them. `None`
    /// under two: there is then nobody to share with. Starts no thread.
    pub fn new(threads: usize, least_rows: u64) -> Option<Arc<Self>> {
        let helpers = threads.checked_sub(1).filter(|n| *n > 0)?;
        Some(Arc::new(Self {
            helpers,
            least_rows,
            pool: OnceLock::new(),
            taken: AtomicBool::new(false),
            bodies: AtomicUsize::new(0),
            fanned: AtomicU64::new(0),
            #[cfg(test)]
            entered: Mutex::new(Vec::new()),
        }))
    }

    pub fn least_rows(&self) -> u64 {
        self.least_rows
    }

    /// Runs of row groups shared out so far.
    pub fn fanned(&self) -> u64 {
        self.fanned.load(Ordering::Relaxed)
    }

    /// Say that an ingest body is in its task, until what is returned is dropped.
    pub fn body(&self) -> Body<'_> {
        self.bodies.fetch_add(1, Ordering::Relaxed);
        Body(self)
    }

    /// The pool for one run of row groups, or `None`: an ingest body is in its task,
    /// another scan holds it, or it could not start. Never waits on either of the first
    /// two. A scan told no chooses its rows in turn, which is always right, and asks
    /// again at its next run.
    pub(crate) fn turn(&self) -> Option<Turn<'_>> {
        if self.bodies.load(Ordering::Relaxed) != 0 {
            return None;
        }
        // Not a `Mutex`: a panic in a scan would poison it, and every scan after would
        // choose in turn for the life of the process.
        self.taken
            .compare_exchange(false, true, Ordering::Acquire, Ordering::Relaxed)
            .ok()?;
        // Made before the pool is looked for, so that it is given back on every way out.
        let turn = Turn(self);
        self.pool().is_some().then_some(turn)
    }

    /// The pool, started the first time it is asked for. A pool that will not start is
    /// said once and is never a failed query: every scan then chooses in turn.
    fn pool(&self) -> Option<&rayon::ThreadPool> {
        self.pool
            .get_or_init(|| {
                rayon::ThreadPoolBuilder::new()
                    .num_threads(self.helpers)
                    .thread_name(|n| format!("kibbr-scan-{n}"))
                    .build()
                    .map_err(|e| {
                        eprintln!(
                            "kibbr: WARN the scan pool could not start its {} thread(s): {e}; \
                             every query chooses its rows on its own thread",
                            self.helpers
                        );
                    })
                    .ok()
            })
            .as_ref()
    }

    /// The threads `each` shared work out from so far.
    #[cfg(test)]
    pub(crate) fn entered(&self) -> Vec<std::thread::ThreadId> {
        lock(&self.entered).clone()
    }

    /// Has the pool been asked for? A test's way of knowing no thread was started.
    #[cfg(test)]
    pub(crate) fn started(&self) -> bool {
        self.pool.get().is_some()
    }
}

impl Turn<'_> {
    /// `work` on every item, each once, on this thread and the pool's. Returns when all
    /// are done. **Blocks the calling thread.**
    ///
    /// The items stay the caller's: each thread takes the next one nobody has taken, so
    /// the thread that asked works too, and a pool thread the system has not run yet
    /// finds nothing left and costs a wait at the end and no more. A lock an item, never
    /// waited on, since each is taken once; it is there because what a scan shares out
    /// may be sent to a thread and not shared between two.
    pub(crate) fn each<T: Send>(&self, items: &[Mutex<T>], work: impl Fn(&mut T) + Sync) {
        let next = AtomicUsize::new(0);
        let claim = || {
            while let Some(item) = items.get(next.fetch_add(1, Ordering::Relaxed)) {
                work(&mut lock(item));
            }
        };
        let Some(pool) = self.0.pool() else {
            return claim();
        };
        #[cfg(test)]
        lock(&self.0.entered).push(std::thread::current().id());
        self.0.fanned.fetch_add(1, Ordering::Relaxed);
        let helpers = self.0.helpers.min(items.len().saturating_sub(1));
        pool.in_place_scope(|scope| {
            for _ in 0..helpers {
                scope.spawn(|_| claim());
            }
            claim();
        });
    }
}

#[cfg(test)]
mod tests {
    use std::panic::{catch_unwind, AssertUnwindSafe};
    use std::path::Path;
    use std::time::Duration;

    use super::*;

    /// Every `.rs` file under `dir`, by its path below `root` with `/` between the
    /// parts, and its text.
    fn sources(root: &Path, dir: &Path, out: &mut Vec<(String, String)>) {
        for entry in std::fs::read_dir(dir).expect("a source directory") {
            let path = entry.expect("an entry").path();
            if path.is_dir() {
                sources(root, &path, out);
            } else if path.extension().is_some_and(|ext| ext == "rs") {
                let below = path.strip_prefix(root).expect("under the root");
                let name: Vec<String> = below
                    .components()
                    .map(|part| part.as_os_str().to_string_lossy().into_owned())
                    .collect();
                let text = std::fs::read_to_string(&path).expect("a source file");
                out.push((name.join("/"), text));
            }
        }
    }

    /// **rayon is named in this file and in no other, and a turn on the pool is taken in
    /// the disk scan and nowhere else.** A handler is an `async fn` on a runtime worker,
    /// and a `par_iter` written into one would stall every request in flight while it
    /// ran; so would a turn taken from one. Neither can be written without one of these
    /// two words, and this is what says so the day one is.
    ///
    /// It cannot see where the pool is dropped: `serve::run` holds that, by the order
    /// of two of its locals.
    #[test]
    fn rayon_is_named_in_this_file_alone_and_a_turn_is_taken_in_the_scan_alone() {
        let src = Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
        let mut files = Vec::new();
        sources(&src, &src, &mut files);
        assert!(files.len() > 40, "{} source files", files.len());
        let holding = |word: &str| -> Vec<&str> {
            let mut names: Vec<&str> = files
                .iter()
                .filter(|(_, text)| text.contains(word))
                .map(|(name, _)| name.as_str())
                .collect();
            names.sort_unstable();
            names
        };
        assert_eq!(holding("rayon"), ["store/fan.rs"]);
        assert_eq!(holding(".turn("), ["store/disk/scan.rs", "store/fan.rs"]);
    }

    /// **Every item, each once**, however many there are and however long each takes:
    /// none, one, as many as the threads, one more or fewer, and a thousand of uneven
    /// length. What is left in each item is what `work` made of that item and no other.
    #[test]
    fn each_does_the_work_on_every_item_once() {
        for threads in [2, 3, 4] {
            let fan = Fan::new(threads, 0).expect("a fan");
            for n in [0usize, 1, 2, 3, 4, 5, 1_000] {
                let items: Vec<Mutex<(usize, u32)>> =
                    (0..n).map(|at| Mutex::new((at, 0))).collect();
                let turn = fan.turn().expect("a turn");
                turn.each(&items, |(at, times)| {
                    // Uneven: one item in seven holds its thread while the rest go by.
                    if *at % 7 == 3 {
                        std::thread::sleep(Duration::from_micros(200));
                    }
                    *times += 1;
                    *at *= 2;
                });
                drop(turn);
                let got: Vec<(usize, u32)> = items.iter().map(|item| *lock(item)).collect();
                let want: Vec<(usize, u32)> = (0..n).map(|at| (at * 2, 1)).collect();
                assert_eq!(got, want, "{n} items on {threads} threads");
            }
            assert_eq!(fan.fanned(), 7);
        }
    }

    /// The work is shared: of a thousand items that each take a moment, some are done on
    /// a thread other than the one that asked, and some on the one that asked.
    #[test]
    fn the_asking_thread_and_the_pool_both_take_items() {
        let fan = Fan::new(4, 0).expect("a fan");
        let items: Vec<Mutex<Option<std::thread::ThreadId>>> =
            (0..1_000).map(|_| Mutex::new(None)).collect();
        let turn = fan.turn().expect("a turn");
        turn.each(&items, |by| {
            std::thread::sleep(Duration::from_micros(20));
            *by = Some(std::thread::current().id());
        });
        let me = std::thread::current().id();
        let mine = items.iter().filter(|by| *lock(by) == Some(me)).count();
        assert!(mine > 0, "the asking thread took nothing");
        assert!(mine < items.len(), "the pool took nothing");
        assert_eq!(fan.entered(), [me]);
    }

    /// Under two threads there is nobody to share with, and no `Fan`.
    #[test]
    fn under_two_threads_there_is_no_fan() {
        assert!(Fan::new(0, 0).is_none());
        assert!(Fan::new(1, 0).is_none());
        let fan = Fan::new(2, 7).expect("a fan");
        assert_eq!((fan.least_rows(), fan.fanned()), (7, 0));
    }

    /// No thread is started until a scan takes a turn: a store that is never asked a
    /// query worth sharing out has none.
    #[test]
    fn no_thread_is_started_before_the_first_turn() {
        let fan = Fan::new(4, 0).expect("a fan");
        assert!(!fan.started());
        drop(fan.body());
        assert!(!fan.started());
        // Refused for a body in its task: still none.
        let body = fan.body();
        assert!(fan.turn().is_none());
        assert!(!fan.started());
        drop(body);
        assert!(fan.turn().is_some());
        assert!(fan.started());
    }

    /// One scan holds the pool at a time, and none does while an ingest body is in its
    /// task. Neither refusal waits, and each ends when what caused it is dropped.
    #[test]
    fn a_turn_is_refused_while_one_is_held_or_a_body_is_in_its_task() {
        let fan = Fan::new(3, 0).expect("a fan");
        let first = fan.turn().expect("the first turn");
        assert!(fan.turn().is_none(), "two scans held the pool");
        drop(first);
        let again = fan.turn().expect("a turn after the first was given back");
        drop(again);

        let one = fan.body();
        let two = fan.body();
        assert!(fan.turn().is_none(), "a turn beside two bodies");
        drop(one);
        assert!(fan.turn().is_none(), "a turn beside one body");
        drop(two);
        assert!(fan.turn().is_some());
        // A body that arrives while a turn is held does not take the turn back: the run
        // of row groups that began in a gap is finished.
        let turn = fan.turn().expect("a turn");
        let late = fan.body();
        let items = [Mutex::new(0), Mutex::new(0)];
        turn.each(&items, |n| *n += 1);
        assert_eq!((*lock(&items[0]), *lock(&items[1])), (1, 1));
        drop((turn, late));
    }

    /// A panic in the work leaves `each` as a panic, on the thread that asked, whichever
    /// thread it began on; and the pool is given back, so the next scan is not refused
    /// for the life of the process.
    #[test]
    fn a_panic_in_the_work_leaves_each_as_a_panic_and_gives_the_pool_back() {
        let fan = Fan::new(4, 0).expect("a fan");
        for bad in [0usize, 1, 37, 99] {
            let items: Vec<Mutex<usize>> = (0..100).map(Mutex::new).collect();
            let ran = catch_unwind(AssertUnwindSafe(|| {
                let turn = fan.turn().expect("a turn");
                turn.each(&items, |at| {
                    if *at == bad {
                        panic!("item {at} is the bad one");
                    }
                });
            }));
            assert!(ran.is_err(), "the panic of item {bad} was lost");
            let turn = fan.turn().expect("a turn after a panic");
            let items: Vec<Mutex<usize>> = (0..10).map(Mutex::new).collect();
            turn.each(&items, |at| *at += 1);
            let got: Vec<usize> = items.iter().map(|item| *lock(item)).collect();
            assert_eq!(got, (1..=10).collect::<Vec<_>>());
        }
    }
}
