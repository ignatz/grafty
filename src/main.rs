use futures::TryStreamExt;
use futures::stream::FuturesUnordered;
use graft::remote::RemoteConfig;
use graft::setup::GraftConfig;
use rusqlite::{Connection, OpenFlags};
use std::path::PathBuf;
use std::time::{Duration, Instant};
use tokio::task::{JoinError, spawn_blocking};
use tracing_subscriber::prelude::*;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("GraftInit: {0}")]
    GraftInit(#[from] graft::setup::InitErr),
    #[error("Rusqlite: {0}")]
    Rusqlite(#[from] rusqlite::Error),
    #[error("Thread panic: {0}")]
    JoinErr(#[from] JoinError),
}

pub fn apply_default_pragmas(conn: &rusqlite::Connection) -> Result<(), rusqlite::Error> {
    conn.pragma_update(None, "busy_timeout", 10000)?;
    conn.pragma_update(None, "journal_mode", "WAL")?;
    conn.pragma_update(None, "journal_size_limit", 200000000)?;
    // Sync the file system less often.
    conn.pragma_update(None, "synchronous", "NORMAL")?;
    conn.pragma_update(None, "foreign_keys", "ON")?;
    conn.pragma_update(None, "temp_store", "MEMORY")?;
    // TODO: we could consider pushing this further down-stream to optimize
    // for different use-cases, e.g. main vs logs.
    conn.pragma_update(None, "cache_size", -16000)?;
    // Keep SQLite default 4KB page_size
    // conn.pragma_update(None, "page_size", 4096)?;

    // Safety feature around application-defined functions recommended by
    // https://sqlite.org/appfunc.html
    conn.pragma_update(None, "trusted_schema", "OFF")?;

    return Ok(());
}

pub fn apply_graft_pragmas(conn: &rusqlite::Connection) -> Result<(), rusqlite::Error> {
    // graft recommends only setting journal_mode=MEMORY
    // see: https://graft.rs/docs/sqlite/compatibility/
    conn.pragma_update(None, "journal_mode", "MEMORY")?;

    conn.pragma_update(None, "busy_timeout", 10000)?;
    conn.pragma_update(None, "temp_store", "MEMORY")?;
    conn.pragma_update(None, "trusted_schema", "OFF")?;
    conn.pragma_update(None, "cache_size", -16000)?;

    return Ok(());
}

pub fn connect(
    mut path: PathBuf,
    use_graft: bool,
    exclusive_lock: bool,
) -> Result<rusqlite::Connection, Error> {
    if use_graft {
        path = PathBuf::from(format!("file:{}?vfs=graft", path.to_string_lossy()));
    }

    let flags = OpenFlags::SQLITE_OPEN_READ_WRITE
        | OpenFlags::SQLITE_OPEN_CREATE
        // | OpenFlags::SQLITE_OPEN_NO_MUTEX
        | OpenFlags::SQLITE_OPEN_URI;

    let conn = rusqlite::Connection::open_with_flags(path, flags)?;

    if use_graft {
        apply_graft_pragmas(&conn)?;
    } else {
        apply_default_pragmas(&conn)?;
    }

    if exclusive_lock {
        conn.pragma_update(None, "locking_mode", "exclusive")?;
    }

    return Ok(conn);
}

#[derive(Debug)]
#[allow(dead_code, reason = "debug impl used")]
struct Stats {
    total: Duration,
    avg: Duration,
}

async fn bench_all(
    paths: &[PathBuf],
    use_graft: bool,
    exclusive_lock: bool,
    bench: fn(&mut Connection) -> Result<Duration, Error>,
) -> Result<Stats, Error> {
    let start = Instant::now();
    let mut futures = FuturesUnordered::new();

    for path in paths {
        let path = path.clone();
        futures.push(spawn_blocking(move || {
            let mut conn = connect(path, use_graft, exclusive_lock)?;
            bench(&mut conn)
        }));
    }

    let mut sum = Duration::ZERO;
    // wait for all futures to complete
    while let Some(result) = futures.try_next().await? {
        sum += result?;
    }

    Ok(Stats {
        total: Instant::now() - start,
        avg: sum / paths.len() as u32,
    })
}

fn write_bench_single(conn: &mut Connection) -> Result<Duration, Error> {
    conn.execute("DROP TABLE IF EXISTS test", ())?;
    let start = Instant::now();
    conn.execute("CREATE TABLE test (id INTEGER PRIMARY KEY) STRICT", ())?;
    for i in 0..20000 {
        conn.execute("INSERT INTO test (id) VALUES (?1)", (i,))?;
    }
    Ok(Instant::now() - start)
}

fn write_bench_tx(conn: &mut Connection) -> Result<Duration, Error> {
    conn.execute("DROP TABLE IF EXISTS test", ())?;
    let start = Instant::now();
    let tx = conn.transaction()?;
    tx.execute("CREATE TABLE test (id INTEGER PRIMARY KEY) STRICT", ())?;
    for i in 0..20000 {
        tx.execute("INSERT INTO test (id) VALUES (?1)", (i,))?;
    }
    tx.commit()?;
    Ok(Instant::now() - start)
}

/// only works if `write_bench_single` was previously run on the same connection
fn read_bench_single(conn: &mut Connection) -> Result<Duration, Error> {
    let start = Instant::now();
    for _ in 0..2 {
        for i in 0..20000 {
            conn.query_row("SELECT id FROM test WHERE id = ?1", (i,), |row| {
                row.get::<_, i64>(0)
            })?;
        }
    }
    Ok(Instant::now() - start)
}

/// only works if `write_bench` or `write_bench_tx` was previously run on the
/// same connection
fn read_bench_tx(conn: &mut Connection) -> Result<Duration, Error> {
    let start = Instant::now();
    let tx = conn.transaction()?;
    for _ in 0..2 {
        for i in 0..20000 {
            tx.query_row("SELECT id FROM test WHERE id = ?1", (i,), |row| {
                row.get::<_, i64>(0)
            })?;
        }
    }
    Ok(Instant::now() - start)
}

#[tokio::main]
async fn main() {
    tracing_subscriber::registry()
        .with(tracing_subscriber::filter::LevelFilter::INFO)
        .with(tracing_subscriber::fmt::layer())
        .init();

    let test_dir = PathBuf::from("./data");

    // reset the test dir before starting test
    if std::fs::exists(&test_dir).unwrap() {
        std::fs::remove_dir_all(&test_dir).unwrap();
    }
    std::fs::create_dir(&test_dir).unwrap();

    const CONCURRENCY: usize = 8;

    // create unique path names for each concurrent db in the test directory
    let paths = (0..CONCURRENCY)
        .map(|i| test_dir.join(format!("db{i}.db")))
        .collect::<Vec<_>>();

    // register graft
    let config = GraftConfig {
        data_dir: test_dir.join("graft"),
        remote: RemoteConfig::Memory,
        autosync: None,
    };
    graft_sqlite::register_static("graft", false, config).unwrap();

    struct Test {
        name: &'static str,
        bench: fn(&mut Connection) -> Result<Duration, Error>,
    }

    let tests = [
        Test {
            name: "write_bench_single",
            bench: write_bench_single,
        },
        Test {
            name: "read_bench_single",
            bench: read_bench_single,
        },
        Test {
            name: "write_bench_tx",
            bench: write_bench_tx,
        },
        Test {
            name: "read_bench_tx",
            bench: read_bench_tx,
        },
    ];

    for Test { name, bench } in tests {
        let stats = bench_all(&paths, false, false, bench).await.unwrap();
        println!("{name}, plain sqlite: {stats:?}");
        let stats = bench_all(&paths, false, true, bench).await.unwrap();
        println!("{name}, plain exclusive sqlite: {stats:?}");
        let stats = bench_all(&paths, true, false, bench).await.unwrap();
        println!("{name}, graft sqlite: {stats:?}");
        let stats = bench_all(&paths, true, true, bench).await.unwrap();
        println!("{name}, graft exclusive sqlite: {stats:?}");
    }
}
