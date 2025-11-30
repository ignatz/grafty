use base64::prelude::*;
use rand::distr::{Alphanumeric, SampleString};
use rusqlite::OpenFlags;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Instant;
use tracing_subscriber::prelude::*;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("GraftInit: {0}")]
    GraftInit(#[from] Arc<graft_kernel::setup::InitErr>),
    #[error("Rusqlite: {0}")]
    Rusqlite(#[from] rusqlite::Error),
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

fn graft_config() -> graft_kernel::setup::GraftConfig {
    let path = PathBuf::from("./graft");
    let data_dir = path.join("local");
    if !data_dir.exists() {
        if let Err(err) = std::fs::create_dir_all(&data_dir) {
            log::error!("Failed to create {data_dir:?}: {err}");
        }
    }

    return graft_kernel::setup::GraftConfig {
        data_dir,
        // Neglect remote for now. We're just experimenting.
        remote: graft_kernel::remote::RemoteConfig::Memory,
        autosync: None,
    };
}

pub fn connect(path: PathBuf) -> Result<rusqlite::Connection, Error> {
    let mut graft_tag: Option<PathBuf> = None;

    // Expected something like "file:///tmp/dir?vfs=graft".
    if let Ok(url) = url::Url::parse(&path.to_string_lossy()) {
        if url.query_pairs().any(|(k, v)| {
            return k == "vfs" && v == "graft";
        }) {
            use std::sync::OnceLock;

            static ONCE: OnceLock<Result<(), Arc<graft_kernel::setup::InitErr>>> = OnceLock::new();
            ONCE.get_or_init(|| {
                graft_sqlite::register_static("graft", false, graft_config()).map_err(|err| {
                    let (e, _trace) = err.into();
                    return e.into();
                })
            })
            .as_ref()
            .map_err(|err| err.clone())?;

            // NOTE: this is a hack, just wasn't sure if there's any limits on what's a valid
            // tag :shrug:
            let tag = BASE64_STANDARD.encode(url.path());
            graft_tag = Some(PathBuf::from(format!("file:{tag}?vfs=graft")));
            log::info!("Using graft tag: {graft_tag:?}");
        }
    }

    let flags = OpenFlags::SQLITE_OPEN_READ_WRITE
        | OpenFlags::SQLITE_OPEN_CREATE
        | OpenFlags::SQLITE_OPEN_NO_MUTEX
        | OpenFlags::SQLITE_OPEN_URI;

    let conn = rusqlite::Connection::open_with_flags(graft_tag.unwrap_or(path), flags)?;

    apply_default_pragmas(&conn)?;

    return Ok(conn);
}

#[tokio::main]
async fn main() {
    tracing_subscriber::registry()
        .with(tracing_subscriber::filter::LevelFilter::INFO)
        .with(tracing_subscriber::fmt::layer())
        .init();

    let start = Instant::now();

    let tasks: Vec<_> = (0..64)
        .map(|_| {
            tokio::spawn(async {
                let use_graft = true;
                let c = if use_graft {
                    let tag = Alphanumeric.sample_string(&mut rand::rng(), 16);
                    connect(format!("file:{tag}?vfs=graft").into()).unwrap()
                } else {
                    let tmp_dir = tempfile::TempDir::new().unwrap();
                    connect(
                        format!("file:{}/main.db?vfs=unix", tmp_dir.path().to_string_lossy())
                            .into(),
                    )
                    .unwrap()
                };

                c.execute("CREATE TABLE test (id INTEGER PRIMARY KEY) STRICT", ())
                    .unwrap();

                for i in 0..50000 {
                    c.execute("INSERT INTO test (id) VALUES (?1)", (i,))
                        .unwrap();
                }
            })
        })
        .collect();

    futures::future::join_all(tasks).await;

    println!("Done, done, done: {:?}", Instant::now() - start);
}
