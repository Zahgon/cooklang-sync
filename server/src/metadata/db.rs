use std::time::Duration;

use diesel::dsl::sql;
use diesel::prelude::*;
use diesel::r2d2::{ConnectionManager, Pool};

use super::models::*;
use super::schema::*;

#[cfg(feature = "database_sqlite")]
type DbConnection = diesel::SqliteConnection;

#[cfg(feature = "database_postgres")]
type DbConnection = diesel::PgConnection;

/// Connection pool for the metadata database, replacing
/// `rocket_sync_db_pools`' `#[database("metadata")]` attribute. `run` keeps
/// the same shape the fairing-provided handle had, so handlers still hand a
/// blocking closure to the pool and await the result -- diesel is a blocking
/// library, so the closure runs on the blocking pool rather than the async
/// runtime's worker threads.
#[derive(Clone)]
pub(crate) struct Db(Pool<ConnectionManager<DbConnection>>);

impl Db {
    pub(crate) fn new(database_url: &str) -> Self {
        let manager = ConnectionManager::<DbConnection>::new(database_url);
        let pool = Pool::builder()
            .connection_timeout(Duration::from_secs(POOL_TIMEOUT_SECONDS))
            .build(manager)
            .expect("database connection");

        Db(pool)
    }

    pub(crate) async fn run<F, R>(&self, f: F) -> R
    where
        F: FnOnce(&mut DbConnection) -> R + Send + 'static,
        R: Send + 'static,
    {
        let pool = self.0.clone();

        tokio::task::spawn_blocking(move || {
            let mut conn = pool.get().expect("database connection");

            f(&mut conn)
        })
        .await
        .expect("database task")
    }
}

/// Carried over from `rocket.toml`'s `timeout = 10` for the metadata pool.
const POOL_TIMEOUT_SECONDS: u64 = 10;

#[cfg(feature = "database_postgres")]
#[allow(dead_code)]
pub type DieselBackend = diesel::pg::Pg;

#[cfg(feature = "database_sqlite")]
#[allow(dead_code)]
pub type DieselBackend = diesel::sqlite::Sqlite;

type Result<T, E = diesel::result::Error> = std::result::Result<T, E>;

pub fn insert_new_record(conn: &mut DbConnection, record: NewFileRecord) -> Result<i32> {
    diesel::insert_into(file_records::table)
        .values(record)
        .returning(file_records::id)
        .get_result(conn)
}

/// Return the most recent record for `(user_id, path)`, or None if this
/// user has never committed that path.
pub fn latest_for_path(
    conn: &mut DbConnection,
    user_id: i32,
    path: &str,
) -> Result<Option<FileRecord>> {
    file_records::table
        .filter(file_records::user_id.eq(user_id))
        .filter(file_records::path.eq(path))
        .order(file_records::id.desc())
        .select(FileRecord::as_select())
        .first::<FileRecord>(conn)
        .optional()
}

pub fn has_files(conn: &mut DbConnection, user_id: i32) -> Result<bool> {
    let subquery = file_records::table
        .filter(file_records::user_id.eq(user_id))
        .group_by(file_records::path)
        .select(diesel::dsl::max(file_records::id))
        .into_boxed()
        .select(sql::<diesel::sql_types::Integer>("max(id)"));

    let count: i64 = file_records::table
        .filter(file_records::id.eq_any(subquery))
        .filter(file_records::deleted.eq(false))
        .count()
        .get_result(conn)?;

    Ok(count > 0)
}

pub fn list(conn: &mut DbConnection, user_id: i32, jid: i32) -> Result<Vec<FileRecord>> {
    let subquery = file_records::table
        .filter(file_records::user_id.eq(user_id))
        .group_by(file_records::path)
        .select(diesel::dsl::max(file_records::id))
        .into_boxed()
        .select(sql::<diesel::sql_types::Integer>("max(id)"));

    file_records::table
        .filter(file_records::id.gt(jid))
        .filter(file_records::id.eq_any(subquery))
        .select(FileRecord::as_select())
        .load(conn)
}
