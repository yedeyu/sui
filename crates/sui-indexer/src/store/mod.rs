// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

pub(crate) use indexer_analytical_store::*;
pub(crate) use indexer_store::*;
pub use pg_indexer_analytical_store::PgIndexerAnalyticalStore;
pub use pg_indexer_store::PgIndexerStore;

mod indexer_analytical_store;
pub mod indexer_store;
pub mod module_resolver;
mod pg_indexer_analytical_store;
mod pg_indexer_store;
mod pg_partition_manager;
mod query;

pub(crate) mod diesel_macro {
    #[cfg(feature = "mysql-feature")]
    macro_rules! read_only_blocking_mysql {
                ($pool:expr, $query:expr) => {{
                     let pool_conn = crate::db::get_pool_connection($pool)?;
                     let mysql_pool_conn = unsafe { &mut *(&pool_conn as *const _ as *mut crate::db::PooledConnection<diesel::MysqlConnection>) };
                     mysql_pool_conn.transaction($query).map_err(|e| IndexerError::PostgresReadError(e.to_string()))
                }};
            }

    #[cfg(feature = "postgres-feature")]
    macro_rules! read_only_blocking_pg {
                ($pool:expr, $query:expr) => {{
                    let pool_conn = crate::db::get_pool_connection($pool)?;
                    let pg_pool_conn = unsafe { &mut *(&pool_conn as *const _ as *mut crate::db::PooledConnection<diesel::PgConnection>) };
                    pg_pool_conn.build_transaction().read_only().run($query).map_err(|e| IndexerError::PostgresReadError(e.to_string()))
                }};
            }

    macro_rules! read_only_blocking {
    ($pool:expr, $query:expr) => {{
        #[cfg(feature = "postgres-feature")]
        {
            read_only_blocking_pg!($pool, $query)
        }

        #[cfg(feature = "mysql-feature")]
        {
            read_only_blocking_mysql!($pool, $query)
        }
    }};
}

    macro_rules! transactional_blocking_with_retry {
        ($pool:expr, $query:expr, $max_elapsed:expr) => {{
             let pool_conn = crate::db::get_pool_connection($pool)?;
             let pg_pool_conn = unsafe { &mut *(&pool_conn as *const _ as *mut crate::db::PooledConnection<diesel::PgConnection>) };
             pg_pool_conn.build_transaction().read_only().run($query).map_err(|e| IndexerError::PostgresReadError(e.to_string()))

        //     let mut backoff = backoff::ExponentialBackoff::default();
        //     backoff.max_elapsed_time = Some($max_elapsed);
        //
        //     let result = match backoff::retry(backoff, || {
        //         let mut pool_conn: crate::db::PooledConnection<diesel::PgConnection> = crate::db::get_pool_connection($pool).map_err(|e| {
        //             backoff::Error::Transient {
        //                 err: IndexerError::PostgresWriteError(e.to_string()),
        //                 retry_after: None,
        //             }
        //         })?;
        //         pool_conn
        //         .build_transaction()
        //         .read_write()
        //         .run($query)
        //         .map_err(|e| {
        //             tracing::error!("Error with persisting data into DB: {:?}", e);
        //             backoff::Error::Transient {
        //                 err: IndexerError::PostgresWriteError(e.to_string()),
        //                 retry_after: None,
        //             }
        //         })
        //     }) {
        //         Ok(v) => Ok(v),
        //         Err(backoff::Error::Transient { err, .. }) => Err(err),
        //         Err(backoff::Error::Permanent(err)) => Err(err),
        //     };
        //
        //     result
        }};
    }

    use std::any::Any;
    use diesel::connection::SimpleConnection;
    use diesel::PgConnection;
    use diesel::r2d2::ConnectionManager;
    pub(crate) use read_only_blocking;
    #[cfg(feature = "mysql-feature")]
    pub(crate) use read_only_blocking_mysql;
    #[cfg(feature = "postgres-feature")]
    pub(crate) use read_only_blocking_pg;
    pub(crate) use transactional_blocking_with_retry;
    use crate::store::diesel_macro;
}
