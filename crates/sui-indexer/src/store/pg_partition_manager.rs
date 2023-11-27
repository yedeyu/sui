// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use anyhow::anyhow;
use diesel::sql_types::VarChar;
use diesel::{QueryableByName, RunQueryDsl};
use std::collections::BTreeMap;
use std::str::FromStr;
use std::time::Duration;
use tracing::info;

use crate::handlers::EpochToCommit;
use crate::models_v2::epoch::StoredEpochInfo;
use crate::store::diesel_macro::{read_only_blocking, transactional_blocking_with_retry};
use crate::IndexerError;
use crate::PgConnectionPool;

const GET_PARTITION_SQL: &str = r"
SELECT parent.relname                           AS table_name,
       MAX(SUBSTRING(child.relname FROM '\d$')) AS last_partition
FROM pg_inherits
         JOIN pg_class parent ON pg_inherits.inhparent = parent.oid
         JOIN pg_class child ON pg_inherits.inhrelid = child.oid
         JOIN pg_namespace nmsp_parent ON nmsp_parent.oid = parent.relnamespace
         JOIN pg_namespace nmsp_child ON nmsp_child.oid = child.relnamespace
WHERE parent.relkind = 'p'
GROUP BY table_name;
";

#[derive(Clone)]
pub struct PgPartitionManager {
    cp: PgConnectionPool,
}

#[derive(Clone, Debug)]
pub struct EpochPartitionData {
    last_epoch: u64,
    next_epoch: u64,
    last_epoch_start_cp: u64,
    next_epoch_start_cp: u64,
}

impl EpochPartitionData {
    pub fn compose_data(epoch: EpochToCommit, last_db_epoch: StoredEpochInfo) -> Self {
        let last_epoch = last_db_epoch.epoch as u64;
        let last_epoch_start_cp = last_db_epoch.first_checkpoint_id as u64;
        let next_epoch = epoch.new_epoch.epoch;
        let next_epoch_start_cp = epoch.new_epoch.first_checkpoint_id;
        Self {
            last_epoch,
            next_epoch,
            last_epoch_start_cp,
            next_epoch_start_cp,
        }
    }
}

impl PgPartitionManager {
    pub fn new(cp: PgConnectionPool) -> Result<Self, IndexerError> {
        let manager = Self { cp };
        let tables = manager.get_table_partitions()?;
        info!(
            "Found {} tables with partitions : [{:?}]",
            tables.len(),
            tables
        );
        Ok(manager)
    }

    pub fn get_table_partitions(&self) -> Result<BTreeMap<String, u64>, IndexerError> {
        #[derive(QueryableByName, Debug, Clone)]
        struct PartitionedTable {
            #[diesel(sql_type = VarChar)]
            table_name: String,
            #[diesel(sql_type = VarChar)]
            last_partition: String,
        }

        Ok(
            read_only_blocking!(&self.cp, |conn| diesel::RunQueryDsl::load(
                diesel::sql_query(GET_PARTITION_SQL),
                conn
            ))?
            .into_iter()
            .map(|table: PartitionedTable| {
                u64::from_str(&table.last_partition)
                    .map(|last_partition| (table.table_name, last_partition))
                    .map_err(|e| anyhow!(e))
            })
            .collect::<Result<_, _>>()?,
        )
    }

    pub fn advance_table_epoch_partition(
        &self,
        table: String,
        last_partition: u64,
        data: &EpochPartitionData,
    ) -> Result<(), IndexerError> {
        info!("epoch partition data is {:?}", data.clone());
        let (last_epoch, next_epoch, last_epoch_start_cp, next_epoch_start_cp) = (
            data.last_epoch,
            data.next_epoch,
            data.last_epoch_start_cp,
            data.next_epoch_start_cp,
        );
        if next_epoch == 0 {
            tracing::info!("Epoch 0 partition has been crate in migrations, skipped.");
            return Ok(());
        }
        assert!(
            last_partition == last_epoch,
            "last_partition != last_epoch for table {}",
            table
        );
        let detach_last_partition =
            format!("ALTER TABLE {table} DETACH PARTITION {table}_partition_{last_epoch};");
        transactional_blocking_with_retry!(
            &self.cp,
            |conn| { RunQueryDsl::execute(diesel::sql_query(detach_last_partition.clone()), conn) },
            Duration::from_secs(10)
        )?;

        // reattach last partition and create new partition in a separate DB transaction,
        // to avoid dependency on serializable(), which might not always supported by DB.
        let reattach_last_partition = format!(
            "ALTER TABLE {table} ATTACH PARTITION {table}_partition_{last_epoch} FOR VALUES FROM ({last_epoch_start_cp}) TO ({next_epoch_start_cp});"
        );
        let create_new_partition = format!(
            "CREATE TABLE {table}_partition_{next_epoch} PARTITION OF {table}
            FOR VALUES FROM ({next_epoch_start_cp}) TO (MAXVALUE);"
        );
        transactional_blocking_with_retry!(
            &self.cp,
            |conn| {
                RunQueryDsl::execute(diesel::sql_query(reattach_last_partition.clone()), conn)?;
                RunQueryDsl::execute(diesel::sql_query(create_new_partition.clone()), conn)
            },
            Duration::from_secs(10)
        )?;
        Ok(())
    }
}
