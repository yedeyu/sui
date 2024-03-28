// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use crate::SecurityWatchdogConfig;
use anyhow::anyhow;
use arrow_array::{Float32Array, RecordBatch};
use snowflake_api::{QueryResult, SnowflakeApi};

#[async_trait::async_trait]
pub trait QueryRunner: Send + Sync + 'static {
    /// Asynchronously runs the given SQL query and returns the result as a floating-point number.
    /// Only the first row and first column in returned, so it is important that users of this trait
    /// use it for a query which returns only a single floating point result
    async fn run(&self, query: String) -> anyhow::Result<f64>;
}

pub struct SnowflakeQueryRunner {
    api: SnowflakeApi,
}

impl SnowflakeQueryRunner {
    /// Creates a new `SnowflakeQueryRunner` with the specified connection parameters.
    ///
    /// # Arguments
    /// * `account_identifier` - Snowflake account identifier.
    /// * `warehouse` - The Snowflake warehouse to use.
    /// * `database` - The database to query against.
    /// * `schema` - The schema within the database.
    /// * `user` - Username for authentication.
    /// * `role` - User role for executing queries.
    /// * `passwd` - Password for authentication.
    pub fn new(
        account_identifier: &str,
        warehouse: &str,
        database: &str,
        schema: &str,
        user: &str,
        role: &str,
        passwd: &str,
    ) -> anyhow::Result<Self> {
        let api = SnowflakeApi::with_password_auth(
            account_identifier,
            Some(warehouse),
            Some(database),
            Some(schema),
            user,
            Some(role),
            passwd,
        )
        .expect("Failed to build sf api client");
        Ok(SnowflakeQueryRunner { api })
    }

    pub fn from_config(config: &SecurityWatchdogConfig) -> anyhow::Result<Self> {
        Self::new(
            config.sf_account_identifier.unwrap().as_str(),
            config.sf_warehouse.unwrap().as_str(),
            config.sf_database.unwrap().as_str(),
            config.sf_schema.unwrap().as_str(),
            config.sf_username.unwrap().as_str(),
            config.sf_role.unwrap().as_str(),
            config.sf_password.unwrap().as_str(),
        )
    }

    /// Parses the result of a Snowflake query from a `Vec<RecordBatch>` into a single `f64` value.
    fn parse(&self, res: Vec<RecordBatch>) -> anyhow::Result<f64> {
        res.first()
            .ok_or_else(|| anyhow!("No results found in RecordBatch"))?
            .columns()
            .first()
            .ok_or_else(|| anyhow!("No columns found in record"))?
            .as_any()
            .downcast_ref::<Float32Array>()
            .ok_or_else(|| anyhow!("Column is not Float32Array"))?
            .value(0)
            .as_f64()
            .ok_or_else(|| anyhow!("Failed to convert value to f64"))
    }
}

impl QueryRunner for SnowflakeQueryRunner {
    async fn run(&self, query: String) -> anyhow::Result<f64> {
        let res = self.api.exec(&query).await?;
        match res {
            QueryResult::Arrow(records) => self.parse(records),
            // Handle other result types (Json, Empty) with a unified error message
            _ => Err(anyhow!("Unexpected query result type")),
        }
    }
}
