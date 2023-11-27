use std::borrow::Cow;

use futures_core::future::BoxFuture;

use crate::error::Error;
use crate::executor::Executor;
use crate::mssql::protocol::packet::PacketType;
use crate::mssql::protocol::sql_batch::SqlBatch;
use crate::mssql::{Mssql, MssqlConnection};
use crate::transaction::TransactionManager;
use crate::mssql::protocol::packet::Status;
use crate::mssql::protocol::packet::PacketHeader;
use crate::io::Encode;

/// Implementation of [`TransactionManager`] for MSSQL.
pub struct MssqlTransactionManager;

impl TransactionManager for MssqlTransactionManager {
    type Database = Mssql;

    fn begin(conn: &mut MssqlConnection) -> BoxFuture<'_, Result<(), Error>> {
        Box::pin(async move {
            let depth = conn.stream.transaction_depth;

            let query = if depth == 0 {
                Cow::Borrowed("BEGIN TRAN ")
            } else {
                Cow::Owned(format!("SAVE TRAN _sqlx_savepoint_{}", depth))
            };

            conn.execute(&*query).await?;
            conn.stream.transaction_depth = depth + 1;

            Ok(())
        })
    }

    fn commit(conn: &mut MssqlConnection) -> BoxFuture<'_, Result<(), Error>> {
        Box::pin(async move {
            let depth = conn.stream.transaction_depth;

            if depth > 0 {
                if depth == 1 {
                    // savepoints are not released in MSSQL
                    conn.execute("COMMIT TRAN").await?;
                }

                conn.stream.transaction_depth = depth - 1;
            }

            Ok(())
        })
    }

    fn rollback(conn: &mut MssqlConnection) -> BoxFuture<'_, Result<(), Error>> {
        Box::pin(async move {
            let depth = conn.stream.transaction_depth;

            if depth > 0 {
                let query = if depth == 1 {
                    Cow::Borrowed("ROLLBACK TRAN")
                } else {
                    Cow::Owned(format!("ROLLBACK TRAN _sqlx_savepoint_{}", depth - 1))
                };

                conn.execute(&*query).await?;
                conn.stream.transaction_depth = depth - 1;
            }

            Ok(())
        })
    }

    fn start_rollback(conn: &mut MssqlConnection) {
        let depth = conn.stream.transaction_depth;

        if depth > 0 {
            let query = if depth == 1 {
                Cow::Borrowed("ROLLBACK TRAN")
            } else {
                Cow::Owned(format!("ROLLBACK TRAN _sqlx_savepoint_{}", depth - 1))
            };

            conn.stream.pending_done_count += 1;
            let header = PacketHeader {
                r#type: PacketType::SqlBatch,
                status: Status::END_OF_MESSAGE,
                length: 0,
                server_process_id: 0,
                packet_id: 1,
            };
            let batch = SqlBatch {
                transaction_descriptor: conn.stream.transaction_descriptor,
                sql: &*query,
            };
            let mut batch_buf = Vec::<u8>::new();
            batch.encode_with(&mut batch_buf, ());

            let _ = conn.stream.write_packet1(
                header,
                &batch_buf,
            );

            conn.stream.transaction_depth = depth - 1;
        }
    }
}
