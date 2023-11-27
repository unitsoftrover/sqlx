use crate::describe::Describe;
use crate::error::Error;
use crate::executor::{Execute, Executor};
use crate::logger::QueryLogger;
use crate::mssql::connection::prepare::prepare;
use crate::mssql::protocol::col_meta_data::Flags;
use crate::mssql::protocol::done::Status as DoneStatus;
use crate::mssql::protocol::message::Message;
use crate::mssql::protocol::packet::PacketType;
use crate::mssql::protocol::rpc::{OptionFlags, Procedure, RpcRequest};
use crate::mssql::protocol::sql_batch::SqlBatch;
use crate::mssql::{
    Mssql, MssqlArguments, MssqlConnection, MssqlQueryResult, MssqlRow, MssqlStatement,
    MssqlTypeInfo,
};
use either::Either;
use futures_core::future::BoxFuture;
use futures_core::stream::BoxStream;
use futures_util::TryStreamExt;
use std::borrow::Cow;
use std::sync::Arc;
use crate::mssql::protocol::packet::PacketHeader;
use crate::mssql::protocol::packet::Status;

impl MssqlConnection {
    async fn run(&mut self, query: &str, arguments: Option<MssqlArguments>) -> Result<(), Error> {
        self.stream.wait_until_ready().await?;
        self.stream.pending_done_count += 1;

        if let Some(mut arguments) = arguments {
            let proc = Either::Right(Procedure::ExecuteSql);
            let mut proc_args = MssqlArguments::default();

            // SQL
            proc_args.add_named("stmt", query);

            if !arguments.data.is_empty() {
                // Declarations
                //  NAME TYPE, NAME TYPE, ...
                proc_args.add_named("params", &*arguments.declarations);

                // Add the list of SQL parameters _after_ our RPC parameters
                proc_args.append(&mut arguments);
            }
            let header = PacketHeader {
                r#type: PacketType::Rpc,
                status: Status::END_OF_MESSAGE,
                length: 0,
                server_process_id: 0,
                packet_id: self.stream.new_packet_id(),
            };

            // let payload = RpcRequest {
            //     transaction_descriptor: self.stream.transaction_descriptor,
            //     arguments: &proc_args,
            //     procedure: proc,
            //     options: OptionFlags::empty(),
            // };
            // let mut payload_buf = Vec::<u8>::new();
            // payload.encode(&mut payload_buf);
            // let packet_size = self.stream.packet_size - 8;        
            // while !payload_buf.is_empty(){
            //     let writable = std::cmp::min(packet_size, payload_buf.len());
            //     let payload_split = payload_buf.split_off(writable);
            //     let mut header1 = header.clone();
            //     if payload_split.is_empty(){
            //         header1.status = Status::END_OF_MESSAGE;                    
            //     }
            //     else {
            //         header1.status = Status::NORMAL;
            //     }
                
            //     self.stream.write_packet1(
            //         header1,
            //         &payload_buf,
            //     );
            //     payload_buf = payload_split;
            //     self.stream.flush().await?;
            // }           

            self.stream.write_packet(
                header,
                RpcRequest {
                    transaction_descriptor: self.stream.transaction_descriptor,
                    arguments: &proc_args,
                    procedure: proc,
                    options: OptionFlags::empty(),
                },
            ).await?;
        } else {
            let header = PacketHeader {
                r#type: PacketType::SqlBatch,
                status: Status::END_OF_MESSAGE,
                length: 0,
                server_process_id: 0,
                packet_id: 1,
            };

            self.stream.write_packet(
                header,
                SqlBatch {
                    transaction_descriptor: self.stream.transaction_descriptor,
                    sql: query,
                },
            ).await?;
            self.stream.flush().await?;
        }        

        Ok(())
    }
}

impl<'c> Executor<'c> for &'c mut MssqlConnection {
    type Database = Mssql;

    fn fetch_many<'e, 'q: 'e, E: 'q>(
        self,
        mut query: E,
    ) -> BoxStream<'e, Result<Either<MssqlQueryResult, MssqlRow>, Error>>
    where
        'c: 'e,
        E: Execute<'q, Self::Database>,
    {
        let sql = query.sql();
        // println!("fetch_many sql:{}", sql);
        let arguments = query.take_arguments();
        let mut logger = QueryLogger::new(sql, self.log_settings.clone());

        Box::pin(try_stream! {
            self.run(sql, arguments).await?;

            loop {
                let message = self.stream.recv_message().await?;

                match message {
                    Message::Row(row) => {
                        let columns = Arc::clone(&self.stream.columns);
                        let column_names = Arc::clone(&self.stream.column_names);

                        logger.increment_rows_returned();

                        r#yield!(Either::Right(MssqlRow { row, column_names, columns }));
                    }

                    Message::Done(done) | Message::DoneProc(done) => {
                        if !done.status.contains(DoneStatus::DONE_MORE) {
                            self.stream.handle_done(&done);
                        }

                        if done.status.contains(DoneStatus::DONE_COUNT) {
                            let rows_affected = done.affected_rows;
                            logger.increase_rows_affected(rows_affected);
                            r#yield!(Either::Left(MssqlQueryResult {
                                rows_affected,
                            }));
                        }

                        if !done.status.contains(DoneStatus::DONE_MORE) {
                            break;
                        }
                    }

                    Message::DoneInProc(done) => {
                        if done.status.contains(DoneStatus::DONE_COUNT) {
                            let rows_affected = done.affected_rows;
                            logger.increase_rows_affected(rows_affected);
                            r#yield!(Either::Left(MssqlQueryResult {
                                rows_affected,
                            }));
                        }
                    }

                    _ => {}
                }
            }

            Ok(())
        })
    }

    fn fetch_optional<'e, 'q: 'e, E: 'q>(
        self,
        query: E,
    ) -> BoxFuture<'e, Result<Option<MssqlRow>, Error>>
    where
        'c: 'e,
        E: Execute<'q, Self::Database>,
    {
        let mut s = self.fetch_many(query);

        Box::pin(async move {
            while let Some(v) = s.try_next().await? {
                if let Either::Right(r) = v {
                    return Ok(Some(r));
                }
            }

            Ok(None)
        })
    }

    fn prepare_with<'e, 'q: 'e>(
        self,
        sql: &'q str,
        _parameters: &[MssqlTypeInfo],
    ) -> BoxFuture<'e, Result<MssqlStatement<'q>, Error>>
    where
        'c: 'e,
    {
        Box::pin(async move {
            let metadata = prepare(self, sql).await?;

            Ok(MssqlStatement {
                sql: Cow::Borrowed(sql),
                metadata,
            })
        })
    }

    fn describe<'e, 'q: 'e>(
        self,
        sql: &'q str,
    ) -> BoxFuture<'e, Result<Describe<Self::Database>, Error>>
    where
        'c: 'e,
    {
        Box::pin(async move {
            let metadata = prepare(self, sql).await?;

            let mut nullable = Vec::with_capacity(metadata.columns.len());

            for col in metadata.columns.iter() {
                // if col.flags.contains(Flags::NULLABLE) {
                //     println!("col: {} nullable.\n", col.name);
                // } else {
                //     println!("col: {} not nullable################\n", col.name);
                // }
                nullable.push(Some(col.flags.contains(Flags::NULLABLE)));
            }
            // println!("nullable*********{:?}\n", nullable);

            Ok(Describe {
                nullable,
                columns: (metadata.columns).clone(),
                parameters: None,
            })
        })
    }
}
