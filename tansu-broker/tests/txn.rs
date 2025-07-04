// Copyright ⓒ 2024-2025 Peter Morgan <peter.james.morgan@gmail.com>
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as
// published by the Free Software Foundation, either version 3 of the
// License, or (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <https://www.gnu.org/licenses/>.

use std::collections::BTreeMap;

use bytes::Bytes;
use common::{StorageType, alphanumeric_string, init_tracing, register_broker};
use rand::{prelude::*, rng};
use tansu_broker::Result;
use tansu_sans_io::{
    BatchAttribute, ControlBatch, EndTransactionMarker, ErrorCode, IsolationLevel,
    add_partitions_to_txn_request::AddPartitionsToTxnTopic,
    add_partitions_to_txn_response::{
        AddPartitionsToTxnPartitionResult, AddPartitionsToTxnTopicResult,
    },
    create_topics_request::CreatableTopic,
    record::{Record, inflated},
    txn_offset_commit_request::{TxnOffsetCommitRequestPartition, TxnOffsetCommitRequestTopic},
};
use tansu_storage::{
    ListOffsetRequest, Storage, StorageContainer, TopicId, Topition, TxnAddPartitionsRequest,
    TxnOffsetCommitRequest,
};
use tracing::{debug, error};
use url::Url;
use uuid::Uuid;

pub mod common;

pub async fn simple_txn_commit_offset_commit(
    cluster_id: Uuid,
    broker_id: i32,
    mut sc: StorageContainer,
) -> Result<()> {
    register_broker(&cluster_id, broker_id, &mut sc).await?;

    let topic_name: String = alphanumeric_string(15);
    debug!(?topic_name);

    let num_partitions = 6;
    let replication_factor = 0;

    let assignments = Some([].into());
    let configs = Some([].into());

    let topic_id = sc
        .create_topic(
            CreatableTopic {
                name: topic_name.clone(),
                num_partitions,
                replication_factor,
                assignments: assignments.clone(),
                configs: configs.clone(),
            },
            false,
        )
        .await?;
    debug!(?topic_id);

    let transaction_timeout_ms = 10_000;

    let partition_index = rng().random_range(0..num_partitions);

    let transaction_id = alphanumeric_string(10);
    let group_id = alphanumeric_string(10);

    let producer = sc
        .init_producer(
            Some(transaction_id.as_str()),
            transaction_timeout_ms,
            Some(-1),
            Some(-1),
        )
        .await
        .inspect(|producer| debug!(transaction_id, ?producer))
        .inspect_err(|err| error!(?err, transaction_id, transaction_timeout_ms))?;

    let topition = Topition::new(topic_name.clone(), partition_index);

    let offsets = sc
        .offset_fetch(Some(group_id.as_str()), &[topition.clone()], Some(false))
        .await
        .inspect(|offsets| debug!(?offsets, ?topition))?;

    assert!(offsets.contains_key(&topition));
    assert_eq!(Some(&-1), offsets.get(&topition));

    let committed_offset = 32123;

    let result = sc
        .txn_offset_commit(TxnOffsetCommitRequest {
            transaction_id: transaction_id.clone(),
            group_id: group_id.clone(),
            producer_id: producer.id,
            producer_epoch: producer.epoch,
            generation_id: None,
            member_id: None,
            group_instance_id: None,
            topics: vec![TxnOffsetCommitRequestTopic {
                name: topic_name.clone(),
                partitions: Some(vec![TxnOffsetCommitRequestPartition {
                    partition_index,
                    committed_offset,
                    committed_leader_epoch: None,
                    committed_metadata: None,
                }]),
            }],
        })
        .await?;

    assert_eq!(1, result.len());
    assert_eq!(topic_name, result[0].name);
    assert_eq!(1, result[0].partitions.as_ref().unwrap_or(&vec![]).len());
    assert_eq!(
        ErrorCode::None,
        ErrorCode::try_from(result[0].partitions.as_ref().unwrap()[0].error_code)?
    );

    let offsets = sc
        .offset_fetch(Some(group_id.as_str()), &[topition.clone()], Some(false))
        .await
        .inspect(|offsets| debug!(?offsets, ?topition))?;

    assert!(offsets.contains_key(&topition));
    assert_eq!(Some(&-1), offsets.get(&topition));

    let commit = true;
    assert_eq!(
        ErrorCode::None,
        sc.txn_end(transaction_id.as_str(), producer.id, producer.epoch, commit)
            .await
            .inspect(|status| debug!(transaction_id, ?producer, commit, ?status))
            .inspect_err(|err| error!(?err, transaction_id, ?producer, commit))?
    );

    let offsets = sc
        .offset_fetch(Some(group_id.as_str()), &[topition.clone()], Some(false))
        .await
        .inspect(|offsets| debug!(?offsets, ?topition))?;

    assert!(offsets.contains_key(&topition));
    assert_eq!(Some(&committed_offset), offsets.get(&topition));

    assert_eq!(
        ErrorCode::None,
        sc.delete_topic(&TopicId::from(topic_id)).await?
    );

    Ok(())
}

pub async fn simple_txn_commit_offset_abort(
    cluster_id: Uuid,
    broker_id: i32,
    mut sc: StorageContainer,
) -> Result<()> {
    register_broker(&cluster_id, broker_id, &mut sc).await?;

    let topic_name: String = alphanumeric_string(15);
    debug!(?topic_name);

    let num_partitions = 6;
    let replication_factor = 0;

    let assignments = Some([].into());
    let configs = Some([].into());

    let topic_id = sc
        .create_topic(
            CreatableTopic {
                name: topic_name.clone(),
                num_partitions,
                replication_factor,
                assignments: assignments.clone(),
                configs: configs.clone(),
            },
            false,
        )
        .await?;
    debug!(?topic_id);

    let transaction_timeout_ms = 10_000;

    let partition_index = rng().random_range(0..num_partitions);

    let transaction_id = alphanumeric_string(10);
    let group_id = alphanumeric_string(10);

    let producer = sc
        .init_producer(
            Some(transaction_id.as_str()),
            transaction_timeout_ms,
            Some(-1),
            Some(-1),
        )
        .await
        .inspect(|producer| debug!(transaction_id, ?producer))
        .inspect_err(|err| error!(?err, transaction_id, transaction_timeout_ms))?;

    let topition = Topition::new(topic_name.clone(), partition_index);

    let offsets = sc
        .offset_fetch(Some(group_id.as_str()), &[topition.clone()], Some(false))
        .await
        .inspect(|offsets| debug!(?offsets, ?topition))?;

    assert!(offsets.contains_key(&topition));
    assert_eq!(Some(&-1), offsets.get(&topition));

    let committed_offset = 32123;

    let result = sc
        .txn_offset_commit(TxnOffsetCommitRequest {
            transaction_id: transaction_id.clone(),
            group_id: group_id.clone(),
            producer_id: producer.id,
            producer_epoch: producer.epoch,
            generation_id: None,
            member_id: None,
            group_instance_id: None,
            topics: vec![TxnOffsetCommitRequestTopic {
                name: topic_name.clone(),
                partitions: Some(vec![TxnOffsetCommitRequestPartition {
                    partition_index,
                    committed_offset,
                    committed_leader_epoch: None,
                    committed_metadata: None,
                }]),
            }],
        })
        .await?;

    assert_eq!(1, result.len());
    assert_eq!(topic_name, result[0].name);
    assert_eq!(1, result[0].partitions.as_ref().unwrap_or(&vec![]).len());
    assert_eq!(
        ErrorCode::None,
        ErrorCode::try_from(result[0].partitions.as_ref().unwrap()[0].error_code)?
    );

    let offsets = sc
        .offset_fetch(Some(group_id.as_str()), &[topition.clone()], Some(false))
        .await
        .inspect(|offsets| debug!(?offsets, ?topition))?;

    assert!(offsets.contains_key(&topition));
    assert_eq!(Some(&-1), offsets.get(&topition));

    let commit = false;
    assert_eq!(
        ErrorCode::None,
        sc.txn_end(transaction_id.as_str(), producer.id, producer.epoch, commit)
            .await
            .inspect(|status| debug!(transaction_id, ?producer, commit, ?status))
            .inspect_err(|err| error!(?err, transaction_id, ?producer, commit))?
    );

    let offsets = sc
        .offset_fetch(Some(group_id.as_str()), &[topition.clone()], Some(false))
        .await
        .inspect(|offsets| debug!(?offsets, ?topition))?;

    assert!(offsets.contains_key(&topition));
    assert_eq!(Some(&-1), offsets.get(&topition));

    assert_eq!(
        ErrorCode::None,
        sc.delete_topic(&TopicId::from(topic_id)).await?
    );

    Ok(())
}

pub async fn simple_txn_produce_commit(
    cluster_id: Uuid,
    broker_id: i32,
    mut sc: StorageContainer,
) -> Result<()> {
    register_broker(&cluster_id, broker_id, &mut sc).await?;

    let topic_name: String = alphanumeric_string(15);
    debug!(?topic_name);

    let num_partitions = 6;
    let replication_factor = 0;

    let assignments = Some([].into());
    let configs = Some([].into());

    let topic_id = sc
        .create_topic(
            CreatableTopic {
                name: topic_name.clone(),
                num_partitions,
                replication_factor,
                assignments: assignments.clone(),
                configs: configs.clone(),
            },
            false,
        )
        .await?;
    debug!(?topic_id);

    let transaction_timeout_ms = 10_000;

    let partition_index = rng().random_range(0..num_partitions);
    let topition = Topition::new(topic_name.clone(), partition_index);
    let num_records = 6;
    let num_transactions = 1;
    let mut offset_producer = BTreeMap::new();

    let list_offsets_before = sc
        .list_offsets(
            IsolationLevel::ReadUncommitted,
            &[(topition.clone(), ListOffsetRequest::Latest)],
        )
        .await?;
    assert_eq!(1, list_offsets_before.len());
    assert_eq!(topic_name, list_offsets_before[0].0.topic());
    assert_eq!(partition_index, list_offsets_before[0].0.partition());
    assert_eq!(ErrorCode::None, list_offsets_before[0].1.error_code);
    assert_eq!(Some(0), list_offsets_before[0].1.offset);

    let transactions = {
        let mut transactions = Vec::new();

        for transaction in (0..num_transactions).map(|_| alphanumeric_string(10)) {
            let producer = sc
                .init_producer(
                    Some(transaction.as_str()),
                    transaction_timeout_ms,
                    Some(-1),
                    Some(-1),
                )
                .await
                .inspect(|producer| debug!(transaction, ?producer))
                .inspect_err(|err| error!(?err, transaction, transaction_timeout_ms))?;

            let add_partitions = sc
                .txn_add_partitions(TxnAddPartitionsRequest::VersionZeroToThree {
                    transaction_id: transaction.clone(),
                    producer_id: producer.id,
                    producer_epoch: producer.epoch,
                    topics: [AddPartitionsToTxnTopic {
                        name: topic_name.clone(),
                        partitions: Some([partition_index].into()),
                    }]
                    .into(),
                })
                .await
                .inspect(|add_partitions| {
                    debug!(
                        transaction,
                        ?producer,
                        topic = topic_name,
                        partition = partition_index,
                        ?add_partitions
                    )
                })
                .inspect_err(|err| {
                    error!(
                        ?err,
                        transaction,
                        ?producer,
                        topic = topic_name,
                        partition = partition_index,
                    )
                })?;

            assert_eq!(
                [AddPartitionsToTxnTopicResult {
                    name: topic_name.clone(),
                    results_by_partition: Some(
                        [AddPartitionsToTxnPartitionResult {
                            partition_index,
                            partition_error_code: ErrorCode::None.into(),
                        }]
                        .into()
                    )
                }],
                add_partitions.zero_to_three()
            );

            for base_sequence in 0..num_records {
                let key = Bytes::copy_from_slice(alphanumeric_string(15).as_bytes());
                let value = Bytes::copy_from_slice(alphanumeric_string(15).as_bytes());

                let batch = inflated::Batch::builder()
                    .record(
                        Record::builder()
                            .key(key.clone().into())
                            .value(value.clone().into()),
                    )
                    .attributes(BatchAttribute::default().transaction(true).into())
                    .producer_id(producer.id)
                    .producer_epoch(producer.epoch)
                    .base_sequence(base_sequence)
                    .build()
                    .and_then(TryInto::try_into)
                    .inspect(|deflated| debug!(base_sequence, ?deflated, ?producer))
                    .inspect_err(|err| error!(?err, base_sequence, ?producer))?;

                let offset = sc
                    .produce(Some(transaction.as_str()), &topition, batch)
                    .await
                    .inspect(|offset| debug!(?offset))
                    .inspect_err(|err| error!(?err, ?topition))?;

                assert_eq!(None, offset_producer.insert(offset, (producer, key, value)));
            }

            transactions.push((transaction, producer));
        }

        transactions
    };

    {
        let list_offset_type = ListOffsetRequest::Latest;
        let isolation_level = IsolationLevel::ReadUncommitted;

        let list_offsets_after = sc
            .list_offsets(isolation_level, &[(topition.clone(), list_offset_type)])
            .await
            .inspect(|list_offsets_after| {
                debug!(
                    ?list_offsets_after,
                    ?isolation_level,
                    ?topition,
                    ?list_offset_type,
                )
            })
            .inspect_err(|err| error!(?err, ?isolation_level, ?topition, ?list_offset_type,))?;

        assert_eq!(1, list_offsets_after.len());
        assert_eq!(topic_name, list_offsets_after[0].0.topic());
        assert_eq!(partition_index, list_offsets_after[0].0.partition());
        assert_eq!(ErrorCode::None, list_offsets_after[0].1.error_code);

        let offset = i64::from(num_records * num_transactions);

        assert_eq!(Some(offset), list_offsets_after[0].1.offset);
    }

    {
        let list_offset_type = ListOffsetRequest::Latest;
        let isolation_level = IsolationLevel::ReadCommitted;

        let list_offsets_after = sc
            .list_offsets(isolation_level, &[(topition.clone(), list_offset_type)])
            .await
            .inspect(|list_offsets_after| {
                debug!(
                    ?list_offsets_after,
                    ?isolation_level,
                    ?topition,
                    ?list_offset_type,
                )
            })
            .inspect_err(|err| error!(?err, ?isolation_level, ?topition, ?list_offset_type,))?;

        assert_eq!(1, list_offsets_after.len());
        assert_eq!(topic_name, list_offsets_after[0].0.topic());
        assert_eq!(partition_index, list_offsets_after[0].0.partition());
        assert_eq!(ErrorCode::None, list_offsets_after[0].1.error_code);
        assert_eq!(Some(0), list_offsets_after[0].1.offset);
    }

    // commit transaction 1
    //
    {
        let transaction_id = transactions[0].0.as_str();
        let producer_id = transactions[0].1.id;
        let producer_epoch = transactions[0].1.epoch;
        let commit = true;

        assert_eq!(
            ErrorCode::None,
            sc.txn_end(transaction_id, producer_id, producer_epoch, commit)
                .await
                .inspect(|status| debug!(
                    transaction_id,
                    producer_id,
                    producer_epoch,
                    commit,
                    ?status
                ))
                .inspect_err(|err| error!(
                    ?err,
                    transaction_id, producer_id, producer_epoch, commit
                ))?
        );
    }

    {
        let isolation_level = IsolationLevel::ReadUncommitted;
        let list_offset_type = ListOffsetRequest::Latest;

        let list_offsets_after = sc
            .list_offsets(isolation_level, &[(topition.clone(), list_offset_type)])
            .await
            .inspect(|list_offsets_after| {
                debug!(
                    ?list_offsets_after,
                    ?isolation_level,
                    ?topition,
                    ?list_offset_type,
                )
            })
            .inspect_err(|err| error!(?err, ?isolation_level, ?topition, ?list_offset_type,))?;

        assert_eq!(1, list_offsets_after.len());
        assert_eq!(topic_name, list_offsets_after[0].0.topic());
        assert_eq!(partition_index, list_offsets_after[0].0.partition());
        assert_eq!(ErrorCode::None, list_offsets_after[0].1.error_code);

        // end txn marker is now present for txn 1
        //
        let offset = i64::from(num_records * num_transactions) + 1;

        assert_eq!(Some(offset), list_offsets_after[0].1.offset);
    }

    {
        // txn 1 is committed
        //
        let isolation_level = IsolationLevel::ReadCommitted;
        let list_offset_type = ListOffsetRequest::Latest;

        let list_offsets_after = sc
            .list_offsets(isolation_level, &[(topition.clone(), list_offset_type)])
            .await
            .inspect(|list_offsets_after| {
                debug!(
                    ?list_offsets_after,
                    ?isolation_level,
                    ?topition,
                    ?list_offset_type,
                )
            })
            .inspect_err(|err| error!(?err, ?isolation_level, ?topition, ?list_offset_type,))?;

        assert_eq!(1, list_offsets_after.len());
        assert_eq!(topic_name, list_offsets_after[0].0.topic());
        assert_eq!(partition_index, list_offsets_after[0].0.partition());
        assert_eq!(ErrorCode::None, list_offsets_after[0].1.error_code);

        // end txn marker is now present for txn 1
        //
        let offset = i64::from(num_records * num_transactions) + 1;
        assert_eq!(Some(offset), list_offsets_after[0].1.offset);
    }

    assert_eq!(
        ErrorCode::None,
        sc.delete_topic(&TopicId::from(topic_id)).await?
    );

    Ok(())
}

pub async fn simple_txn_produce_abort(
    cluster_id: Uuid,
    broker_id: i32,
    mut sc: StorageContainer,
) -> Result<()> {
    register_broker(&cluster_id, broker_id, &mut sc).await?;

    let topic_name: String = alphanumeric_string(15);
    debug!(?topic_name);

    let num_partitions = 6;
    let replication_factor = 0;

    let assignments = Some([].into());
    let configs = Some([].into());

    let topic_id = sc
        .create_topic(
            CreatableTopic {
                name: topic_name.clone(),
                num_partitions,
                replication_factor,
                assignments: assignments.clone(),
                configs: configs.clone(),
            },
            false,
        )
        .await?;
    debug!(?topic_id);

    let transaction_timeout_ms = 10_000;

    let partition_index = rng().random_range(0..num_partitions);
    let topition = Topition::new(topic_name.clone(), partition_index);
    let num_records = 6;
    let num_transactions = 1;
    let mut offset_producer = BTreeMap::new();

    let list_offsets_before = sc
        .list_offsets(
            IsolationLevel::ReadUncommitted,
            &[(topition.clone(), ListOffsetRequest::Latest)],
        )
        .await?;
    assert_eq!(1, list_offsets_before.len());
    assert_eq!(topic_name, list_offsets_before[0].0.topic());
    assert_eq!(partition_index, list_offsets_before[0].0.partition());
    assert_eq!(ErrorCode::None, list_offsets_before[0].1.error_code);
    assert_eq!(Some(0), list_offsets_before[0].1.offset);

    let transactions = {
        let mut transactions = Vec::new();

        for transaction in (0..num_transactions).map(|_| alphanumeric_string(10)) {
            let producer = sc
                .init_producer(
                    Some(transaction.as_str()),
                    transaction_timeout_ms,
                    Some(-1),
                    Some(-1),
                )
                .await
                .inspect(|producer| debug!(transaction, ?producer))
                .inspect_err(|err| error!(?err, transaction, transaction_timeout_ms))?;

            let add_partitions = sc
                .txn_add_partitions(TxnAddPartitionsRequest::VersionZeroToThree {
                    transaction_id: transaction.clone(),
                    producer_id: producer.id,
                    producer_epoch: producer.epoch,
                    topics: [AddPartitionsToTxnTopic {
                        name: topic_name.clone(),
                        partitions: Some([partition_index].into()),
                    }]
                    .into(),
                })
                .await
                .inspect(|add_partitions| {
                    debug!(
                        transaction,
                        ?producer,
                        topic = topic_name,
                        partition = partition_index,
                        ?add_partitions
                    )
                })
                .inspect_err(|err| {
                    error!(
                        ?err,
                        transaction,
                        ?producer,
                        topic = topic_name,
                        partition = partition_index,
                    )
                })?;

            assert_eq!(
                [AddPartitionsToTxnTopicResult {
                    name: topic_name.clone(),
                    results_by_partition: Some(
                        [AddPartitionsToTxnPartitionResult {
                            partition_index,
                            partition_error_code: ErrorCode::None.into(),
                        }]
                        .into()
                    )
                }],
                add_partitions.zero_to_three()
            );

            for base_sequence in 0..num_records {
                let key = Bytes::copy_from_slice(alphanumeric_string(15).as_bytes());
                let value = Bytes::copy_from_slice(alphanumeric_string(15).as_bytes());

                let batch = inflated::Batch::builder()
                    .record(
                        Record::builder()
                            .key(key.clone().into())
                            .value(value.clone().into()),
                    )
                    .attributes(BatchAttribute::default().transaction(true).into())
                    .producer_id(producer.id)
                    .producer_epoch(producer.epoch)
                    .base_sequence(base_sequence)
                    .build()
                    .and_then(TryInto::try_into)
                    .inspect(|deflated| debug!(base_sequence, ?deflated, ?producer))
                    .inspect_err(|err| error!(?err, base_sequence, ?producer))?;

                let offset = sc
                    .produce(Some(transaction.as_str()), &topition, batch)
                    .await
                    .inspect(|offset| debug!(?offset))
                    .inspect_err(|err| error!(?err, ?topition))?;

                assert_eq!(None, offset_producer.insert(offset, (producer, key, value)));
            }

            transactions.push((transaction, producer));
        }

        transactions
    };

    {
        let list_offset_type = ListOffsetRequest::Latest;
        let isolation_level = IsolationLevel::ReadUncommitted;

        let list_offsets_after = sc
            .list_offsets(isolation_level, &[(topition.clone(), list_offset_type)])
            .await
            .inspect(|list_offsets_after| {
                debug!(
                    ?list_offsets_after,
                    ?isolation_level,
                    ?topition,
                    ?list_offset_type,
                )
            })
            .inspect_err(|err| error!(?err, ?isolation_level, ?topition, ?list_offset_type,))?;

        assert_eq!(1, list_offsets_after.len());
        assert_eq!(topic_name, list_offsets_after[0].0.topic());
        assert_eq!(partition_index, list_offsets_after[0].0.partition());
        assert_eq!(ErrorCode::None, list_offsets_after[0].1.error_code);

        let offset = i64::from(num_records * num_transactions);

        assert_eq!(Some(offset), list_offsets_after[0].1.offset);
    }

    {
        let list_offset_type = ListOffsetRequest::Latest;
        let isolation_level = IsolationLevel::ReadCommitted;

        let list_offsets_after = sc
            .list_offsets(isolation_level, &[(topition.clone(), list_offset_type)])
            .await
            .inspect(|list_offsets_after| {
                debug!(
                    ?list_offsets_after,
                    ?isolation_level,
                    ?topition,
                    ?list_offset_type,
                )
            })
            .inspect_err(|err| error!(?err, ?isolation_level, ?topition, ?list_offset_type,))?;

        assert_eq!(1, list_offsets_after.len());
        assert_eq!(topic_name, list_offsets_after[0].0.topic());
        assert_eq!(partition_index, list_offsets_after[0].0.partition());
        assert_eq!(ErrorCode::None, list_offsets_after[0].1.error_code);
        assert_eq!(Some(0), list_offsets_after[0].1.offset);
    }

    // abort transaction 1
    //
    {
        let transaction_id = transactions[0].0.as_str();
        let producer_id = transactions[0].1.id;
        let producer_epoch = transactions[0].1.epoch;
        let commit = false;

        assert_eq!(
            ErrorCode::None,
            sc.txn_end(transaction_id, producer_id, producer_epoch, commit)
                .await
                .inspect(|status| debug!(
                    transaction_id,
                    producer_id,
                    producer_epoch,
                    commit,
                    ?status
                ))
                .inspect_err(|err| error!(
                    ?err,
                    transaction_id, producer_id, producer_epoch, commit
                ))?
        );
    }

    {
        let isolation_level = IsolationLevel::ReadUncommitted;
        let list_offset_type = ListOffsetRequest::Latest;

        let list_offsets_after = sc
            .list_offsets(isolation_level, &[(topition.clone(), list_offset_type)])
            .await
            .inspect(|list_offsets_after| {
                debug!(
                    ?list_offsets_after,
                    ?isolation_level,
                    ?topition,
                    ?list_offset_type,
                )
            })
            .inspect_err(|err| error!(?err, ?isolation_level, ?topition, ?list_offset_type,))?;

        assert_eq!(1, list_offsets_after.len());
        assert_eq!(topic_name, list_offsets_after[0].0.topic());
        assert_eq!(partition_index, list_offsets_after[0].0.partition());
        assert_eq!(ErrorCode::None, list_offsets_after[0].1.error_code);

        // end txn marker is now present for txn 1
        //
        let offset = i64::from(num_records * num_transactions) + 1;
        assert_eq!(Some(offset), list_offsets_after[0].1.offset);
    }

    {
        // txn 1 is committed
        //
        let isolation_level = IsolationLevel::ReadCommitted;
        let list_offset_type = ListOffsetRequest::Latest;

        let list_offsets_after = sc
            .list_offsets(isolation_level, &[(topition.clone(), list_offset_type)])
            .await
            .inspect(|list_offsets_after| {
                debug!(
                    ?list_offsets_after,
                    ?isolation_level,
                    ?topition,
                    ?list_offset_type,
                )
            })
            .inspect_err(|err| error!(?err, ?isolation_level, ?topition, ?list_offset_type,))?;

        assert_eq!(1, list_offsets_after.len());
        assert_eq!(topic_name, list_offsets_after[0].0.topic());
        assert_eq!(partition_index, list_offsets_after[0].0.partition());
        assert_eq!(ErrorCode::None, list_offsets_after[0].1.error_code);

        // end txn marker is now present for txn 1
        //
        let offset = i64::from(num_records * num_transactions) + 1;
        assert_eq!(Some(offset), list_offsets_after[0].1.offset);
    }

    assert_eq!(
        ErrorCode::None,
        sc.delete_topic(&TopicId::from(topic_id)).await?
    );

    Ok(())
}

// txns that overlap on the same topition
//
pub async fn with_overlap(
    cluster_id: Uuid,
    broker_id: i32,
    mut sc: StorageContainer,
) -> Result<()> {
    register_broker(&cluster_id, broker_id, &mut sc).await?;

    let topic_name: String = alphanumeric_string(15);
    debug!(?topic_name);

    let num_partitions = 6;
    let replication_factor = 0;

    let assignments = Some([].into());
    let configs = Some([].into());

    let topic_id = sc
        .create_topic(
            CreatableTopic {
                name: topic_name.clone(),
                num_partitions,
                replication_factor,
                assignments: assignments.clone(),
                configs: configs.clone(),
            },
            false,
        )
        .await?;
    debug!(?topic_id);

    let transaction_timeout_ms = 10_000;

    let partition_index = rng().random_range(0..num_partitions);
    let topition = Topition::new(topic_name.clone(), partition_index);
    let num_records = 6;
    let num_transactions = 2;
    let mut offset_producer = BTreeMap::new();

    let list_offsets_before = sc
        .list_offsets(
            IsolationLevel::ReadUncommitted,
            &[(topition.clone(), ListOffsetRequest::Latest)],
        )
        .await?;
    assert_eq!(1, list_offsets_before.len());
    assert_eq!(topic_name, list_offsets_before[0].0.topic());
    assert_eq!(partition_index, list_offsets_before[0].0.partition());
    assert_eq!(ErrorCode::None, list_offsets_before[0].1.error_code);
    assert_eq!(Some(0), list_offsets_before[0].1.offset);

    let transactions = {
        let mut transactions = Vec::new();

        for transaction in (0..num_transactions).map(|_| alphanumeric_string(10)) {
            let producer = sc
                .init_producer(
                    Some(transaction.as_str()),
                    transaction_timeout_ms,
                    Some(-1),
                    Some(-1),
                )
                .await
                .inspect(|producer| debug!(transaction, ?producer))
                .inspect_err(|err| error!(?err, transaction, transaction_timeout_ms))?;

            let add_partitions = sc
                .txn_add_partitions(TxnAddPartitionsRequest::VersionZeroToThree {
                    transaction_id: transaction.clone(),
                    producer_id: producer.id,
                    producer_epoch: producer.epoch,
                    topics: [AddPartitionsToTxnTopic {
                        name: topic_name.clone(),
                        partitions: Some([partition_index].into()),
                    }]
                    .into(),
                })
                .await
                .inspect(|add_partitions| {
                    debug!(
                        transaction,
                        ?producer,
                        topic = topic_name,
                        partition = partition_index,
                        ?add_partitions
                    )
                })
                .inspect_err(|err| {
                    error!(
                        ?err,
                        transaction,
                        ?producer,
                        topic = topic_name,
                        partition = partition_index,
                    )
                })?;

            assert_eq!(
                [AddPartitionsToTxnTopicResult {
                    name: topic_name.clone(),
                    results_by_partition: Some(
                        [AddPartitionsToTxnPartitionResult {
                            partition_index,
                            partition_error_code: ErrorCode::None.into(),
                        }]
                        .into()
                    )
                }],
                add_partitions.zero_to_three()
            );

            for base_sequence in 0..num_records {
                let key = Bytes::copy_from_slice(alphanumeric_string(15).as_bytes());
                let value = Bytes::copy_from_slice(alphanumeric_string(15).as_bytes());

                let batch = inflated::Batch::builder()
                    .record(
                        Record::builder()
                            .key(key.clone().into())
                            .value(value.clone().into()),
                    )
                    .attributes(BatchAttribute::default().transaction(true).into())
                    .producer_id(producer.id)
                    .producer_epoch(producer.epoch)
                    .base_sequence(base_sequence)
                    .build()
                    .and_then(TryInto::try_into)
                    .inspect(|deflated| debug!(base_sequence, ?deflated, ?producer))
                    .inspect_err(|err| error!(?err, base_sequence, ?producer))?;

                let offset = sc
                    .produce(Some(transaction.as_str()), &topition, batch)
                    .await
                    .inspect(|offset| debug!(?offset))
                    .inspect_err(|err| error!(?err, ?topition))?;

                assert_eq!(None, offset_producer.insert(offset, (producer, key, value)));
            }

            transactions.push((transaction, producer));
        }

        transactions
    };

    {
        let list_offset_type = ListOffsetRequest::Latest;
        let isolation_level = IsolationLevel::ReadUncommitted;

        let list_offsets_after = sc
            .list_offsets(isolation_level, &[(topition.clone(), list_offset_type)])
            .await
            .inspect(|list_offsets_after| {
                debug!(
                    ?list_offsets_after,
                    ?isolation_level,
                    ?topition,
                    ?list_offset_type,
                )
            })
            .inspect_err(|err| error!(?err, ?isolation_level, ?topition, ?list_offset_type,))?;

        assert_eq!(1, list_offsets_after.len());
        assert_eq!(topic_name, list_offsets_after[0].0.topic());
        assert_eq!(partition_index, list_offsets_after[0].0.partition());
        assert_eq!(ErrorCode::None, list_offsets_after[0].1.error_code);

        let offset = i64::from(num_records * num_transactions);

        assert_eq!(Some(offset), list_offsets_after[0].1.offset);
    }

    {
        let list_offset_type = ListOffsetRequest::Latest;
        let isolation_level = IsolationLevel::ReadCommitted;

        let list_offsets_after = sc
            .list_offsets(isolation_level, &[(topition.clone(), list_offset_type)])
            .await
            .inspect(|list_offsets_after| {
                debug!(
                    ?list_offsets_after,
                    ?isolation_level,
                    ?topition,
                    ?list_offset_type,
                )
            })
            .inspect_err(|err| error!(?err, ?isolation_level, ?topition, ?list_offset_type,))?;

        assert_eq!(1, list_offsets_after.len());
        assert_eq!(topic_name, list_offsets_after[0].0.topic());
        assert_eq!(partition_index, list_offsets_after[0].0.partition());
        assert_eq!(ErrorCode::None, list_offsets_after[0].1.error_code);
        assert_eq!(Some(0), list_offsets_after[0].1.offset);
    }

    // commit transaction 1
    //
    {
        let transaction_id = transactions[0].0.as_str();
        let producer_id = transactions[0].1.id;
        let producer_epoch = transactions[0].1.epoch;
        let commit = true;

        assert_eq!(
            ErrorCode::None,
            sc.txn_end(transaction_id, producer_id, producer_epoch, commit)
                .await
                .inspect(|status| debug!(
                    transaction_id,
                    producer_id,
                    producer_epoch,
                    commit,
                    ?status
                ))
                .inspect_err(|err| error!(
                    ?err,
                    transaction_id, producer_id, producer_epoch, commit
                ))?
        );
    }

    {
        let isolation_level = IsolationLevel::ReadUncommitted;
        let list_offset_type = ListOffsetRequest::Latest;

        let list_offsets_after = sc
            .list_offsets(isolation_level, &[(topition.clone(), list_offset_type)])
            .await
            .inspect(|list_offsets_after| {
                debug!(
                    ?list_offsets_after,
                    ?isolation_level,
                    ?topition,
                    ?list_offset_type,
                )
            })
            .inspect_err(|err| error!(?err, ?isolation_level, ?topition, ?list_offset_type,))?;

        assert_eq!(1, list_offsets_after.len());
        assert_eq!(topic_name, list_offsets_after[0].0.topic());
        assert_eq!(partition_index, list_offsets_after[0].0.partition());
        assert_eq!(ErrorCode::None, list_offsets_after[0].1.error_code);

        // end txn marker is now present for txn 1
        //
        let offset = i64::from(num_records * num_transactions) + 1;

        assert_eq!(Some(offset), list_offsets_after[0].1.offset);
    }

    {
        // txn 1 is committed, but isn't visible at read committed because it overlaps with txn 2
        //
        let isolation_level = IsolationLevel::ReadCommitted;
        let list_offset_type = ListOffsetRequest::Latest;

        let list_offsets_after = sc
            .list_offsets(isolation_level, &[(topition.clone(), list_offset_type)])
            .await
            .inspect(|list_offsets_after| {
                debug!(
                    ?list_offsets_after,
                    ?isolation_level,
                    ?topition,
                    ?list_offset_type,
                )
            })
            .inspect_err(|err| error!(?err, ?isolation_level, ?topition, ?list_offset_type,))?;

        assert_eq!(1, list_offsets_after.len());
        assert_eq!(topic_name, list_offsets_after[0].0.topic());
        assert_eq!(partition_index, list_offsets_after[0].0.partition());
        assert_eq!(ErrorCode::None, list_offsets_after[0].1.error_code);
        assert_eq!(Some(0), list_offsets_after[0].1.offset);
    }

    // commit transaction 2
    //
    {
        let transaction_id = transactions[1].0.as_str();
        let producer_id = transactions[1].1.id;
        let producer_epoch = transactions[1].1.epoch;
        let commit = true;

        assert_eq!(
            ErrorCode::None,
            sc.txn_end(transaction_id, producer_id, producer_epoch, commit)
                .await
                .inspect(|status| debug!(
                    transaction_id,
                    producer_id,
                    producer_epoch,
                    commit,
                    ?status
                ))
                .inspect_err(|err| error!(
                    ?err,
                    transaction_id, producer_id, producer_epoch, commit
                ))?
        );
    }

    {
        let isolation_level = IsolationLevel::ReadUncommitted;
        let list_offset_type = ListOffsetRequest::Latest;

        let list_offsets_after = sc
            .list_offsets(isolation_level, &[(topition.clone(), list_offset_type)])
            .await
            .inspect(|list_offsets_after| {
                debug!(
                    ?list_offsets_after,
                    ?isolation_level,
                    ?topition,
                    ?list_offset_type,
                )
            })
            .inspect_err(|err| error!(?err, ?isolation_level, ?topition, ?list_offset_type,))?;

        assert_eq!(1, list_offsets_after.len());
        assert_eq!(topic_name, list_offsets_after[0].0.topic());
        assert_eq!(partition_index, list_offsets_after[0].0.partition());
        assert_eq!(ErrorCode::None, list_offsets_after[0].1.error_code);

        // end txn marker is now present for txn 2
        //
        let offset: i64 = i64::from(num_records * num_transactions) + 2;

        assert_eq!(Some(offset), list_offsets_after[0].1.offset);
    }

    {
        let isolation_level = IsolationLevel::ReadCommitted;
        let list_offset_type = ListOffsetRequest::Latest;

        let list_offsets_after = sc
            .list_offsets(isolation_level, &[(topition.clone(), list_offset_type)])
            .await
            .inspect(|list_offsets_after| {
                debug!(
                    ?list_offsets_after,
                    ?isolation_level,
                    ?topition,
                    ?list_offset_type,
                )
            })
            .inspect_err(|err| error!(?err, ?isolation_level, ?topition, ?list_offset_type,))?;

        assert_eq!(1, list_offsets_after.len());
        assert_eq!(topic_name, list_offsets_after[0].0.topic());
        assert_eq!(partition_index, list_offsets_after[0].0.partition());
        assert_eq!(ErrorCode::None, list_offsets_after[0].1.error_code);

        // end txn marker is now present for txn 2
        //
        let offset: i64 = i64::from(num_records * num_transactions) + 2;

        assert_eq!(Some(offset), list_offsets_after[0].1.offset);
    }

    assert_eq!(
        ErrorCode::None,
        sc.delete_topic(&TopicId::from(topic_id)).await?
    );

    Ok(())
}

pub async fn init_producer_twice(
    cluster_id: Uuid,
    broker_id: i32,
    mut sc: StorageContainer,
) -> Result<()> {
    register_broker(&cluster_id, broker_id, &mut sc).await?;

    let topic_name: String = alphanumeric_string(15);
    debug!(?topic_name);

    let num_partitions = 6;
    let replication_factor = 0;

    let assignments = Some([].into());
    let configs = Some([].into());

    let topic_id = sc
        .create_topic(
            CreatableTopic {
                name: topic_name.clone(),
                num_partitions,
                replication_factor,
                assignments: assignments.clone(),
                configs: configs.clone(),
            },
            false,
        )
        .await?;
    debug!(?topic_id);

    let transaction_timeout_ms = 10_000;

    let partition_index = rng().random_range(0..num_partitions);
    let topition = Topition::new(topic_name.clone(), partition_index);
    let num_records = 6;
    let num_transactions = 2;
    let mut offset_producer = BTreeMap::new();

    let transactions = {
        let mut transactions = Vec::new();

        for transaction in (0..num_transactions).map(|_| alphanumeric_string(10)) {
            let producer = sc
                .init_producer(
                    Some(transaction.as_str()),
                    transaction_timeout_ms,
                    Some(-1),
                    Some(-1),
                )
                .await
                .inspect_err(|err| error!(?err))?;
            debug!(?producer);

            let add_partitions = sc
                .txn_add_partitions(TxnAddPartitionsRequest::VersionZeroToThree {
                    transaction_id: transaction.clone(),
                    producer_id: producer.id,
                    producer_epoch: producer.epoch,
                    topics: [AddPartitionsToTxnTopic {
                        name: topic_name.clone(),
                        partitions: Some([partition_index].into()),
                    }]
                    .into(),
                })
                .await
                .inspect_err(|err| error!(?err))?;
            debug!(?add_partitions);

            assert_eq!(
                [AddPartitionsToTxnTopicResult {
                    name: topic_name.clone(),
                    results_by_partition: Some(
                        [AddPartitionsToTxnPartitionResult {
                            partition_index,
                            partition_error_code: ErrorCode::None.into(),
                        }]
                        .into()
                    )
                }],
                add_partitions.zero_to_three()
            );

            for base_sequence in 0..num_records {
                debug!(base_sequence);

                let key = Bytes::copy_from_slice(alphanumeric_string(15).as_bytes());
                let value = Bytes::copy_from_slice(alphanumeric_string(15).as_bytes());

                let batch = inflated::Batch::builder()
                    .record(
                        Record::builder()
                            .key(key.clone().into())
                            .value(value.clone().into()),
                    )
                    .attributes(BatchAttribute::default().transaction(true).into())
                    .producer_id(producer.id)
                    .producer_epoch(producer.epoch)
                    .base_sequence(base_sequence)
                    .build()
                    .and_then(TryInto::try_into)
                    .inspect(|deflated| debug!(?deflated))
                    .inspect_err(|err| error!(?err))?;

                debug!(base_sequence, ?batch);

                let offset = sc
                    .produce(Some(transaction.as_str()), &topition, batch)
                    .await
                    .inspect(|offset| debug!(?offset))
                    .inspect_err(|err| error!(?err))?;

                debug!(offset);

                assert_eq!(None, offset_producer.insert(offset, (producer, key, value)));
            }

            transactions.push((transaction, producer));
        }

        transactions
    };

    {
        let list_offsets_after = sc
            .list_offsets(
                IsolationLevel::ReadUncommitted,
                &[(topition.clone(), ListOffsetRequest::Latest)],
            )
            .await
            .inspect_err(|err| error!(?err))?;
        assert_eq!(1, list_offsets_after.len());
        assert_eq!(topic_name, list_offsets_after[0].0.topic());
        assert_eq!(partition_index, list_offsets_after[0].0.partition());
        assert_eq!(ErrorCode::None, list_offsets_after[0].1.error_code);

        let offset = i64::from(num_records * num_transactions);
        assert_eq!(Some(offset), list_offsets_after[0].1.offset);
    }

    {
        let list_offsets_after = sc
            .list_offsets(
                IsolationLevel::ReadCommitted,
                &[(topition.clone(), ListOffsetRequest::Latest)],
            )
            .await
            .inspect_err(|err| error!(?err))?;
        assert_eq!(1, list_offsets_after.len());
        assert_eq!(topic_name, list_offsets_after[0].0.topic());
        assert_eq!(partition_index, list_offsets_after[0].0.partition());
        assert_eq!(ErrorCode::None, list_offsets_after[0].1.error_code);
        assert_eq!(Some(0), list_offsets_after[0].1.offset);
    }

    // commit transactions[0]
    //
    assert_eq!(
        ErrorCode::None,
        sc.txn_end(
            transactions[0].0.as_str(),
            transactions[0].1.id,
            transactions[0].1.epoch,
            true
        )
        .await
        .inspect_err(|err| error!(?err))?
    );

    {
        let list_offsets = sc
            .list_offsets(
                IsolationLevel::ReadUncommitted,
                &[(topition.clone(), ListOffsetRequest::Latest)],
            )
            .await
            .inspect_err(|err| error!(?err))?;
        assert_eq!(1, list_offsets.len());
        assert_eq!(topic_name, list_offsets[0].0.topic());
        assert_eq!(partition_index, list_offsets[0].0.partition());
        assert_eq!(ErrorCode::None, list_offsets[0].1.error_code);

        let offset = i64::from(num_records * num_transactions) + 1;
        assert_eq!(
            // end txn marker is now present for transactions[0], but it overlaps with [1]
            //
            Some(offset),
            list_offsets[0].1.offset
        );
    }

    let min_bytes = 1;
    let max_bytes = 50 * 1024;

    let batches = sc
        .fetch(
            &topition,
            (num_records * num_transactions) as i64,
            min_bytes,
            max_bytes as u32,
            IsolationLevel::ReadUncommitted,
        )
        .await
        .and_then(|batches| {
            batches.into_iter().try_fold(Vec::new(), |mut acc, batch| {
                inflated::Batch::try_from(batch)
                    .map(|inflated| {
                        acc.push(inflated);
                        acc
                    })
                    .map_err(Into::into)
            })
        })
        .inspect_err(|err| error!(?err))?;

    debug!(min_bytes, max_bytes, ?batches);

    assert_eq!(1, batches.len());
    assert_eq!(transactions[0].1.id, batches[0].producer_id);
    assert_eq!(transactions[0].1.epoch, batches[0].producer_epoch);
    assert_eq!(1, batches[0].records.len());

    // the fetched batch has transactions[0] commit end transaction marker
    assert_eq!(
        Some(ControlBatch::default().commit().try_into()?),
        batches[0].records[0].key
    );

    assert_eq!(
        Some(EndTransactionMarker::default().try_into()?),
        batches[0].records[0].value
    );

    // init producer on transactions[1], will cause abort of
    // [1], with both [0] and [1] then visible at read committed.
    //
    let producer = sc
        .init_producer(
            Some(transactions[1].0.as_str()),
            transaction_timeout_ms,
            Some(-1),
            Some(-1),
        )
        .await
        .inspect_err(|err| error!(?err))?;

    assert_eq!(transactions[1].1.id, producer.id);

    // new epoch for [1] producer
    assert_eq!(transactions[1].1.epoch + 1, producer.epoch);

    {
        let list_offsets = sc
            .list_offsets(
                IsolationLevel::ReadUncommitted,
                &[(topition.clone(), ListOffsetRequest::Latest)],
            )
            .await
            .inspect_err(|err| error!(?err))?;
        assert_eq!(1, list_offsets.len());
        assert_eq!(topic_name, list_offsets[0].0.topic());
        assert_eq!(partition_index, list_offsets[0].0.partition());
        assert_eq!(ErrorCode::None, list_offsets[0].1.error_code);

        let offset = i64::from(num_records * num_transactions) + 2;
        assert_eq!(
            // end txn marker is now present for transactions[1]
            //
            Some(offset),
            list_offsets[0].1.offset
        );
    }

    let batches = sc
        .fetch(
            &topition,
            (num_records * num_transactions + 1) as i64,
            min_bytes,
            max_bytes as u32,
            IsolationLevel::ReadUncommitted,
        )
        .await
        .and_then(|batches| {
            batches.into_iter().try_fold(Vec::new(), |mut acc, batch| {
                inflated::Batch::try_from(batch)
                    .map(|inflated| {
                        acc.push(inflated);
                        acc
                    })
                    .map_err(Into::into)
            })
        })
        .inspect_err(|err| error!(?err))?;

    debug!(min_bytes, max_bytes, ?batches);

    assert_eq!(1, batches.len());
    assert_eq!(transactions[1].1.id, batches[0].producer_id);
    assert_eq!(transactions[1].1.epoch, batches[0].producer_epoch);
    assert_eq!(1, batches[0].records.len());

    // transactions[1] abort end transaction marker
    assert_eq!(
        Some(ControlBatch::default().abort().try_into()?),
        batches[0].records[0].key
    );

    assert_eq!(
        Some(EndTransactionMarker::default().try_into()?),
        batches[0].records[0].value
    );

    assert_eq!(
        ErrorCode::None,
        sc.delete_topic(&TopicId::from(topic_id)).await?
    );

    Ok(())
}

mod pg {
    use super::*;

    fn storage_container(cluster: impl Into<String>, node: i32) -> Result<StorageContainer> {
        Url::parse("tcp://127.0.0.1/")
            .map_err(Into::into)
            .and_then(|advertised_listener| {
                common::storage_container(
                    StorageType::Postgres,
                    cluster,
                    node,
                    advertised_listener,
                    None,
                )
            })
    }

    #[tokio::test]
    async fn simple_txn_commit_offset_abort() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        super::simple_txn_commit_offset_abort(
            cluster_id,
            broker_id,
            storage_container(cluster_id, broker_id)?,
        )
        .await
    }

    #[tokio::test]
    async fn simple_txn_commit_offset_commit() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        super::simple_txn_commit_offset_commit(
            cluster_id,
            broker_id,
            storage_container(cluster_id, broker_id)?,
        )
        .await
    }

    #[tokio::test]
    async fn simple_txn_produce_commit() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        super::simple_txn_produce_commit(
            cluster_id,
            broker_id,
            storage_container(cluster_id, broker_id)?,
        )
        .await
    }

    #[tokio::test]
    async fn simple_txn_produce_abort() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        super::simple_txn_produce_abort(
            cluster_id,
            broker_id,
            storage_container(cluster_id, broker_id)?,
        )
        .await
    }

    #[tokio::test]
    async fn with_overlap() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        super::with_overlap(
            cluster_id,
            broker_id,
            storage_container(cluster_id, broker_id)?,
        )
        .await
    }

    #[tokio::test]
    async fn init_producer_twice() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        super::init_producer_twice(
            cluster_id,
            broker_id,
            storage_container(cluster_id, broker_id)?,
        )
        .await
    }
}

mod in_memory {
    use super::*;

    fn storage_container(cluster: impl Into<String>, node: i32) -> Result<StorageContainer> {
        Url::parse("tcp://127.0.0.1/")
            .map_err(Into::into)
            .and_then(|advertised_listener| {
                common::storage_container(
                    StorageType::InMemory,
                    cluster,
                    node,
                    advertised_listener,
                    None,
                )
            })
    }

    #[tokio::test]
    async fn simple_txn_commit_offset_abort() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        super::simple_txn_commit_offset_abort(
            cluster_id,
            broker_id,
            storage_container(cluster_id, broker_id)?,
        )
        .await
    }

    #[tokio::test]
    async fn simple_txn_commit_offset_commit() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        super::simple_txn_commit_offset_commit(
            cluster_id,
            broker_id,
            storage_container(cluster_id, broker_id)?,
        )
        .await
    }

    #[tokio::test]
    async fn simple_txn_produce_commit() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        super::simple_txn_produce_commit(
            cluster_id,
            broker_id,
            storage_container(cluster_id, broker_id)?,
        )
        .await
    }

    #[tokio::test]
    async fn simple_txn_produce_abort() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        super::simple_txn_produce_abort(
            cluster_id,
            broker_id,
            storage_container(cluster_id, broker_id)?,
        )
        .await
    }

    #[tokio::test]
    async fn with_overlap() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        super::with_overlap(
            cluster_id,
            broker_id,
            storage_container(cluster_id, broker_id)?,
        )
        .await
    }

    #[tokio::test]
    async fn init_producer_twice() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        super::init_producer_twice(
            cluster_id,
            broker_id,
            storage_container(cluster_id, broker_id)?,
        )
        .await
    }
}
