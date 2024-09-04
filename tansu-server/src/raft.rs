// Copyright ⓒ 2024 Peter Morgan <peter.james.morgan@gmail.com>
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

use crate::{command::Command, Result, State, RAFT_LOG, RAFT_STATE};
use bytes::Bytes;
use std::{
    collections::BTreeMap,
    future::Future,
    io::Cursor,
    ops::RangeFrom,
    pin::Pin,
    sync::{Arc, Mutex, MutexGuard},
    task::{Context, Poll, Waker},
    time::Duration,
};
use tansu_kafka_sans_io::{
    record::{batch, compression, Record},
    Body,
};
use tansu_raft::{
    blocking::{persistent::PersistentManager, PersistentState, ProvidePersistentState},
    log_key_for_index, ApplyState, Configuration, Error as RaftError, Follower, Index, KvStore,
    LogEntry, PersistentEntry, ProvideApplyState, ProvideConfiguration, ProvideKvStore,
    ProvideServer, Server,
};
use tansu_storage::{Storage, Topition};
use tracing::{debug, instrument};
use url::Url;
use uuid::Uuid;

#[derive(Clone, Debug, Hash, Eq, Ord, PartialEq, PartialOrd)]
pub struct Config {
    election_timeout: Duration,
    listener: Url,
}

impl Config {
    pub fn new(election_timeout: Duration, listener: Url) -> Self {
        Self {
            election_timeout,
            listener,
        }
    }
}

impl Configuration for Config {
    fn election_timeout(&self) -> Result<Duration, RaftError> {
        Ok(self.election_timeout)
    }

    fn listener_url(&self) -> Result<Url, RaftError> {
        Ok(self.listener.clone())
    }
}

impl ProvideConfiguration for Config {
    fn provide_configuration(&self) -> Result<Box<dyn Configuration>, RaftError> {
        Ok(Box::new(self.clone()))
    }
}

#[derive(Clone, Debug)]
pub struct StorageFactory {
    pub storage: Arc<Mutex<Storage>>,
}

impl ProvideKvStore for StorageFactory {
    fn provide_kv_store(&self) -> Result<Box<dyn KvStore>, RaftError> {
        let storage = self.storage.clone();
        Ok(Box::new(SegmentStorage { storage }))
    }
}

#[derive(Clone, Debug)]
pub struct SegmentStorage {
    storage: Arc<Mutex<Storage>>,
}

#[derive(Clone, Debug)]
enum Offset {
    Offset(i64),
    Latest,
}

impl SegmentStorage {
    fn storage_lock(&self) -> Result<MutexGuard<'_, Storage>> {
        self.storage.lock().map_err(Into::into)
    }

    fn topition(key: Bytes) -> Result<Topition> {
        std::str::from_utf8(&key)
            .map(|key| {
                if key == "raft/state" {
                    Topition::new(RAFT_STATE, 0)
                } else {
                    Topition::new(RAFT_LOG, 0)
                }
            })
            .map_err(Into::into)
    }

    fn offset(key: Bytes) -> Result<Offset> {
        std::str::from_utf8(&key)
            .map_err(Into::into)
            .and_then(|key| {
                if key == "raft/state" {
                    Ok(Offset::Latest)
                } else {
                    str::parse::<i64>(&key[key.len() - 20..])
                        .map_err(Into::into)
                        .map(Offset::Offset)
                }
            })
    }
}

impl KvStore for SegmentStorage {
    fn get(&self, key: Bytes) -> Result<Option<Bytes>, RaftError> {
        Self::topition(key.clone())
            .inspect(|topition| debug!(?topition, ?key))
            .and_then(|topition| {
                self.storage_lock().and_then(|mut storage| {
                    Self::offset(key.clone())
                        .inspect(|offset| debug!(?offset))
                        .and_then(|offset| match offset {
                            Offset::Offset(offset) => Ok(offset - 1),

                            Offset::Latest => storage.high_watermark(&topition).map_err(Into::into),
                        })
                        .inspect(|offset| debug!(?offset))
                        .and_then(|offset| {
                            storage
                                .fetch(&topition, offset)
                                .map_err(Into::into)
                                .and_then(|deflated| {
                                    batch::Batch::try_from(deflated).map_err(Into::into)
                                })
                                .map(|batch| batch.records[0].value.clone())
                        })
                })
            })
            .inspect(|value| debug!(?value))
            .or(Ok(None))
    }

    fn put(&mut self, key: Bytes, value: Bytes) -> Result<(), RaftError> {
        debug!(?key, ?value);
        batch::Batch::builder()
            .record(Record::builder().value(value.into()))
            .build()
            .and_then(compression::Batch::try_from)
            .map_err(Into::into)
            .and_then(|batch| {
                Self::topition(key.clone())
                    .inspect(|topition| debug!(?topition, ?key))
                    .and_then(|topition| {
                        self.storage_lock().and_then(|mut storage| {
                            storage
                                .produce(&topition, batch)
                                .inspect(|offset| debug!(?offset))
                                .map(|_| ())
                                .map_err(Into::into)
                        })
                    })
            })
            .map_err(Into::into)
    }

    fn remove_range_from(&mut self, _range: RangeFrom<Bytes>) -> Result<(), RaftError> {
        todo!()
    }
}

#[derive(Clone, Debug, Default, Hash, Eq, Ord, PartialEq, PartialOrd)]
pub struct BTreeMapKvFactory {
    pub prev_log_index: Index,
    pub log_entries: Vec<LogEntry>,
}

impl ProvideKvStore for BTreeMapKvFactory {
    fn provide_kv_store(&self) -> Result<Box<dyn KvStore>, RaftError> {
        let mut index = self.prev_log_index;
        let mut db = BTreeMap::new();
        for entry in &self.log_entries {
            index += 1;
            _ = db.insert(log_key_for_index(index)?, Bytes::from(entry));
        }

        Ok(Box::new(BTreeMapKv { db }))
    }
}

#[derive(Clone, Debug, Default, Hash, Eq, Ord, PartialEq, PartialOrd)]
pub struct BTreeMapKv {
    db: BTreeMap<Bytes, Bytes>,
}

impl KvStore for BTreeMapKv {
    fn get(&self, key: Bytes) -> Result<Option<Bytes>, RaftError> {
        Ok(self.db.get(&key).cloned())
    }

    fn put(&mut self, key: Bytes, value: Bytes) -> Result<(), RaftError> {
        _ = self.db.insert(key, value);
        Ok(())
    }

    fn remove_range_from(&mut self, range: RangeFrom<Bytes>) -> Result<(), RaftError> {
        self.db.retain(|key, _| !range.contains(key));
        Ok(())
    }
}

#[derive(Clone, Debug, Default)]
pub struct Applicator {
    pending_when_applied: Arc<Mutex<BTreeMap<Uuid, WhenApplied>>>,
    pending_current_state: Arc<Mutex<Vec<Waker>>>,
    current: Arc<Mutex<Option<State>>>,
}

#[derive(Clone, Debug)]
struct WhenApplied {
    waker: Waker,
    shared: Arc<Mutex<Option<Body>>>,
}

impl Applicator {
    pub fn new() -> Self {
        Self {
            pending_when_applied: Arc::new(Mutex::new(BTreeMap::new())),
            pending_current_state: Arc::new(Mutex::new(Vec::new())),
            current: Arc::new(Mutex::new(None)),
        }
    }

    fn pending_when_applied_lock(&self) -> Result<MutexGuard<'_, BTreeMap<Uuid, WhenApplied>>> {
        self.pending_when_applied
            .lock()
            .map_err(|error| error.into())
    }

    fn pending_current_state_lock(&self) -> Result<MutexGuard<'_, Vec<Waker>>> {
        self.pending_current_state
            .lock()
            .map_err(|error| error.into())
    }

    fn current_lock(&self) -> Result<MutexGuard<'_, Option<State>>> {
        self.current.lock().map_err(|error| error.into())
    }

    #[instrument]
    fn register_when_applied(
        &self,
        id: Uuid,
        waker: Waker,
        shared: Arc<Mutex<Option<Body>>>,
    ) -> Result<()> {
        self.pending_when_applied_lock()
            .inspect(|on_application| debug!(?on_application))
            .map(|mut on_application| on_application.insert(id, WhenApplied { waker, shared }))
            .and(Ok(()))
    }

    #[instrument]
    fn unregister_when_applied(&self, id: Uuid) {
        if let Ok(mut on_application) = self.pending_when_applied_lock() {
            _ = on_application.remove(&id);
        }
    }

    fn register_current_state(&self, waker: Waker) -> Result<()> {
        self.pending_current_state_lock()
            .map(|mut pending| pending.push(waker))
            .and(Ok(()))
    }

    pub async fn when_applied(&self, id: Uuid) -> Body {
        WhenAppliedFuture::new(id, self).await
    }

    pub async fn with_current_state(&self) -> State {
        WithCurrentStateFuture::new(self).await
    }
}

impl ApplyState for Applicator {
    #[instrument]
    fn apply(
        &self,
        index: Index,
        state: Option<Bytes>,
        command: Bytes,
    ) -> Result<Bytes, RaftError> {
        let mut state = state
            .map_or_else(|| Bytes::try_from(State::new()), Ok)
            .and_then(State::try_from)
            .inspect(|state| debug!(?state))?;

        let command = Box::<dyn Command>::try_from(&command)?;

        let body = command.apply(&mut state);

        // update the state to include the Raft index that has been applied
        state.applied = index;
        debug!(?body, ?state);

        _ = self
            .current_lock()
            .map(|mut current| current.replace(state.clone()))?;

        self.pending_current_state_lock()
            .inspect(|pending| debug!(?pending))
            .map(|mut pending| {
                for ws in pending.drain(..) {
                    ws.wake()
                }
            })?;

        if let Some(id) = command.id() {
            debug!(?id, ?state);

            self.pending_when_applied_lock()
                .inspect(|when_applied| debug!(?when_applied))
                .and_then(|mut when_applied| {
                    when_applied.remove(&id).map_or(Ok(()), |ws| {
                        debug!(?ws);

                        ws.shared
                            .lock()
                            .map(|mut shared| {
                                debug!(?shared);
                                *shared = Some(body);
                                ws.waker.wake()
                            })
                            .map_err(|error| error.into())
                    })
                })?;
        }

        Bytes::try_from(state).map_err(Into::into)
    }
}

#[derive(Clone, Debug)]
struct WhenAppliedFuture<'a> {
    id: Uuid,
    applicator: &'a Applicator,
    state: Arc<Mutex<Option<Body>>>,
}

impl<'a> WhenAppliedFuture<'a> {
    fn lock(&self) -> Result<MutexGuard<'_, Option<Body>>> {
        self.state.lock().map_err(|error| error.into())
    }

    fn new(id: Uuid, applicator: &'a Applicator) -> Self {
        debug!(?id, ?applicator);

        Self {
            id,
            applicator,
            state: Arc::new(Mutex::new(None)),
        }
    }
}

impl<'a> Future for WhenAppliedFuture<'a> {
    type Output = Body;

    #[instrument]
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.lock()
            .map(|locked| {
                if let Some(ref state) = *locked {
                    debug!("poll, ready: {:?}", state);
                    Poll::Ready(state.clone())
                } else {
                    debug!("poll, pending registering waker");
                    self.applicator
                        .register_when_applied(self.id, cx.waker().clone(), self.state.clone())
                        .unwrap();
                    Poll::Pending
                }
            })
            .unwrap()
    }
}

impl<'a> Drop for WhenAppliedFuture<'a> {
    #[instrument]
    fn drop(&mut self) {
        self.applicator.unregister_when_applied(self.id)
    }
}

#[derive(Clone, Debug)]
struct WithCurrentStateFuture<'a> {
    applicator: &'a Applicator,
    state: Arc<Mutex<Option<State>>>,
}

impl<'a> WithCurrentStateFuture<'a> {
    fn lock(&self) -> Result<MutexGuard<'_, Option<State>>> {
        self.state.lock().map_err(|error| error.into())
    }

    fn new(applicator: &'a Applicator) -> Self {
        Self {
            applicator,
            state: applicator.current.clone(),
        }
    }
}

impl<'a> Future for WithCurrentStateFuture<'a> {
    type Output = State;

    #[instrument]
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.lock()
            .map(|locked| {
                if let Some(ref state) = *locked {
                    debug!("poll, ready: {state:?}");
                    Poll::Ready(state.clone())
                } else {
                    debug!("poll, pending registering waker");
                    self.applicator
                        .register_current_state(cx.waker().clone())
                        .unwrap();
                    Poll::Pending
                }
            })
            .unwrap()
    }
}

#[derive(Clone, Debug, Default)]
pub struct ApplyStateFactory {
    applicator: Applicator,
}

impl ApplyStateFactory {
    pub fn new(applicator: Applicator) -> Self {
        Self { applicator }
    }
}

impl ProvideApplyState for ApplyStateFactory {
    fn provide_apply_state(&self) -> Result<Box<dyn ApplyState>, RaftError> {
        Ok(Box::new(self.applicator.clone()))
    }
}

#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct PersistentStateFactory {
    pub id: Url,
    pub initial_persistent_state: Vec<u8>,
}

impl ProvidePersistentState for PersistentStateFactory {
    fn provide_persistent_state(&self) -> Result<Box<dyn PersistentState>, RaftError> {
        let inner = Cursor::new(self.initial_persistent_state.clone());
        Ok(Box::new(
            PersistentManager::builder()
                .inner(inner)
                .id(self.id.clone())
                .recover()?,
        ))
    }
}

#[derive(Clone, Debug)]
pub struct StoragePersistentStateFactory {
    pub id: Url,
    pub storage: Arc<Mutex<Storage>>,
}

impl ProvidePersistentState for StoragePersistentStateFactory {
    fn provide_persistent_state(&self) -> Result<Box<dyn PersistentState>, RaftError> {
        let id = self.id.clone();
        let storage = self.storage.clone();

        Ok(Box::new(StoragePersistentState { id, storage }))
    }
}

#[derive(Clone, Debug)]
pub struct StoragePersistentState {
    id: Url,
    storage: Arc<Mutex<Storage>>,
}

impl StoragePersistentState {
    const TOPIC_NAME: &'static str = "raft_persistent";

    fn storage_lock(&mut self) -> Result<MutexGuard<'_, Storage>> {
        self.storage.lock().map_err(Into::into)
    }

    fn topition() -> Topition {
        Topition::new(Self::TOPIC_NAME, 0)
    }
}

impl PersistentState for StoragePersistentState {
    fn read(&mut self) -> Result<PersistentEntry, RaftError> {
        let topition = Self::topition();

        self.storage_lock()
            .and_then(|mut storage| {
                storage
                    .high_watermark(&topition)
                    .and_then(|offset| storage.fetch(&topition, offset))
                    .map_err(Into::into)
            })
            .and_then(|deflated| batch::Batch::try_from(deflated).map_err(Into::into))
            .map_err(Into::into)
            .and_then(|batch| {
                batch.records[0]
                    .value
                    .as_ref()
                    .map_or(Ok(PersistentEntry::default()), |value| {
                        PersistentEntry::try_from(value.to_owned())
                    })
            })
            .or(Ok(PersistentEntry::default()))
            .inspect(|entry| debug!(?entry))
    }

    fn write(&mut self, entry: PersistentEntry) -> Result<(), RaftError> {
        let value = Bytes::from(entry);

        batch::Batch::builder()
            .record(Record::builder().value(value.into()))
            .build()
            .and_then(compression::Batch::try_from)
            .map_err(Into::into)
            .and_then(|batch| {
                let topition = Self::topition();
                self.storage_lock().and_then(|mut storage| {
                    storage
                        .produce(&topition, batch)
                        .inspect(|offset| debug!(?offset))
                        .map(|_| ())
                        .map_err(Into::into)
                })
            })
            .map_err(Into::into)
    }

    fn id(&self) -> Url {
        self.id.clone()
    }
}

#[derive(Clone, Debug)]
pub struct ServerFactory {
    pub storage: Arc<Mutex<Storage>>,
}

impl ServerFactory {
    fn storage_lock(&mut self) -> Result<MutexGuard<'_, Storage>> {
        self.storage.lock().map_err(Into::into)
    }

    fn commit_index(&mut self) -> Result<u64> {
        let log = Topition::new(RAFT_LOG, 0);

        self.storage_lock().and_then(|storage| {
            storage
                .high_watermark(&log)
                .or(Ok(-1))
                .inspect(|high_watermark| debug!(?high_watermark))
                .and_then(|offset| u64::try_from(offset + 1))
                .map_err(Into::into)
        })
    }

    fn last_applied(&mut self) -> Result<u64> {
        let state = Topition::new(RAFT_STATE, 0);

        self.storage_lock().and_then(|mut storage| {
            storage
                .high_watermark(&state)
                .map_err(Into::into)
                .inspect(|high_watermark| debug!(?high_watermark))
                .and_then(|offset| storage.fetch(&state, offset).map_err(Into::into))
                .and_then(|deflated| batch::Batch::try_from(deflated).map_err(Into::into))
                .and_then(|batch| {
                    if let Some(value) = batch.records[0].value.as_ref() {
                        State::try_from(value).map(|state| state.applied)
                    } else {
                        Ok(0)
                    }
                })
                .or(Ok(0))
        })
    }
}

impl ProvideServer for ServerFactory {
    #[instrument]
    fn provide_server(&mut self) -> Result<Box<dyn Server>, RaftError> {
        self.commit_index()
            .map_err(Into::into)
            .and_then(|commit_index| {
                self.last_applied().map_err(Into::into).map(|last_applied| {
                    Box::new(
                        Follower::builder()
                            .commit_index(commit_index)
                            .last_applied(last_applied)
                            .build(),
                    ) as Box<dyn Server>
                })
            })
    }
}
