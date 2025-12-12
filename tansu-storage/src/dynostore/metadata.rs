// Copyright ⓒ 2024-2025 Peter Morgan <peter.james.morgan@gmail.com>
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::{
    collections::HashMap,
    fmt::Display,
    ops::{Deref, DerefMut},
    slice::from_ref,
    sync::{Arc, LazyLock, Mutex, MutexGuard},
    time::{Duration, SystemTime},
};

use async_trait::async_trait;
use futures::stream::BoxStream;
use object_store::{
    GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    PutMultipartOptions, PutOptions, PutPayload, PutResult, UpdateVersion, path::Path,
};
use opentelemetry::{KeyValue, metrics::Counter};
use tracing::{debug, instrument};

use crate::{Error, dynostore::object_store_error_name};

use super::METER;

#[derive(Clone, Debug, Eq, PartialEq)]
struct CacheEntry {
    version: UpdateVersion,
    tagged_at: SystemTime,
}

impl From<&PutResult> for CacheEntry {
    fn from(put_result: &PutResult) -> Self {
        debug!(?put_result);

        let e_tag = put_result.e_tag.clone();
        let version = put_result.version.clone();

        Self {
            version: UpdateVersion { e_tag, version },
            tagged_at: SystemTime::now(),
        }
    }
}

impl From<&GetResult> for CacheEntry {
    fn from(get_result: &GetResult) -> Self {
        debug!(?get_result);

        let e_tag = get_result.meta.e_tag.clone();
        let version = get_result.meta.version.clone();

        Self {
            version: UpdateVersion { e_tag, version },
            tagged_at: SystemTime::now(),
        }
    }
}

impl CacheEntry {
    fn hit(&mut self) {
        self.tagged_at = SystemTime::now();
    }
}

#[derive(Clone, Debug)]
pub(super) struct Cache<O> {
    entries: Arc<Mutex<HashMap<Path, CacheEntry>>>,
    object_store: O,
    retention: Duration,
}

impl<O> Display for Cache<O> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Cache").finish()
    }
}

#[allow(dead_code)]
impl<O> Cache<O>
where
    O: ObjectStore,
{
    pub(super) fn new(object_store: O, retention: Duration) -> Self {
        let entries = Arc::new(Mutex::new(HashMap::new()));

        Self {
            entries,
            object_store,
            retention,
        }
    }

    fn into_inner(self) -> O {
        self.object_store
    }

    fn inner(&self) -> &O {
        &self.object_store
    }

    fn evict(
        &self,
        guard: &mut MutexGuard<'_, HashMap<Path, CacheEntry>>,
        attributes: &[KeyValue],
    ) {
        let now = SystemTime::now();

        let original = guard.deref().len();
        guard.deref_mut().retain(|_location, entry| {
            now.duration_since(entry.tagged_at)
                .is_ok_and(|elapsed| elapsed.as_millis() < self.retention.as_millis())
        });

        let mut a = vec![KeyValue::new("outcome", "evict")];
        a.extend_from_slice(attributes);

        ENTRIES.add(
            u64::try_from(original - guard.deref().len()).unwrap_or_default(),
            &a,
        );
    }
}

static REQUESTS: LazyLock<Counter<u64>> = LazyLock::new(|| {
    METER
        .u64_counter("tansu_objectstore_cache_requests")
        .with_description("object_store cache requests")
        .build()
});

static ERRORS: LazyLock<Counter<u64>> = LazyLock::new(|| {
    METER
        .u64_counter("tansu_objectstore_cache_errors")
        .with_description("object_store cache errors")
        .build()
});

static OUTCOMES: LazyLock<Counter<u64>> = LazyLock::new(|| {
    METER
        .u64_counter("tansu_objectstore_cache_outcomes")
        .with_description("object_store cache outcomes")
        .build()
});

static ENTRIES: LazyLock<Counter<u64>> = LazyLock::new(|| {
    METER
        .u64_counter("tansu_objectstore_cache_entries")
        .with_description("object_store cache entries")
        .build()
});

#[async_trait]
impl<O> ObjectStore for Cache<O>
where
    O: ObjectStore,
{
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> Result<PutResult, object_store::Error> {
        debug!(%location, ?opts);

        let method = KeyValue::new("method", "put_opts");

        REQUESTS.add(1, from_ref(&method));

        self.object_store
            .put_opts(location, payload, opts)
            .await
            .inspect(|put_result| {
                debug!(?put_result);
                if let Ok(mut guard) = self.entries.lock() {
                    self.evict(&mut guard, from_ref(&method));

                    let replacement = CacheEntry::from(put_result);

                    let outcome = match guard
                        .deref_mut()
                        .insert(location.to_owned(), replacement.clone())
                    {
                        None => "add",

                        Some(existing) if existing == replacement => "existing",

                        Some(_) => "replace",
                    };

                    debug!(%location, outcome);

                    ENTRIES.add(1, &[method.clone(), KeyValue::new("outcome", outcome)]);
                }
            })
            .inspect_err(|error| {
                debug!(%location, ?error);

                if let Ok(mut guard) = self.entries.lock() {
                    self.evict(&mut guard, from_ref(&method));

                    if guard.deref_mut().remove(location).is_some() {
                        ENTRIES.add(1, &[method.clone(), KeyValue::new("outcome", "error")]);
                    }
                }

                ERRORS.add(
                    1,
                    &[
                        method,
                        KeyValue::new("error", object_store_error_name(error)),
                    ],
                );
            })
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> Result<Box<dyn MultipartUpload>, object_store::Error> {
        debug!(%location, ?opts);

        let method = KeyValue::new("method", "put_multipart_opts");

        REQUESTS.add(1, from_ref(&method));

        self.object_store
            .put_multipart_opts(location, opts)
            .await
            .inspect_err(|error| {
                debug!(%location, ?error);

                ERRORS.add(
                    1,
                    &[
                        method,
                        KeyValue::new("error", object_store_error_name(error)),
                    ],
                );
            })
    }

    #[instrument(skip_all, fields(%location), ret)]
    async fn get_opts(
        &self,
        location: &Path,
        options: GetOptions,
    ) -> Result<GetResult, object_store::Error> {
        debug!(?options);

        let method = KeyValue::new("method", "get_opts");

        REQUESTS.add(1, from_ref(&method));

        if let Ok(mut guard) = self.entries.lock() {
            self.evict(&mut guard, from_ref(&method));

            if let Some(entry) = guard.deref_mut().get_mut(location) {
                debug!(?entry);

                if let Some(ref cached_e_tag) = entry.version.e_tag {
                    debug!(cached_e_tag);

                    if let Some(ref presented) = options.if_none_match {
                        debug!(cached_e_tag, presented);

                        if cached_e_tag == presented {
                            entry.hit();

                            let outcome = "hit";

                            OUTCOMES.add(1, &[method, KeyValue::new("outcome", outcome)]);
                            debug!(outcome);

                            return Err(object_store::Error::NotModified {
                                path: location.to_string(),
                                source: Box::new(Error::PhantomCached()),
                            });
                        } else {
                            let outcome = "no_match";
                            debug!(outcome);

                            OUTCOMES.add(1, &[method.clone(), KeyValue::new("outcome", outcome)]);
                        }
                    } else {
                        let outcome = "miss";

                        debug!(outcome);
                        OUTCOMES.add(1, &[method.clone(), KeyValue::new("outcome", outcome)]);
                    }
                } else {
                    let outcome = "miss";

                    debug!(outcome);
                    OUTCOMES.add(1, &[method.clone(), KeyValue::new("outcome", outcome)]);
                }
            } else {
                let outcome = "miss";

                debug!(outcome);
                OUTCOMES.add(1, &[method.clone(), KeyValue::new("outcome", outcome)]);
            }
        }

        self.object_store
            .get_opts(location, options)
            .await
            .inspect(|get_result| {
                let e_tag = get_result.meta.e_tag.clone();
                let version = get_result.meta.version.clone();

                if let Ok(mut guard) = self.entries.lock() {
                    debug!(%location, e_tag, version);

                    let replacement = CacheEntry::from(get_result);

                    let outcome = match guard
                        .deref_mut()
                        .insert(location.to_owned(), replacement.clone())
                    {
                        None => "add",

                        Some(existing) if existing == replacement => "existing",

                        Some(_) => "replace",
                    };

                    ENTRIES.add(1, &[method.clone(), KeyValue::new("outcome", outcome)]);
                }
            })
            .inspect_err(|error| {
                debug!(%location, ?error);

                if let Ok(mut guard) = self.entries.lock() {
                    debug!(%location);
                    self.evict(&mut guard, from_ref(&method));

                    if guard.deref_mut().remove(location).is_some() {
                        ENTRIES.add(1, &[method.clone(), KeyValue::new("outcome", "error")]);
                    }
                }

                ERRORS.add(
                    1,
                    &[
                        method,
                        KeyValue::new("error", object_store_error_name(error)),
                    ],
                );
            })
    }

    async fn delete(&self, location: &Path) -> Result<(), object_store::Error> {
        debug!(%location);

        REQUESTS.add(1, &[KeyValue::new("method", "delete")]);

        self.object_store
            .delete(location)
            .await
            .inspect(|_| {
                if let Ok(mut guard) = self.entries.lock()
                    && guard.deref_mut().remove(location).is_some()
                {
                    ENTRIES.add(1, &[KeyValue::new("outcome", "delete")]);
                }
            })
            .inspect_err(|error| {
                debug!(%location, ?error);

                ERRORS.add(
                    1,
                    &[
                        KeyValue::new("method", "delete"),
                        KeyValue::new("error", object_store_error_name(error)),
                    ],
                );
            })
    }

    fn list(
        &self,
        prefix: Option<&Path>,
    ) -> BoxStream<'static, Result<ObjectMeta, object_store::Error>> {
        debug!(?prefix);
        REQUESTS.add(1, &[KeyValue::new("method", "list")]);
        self.object_store.list(prefix)
    }

    async fn list_with_delimiter(
        &self,
        prefix: Option<&Path>,
    ) -> Result<ListResult, object_store::Error> {
        debug!(?prefix);
        REQUESTS.add(1, &[KeyValue::new("method", "list_with_delimiter")]);
        self.object_store
            .list_with_delimiter(prefix)
            .await
            .inspect_err(|error| {
                debug!(?prefix, ?error);

                ERRORS.add(
                    1,
                    &[
                        KeyValue::new("method", "list_with_delimiter"),
                        KeyValue::new("error", object_store_error_name(error)),
                    ],
                );
            })
    }

    async fn copy(&self, from: &Path, to: &Path) -> Result<(), object_store::Error> {
        debug!(%from, %to);
        REQUESTS.add(1, &[KeyValue::new("method", "copy")]);
        self.object_store.copy(from, to).await.inspect_err(|error| {
            debug!(%from, %to, ?error);

            ERRORS.add(
                1,
                &[
                    KeyValue::new("method", "copy"),
                    KeyValue::new("error", object_store_error_name(error)),
                ],
            );
        })
    }

    async fn copy_if_not_exists(&self, from: &Path, to: &Path) -> Result<(), object_store::Error> {
        debug!(%from, %to);
        REQUESTS.add(1, &[KeyValue::new("method", "copy_if_not_exists")]);
        self.object_store
            .copy_if_not_exists(from, to)
            .await
            .inspect_err(|error| {
                debug!(%from, %to, ?error);

                ERRORS.add(
                    1,
                    &[
                        KeyValue::new("method", "copy_if_not_exists"),
                        KeyValue::new("error", object_store_error_name(error)),
                    ],
                );
            })
    }
}

#[cfg(test)]
mod tests {
    use crate::Result;
    use bytes::Bytes;
    use object_store::memory::InMemory;
    use serde::{Deserialize, Serialize};
    use tokio::time::sleep;
    use tracing::subscriber::DefaultGuard;
    use tracing_subscriber::EnvFilter;

    use super::*;

    #[derive(
        Clone, Debug, Default, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize,
    )]
    struct X(i32);

    #[derive(Clone, Debug)]
    struct Counter<O> {
        put_opts: Arc<Mutex<u64>>,
        get_opts: Arc<Mutex<u64>>,
        object_store: O,
    }

    #[allow(dead_code)]
    impl<O> Counter<O> {
        fn new(object_store: O) -> Self {
            Self {
                put_opts: Default::default(),
                get_opts: Default::default(),
                object_store,
            }
        }

        fn put_opts(&self) -> Result<u64> {
            self.put_opts
                .lock()
                .map(|guard| *guard.deref())
                .map_err(Into::into)
        }

        fn get_opts(&self) -> Result<u64> {
            self.get_opts
                .lock()
                .map(|guard| *guard.deref())
                .map_err(Into::into)
        }
    }

    impl<O> Display for Counter<O> {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.debug_struct("Counter").finish()
        }
    }

    #[async_trait]
    impl<O> ObjectStore for Counter<O>
    where
        O: ObjectStore,
    {
        async fn put_opts(
            &self,
            location: &Path,
            payload: PutPayload,
            opts: PutOptions,
        ) -> Result<PutResult, object_store::Error> {
            if let Ok(mut guard) = self.put_opts.lock() {
                *guard += 1;
            }

            self.object_store.put_opts(location, payload, opts).await
        }

        async fn put_multipart_opts(
            &self,
            location: &Path,
            opts: PutMultipartOptions,
        ) -> Result<Box<dyn MultipartUpload>, object_store::Error> {
            self.object_store.put_multipart_opts(location, opts).await
        }

        async fn get_opts(
            &self,
            location: &Path,
            options: GetOptions,
        ) -> Result<GetResult, object_store::Error> {
            if let Ok(mut guard) = self.get_opts.lock() {
                *guard += 1;
            }

            self.object_store.get_opts(location, options).await
        }

        async fn delete(&self, location: &Path) -> Result<(), object_store::Error> {
            self.object_store.delete(location).await
        }

        fn list(
            &self,
            prefix: Option<&Path>,
        ) -> BoxStream<'static, Result<ObjectMeta, object_store::Error>> {
            self.object_store.list(prefix)
        }

        async fn list_with_delimiter(
            &self,
            prefix: Option<&Path>,
        ) -> Result<ListResult, object_store::Error> {
            self.object_store.list_with_delimiter(prefix).await
        }

        async fn copy(&self, from: &Path, to: &Path) -> Result<(), object_store::Error> {
            self.object_store.copy(from, to).await
        }

        async fn copy_if_not_exists(
            &self,
            from: &Path,
            to: &Path,
        ) -> Result<(), object_store::Error> {
            self.object_store.copy_if_not_exists(from, to).await
        }
    }

    fn init_tracing() -> Result<DefaultGuard> {
        use std::{fs::File, sync::Arc, thread};

        Ok(tracing::subscriber::set_default(
            tracing_subscriber::fmt()
                .with_level(true)
                .with_line_number(true)
                .with_thread_names(false)
                .with_env_filter(EnvFilter::from_default_env().add_directive(
                    format!("{}=debug", env!("CARGO_PKG_NAME").replace("-", "_")).parse()?,
                ))
                .with_writer(
                    thread::current()
                        .name()
                        .ok_or(Error::Message(String::from("unnamed thread")))
                        .and_then(|name| {
                            File::create(format!("../logs/{}/{name}.log", env!("CARGO_PKG_NAME"),))
                                .map_err(Into::into)
                        })
                        .map(Arc::new)?,
                )
                .finish(),
        ))
    }

    #[tokio::test]
    async fn get() -> Result<()> {
        let _guard = init_tracing()?;

        let id = "test";
        let path = Path::from(format!("/abc/{id}.json"));

        let object_store = Counter::new(InMemory::new());

        _ = object_store
            .put(
                &path,
                serde_json::to_vec(&X(6))
                    .map(Bytes::from)
                    .map(PutPayload::from)?,
            )
            .await?;

        let duration = Duration::from_millis(100);

        let cache = Cache::new(object_store, duration);

        assert_eq!(0, cache.inner().get_opts()?);

        let metadata = cache.get(&path).await?;
        assert_eq!(1, cache.inner().get_opts()?);

        let options = GetOptions {
            if_none_match: metadata.meta.e_tag.clone(),
            ..Default::default()
        };

        assert!(matches!(
            cache.get_opts(&path, options).await,
            Err(object_store::Error::NotModified { .. })
        ));
        assert_eq!(1, cache.inner().get_opts()?);

        Ok(())
    }

    #[tokio::test]
    async fn get_evict_get() -> Result<()> {
        let _guard = init_tracing()?;

        let id = "test";
        let path = Path::from(format!("/abc/{id}.json"));

        let object_store = Counter::new(InMemory::new());

        _ = object_store
            .put(
                &path,
                serde_json::to_vec(&X(6))
                    .map(Bytes::from)
                    .map(PutPayload::from)?,
            )
            .await?;

        let duration = Duration::from_millis(100);
        let cache = Cache::new(object_store, duration);

        assert_eq!(0, cache.inner().get_opts()?);

        let metadata = cache.get(&path).await?;
        assert_eq!(1, cache.inner().get_opts()?);

        let options = GetOptions {
            if_none_match: metadata.meta.e_tag.clone(),
            ..Default::default()
        };

        assert!(matches!(
            cache.get_opts(&path, options.clone()).await,
            Err(object_store::Error::NotModified { .. })
        ));
        assert_eq!(1, cache.inner().get_opts()?);

        sleep(duration).await;

        assert!(matches!(
            cache.get_opts(&path, options).await,
            Err(object_store::Error::NotModified { .. })
        ));
        assert_eq!(2, cache.inner().get_opts()?);

        Ok(())
    }

    #[tokio::test]
    async fn put_get() -> Result<()> {
        let _guard = init_tracing()?;

        let id = "test";
        let path = Path::from(format!("/abc/{id}.json"));

        let duration = Duration::from_millis(100);
        let cache = Cache::new(Counter::new(InMemory::new()), duration);

        assert_eq!(0, cache.inner().put_opts()?);
        assert_eq!(0, cache.inner().get_opts()?);

        let put_result = cache
            .put(
                &path,
                serde_json::to_vec(&X(6))
                    .map(Bytes::from)
                    .map(PutPayload::from)?,
            )
            .await?;
        assert_eq!(1, cache.inner().put_opts()?);
        assert_eq!(0, cache.inner().get_opts()?);

        let options = GetOptions {
            if_none_match: put_result.e_tag,
            ..Default::default()
        };

        assert!(matches!(
            cache.get_opts(&path, options.clone()).await,
            Err(object_store::Error::NotModified { .. })
        ));
        assert_eq!(0, cache.inner().get_opts()?);

        assert!(matches!(
            cache.get_opts(&path, options).await,
            Err(object_store::Error::NotModified { .. })
        ));
        assert_eq!(0, cache.inner().get_opts()?);

        Ok(())
    }

    #[tokio::test]
    async fn put_delete_get() -> Result<()> {
        let _guard = init_tracing()?;

        let id = "test";
        let path = Path::from(format!("/abc/{id}.json"));

        let duration = Duration::from_millis(100);
        let cache = Cache::new(Counter::new(InMemory::new()), duration);

        assert_eq!(0, cache.inner().put_opts()?);
        assert_eq!(0, cache.inner().get_opts()?);

        let put_result = cache
            .put(
                &path,
                serde_json::to_vec(&X(6))
                    .map(Bytes::from)
                    .map(PutPayload::from)?,
            )
            .await?;
        assert_eq!(1, cache.inner().put_opts()?);
        assert_eq!(0, cache.inner().get_opts()?);

        cache.delete(&path).await?;

        let options = GetOptions {
            if_none_match: put_result.e_tag,
            ..Default::default()
        };

        assert!(matches!(
            cache.get_opts(&path, options.clone()).await,
            Err(object_store::Error::NotFound { .. })
        ));
        assert_eq!(1, cache.inner().get_opts()?);

        Ok(())
    }
}
