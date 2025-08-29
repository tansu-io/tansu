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
    fmt::{self, Debug},
    marker::PhantomData,
};

use bytes::Bytes;
use opentelemetry::KeyValue;
use rama::{Context, Layer, Service, context::Extensions, matcher::Matcher, service::BoxService};
use tansu_sans_io::{ApiKey, Body, Frame, Header, Request, Response, RootMessageMeta};
use tracing::{Instrument as _, Level, debug, error, span};

use crate::{API_ERRORS, API_REQUESTS};

/// A [Matcher] of [`Request`]s using their [API key][`ApiKey`].
#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct RequestApiKeyMatcher(pub i16);

impl<State, Q> Matcher<State, Q> for RequestApiKeyMatcher
where
    Q: Request,
    State: Clone + Debug,
{
    fn matches(&self, ext: Option<&mut Extensions>, ctx: &Context<State>, req: &Q) -> bool {
        debug!(?ext, ?ctx, ?req);
        Q::KEY == self.0
    }
}

/// A [`Matcher`] of [`Frame`]s using their [API key][`Frame#method.api_key`]
#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct FrameApiKeyMatcher(pub i16);

impl<State> Matcher<State, Frame> for FrameApiKeyMatcher
where
    State: Clone + Debug,
{
    fn matches(&self, ext: Option<&mut Extensions>, ctx: &Context<State>, req: &Frame) -> bool {
        debug!(?ext, ?ctx, ?req);
        req.api_key().is_ok_and(|api_key| api_key == self.0)
    }
}

/// A [`Layer`] for handling API [`Request`]s responding with an API [`Response`]
#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct RequestLayer<Q> {
    request: PhantomData<Q>,
}

impl<Q> RequestLayer<Q> {
    pub fn new() -> Self {
        Self {
            request: PhantomData,
        }
    }
}

impl<S, Q> Layer<S> for RequestLayer<Q> {
    type Service = RequestService<S, Q>;

    fn layer(&self, inner: S) -> Self::Service {
        Self::Service {
            inner,
            request: PhantomData,
        }
    }
}

/// A [`Service`] that handles API [`Request`]s responding with an API [`Response`]
#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct RequestService<S, Q> {
    inner: S,
    request: PhantomData<Q>,
}

impl<State, S, Q> Service<State, Q> for RequestService<S, Q>
where
    S: Service<State, Q>,
    Q: Request,
    S::Error: From<<Q as TryFrom<Body>>::Error> + From<<S as Service<State, Q>>::Error>,
    S::Response: Response,
    Body: From<<S as Service<State, Q>>::Response>,
    State: Send + Sync + 'static,
{
    type Response = S::Response;
    type Error = S::Error;

    async fn serve(&self, ctx: Context<State>, req: Q) -> Result<Self::Response, Self::Error> {
        debug!(?req);
        self.inner
            .serve(ctx, req)
            .await
            .inspect(|response| debug!(?response))
    }
}

impl<S, Q> ApiKey for RequestService<S, Q>
where
    Q: Request,
{
    const KEY: i16 = Q::KEY;
}

/// A [`Layer`] that transforms [`Frame`] into [`Request`]
#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct FrameRequestLayer<Q> {
    request: PhantomData<Q>,
}

impl<Q> FrameRequestLayer<Q> {
    pub fn new() -> Self {
        Self {
            request: PhantomData,
        }
    }
}

impl<S, Q> Layer<S> for FrameRequestLayer<Q> {
    type Service = FrameRequestService<S, Q>;

    fn layer(&self, inner: S) -> Self::Service {
        Self::Service {
            inner,
            request: PhantomData,
        }
    }
}

/// A [`Service`] that transforms a [`Frame`] into a [`Request`]
#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct FrameRequestService<S, Q> {
    inner: S,
    request: PhantomData<Q>,
}

impl<S, Q, State> Service<State, Frame> for FrameRequestService<S, Q>
where
    S: Service<State, Q>,
    S::Response: Response,
    S::Error: From<tansu_sans_io::Error>,
    Q: Request + TryFrom<Body>,
    <Q as TryFrom<Body>>::Error: Into<S::Error>,
    State: Send + Sync + 'static,
{
    type Response = Frame;
    type Error = S::Error;

    async fn serve(&self, ctx: Context<State>, req: Frame) -> Result<Self::Response, Self::Error> {
        debug!(?req);
        let correlation_id = req.correlation_id()?;

        let req = Q::try_from(req.body).map_err(Into::into)?;

        self.inner.serve(ctx, req).await.map(|response| Frame {
            size: 0,
            header: Header::Response { correlation_id },
            body: response.into(),
        })
    }
}

impl<S, Q, State> Matcher<State, Frame> for FrameRequestService<S, Q>
where
    S: Clone + Send + Sync + 'static,
    Q: Request,
    State: Clone + Debug,
{
    fn matches(&self, ext: Option<&mut Extensions>, ctx: &Context<State>, req: &Frame) -> bool {
        debug!(?ext, ?ctx, ?req);
        req.api_key().is_ok_and(|api_key| api_key == Q::KEY)
    }
}

/// A [`Layer`] that transforms [`Bytes`] into [`Frame`]s
#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct BytesFrameLayer;

impl<S> Layer<S> for BytesFrameLayer {
    type Service = BytesFrameService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        Self::Service { inner }
    }
}

/// A [`Service`] transforming [`Bytes`]s into [`Frame`]s
#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct BytesFrameService<S> {
    inner: S,
}

impl<S, State> Service<State, Bytes> for BytesFrameService<S>
where
    S: Service<State, Frame, Response = Frame>,
    State: Clone + Send + Sync + 'static,
    S::Error: From<tansu_sans_io::Error> + Debug,
{
    type Response = Bytes;
    type Error = S::Error;

    async fn serve(&self, ctx: Context<State>, req: Bytes) -> Result<Self::Response, Self::Error> {
        let req = Frame::request_from_bytes(req).inspect(|req| debug!(?req))?;
        let api_key = req.api_key()?;
        let api_version = req.api_version()?;
        let correlation_id = req.correlation_id()?;

        let span = span!(
            Level::DEBUG,
            "frame",
            api_name = req.api_name(),
            api_version,
            correlation_id
        );

        async move {
            let attributes = vec![
                KeyValue::new("api_key", api_key as i64),
                KeyValue::new("api_version", api_version as i64),
            ];

            self.inner
                .serve(ctx, req)
                .await
                .inspect(|response| debug!(?response))
                .and_then(|Frame { body, .. }| {
                    Frame::response(
                        Header::Response { correlation_id },
                        body,
                        api_key,
                        api_version,
                    )
                    .map_err(Into::into)
                })
                .inspect(|response| {
                    debug!(?response);
                    API_REQUESTS.add(1, &attributes);
                })
                .inspect_err(|err| {
                    error!(api_key, api_version, ?err);
                    API_ERRORS.add(1, &attributes);
                })
        }
        .instrument(span)
        .await
    }
}

/// A [`Layer`] that transforms [`Frame`]s into [`Bytes`]
#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct FrameBytesLayer;

impl<S> Layer<S> for FrameBytesLayer {
    type Service = FrameBytesService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        Self::Service { inner }
    }
}

/// A [`Service`] that transforms [`Frame`]s into [`Bytes`]
#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct FrameBytesService<S> {
    inner: S,
}

impl<S, State> Service<State, Frame> for FrameBytesService<S>
where
    S: Service<State, Bytes, Response = Bytes>,
    S::Error: From<tansu_sans_io::Error>,
    State: Send + Sync + 'static,
{
    type Response = Frame;
    type Error = S::Error;

    async fn serve(&self, ctx: Context<State>, req: Frame) -> Result<Self::Response, Self::Error> {
        debug!(?req);

        let api_key = req.api_key()?;
        let api_version = req.api_version()?;

        let req = Frame::request(req.header, req.body)?;

        self.inner
            .serve(ctx, req)
            .await
            .and_then(|response| {
                Frame::response_from_bytes(response, api_key, api_version).map_err(Into::into)
            })
            .inspect(|response| debug!(?response))
    }
}

/// A [`Layer`] that transforms [`Frame`] into [`Body`]
#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct FrameBodyLayer;

impl<S> Layer<S> for FrameBodyLayer {
    type Service = FrameBodyService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        Self::Service { inner }
    }
}

/// A [`Service`] that transforms [`Frame`] into [`Body`]
#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct FrameBodyService<S> {
    inner: S,
}

impl<S, State> Service<State, Frame> for FrameBodyService<S>
where
    S: Service<State, Body, Response = Body>,
    S::Error: From<tansu_sans_io::Error>,
    State: Send + Sync + 'static,
{
    type Response = Frame;

    type Error = S::Error;

    async fn serve(&self, ctx: Context<State>, req: Frame) -> Result<Self::Response, Self::Error> {
        let correlation_id = req.correlation_id()?;

        self.inner.serve(ctx, req.body).await.map(|body| Frame {
            size: 0,
            header: Header::Response { correlation_id },
            body,
        })
    }
}

/// A [`Layer`] that transforms [`Body`] into [`Request`]
#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct BodyRequestLayer<Q> {
    request: PhantomData<Q>,
}

impl<Q> BodyRequestLayer<Q> {
    pub fn new() -> Self {
        Self {
            request: PhantomData,
        }
    }
}

impl<S, Q> Layer<S> for BodyRequestLayer<Q> {
    type Service = BodyRequestService<S, Q>;

    fn layer(&self, inner: S) -> Self::Service {
        Self::Service {
            inner,
            request: PhantomData,
        }
    }
}

/// A [`Layer`] that transforms [`Body`] into [`Request`]
#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct BodyRequestService<S, Q> {
    inner: S,
    request: PhantomData<Q>,
}

impl<S, Q> ApiKey for BodyRequestService<S, Q>
where
    Q: Request,
{
    const KEY: i16 = Q::KEY;
}

impl<S, State, Q> Service<State, Body> for BodyRequestService<S, Q>
where
    S: Service<State, Q>,
    Q: Request,
    S::Error: From<<Q as TryFrom<Body>>::Error> + From<<S as Service<State, Q>>::Error>,
    Body: From<<S as Service<State, Q>>::Response>,
    State: Send + Sync + 'static,
{
    type Response = Body;
    type Error = S::Error;

    async fn serve(&self, ctx: Context<State>, req: Body) -> Result<Self::Response, Self::Error> {
        let req = Q::try_from(req)?;
        self.inner.serve(ctx, req).await.map(Body::from)
    }
}

/// A [`Layer`] that transforms [`Request`] into [`Frame`]
#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct RequestFrameLayer;

impl<S> Layer<S> for RequestFrameLayer {
    type Service = RequestFrameService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        Self::Service { inner }
    }
}

/// A [`Service`] that transforms [`Request`] into [`Frame`]
#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct RequestFrameService<S> {
    inner: S,
}

impl<S, State, Q> Service<State, Q> for RequestFrameService<S>
where
    Q: Request,
    S: Service<State, Frame, Response = Frame>,
    S::Error: From<<<Q as Request>::Response as TryFrom<Body>>::Error>,
    State: Send + Sync + 'static,
{
    type Response = Q::Response;
    type Error = S::Error;

    async fn serve(&self, ctx: Context<State>, req: Q) -> Result<Self::Response, Self::Error> {
        debug!(?req);

        let api_key = Q::KEY;
        let api_version = RootMessageMeta::messages()
            .requests()
            .get(&api_key)
            .map(|message_meta| message_meta.version.valid().end)
            .unwrap_or_default();
        let correlation_id = 0;
        let client_id = Some(env!("CARGO_CRATE_NAME").into());

        let req = Frame {
            size: 0,
            header: Header::Request {
                api_key,
                api_version,
                correlation_id,
                client_id,
            },
            body: req.into(),
        };

        self.inner
            .serve(ctx, req)
            .await
            .and_then(|response| Q::Response::try_from(response.body).map_err(Into::into))
            .inspect(|response| debug!(?response))
    }
}

impl<S, State, Q, E> From<RequestService<S, Q>> for BoxService<State, Body, Body, E>
where
    S: Service<State, Q, Error = E>,
    Q: Request,
    <S as Service<State, Q>>::Response: Response,
    E: From<<Q as TryFrom<Body>>::Error> + From<<S as Service<State, Q>>::Error>,
    Body: From<<S as Service<State, Q>>::Response>,
    State: Send + Sync + 'static,
{
    fn from(value: RequestService<S, Q>) -> Self {
        BodyRequestLayer::<Q>::new().into_layer(value).boxed()
    }
}

impl<S, State, Q, E> From<RequestService<S, Q>> for BoxService<State, Frame, Frame, E>
where
    S: Service<State, Q, Error = E>,
    Q: Request,
    <S as Service<State, Q>>::Response: Response,
    E: From<tansu_sans_io::Error>
        + From<<Q as TryFrom<Body>>::Error>
        + From<<S as Service<State, Q>>::Error>,
    Body: From<<S as Service<State, Q>>::Response>,
    State: Send + Sync + 'static,
{
    fn from(value: RequestService<S, Q>) -> Self {
        (FrameBodyLayer, BodyRequestLayer::<Q>::new())
            .into_layer(value)
            .boxed()
    }
}

impl<S, State, Q, E> From<BodyRequestService<S, Q>> for BoxService<State, Frame, Frame, E>
where
    S: Service<State, Q, Error = E>,
    Q: Request,
    E: From<tansu_sans_io::Error>
        + From<<Q as TryFrom<Body>>::Error>
        + From<<S as Service<State, Q>>::Error>,
    Body: From<<S as Service<State, Q>>::Response>,
    State: Send + Sync + 'static,
{
    fn from(value: BodyRequestService<S, Q>) -> Self {
        FrameBodyLayer.into_layer(value).boxed()
    }
}

/// A [`Service`] that transforms [`Frame`] into a [`Frame`] using a closure.
#[derive(Clone, Copy, Debug, Hash)]
pub struct FrameService<F> {
    response: F,
}

impl<State, E, F> Service<State, Frame> for FrameService<F>
where
    F: Fn(Context<State>, Frame) -> Result<Frame, E> + Clone + Send + Sync + 'static,
    E: Send + Sync + 'static,
    State: Send + Sync + 'static,
{
    type Response = Frame;
    type Error = E;

    async fn serve(&self, ctx: Context<State>, req: Frame) -> Result<Self::Response, Self::Error> {
        (self.response)(ctx, req)
    }
}

impl<F> FrameService<F> {
    pub fn new<State, E>(response: F) -> Self
    where
        F: Fn(Context<State>, Frame) -> Result<Frame, E> + Clone,
        E: Send + Sync + 'static,
    {
        Self { response }
    }
}

/// A [`Service`] that transforms [`Request`] into a [`Response`] using a closure.
#[derive(Clone, Copy, Hash)]
pub struct ResponseService<F> {
    response: F,
}

impl<F> Debug for ResponseService<F> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ResponseService").finish()
    }
}

impl<State, Q, E, F> Service<State, Q> for ResponseService<F>
where
    F: Fn(Context<State>, Q) -> Result<Q::Response, E> + Clone + Send + Sync + 'static,
    Q: Request,
    E: Send + Sync + 'static,
    State: Send + Sync + 'static,
{
    type Response = Q::Response;
    type Error = E;

    async fn serve(&self, ctx: Context<State>, req: Q) -> Result<Self::Response, Self::Error> {
        (self.response)(ctx, req)
    }
}

impl<F> ResponseService<F> {
    pub fn new<State, Q, E>(response: F) -> Self
    where
        F: Fn(Context<State>, Q) -> Result<Q::Response, E> + Clone,
        Q: Request,
        E: Send + Sync + 'static,
    {
        Self { response }
    }
}
