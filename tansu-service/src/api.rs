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

use std::{collections::BTreeMap, marker::PhantomData, sync::Arc};

use rama::{Context, Service, service::BoxService};
use tansu_sans_io::{
    ApiKey, ApiVersionsRequest, ApiVersionsResponse, Body, ErrorCode, Frame, Header,
    RootMessageMeta, api_versions_response::ApiVersion,
};

use crate::Error;

#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ApiVersionRange {
    pub min_version: i16,
    pub max_version: i16,
}

/// An [`ApiVersionsResponse`] [`Service`] with a supported set of API and versions from [`RootMessageMeta`].
#[derive(Clone, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ApiVersionsService<E> {
    advertised_versions: BTreeMap<i16, ApiVersionRange>,
    error: PhantomData<E>,
}

impl<E> ApiVersionsService<E> {
    fn advertised_api_keys(&self) -> Vec<ApiVersion> {
        self.advertised_versions
            .iter()
            .map(|(api_key, range)| {
                ApiVersion::default()
                    .api_key(*api_key)
                    .min_version(range.min_version)
                    .max_version(range.max_version)
            })
            .collect()
    }

    fn supported_api_versions() -> Vec<ApiVersion> {
        vec![
            ApiVersion::default()
                .api_key(ApiVersionsRequest::KEY)
                .min_version(0)
                .max_version(4),
        ]
    }

    fn normal_response(&self) -> ApiVersionsResponse {
        ApiVersionsResponse::default()
            .finalized_features(Some([].into()))
            .finalized_features_epoch(Some(-1))
            .supported_features(Some([].into()))
            .zk_migration_ready(Some(false))
            .error_code(ErrorCode::None.into())
            .api_keys(Some(self.advertised_api_keys().into()))
            .throttle_time_ms(Some(0))
    }

    fn unsupported_version_response(&self) -> ApiVersionsResponse {
        ApiVersionsResponse::default()
            .error_code(ErrorCode::UnsupportedVersion.into())
            .api_keys(Some(Self::supported_api_versions().into()))
    }
}

impl<State, E> Service<State, ApiVersionsRequest> for ApiVersionsService<E>
where
    State: Clone + Send + Sync + 'static,
    E: std::error::Error + Send + Sync + 'static,
{
    type Response = ApiVersionsResponse;
    type Error = E;

    async fn serve(
        &self,
        _ctx: Context<State>,
        _req: ApiVersionsRequest,
    ) -> Result<Self::Response, Self::Error> {
        Ok::<_, E>(self.normal_response())
    }
}

impl<State, E> Service<State, Body> for ApiVersionsService<E>
where
    State: Clone + Send + Sync + 'static,
    E: std::error::Error + From<tansu_sans_io::Error> + Send + Sync + 'static,
{
    type Response = Body;
    type Error = E;

    async fn serve(&self, ctx: Context<State>, req: Body) -> Result<Self::Response, Self::Error> {
        let req = ApiVersionsRequest::try_from(req)?;
        self.serve(ctx, req).await.map(Into::into)
    }
}

impl<State, E> Service<State, Frame> for ApiVersionsService<E>
where
    State: Clone + Send + Sync + 'static,
    E: std::error::Error + From<tansu_sans_io::Error> + Send + Sync + 'static,
{
    type Response = Frame;
    type Error = E;

    async fn serve(&self, _ctx: Context<State>, req: Frame) -> Result<Self::Response, Self::Error> {
        let correlation_id = req.correlation_id()?;
        let body = if req.api_version()? > 4 {
            self.unsupported_version_response()
        } else {
            self.normal_response()
        };

        Ok(Frame {
            size: 0,
            header: Header::Response { correlation_id },
            body: body.into(),
        })
    }
}

/// Route [`Frame`] to a [`Service`] via [API key][`Frame#method.api_key`]
///
/// A simple example that routes [`MetadataRequest`][`tansu_sans_io::MetadataRequest`]
/// and [`CreateTopicsRequest`][`tansu_sans_io::CreateTopicsRequest`].
/// [`ApiVersionsRequest`][`tansu_sans_io::ApiVersionsRequest`] is created by the
/// builder using either an explicit advertised-version map or the descriptor-backed
/// route versions when no explicit map has been configured.
///
/// ```
/// # use rama::Layer as _;
/// # use tansu_sans_io::{CreateTopicsRequest, CreateTopicsResponse, MetadataRequest, MetadataResponse};
/// # use tansu_service::{Error, FrameRouteService, RequestLayer, ResponseService};
/// # #[tokio::main]
/// # async fn main() -> Result<(), Error> {
/// let router = FrameRouteService::<(), Error>::builder()
///     .with_service(
///         RequestLayer::<MetadataRequest>::new().into_layer(ResponseService::new(|_, _| {
///             Ok(MetadataResponse::default()
///                 .brokers(Some([].into()))
///                 .topics(Some([].into()))
///                 .cluster_id(Some("tansu".into()))
///                 .controller_id(Some(111))
///                 .throttle_time_ms(Some(0))
///                 .cluster_authorized_operations(Some(-1)))
///         })),
///     )
///     .and_then(|builder| {
///         builder.with_service(RequestLayer::<CreateTopicsRequest>::new().into_layer(
///             ResponseService::new(|_, _| {
///                 Ok(CreateTopicsResponse::default()
///                     .throttle_time_ms(Some(0))
///                     .topics(Some([].into())))
///             }),
///         ))
///     })
///     .and_then(|builder| builder.build())?;
/// # Ok(())
/// # }
/// ```
#[derive(Clone, Debug, Default)]
pub struct FrameRouteService<State = (), E = Error> {
    routes: Arc<BTreeMap<i16, BoxService<State, Frame, Frame, E>>>,
    advertised_versions: Arc<BTreeMap<i16, ApiVersionRange>>,
}

impl<State, E> FrameRouteService<State, E>
where
    State: Clone + Send + Sync + 'static,
    E: std::error::Error + From<tansu_sans_io::Error> + From<Error> + Send + Sync + 'static,
{
    pub fn new(
        routes: Arc<BTreeMap<i16, BoxService<State, Frame, Frame, E>>>,
        advertised_versions: Arc<BTreeMap<i16, ApiVersionRange>>,
    ) -> Self {
        Self {
            routes,
            advertised_versions,
        }
    }

    /// Returns the routed API keys in ascending order.
    pub fn route_keys(&self) -> Vec<i16> {
        self.routes.keys().copied().collect()
    }

    pub fn builder() -> FrameRouteBuilder<State, E> {
        FrameRouteBuilder::<State, E>::new()
    }
}

impl<State, E> Service<State, Frame> for FrameRouteService<State, E>
where
    State: Clone + Send + Sync + 'static,
    E: std::error::Error + From<tansu_sans_io::Error> + From<Error> + Send + Sync + 'static,
{
    type Response = Frame;
    type Error = E;

    async fn serve(&self, ctx: Context<State>, req: Frame) -> Result<Self::Response, Self::Error> {
        let api_key = req.api_key()?;
        let api_version = req.api_version()?;

        if let Some(range) = self.advertised_versions.get(&api_key) {
            if api_version < range.min_version || api_version > range.max_version {
                if api_key != ApiVersionsRequest::KEY {
                    return Ok(unsupported_version_response_frame(
                        api_key,
                        req.correlation_id()?,
                    )?);
                }
            }
        }

        if let Some(service) = self.routes.get(&api_key) {
            service.serve(ctx, req).await
        } else {
            Err(E::from(Error::UnknownServiceFrame(Box::new(req))))
        }
    }
}

/// A [`Frame`] route builder providing an [`ApiVersionsResponse`] for all available routes
#[derive(Debug)]
pub struct FrameRouteBuilder<State, E> {
    routes: BTreeMap<i16, BoxService<State, Frame, Frame, E>>,
    advertised_versions: Option<BTreeMap<i16, ApiVersionRange>>,
}

impl<State, E> FrameRouteBuilder<State, E>
where
    State: Clone + Send + Sync + 'static,
    E: std::error::Error + From<tansu_sans_io::Error> + Send + Sync + 'static,
{
    fn new() -> Self {
        Self {
            routes: BTreeMap::new(),
            advertised_versions: None,
        }
    }

    pub fn with_advertised_versions(
        mut self,
        advertised_versions: BTreeMap<i16, ApiVersionRange>,
    ) -> Self {
        self.advertised_versions = Some(advertised_versions);
        self
    }

    pub fn with_service<S>(self, service: S) -> Result<Self, Error>
    where
        S: Into<BoxService<State, Frame, Frame, E>> + ApiKey,
    {
        self.with_route(S::KEY, service.into())
    }

    pub fn with_route(
        mut self,
        api_key: i16,
        service: BoxService<State, Frame, Frame, E>,
    ) -> Result<Self, Error> {
        self.routes
            .insert(api_key, service)
            .map_or(Ok(self), |_existing| Err(Error::DuplicateRoute(api_key)))
    }

    pub fn build(self) -> Result<FrameRouteService<State, E>, Error> {
        let advertised_versions = self.advertised_versions.clone().unwrap_or_else(|| {
            let requests = RootMessageMeta::messages().requests();
            let mut advertised_versions = BTreeMap::new();

            for api_key in self
                .routes
                .keys()
                .copied()
                .chain(std::iter::once(ApiVersionsRequest::KEY))
            {
                if let Some(meta) = requests.get(&api_key) {
                    let _ = advertised_versions.insert(
                        api_key,
                        ApiVersionRange {
                            min_version: meta.version.valid.start,
                            max_version: meta.version.valid.end,
                        },
                    );
                }
            }

            advertised_versions
        });

        self.with_route(
            ApiVersionsRequest::KEY,
            ApiVersionsService {
                advertised_versions: advertised_versions.clone(),
                error: PhantomData,
            }
            .boxed(),
        )
        .map(|builder| FrameRouteService {
            routes: Arc::new(builder.routes),
            advertised_versions: Arc::new(advertised_versions),
        })
    }
}

fn unsupported_version_response_frame(api_key: i16, correlation_id: i32) -> Result<Frame, Error> {
    use tansu_sans_io::*;

    let error_code: i16 = ErrorCode::UnsupportedVersion.into();

    let body: Option<Body> = match api_key {
        ProduceRequest::KEY => Some(ProduceResponse::default().into()),
        FetchRequest::KEY => Some(FetchResponse::default().error_code(Some(error_code)).into()),
        ListOffsetsRequest::KEY => Some(ListOffsetsResponse::default().into()),
        MetadataRequest::KEY => Some(MetadataResponse::default().into()),
        OffsetCommitRequest::KEY => Some(OffsetCommitResponse::default().into()),
        OffsetFetchRequest::KEY => Some(
            OffsetFetchResponse::default()
                .error_code(Some(error_code))
                .into(),
        ),
        FindCoordinatorRequest::KEY => Some(
            FindCoordinatorResponse::default()
                .error_code(Some(error_code))
                .into(),
        ),
        JoinGroupRequest::KEY => Some(JoinGroupResponse::default().error_code(error_code).into()),
        HeartbeatRequest::KEY => Some(HeartbeatResponse::default().error_code(error_code).into()),
        LeaveGroupRequest::KEY => Some(LeaveGroupResponse::default().error_code(error_code).into()),
        SyncGroupRequest::KEY => Some(SyncGroupResponse::default().error_code(error_code).into()),
        DescribeGroupsRequest::KEY => Some(DescribeGroupsResponse::default().into()),
        ListGroupsRequest::KEY => Some(ListGroupsResponse::default().error_code(error_code).into()),
        SaslHandshakeRequest::KEY => Some(
            SaslHandshakeResponse::default()
                .error_code(error_code)
                .into(),
        ),
        ApiVersionsRequest::KEY => {
            Some(ApiVersionsResponse::default().error_code(error_code).into())
        }
        CreateTopicsRequest::KEY => Some(CreateTopicsResponse::default().into()),
        DeleteTopicsRequest::KEY => Some(DeleteTopicsResponse::default().into()),
        DeleteRecordsRequest::KEY => Some(DeleteRecordsResponse::default().into()),
        InitProducerIdRequest::KEY => Some(
            InitProducerIdResponse::default()
                .error_code(error_code)
                .into(),
        ),
        AddPartitionsToTxnRequest::KEY => Some(AddPartitionsToTxnResponse::default().into()),
        AddOffsetsToTxnRequest::KEY => Some(AddOffsetsToTxnResponse::default().into()),
        EndTxnRequest::KEY => Some(EndTxnResponse::default().error_code(error_code).into()),
        WriteTxnMarkersRequest::KEY => Some(WriteTxnMarkersResponse::default().into()),
        TxnOffsetCommitRequest::KEY => Some(TxnOffsetCommitResponse::default().into()),
        DescribeAclsRequest::KEY => Some(
            DescribeAclsResponse::default()
                .error_code(error_code)
                .into(),
        ),
        CreateAclsRequest::KEY => Some(CreateAclsResponse::default().into()),
        DeleteAclsRequest::KEY => Some(DeleteAclsResponse::default().into()),
        DescribeConfigsRequest::KEY => Some(DescribeConfigsResponse::default().into()),
        AlterConfigsRequest::KEY => Some(AlterConfigsResponse::default().into()),
        AlterReplicaLogDirsRequest::KEY => Some(AlterReplicaLogDirsResponse::default().into()),
        DescribeLogDirsRequest::KEY => Some(
            DescribeLogDirsResponse::default()
                .error_code(Some(error_code))
                .into(),
        ),
        SaslAuthenticateRequest::KEY => Some(
            SaslAuthenticateResponse::default()
                .error_code(error_code)
                .into(),
        ),
        CreatePartitionsRequest::KEY => Some(CreatePartitionsResponse::default().into()),
        CreateDelegationTokenRequest::KEY => Some(
            CreateDelegationTokenResponse::default()
                .error_code(error_code)
                .into(),
        ),
        RenewDelegationTokenRequest::KEY => Some(
            RenewDelegationTokenResponse::default()
                .error_code(error_code)
                .into(),
        ),
        ExpireDelegationTokenRequest::KEY => Some(
            ExpireDelegationTokenResponse::default()
                .error_code(error_code)
                .into(),
        ),
        DescribeDelegationTokenRequest::KEY => Some(
            DescribeDelegationTokenResponse::default()
                .error_code(error_code)
                .into(),
        ),
        DeleteGroupsRequest::KEY => Some(DeleteGroupsResponse::default().into()),
        ElectLeadersRequest::KEY => Some(
            ElectLeadersResponse::default()
                .error_code(Some(error_code))
                .into(),
        ),
        IncrementalAlterConfigsRequest::KEY => {
            Some(IncrementalAlterConfigsResponse::default().into())
        }
        AlterPartitionReassignmentsRequest::KEY => Some(
            AlterPartitionReassignmentsResponse::default()
                .error_code(error_code)
                .into(),
        ),
        ListPartitionReassignmentsRequest::KEY => Some(
            ListPartitionReassignmentsResponse::default()
                .error_code(error_code)
                .into(),
        ),
        OffsetDeleteRequest::KEY => Some(
            OffsetDeleteResponse::default()
                .error_code(error_code)
                .into(),
        ),
        DescribeClientQuotasRequest::KEY => Some(
            DescribeClientQuotasResponse::default()
                .error_code(error_code)
                .into(),
        ),
        AlterClientQuotasRequest::KEY => Some(AlterClientQuotasResponse::default().into()),
        DescribeUserScramCredentialsRequest::KEY => Some(
            DescribeUserScramCredentialsResponse::default()
                .error_code(error_code)
                .into(),
        ),
        AlterUserScramCredentialsRequest::KEY => {
            Some(AlterUserScramCredentialsResponse::default().into())
        }
        UpdateFeaturesRequest::KEY => Some(
            UpdateFeaturesResponse::default()
                .error_code(error_code)
                .into(),
        ),
        DescribeClusterRequest::KEY => Some(
            DescribeClusterResponse::default()
                .error_code(error_code)
                .into(),
        ),
        DescribeProducersRequest::KEY => Some(DescribeProducersResponse::default().into()),
        UnregisterBrokerRequest::KEY => Some(
            UnregisterBrokerResponse::default()
                .error_code(error_code)
                .into(),
        ),
        DescribeTransactionsRequest::KEY => Some(DescribeTransactionsResponse::default().into()),
        ListTransactionsRequest::KEY => Some(
            ListTransactionsResponse::default()
                .error_code(error_code)
                .into(),
        ),
        PushTelemetryRequest::KEY => Some(
            PushTelemetryResponse::default()
                .error_code(error_code)
                .into(),
        ),
        AddRaftVoterRequest::KEY => Some(
            AddRaftVoterResponse::default()
                .error_code(error_code)
                .into(),
        ),
        RemoveRaftVoterRequest::KEY => Some(
            RemoveRaftVoterResponse::default()
                .error_code(error_code)
                .into(),
        ),
        _ => None,
    };

    if let Some(body) = body {
        Ok(Frame {
            size: 0,
            header: Header::Response { correlation_id },
            body: body.into(),
        })
    } else {
        Err(crate::Error::Message(format!(
            "Unsupported API key {} and no default unsupported version response body",
            api_key
        )))
    }
}
