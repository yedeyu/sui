// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{net::SocketAddr, sync::Arc};

use axum::extract::{ConnectInfo, Json, State};
use futures::StreamExt;
use hyper::HeaderMap;
use jsonrpsee::core::server::helpers::BoundedSubscriptions;
use jsonrpsee::core::server::helpers::MethodResponse;
use jsonrpsee::core::server::helpers::MethodSink;
use jsonrpsee::core::server::rpc_module::MethodKind;
use jsonrpsee::server::logger::{self, TransportProtocol};
use jsonrpsee::server::RandomIntegerIdProvider;
use jsonrpsee::types::error::{ErrorCode, BATCHES_NOT_SUPPORTED_CODE, BATCHES_NOT_SUPPORTED_MSG};
use jsonrpsee::types::{ErrorObject, Id, InvalidRequest, Params, Request};
use jsonrpsee::{core::server::rpc_module::Methods, server::logger::Logger};
use serde_json::value::{RawValue, Value};
use sui_types::error::{SuiError, SuiResult};

use crate::routing_layer::RpcRouter;
use sui_json_rpc_api::CLIENT_TARGET_API_VERSION_HEADER;

pub const MAX_RESPONSE_SIZE: u32 = 2 << 30;

#[derive(Clone, Debug)]
pub struct JsonRpcService<L> {
    logger: L,

    id_provider: Arc<RandomIntegerIdProvider>,

    /// Registered server methods.
    methods: Methods,
    rpc_router: RpcRouter,
}

impl<L> JsonRpcService<L> {
    pub fn new(methods: Methods, rpc_router: RpcRouter, logger: L) -> Self {
        Self {
            methods,
            rpc_router,
            logger,
            id_provider: Arc::new(RandomIntegerIdProvider),
        }
    }
}

impl<L: Logger> JsonRpcService<L> {
    fn call_data(&self) -> CallData<'_, L> {
        CallData {
            logger: &self.logger,
            methods: &self.methods,
            rpc_router: &self.rpc_router,
            max_response_body_size: MAX_RESPONSE_SIZE,
            request_start: self.logger.on_request(TransportProtocol::Http),
        }
    }

    fn ws_call_data<'c, 'a: 'c, 'b: 'c>(
        &'a self,
        bounded_subscriptions: BoundedSubscriptions,
        sink: &'b MethodSink,
    ) -> ws::WsCallData<'c, L> {
        ws::WsCallData {
            logger: &self.logger,
            methods: &self.methods,
            max_response_body_size: MAX_RESPONSE_SIZE,
            request_start: self.logger.on_request(TransportProtocol::Http),
            bounded_subscriptions,
            id_provider: &*self.id_provider,
            sink,
        }
    }
}

/// Create a response body.
fn from_template<S: Into<hyper::Body>>(
    status: hyper::StatusCode,
    body: S,
    content_type: &'static str,
) -> hyper::Response<hyper::Body> {
    hyper::Response::builder()
        .status(status)
        .header(
            "content-type",
            hyper::header::HeaderValue::from_static(content_type),
        )
        .body(body.into())
        // Parsing `StatusCode` and `HeaderValue` is infalliable but
        // parsing body content is not.
        .expect("Unable to parse response body for type conversion")
}

/// Create a valid JSON response.
pub(crate) fn ok_response(body: String) -> hyper::Response<hyper::Body> {
    const JSON: &str = "application/json; charset=utf-8";
    from_template(hyper::StatusCode::OK, body, JSON)
}

pub async fn json_rpc_handler<L: Logger>(
    ConnectInfo(client_addr): ConnectInfo<SocketAddr>,
    State(service): State<JsonRpcService<L>>,
    headers: HeaderMap,
    Json(raw_request): Json<Box<RawValue>>,
) -> impl axum::response::IntoResponse {
    // Get version from header.
    let api_version = headers
        .get(CLIENT_TARGET_API_VERSION_HEADER)
        .and_then(|h| h.to_str().ok());
    let response = process_raw_request(&service, api_version, raw_request.get(), client_addr).await;

    ok_response(response.result)
}

async fn process_raw_request<L: Logger>(
    service: &JsonRpcService<L>,
    api_version: Option<&str>,
    raw_request: &str,
    client_addr: SocketAddr,
) -> MethodResponse {
    if let Ok(request) = serde_json::from_str::<Request>(raw_request) {
        process_request(request, api_version, service.call_data(), client_addr).await
    } else if let Ok(_batch) = serde_json::from_str::<Vec<&RawValue>>(raw_request) {
        MethodResponse::error(
            Id::Null,
            ErrorObject::borrowed(BATCHES_NOT_SUPPORTED_CODE, &BATCHES_NOT_SUPPORTED_MSG, None),
        )
    } else {
        let (id, code) = prepare_error(raw_request);
        MethodResponse::error(id, ErrorObject::from(code))
    }
}

async fn process_request<L: Logger>(
    req: Request<'_>,
    api_version: Option<&str>,
    call: CallData<'_, L>,
    client_addr: SocketAddr,
) -> MethodResponse {
    let CallData {
        methods,
        rpc_router,
        logger,
        max_response_body_size,
        request_start,
    } = call;
    let conn_id = 0; // unused

    let name_str = rpc_router.route(&req.method, api_version);
    let raw_params: Option<&RawValue> = req.params;

    // This is really ugly, but it's the only way to do it for now. We will
    // kill this aggressively once we move away from this json rpc framework.
    let (params_string, name): (String, String) =
        match monitored_reroute(raw_params, name_str, client_addr) {
            Ok((params_string, name)) => (params_string, name),
            Err(e) => {
                return MethodResponse {
                    result: format!(
                        "Failed to handle request for method {:?}: {:?}",
                        name_str, e
                    ),
                    success: false,
                    error_code: None,
                };
            }
        };

    let params_str = params_string.as_str();

    let params = if raw_params.is_some() {
        Params::new(Some(params_str))
    } else {
        Params::new(None)
    };

    let id = req.id;

    let response = match methods.method_with_name(&name) {
        None => {
            logger.on_call(
                &name,
                params.clone(),
                logger::MethodKind::Unknown,
                TransportProtocol::Http,
            );
            MethodResponse::error(id, ErrorObject::from(ErrorCode::MethodNotFound))
        }
        Some((name, method)) => match method.inner() {
            MethodKind::Sync(callback) => {
                logger.on_call(
                    name,
                    params.clone(),
                    logger::MethodKind::MethodCall,
                    TransportProtocol::Http,
                );
                (callback)(id, params, max_response_body_size as usize)
            }
            MethodKind::Async(callback) => {
                logger.on_call(
                    name,
                    params.clone(),
                    logger::MethodKind::MethodCall,
                    TransportProtocol::Http,
                );

                let id = id.into_owned();
                let params = params.into_owned();
                (callback)(id, params, conn_id, max_response_body_size as usize, None).await
            }
            MethodKind::Subscription(_) | MethodKind::Unsubscription(_) => {
                logger.on_call(
                    name,
                    params.clone(),
                    logger::MethodKind::Unknown,
                    TransportProtocol::Http,
                );
                // Subscriptions not supported on HTTP
                MethodResponse::error(id, ErrorObject::from(ErrorCode::InternalError))
            }
        },
    };

    logger.on_result(
        &name,
        response.success,
        response.error_code,
        request_start,
        TransportProtocol::Http,
    );
    response
}

pub fn monitored_reroute(
    raw_params: Option<&RawValue>,
    name: &str,
    client_addr: SocketAddr,
) -> SuiResult<(String, String)> {
    match name {
        "sui_executeTransactionBlock" => {
            // add client IP arg to the params, as this is a router redirect
            // from `execute_transaction_block`, which does require the client IP
            let parsed_value: Value = serde_json::from_str(
                raw_params
                    .unwrap_or_else(|| panic!("Expected params for executeTransactionBlock"))
                    .get(),
            )
            .expect("Failed to parse jsonrpsee params");

            let Value::Array(mut params_vec) = parsed_value else {
                panic!("Expected a JSON array");
            };

            params_vec.push(Value::String(client_addr.to_string()));
            let name = String::from("sui_monitoredExecuteTransactionBlock");
            Ok((
                serde_json::to_string(&params_vec).expect("Failed to serialize params"),
                name,
            ))
        }
        "sui_monitoredExecuteTransactionBlock" => {
            // Prevent an attacker calling it directly with a different
            // client IP in order to bypass monitoring
            Err(SuiError::InvalidRpcMethodError)
        }
        // in this case params_string should not be read below. We do this as Params<>
        // object requires a slice whose lifetime is at least as long as this function call,
        // therefore we cannot create a Params object within an if block scope
        other_name => Ok((
            raw_params
                .map(|params| String::from(params.get()))
                .unwrap_or_default(),
            String::from(other_name),
        )),
    }
}

/// Figure out if this is a sufficiently complete request that we can extract an [`Id`] out of, or just plain
/// unparsable garbage.
pub fn prepare_error(data: &str) -> (Id<'_>, ErrorCode) {
    match serde_json::from_str::<InvalidRequest>(data) {
        Ok(InvalidRequest { id }) => (id, ErrorCode::InvalidRequest),
        Err(_) => (Id::Null, ErrorCode::ParseError),
    }
}

#[derive(Debug, Clone)]
pub(crate) struct CallData<'a, L: Logger> {
    logger: &'a L,
    methods: &'a Methods,
    rpc_router: &'a RpcRouter,
    max_response_body_size: u32,
    request_start: L::Instant,
}

pub mod ws {
    use axum::{
        extract::{
            ws::{Message, WebSocket},
            WebSocketUpgrade,
        },
        response::Response,
    };
    use futures::channel::mpsc;
    use jsonrpsee::{
        core::server::{
            helpers::{BoundedSubscriptions, MethodSink},
            rpc_module::ConnState,
        },
        server::IdProvider,
        types::error::reject_too_many_subscriptions,
    };

    use super::*;

    #[derive(Debug, Clone)]
    pub(crate) struct WsCallData<'a, L: Logger> {
        pub bounded_subscriptions: BoundedSubscriptions,
        pub id_provider: &'a dyn IdProvider,
        pub methods: &'a Methods,
        pub max_response_body_size: u32,
        pub sink: &'a MethodSink,
        pub logger: &'a L,
        pub request_start: L::Instant,
    }

    // A WebSocket handler that echos any message it receives.
    //
    // This one we'll be integration testing so it can be written in the regular way.
    pub async fn ws_json_rpc_upgrade<L: Logger>(
        ws: WebSocketUpgrade,
        State(service): State<JsonRpcService<L>>,
    ) -> Response {
        ws.on_upgrade(|ws| ws_json_rpc_handler(ws, service))
    }

    async fn ws_json_rpc_handler<L: Logger>(mut socket: WebSocket, service: JsonRpcService<L>) {
        #[allow(clippy::disallowed_methods)]
        let (tx, mut rx) = mpsc::unbounded::<String>();
        let sink = MethodSink::new_with_limit(tx, MAX_RESPONSE_SIZE, MAX_RESPONSE_SIZE);
        let bounded_subscriptions = BoundedSubscriptions::new(100);

        loop {
            tokio::select! {
                maybe_message = socket.recv() => {
                    if let Some(Ok(message)) = maybe_message {
                        if let Message::Text(msg) = message {
                            let response =
                                process_raw_request(&service, &msg, bounded_subscriptions.clone(), &sink).await;
                            if let Some(response) = response {
                                let _ = sink.send_raw(response.result);
                            }
                        }
                    } else {
                        break;
                    }
                },
                Some(response) = rx.next() => {
                    if socket.send(Message::Text(response)).await.is_err() {
                        break;
                    }
                },
            }
        }
    }

    async fn process_raw_request<L: Logger>(
        service: &JsonRpcService<L>,
        raw_request: &str,
        bounded_subscriptions: BoundedSubscriptions,
        sink: &MethodSink,
    ) -> Option<MethodResponse> {
        if let Ok(request) = serde_json::from_str::<Request>(raw_request) {
            process_request(request, service.ws_call_data(bounded_subscriptions, sink)).await
        } else if let Ok(_batch) = serde_json::from_str::<Vec<&RawValue>>(raw_request) {
            Some(MethodResponse::error(
                Id::Null,
                ErrorObject::borrowed(BATCHES_NOT_SUPPORTED_CODE, &BATCHES_NOT_SUPPORTED_MSG, None),
            ))
        } else {
            let (id, code) = prepare_error(raw_request);
            Some(MethodResponse::error(id, ErrorObject::from(code)))
        }
    }

    async fn process_request<L: Logger>(
        req: Request<'_>,
        call: WsCallData<'_, L>,
    ) -> Option<MethodResponse> {
        let WsCallData {
            methods,
            logger,
            max_response_body_size,
            request_start,
            bounded_subscriptions,
            id_provider,
            sink,
        } = call;
        let conn_id = 0; // unused

        let params = Params::new(req.params.map(|params| params.get()));
        let name = &req.method;
        let id = req.id;

        let response = match methods.method_with_name(name) {
            None => {
                logger.on_call(
                    name,
                    params.clone(),
                    logger::MethodKind::Unknown,
                    TransportProtocol::Http,
                );
                Some(MethodResponse::error(
                    id,
                    ErrorObject::from(ErrorCode::MethodNotFound),
                ))
            }
            Some((name, method)) => match method.inner() {
                MethodKind::Sync(callback) => {
                    logger.on_call(
                        name,
                        params.clone(),
                        logger::MethodKind::MethodCall,
                        TransportProtocol::Http,
                    );
                    Some((callback)(id, params, max_response_body_size as usize))
                }
                MethodKind::Async(callback) => {
                    logger.on_call(
                        name,
                        params.clone(),
                        logger::MethodKind::MethodCall,
                        TransportProtocol::Http,
                    );

                    let id = id.into_owned();
                    let params = params.into_owned();

                    Some(
                        (callback)(id, params, conn_id, max_response_body_size as usize, None)
                            .await,
                    )
                }

                MethodKind::Subscription(callback) => {
                    logger.on_call(
                        name,
                        params.clone(),
                        logger::MethodKind::Subscription,
                        TransportProtocol::WebSocket,
                    );
                    if let Some(cn) = bounded_subscriptions.acquire() {
                        let conn_state = ConnState {
                            conn_id,
                            close_notify: cn,
                            id_provider,
                        };
                        callback(id.clone(), params, sink.clone(), conn_state, None).await;
                        None
                    } else {
                        Some(MethodResponse::error(
                            id,
                            reject_too_many_subscriptions(bounded_subscriptions.max()),
                        ))
                    }
                }

                MethodKind::Unsubscription(callback) => {
                    logger.on_call(
                        name,
                        params.clone(),
                        logger::MethodKind::Unsubscription,
                        TransportProtocol::WebSocket,
                    );

                    Some(callback(
                        id,
                        params,
                        conn_id,
                        max_response_body_size as usize,
                    ))
                }
            },
        };

        if let Some(response) = &response {
            logger.on_result(
                name,
                response.success,
                response.error_code,
                request_start,
                TransportProtocol::WebSocket,
            );
        }
        response
    }
}
