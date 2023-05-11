mod error;
mod server;

use std::net::SocketAddr;

use axum::{
    TypedHeader,
    headers::ContentType,
    extract::{Path, State, Extension, RawBody},
    http::{StatusCode, HeaderMap, HeaderValue},
    response::IntoResponse,
    routing::{get, post},
    Router,
};
use tower_http::services::ServeDir;

use server::{Server, ServerResponse};
use error::Error;

#[tokio::main]
async fn main() -> Result<(), Error> {
    let server = Server::new("./.waste_web_data/", "../frontend_src/")?;

    let addr = SocketAddr::from(([127, 0, 0, 1], 3514));

    let router = Router::new()
        .route("/api/v1/:waste_key", get(get_waste))
        .route("/api/v1", post(put_waste))
        .nest_service("/", ServeDir::new("./frontend_ui/dist/"))
        .with_state(server);

    axum::Server::bind(&addr)
        .serve(router.into_make_service())
        .await
        .unwrap();

    Ok(())
}

fn handle_result(r: Result<ServerResponse, Error>) -> impl IntoResponse {
    match r {
        Ok(v) => (v.status, [("Content-Type", v.content_type)], v.body),
        Err(e) => (
            StatusCode::BAD_REQUEST,
            [("Content-Type", "application/json".to_string())],
            "{}".as_bytes().to_vec(),
        ),
    }
}

async fn get_waste(
    State(mut state): State<Server>,
    Path(waste_key): Path<String>,
) -> impl IntoResponse {
    let result = state.get_waste(waste_key);
    handle_result(result)
}

async fn put_waste(
    State(mut state): State<Server>,
    TypedHeader(type_content): TypedHeader<ContentType>,
    RawBody(body): RawBody,
) -> impl IntoResponse {
    let body = match hyper::body::to_bytes(body).await {
        Ok(b) => b,
        Err(e) => return handle_result(Err(e.into())),
    };
    let result = state.put_waste(type_content.to_string().as_bytes(), &body[..]);
    handle_result(result)
}
