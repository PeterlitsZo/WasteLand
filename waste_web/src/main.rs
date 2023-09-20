mod error;
mod server;

use std::{net::SocketAddr};

use axum::{
    TypedHeader,
    headers::ContentType,
    extract::{Path, State, Extension, RawBody},
    http::{StatusCode, HeaderMap, HeaderValue},
    response::IntoResponse,
    routing::{get, post},
    Router,
};
use hyper::Method;
use tower_http::{services::ServeDir, cors::{CorsLayer, Any}};

use server::{Server, ServerResponse};
use error::Error;

#[tokio::main]
async fn main() -> Result<(), Error> {
    let server = Server::new("./.waste_web_data/")?;

    let addr = SocketAddr::from(([0, 0, 0, 0], 3514));

    let cors = CorsLayer::new()
        .allow_methods(vec![Method::GET, Method::POST])
        .allow_headers(Any)
        .allow_origin(Any);

    let router = Router::new()
        .route("/api/v1/wastes/:waste_key", get(get_waste))
        .route("/api/v1/wastes", post(put_waste).get(list_wastes))
        .nest_service("/", ServeDir::new("./frontend_ui/dist/"))
        .with_state(server)
        .layer(cors);

    println!("Serve at {}...", addr);
    axum::Server::bind(&addr)
        .serve(router.into_make_service())
        .await
        .unwrap();

    Ok(())
}

fn handle_result(r: Result<ServerResponse, Error>) -> impl IntoResponse {
    match r {
        Ok(v) => (
            v.status,
            [
                ("Content-Type", v.content_type),
                ("Access-Control-Allow-Origin", "*".to_string()),
            ],
            v.body
        ),
        Err(e) => (
            StatusCode::BAD_REQUEST,
            [
                ("Content-Type", "application/json".to_string()),
                ("Access-Control-Allow-Origin", "*".to_string()),
            ],
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

async fn list_wastes(
    State(mut state): State<Server>,
) -> impl IntoResponse {
    let result = state.list_wastes();
    handle_result(result)
}
