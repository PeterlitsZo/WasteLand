use std::{
    convert::Infallible,
    future::Future,
    pin::Pin,
    sync::{Arc, Mutex},
};

use axum::{extract::Path, http::StatusCode, response::IntoResponse, http::status::InvalidStatusCode};
use serde_json::json;

use crate::error::Error;

#[derive(Clone)]
pub struct Server {
    database: Arc<Mutex<waste_island::Database>>,
}

pub struct ServerResponse {
    pub status: StatusCode,
    pub content_type: String,
    pub body: Vec<u8>,
}

impl Server {
    pub fn new(database_path: &str) -> Result<Self, Error> {
        let database = waste_island::Database::new(database_path)?;
        Ok(Self {
            database: Arc::new(Mutex::new(database)),
        })
    }

    pub fn list_wastes(&mut self) -> Result<ServerResponse, Error> {
        let mut database = self.database.lock().unwrap();
        let result = database.list()?;
        Ok(ServerResponse {
            status: StatusCode::OK,
            content_type: "application/json".to_string(),
            body: json!({ "data": result }).to_string().as_bytes().to_vec(),
        })
    }

    pub fn get_waste(&mut self, waste_key: String) -> Result<ServerResponse, Error> {
        let mut database = self.database.lock().unwrap();
        let res = database.get(&waste_key)?;
        if res.len() == 0 {
            return Err(Error::new(format!("length = 0, when key = {}", waste_key)));
        }
        let content_type_len = &res[0];
        let content_type = &res[1..1 + *content_type_len as usize];
        let body = &res[1 + *content_type_len as usize..];
        Ok(ServerResponse {
            status: StatusCode::OK,
            content_type: unsafe { String::from_utf8_unchecked(content_type.to_vec()) },
            body: body.to_vec(),
        })
    }

    pub fn put_waste(
        &mut self,
        content_type: &[u8],
        body_data: &[u8],
    ) -> Result<ServerResponse, Error> {
        let mut database = self.database.lock().unwrap();
        let mut data = vec![];
        data.push(content_type.len() as u8);
        data.extend_from_slice(content_type);
        data.extend_from_slice(body_data);
        let name = database.put(&data).unwrap();
        Ok(ServerResponse {
            status: StatusCode::OK,
            content_type: "application/json".to_string(),
            body: format!(r#"{{"type":"OK","name":{:?}}}"#, name)
                .as_bytes()
                .to_vec(),
        })
    }
}

// impl Service<Request<Incoming>> for Server {
//     type Response = Response<Full<Bytes>>;
//     type Error = Infallible;
//     type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;
// 
//     fn call(&mut self, req: Request<Incoming>) -> Self::Future {
//         fn mk_response(
//             result: Result<ServerResponse, Error>,
//         ) -> Result<Response<Full<Bytes>>, Infallible> {
//             let (status, content_type, body) = match result {
//                 Ok(ServerResponse::Matched {
//                     status,
//                     content_type,
//                     body,
//                 }) => (status, content_type, body),
//                 Ok(ServerResponse::Unmatched) => (
//                     404,
//                     "application/json".to_string(),
//                     format!(r#"{{"error": {:?}}}"#, "not found").as_bytes().to_vec(),
//                 ),
//                 Err(e) => (
//                     400,
//                     "application/json".to_string(),
//                     format!(r#"{{"error": {:?}}}"#, e).as_bytes().to_vec(),
//                 )
//             };
// 
//             let response = Response::builder()
//                 .status(status)
//                 .header("Content-Type", content_type)
//                 .body(Full::new(Bytes::from(body)))
//                 .unwrap();
//             Ok(response)
//         }
// 
//         let mut slf = self.clone();
//         let response = async move {
//             match slf.router(req).await {
//                 Ok(ServerResponse::Unmatched) => {
//                     let result = slf.static_server.serve(req).await.unwrap();
//                     mk_response(Ok(ServerResponse::Matched {
//                         status: 200,
//                         content_type: result.headers()["Content-Type"],
//                         body: ()
//                     }))
//                 }
//                 otherwises => mk_response(otherwises)
//             }
//         };
// 
//         Box::pin(response)
//     }
// }
// 