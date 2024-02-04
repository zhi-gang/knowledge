use std::sync::RwLock;

use crate::repository::{Combiner, KnowledgeQueryResult};

use super::repository;
use axum::{http::StatusCode, response::IntoResponse, Json};
use serde::{Serialize, Deserialize};
use tantivy::{Index, IndexReader};
use tracing::instrument;

static mut G_INDEX: RwLock<Option<Index>> = RwLock::new(None);
static mut G_READER: RwLock<Option<IndexReader>> = RwLock::new(None);

const REPOSITPRY_PATH: &str = "repository";

/// The router to create new index repository
///
/// This function will update the globa index and reader
#[instrument]
pub async fn create_index() -> impl IntoResponse {
    match repository::create_index(REPOSITPRY_PATH) {
        Ok((index, reader)) => unsafe {
            *G_INDEX.write().unwrap() = Some(index);
            *G_READER.write().unwrap() = Some(reader);
            (StatusCode::OK, Json("OK".to_string()))
        },
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, Json(e.to_string())),
    }
}

/// The router to load index repository
///
/// This function will update the globa index and reader
#[instrument]
pub async fn load_index() -> impl IntoResponse {
    match repository::load_index(REPOSITPRY_PATH) {
        Ok((index, reader)) => unsafe {
            *G_INDEX.write().unwrap() = Some(index);
            *G_READER.write().unwrap() = Some(reader);
            (StatusCode::OK, Json("OK".to_string()))
        },
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, Json(e.to_string())),
    }
}
#[derive(Debug, Deserialize)]
pub struct DocQueryOnTitleAndBody {
    args: Vec<String>,
    combiner: Combiner,
    limit: usize,
}

fn vs_to_vas(v: &Vec<String>) -> Vec<&str> {
    v.iter().map(AsRef::as_ref).collect()
}

/// The router to find document by title and body
///
/// This function will query the index and return the result
///
/// # Arguments
///
/// * `payload`: the query condition, including the search keywords
///
/// # Returns
///
/// * `Ok(docs)`: the search result, including the matched documents
/// * `Err(e)`: the error message
#[instrument]
pub async fn find_document(Json(payload): Json<DocQueryOnTitleAndBody>) -> impl IntoResponse {
    let (index, reader) = unsafe { (G_INDEX.read().unwrap(), G_READER.read().unwrap()) };

    if index.is_none() || reader.is_none() {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(KnowledgeQueryResult::Failed(
                "index or reader is none".to_string(),
            )),
        )
    } else {
        match repository::query_title_body(
            &index.as_ref().unwrap(),
            &reader.as_ref().unwrap(),
            vs_to_vas(&payload.args),
            payload.combiner,
            payload.limit,
        ) {
            Ok(docs) => (StatusCode::OK, Json(KnowledgeQueryResult::SUCCESS(docs))),
            Err(e) => (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(KnowledgeQueryResult::Failed(e.to_string())),
            ),
        }
    }
}
