//! This module implments the router APIs to interact with the Repository interface

use std::sync::RwLock;

use crate::repository::{Combiner, KnowledgeQueryResult, KnownledgeDocument};

use super::repository;
use axum::{http::StatusCode, response::IntoResponse, Json};
use serde::Deserialize;
use tantivy::{Index, IndexReader};
use tracing::{error, instrument};

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
            *G_INDEX.write().unwrap() = Some(index); //shall manage the memory older index and reader?
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
pub async fn load_index() -> (StatusCode, Json<String>) {
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
        error!( "index or reader is none");
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

#[instrument]
pub async fn push_documents(Json(payload): Json<Vec<KnownledgeDocument>>) -> impl IntoResponse {
    let (index, reader) = unsafe { (G_INDEX.write().unwrap(), G_READER.write().unwrap()) };

    if index.is_none() || reader.is_none() {
        error!( "index or reader is none");
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json("index or reader is none".to_string()),
        )
    } else {
        match repository::add_doc_in_batch(
            &index.as_ref().unwrap(),
            &reader.as_ref().unwrap(),
            payload,
        ) {
            Ok(_) => (StatusCode::OK, Json("OK".to_string())),
            Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, Json(e.to_string())),
        }
    }
}

#[derive(Debug, Deserialize)]
pub struct DocQueryOnTitle {
    title: String,
    limit: usize,
}
#[instrument]
pub async fn find_document_by_title(Json(payload): Json<DocQueryOnTitle>) -> impl IntoResponse {
    let (index, reader) = unsafe { (G_INDEX.read().unwrap(), G_READER.read().unwrap()) };

    if index.is_none() || reader.is_none() {
        error!( "index or reader is none");
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(KnowledgeQueryResult::Failed(
                "index or reader is none".to_string(),
            )),
        )
    } else {
        match repository::query_title(
            &index.as_ref().unwrap(),
            &reader.as_ref().unwrap(),
            &*payload.title,
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

#[derive(Debug, Deserialize)]
pub struct DocRemove {
    title: String,
    ts: String,
}
#[instrument]
pub async fn delete_document(Json(payload): Json<DocRemove>) -> impl IntoResponse {
    let (index, reader) = unsafe { (G_INDEX.write().unwrap(), G_READER.write().unwrap()) };

    if index.is_none() || reader.is_none() {
        error!( "index or reader is none");
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json("index or reader is none".to_string()),
        )
    } else {
        match repository::delete(
            &index.as_ref().unwrap(),
            &reader.as_ref().unwrap(),
            &*payload.title,
            &*payload.ts
        ) {
            Ok(_) => (StatusCode::OK, Json("OK".to_string())),
            Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, Json(e.to_string())),
        }
    }
}
