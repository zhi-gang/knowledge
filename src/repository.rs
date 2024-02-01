//! This is a library to implment full text seach for knownledge documents
//! It is based on the Tantivy librarey
//! It not support distributed deployment
//!
//! author: ZhiGang
//!
//!
//! TODO: add logger
//!

use cang_jie::{CangJieTokenizer, CANG_JIE};
use chrono::Local;
use serde::Deserialize;
use serde::Serialize;
use std::fs;
use tantivy::collector::TopDocs;
use tantivy::query::BooleanQuery;
use tantivy::query::Query;
use tantivy::IndexReader;
// use std::io::BufRead;
// use std::io::BufReader;
use std::path::Path;
use tantivy::doc;
use tantivy::query::QueryParser;
use tantivy::query_grammar::Occur;
use tantivy::schema::*;
use tantivy::Index;
use tantivy::ReloadPolicy;

#[derive(Debug, Deserialize)]
pub struct KnownledgeDocument {
    title: String,
    body: String,
}

/// The function that will create tantivy index in the path
/// it will clear the path first, everything in the path will be removed.
///
/// The schema is solid which has three fields: title, body and create_at
/// title is Text field in Chinese characters
/// body is Text field in Chinese characters
/// create_at is Date field which auto generated when create the document,
/// it will use the value when remove document
///
/// # Arguments
///
/// *`index_path` - path to create index
///
/// # Returns:
///
/// () or Error if index creation failed
///

///
pub fn create_index(index_path: String) -> tantivy::Result<()> {
    fs::remove_dir_all(&*index_path)?;
    fs::create_dir(&*index_path)?;

    let schema = make_schema();

    let index = Index::create_in_dir(&Path::new(&*(index_path)), schema.clone())?;
    index
        .tokenizers()
        .register(CANG_JIE, CangJieTokenizer::default()); // Build cang-jie Tokenizer
    Ok(())
}
/// Load index from path
/// It will register Cang-jie Tokenizer for Chinese characters
///
/// # Arguments
///
/// * `index_path` - path to load index from
///
/// # Returns:
///
/// Index or error
///
pub fn load_index(index_path: String) -> tantivy::Result<(Index, IndexReader)> {
    let index = Index::open_in_dir(index_path)?;
    index
        .tokenizers()
        .register(CANG_JIE, CangJieTokenizer::default());
    let reader = index
        .reader_builder()
        .reload_policy(ReloadPolicy::OnCommit)
        .try_into()?;
    Ok((index, reader))
}

/// Add a single new document to the repository
///
/// # Arguments
///
/// * `index` - The reference to the tantivy index
/// * `doc` - The document to be add
/// * `reader` - The global tantivy reader
///
///  # Returns:
///
/// The create time in string or error
pub fn add_doc(
    index: &Index,
    doc: KnownledgeDocument,
    reader: &IndexReader,
) -> tantivy::Result<String> {
    let mut index_writer = index.writer(50_000_000)?;

    let now = now();
    let document = make_doc(index, &doc, &*now)?;
    index_writer.add_document(document)?;
    index_writer.commit()?;
    reader.reload()?; //refresh the reader
    Ok(now)
}

/// Add a batch documents to the repository
///
/// # Arguments
///
/// * `index` - The reference to the tantivy index
/// * `docs` - The documents to be add
/// * `reader` - The global tantivy reader
///
///  # Returns:
///
/// () or error
pub fn add_doc_in_batch(
    index: &Index,
    docs: Vec<KnownledgeDocument>,
    reader: &IndexReader,
) -> tantivy::Result<()> {
    let mut index_writer = index.writer(50_000_000)?;

    for doc in docs {
        let now = now();
        let document = make_doc(index, &doc, &*now)?;
        index_writer.add_document(document)?;
    }
    index_writer.commit()?;
    reader.reload()?; //refersh the reader;
    Ok(())
}

#[derive(Debug, Serialize, Deserialize, Eq, PartialEq)]
pub enum Combiner {
    AND,
    OR,
}

/// Query the documents for the given `keys` on Title and Body fields,
/// Max `num` results.
///
/// # Arguments
///
/// * `index` - The tantivy index to query.
/// * `reader` - The global tantivy reader.
/// * `keys` - The search keys to query with.
/// * `op` - The combiner to use for multiple keys.
/// * `num` - The maximum number of results to return.
///
/// # Returns
///
/// A vector of `KnownledgeDocument`s that match the search keys.
pub fn query_title_body(
    index: &Index,
    reader: &IndexReader,
    keys: Vec<String>,
    op: Combiner,
    num: usize,
) -> tantivy::Result<Vec<KnownledgeDocument>> {
    if keys.len() == 0 {
        return Ok(vec![]);
    }

    // reader.reload()?; //reload in udpate APIs

    let searcher = reader.searcher();

    let schema = index.schema();
    let title = schema.get_field("title").unwrap();
    let body = schema.get_field("body").unwrap();
    let query_parser = QueryParser::for_index(&index, vec![title, body]);

    let bool_query = build_bool_query(&query_parser, op, keys)?;

    let top_docs = searcher.search(&bool_query, &TopDocs::with_limit(num))?;

    let mut result: Vec<KnownledgeDocument> = Vec::with_capacity(num);
    for (_score, doc_address) in top_docs {
        let retrieved_doc = searcher.doc(doc_address)?;
        let title_str = retrieved_doc
            .get_first(title)
            .map(|field_value| match field_value {
                Value::Str(text) => text.to_string(),
                _ => String::new(), // Handle other value types as needed
            })
            .expect("could not find title in the document");

        let body_str = retrieved_doc
            .get_first(body)
            .map(|field_value| match field_value {
                Value::Str(text) => text.to_string(),
                _ => String::new(), // Handle other value types as needed
            })
            .expect("could not find body in the document");
        result.push(KnownledgeDocument {
            title: title_str,
            body: body_str,
        });
    }

    Ok(result)
}

/// Delete all documents in the repository
pub fn delele_all(index: &Index, reader: &IndexReader) -> tantivy::Result<()> {
    let mut index_writer = index.writer(10_000_000)?;
    index_writer.delete_all_documents()?;
    index_writer.commit()?;
    reader.reload()?;
    Ok(())
}
/// Delete a document from the repository based on its title and create timestamp.
///
/// # Arguments
///
/// * `index` - The reference to the tantivy index.
/// * `reader` - The global tantivy reader.
/// * `title_key` - The title of the document to be deleted.
/// * `ts` - The create timestamp of the document to be deleted. sample: 2023-12-22T12:58:00Z
///
/// # Returns
///
/// () or error
pub fn delete<'a>(index: &Index, reader: &IndexReader, title_key: &'a str, ts:&'a str)->tantivy::Result<()> where{
    let schema = index.schema();
    let title = schema.get_field("title").unwrap();
    let create_at = schema.get_field("create_at").unwrap();

    let query_parser_ts = QueryParser::for_index(&index, vec![create_at]);
    let query_ts = query_parser_ts.parse_query(&*format!("\"create_at:\"{}\"",ts))?;
    let query_parser_title = QueryParser::for_index(&index, vec![title]);
    let query_title = query_parser_title.parse_query(title_key)?;
    let bool_query = BooleanQuery::new(vec![(Occur::Must,query_ts),(Occur::Must,query_title)]);

    let mut index_writer = index.writer(10_000_000)?;
    index_writer.delete_query(Box::new(bool_query))?;
    index_writer.commit()?;
    reader.reload()?;
    Ok(())
}
/// Combine multiple queries into one BoolQuery
fn build_bool_query(
    query_parser: &QueryParser,
    op: Combiner,
    keys: Vec<String>,
) -> tantivy::Result<BooleanQuery> {
    let logic_op = match op {
        Combiner::AND => Occur::Must,
        Combiner::OR => Occur::Should,
    };

    let mut all_query = Vec::<(Occur, Box<dyn Query>)>::with_capacity(keys.len());

    for key in keys {
        let query = query_parser.parse_query(&*key)?;
        all_query.push((logic_op, query));
    }
    Ok(BooleanQuery::new(all_query))
}

fn make_doc<'a>(
    index: &Index,
    doc: &KnownledgeDocument,
    now: &'a str,
) -> tantivy::Result<Document> {
    let content = format!(
        "{{
        \"create_at\": \"{}\",
        \"title\": \"{}\",
        \"body\": \"{}\"
    }}",
        now, doc.title, doc.body,
    );
    let schema = index.schema();
    let document = schema.parse_document(content.as_str())?;
    Ok(document)
}

/// Create schema
///  
/// #Fields
///
/// * `title`: string
/// * `body`: string
/// * `created_at`: date
fn make_schema() -> Schema {
    let mut schema_builder = Schema::builder();

    let text_indexing = TextFieldIndexing::default()
        .set_tokenizer(CANG_JIE) // Set custom tokenizer
        .set_index_option(IndexRecordOption::WithFreqsAndPositions);
    let text_options = TextOptions::default()
        .set_indexing_options(text_indexing)
        .set_stored();
    let date_options = DateOptions::from(INDEXED)
        .set_stored()
        .set_fast()
        .set_precision(tantivy::DateTimePrecision::Seconds);
    let _ = schema_builder.add_date_field("create_at", date_options);
    let _ = schema_builder.add_text_field("title", text_options.clone());
    let _ = schema_builder.add_text_field("body", text_options);

    schema_builder.build()
}

/// Get tantivy formatted date of current local time
fn now() -> String {
    Local::now().format("%Y-%m-%dT%H:%M:%S%.3fZ").to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_now() {
        let now = now();
        assert!(now.len() == 24 );
        assert!(now.contains('-'));
        assert!(now.contains('T'));
        assert!(now.contains(':'));
        assert!(now.contains('.'));
    }
}
// fn add_documents(index: &Index, schema: &Schema) -> tantivy::Result<()> {
//     let mut index_writer = index.writer(50_000_000)?;
//     let title = schema.get_field("title")?;
//     let body = schema.get_field("body")?;

//     let file = std::fs::File::open("output.json")?;
//     let mut reader = BufReader::new(file);

//     let mut line = String::new();

//     loop {
//         if let Ok(n) = reader.read_line(&mut line) {
//             println!("{},{}",n, line);
//             if n == 0 {
//                 break;
//             }
//             if let Ok(data) = serde_json::from_str::<Data>(&*line) {
//                 println!("{}", data.title);
//                 let _ = index_writer.add_document(doc!(
//                     title => data.title,
//                     body => data.body
//                 ));
//             }
//             line.clear();
//         }
//     }
//     index_writer.commit()?;
//     Ok(())
// }

// #[cfg(test)]
// mod tests {
//     use super::*;

//     #[test]
//     fn test_setup() {
//         setup("test_index".to_owned()).unwrap();
//     }
// }
