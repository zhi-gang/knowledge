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
use std::path::Path;
use tantivy::collector::TopDocs;
use tantivy::doc;
use tantivy::query::BooleanQuery;
use tantivy::query::Query;
use tantivy::query::QueryParser;
use tantivy::query_grammar::Occur;
use tantivy::schema::*;
use tantivy::time::format_description::well_known::Rfc3339;
use tantivy::Index;
use tantivy::IndexReader;
use tantivy::ReloadPolicy;
use tantivy::Searcher;
use tantivy::TantivyError;

#[derive(Debug, Serialize, Deserialize)]
pub struct KnownledgeDocument {
    title: String,
    body: String,
}
#[derive(Debug, Serialize, Deserialize)]
pub struct KnownledgeDocumentWithTime {
    doc: KnownledgeDocument,
    create_at: String,
}
impl KnownledgeDocumentWithTime {
    fn pick_text_field(
        retrieved_doc: &Document,
        f: &Field,
        field_name: &str,
    ) -> tantivy::Result<String> {
        match retrieved_doc.get_first(*f) {
            Some(field_value) => match field_value {
                Value::Str(text) => Ok(text.to_string()),
                _ => Err(TantivyError::FieldNotFound(field_name.to_string())),
            },
            None => Err(TantivyError::FieldNotFound(field_name.to_string())),
        }
    }

    fn pick_date_field(
        retrieved_doc: &Document,
        f: &Field,
        field_name: &str,
    ) -> tantivy::Result<String> {
        match retrieved_doc.get_first(*f) {
            Some(field_value) => match field_value {
                Value::Date(ts) => Ok(ts.into_utc().format(&Rfc3339)?),
                _ => Err(TantivyError::FieldNotFound(field_name.to_string())),
            },
            None => Err(TantivyError::FieldNotFound(field_name.to_string())),
        }
    }

    pub fn build_from_document(
        retrieved_doc: Document,
        title: &Field,
        body: &Field,
        create_at: &Field,
    ) -> tantivy::Result<Self> {
        let title_str = Self::pick_text_field(&retrieved_doc, title, "title")?;
        let body_str = Self::pick_text_field(&retrieved_doc, body, "body")?;
        let create_at_str = Self::pick_date_field(&retrieved_doc, create_at, "create_at")?;
        Ok(Self {
            doc: KnownledgeDocument {
                title: title_str,
                body: body_str,
            },
            create_at: create_at_str,
        })
    }
}
pub enum Combiner {
    AND,
    OR,
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
/// (Index and Reader) or Error if index creation failed
pub fn create_index(index_path: &str) -> tantivy::Result<(Index, IndexReader)> {
    let _ = fs::remove_dir_all(&*index_path); //ignore error if directory does not exist
    fs::create_dir(&*index_path)?;

    let schema = make_schema();

    let index = Index::create_in_dir(&Path::new(&*(index_path)), schema.clone())?;
    index
        .tokenizers()
        .register(CANG_JIE, CangJieTokenizer::default()); // Build cang-jie Tokenizer

    let reader = index
        .reader_builder()
        .reload_policy(ReloadPolicy::OnCommit)
        .try_into()?;
    Ok((index, reader))
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
pub fn load_index(index_path: &str) -> tantivy::Result<(Index, IndexReader)> {
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
/// * `reader` - The global tantivy reader
/// * `doc` - The document to be add
///
///  # Returns:
///
/// The create time in string or error
pub fn add_doc(
    index: &Index,
    reader: &IndexReader,
    doc: KnownledgeDocument,
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
/// * `reader` - The global tantivy reader
/// * `docs` - The documents to be add
///
///  # Returns:
///
/// () or error
pub fn add_doc_in_batch(
    index: &Index,
    reader: &IndexReader,
    docs: Vec<KnownledgeDocument>,
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
    keys: Vec<&str>,
    op: Combiner,
    num: usize,
) -> tantivy::Result<Vec<KnownledgeDocumentWithTime>> {
    if keys.len() == 0 {
        return Ok(vec![]);
    }
    // reader.reload()?; //reload in udpate APIs
    let (title, body, create_at) = get_fields(&index)?;

    let query_parser = QueryParser::for_index(&index, vec![title, body]);
    let bool_query = build_bool_query(&query_parser, op, keys)?;
    let searcher = reader.searcher();
    let top_docs: Vec<(f32, tantivy::DocAddress)> =
        searcher.search(&bool_query, &TopDocs::with_limit(num))?;
    build_results(&searcher, top_docs, num, &title, &body, &create_at)
}
/// Query the documents for the given `key` on Title
/// Max `num` results.
///
/// # Arguments
///
/// * `index` - The tantivy index to query.
/// * `reader` - The global tantivy reader.
/// * `title_str` - The search key to query with.
/// * `num` - The maximum number of results to return.
///
/// # Returns
///
/// A vector of `KnownledgeDocument`s that match the search keys.
pub fn query_title(
    index: &Index,
    reader: &IndexReader,
    title_str: &str,
    num: usize,
) -> tantivy::Result<Vec<KnownledgeDocumentWithTime>> {
    let (title, body, create_at) = get_fields(&index)?;

    let query_parser = QueryParser::for_index(&index, vec![title]);
    let query = query_parser.parse_query(title_str)?;

    let searcher = reader.searcher();
    let top_docs = searcher.search(&query, &TopDocs::with_limit(num))?;
    build_results(&searcher, top_docs, num, &title, &body, &create_at)
}
fn build_results(
    searcher: &Searcher,
    top_docs: Vec<(f32, tantivy::DocAddress)>,
    num: usize,
    title: &Field,
    body: &Field,
    create_at: &Field,
) -> tantivy::Result<Vec<KnownledgeDocumentWithTime>> {
    let mut result: Vec<KnownledgeDocumentWithTime> = Vec::with_capacity(num);
    for (_score, doc_address) in top_docs {
        let retrieved_doc = searcher.doc(doc_address)?;
        let res = KnownledgeDocumentWithTime::build_from_document(
            retrieved_doc,
            &title,
            &body,
            &create_at,
        )?;
        result.push(res);
    }
    Ok(result)
}
fn get_fields(index: &Index) -> tantivy::Result<(Field, Field, Field)> {
    let schema = index.schema();
    let title = schema.get_field("title")?;
    let body = schema.get_field("body")?;
    let create_at = schema.get_field("create_at")?;

    Ok((title, body, create_at))
}
/// Delete all documents in the repository
pub fn delele_all(index: &Index, reader: &IndexReader) -> tantivy::Result<()> {
    let mut index_writer = index.writer(15_000_000)?;
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
pub fn delete(
    index: &Index,
    reader: &IndexReader,
    title_key: &str,
    ts: &str,
) -> tantivy::Result<()> where {
    let (title, _, create_at) = get_fields(index)?;

    let query_parser_ts = QueryParser::for_index(&index, vec![create_at]);
    let query_ts = query_parser_ts.parse_query(&*format!("create_at:\"{}\"", ts))?;
    let query_parser_title = QueryParser::for_index(&index, vec![title]);
    let query_title = query_parser_title.parse_query(title_key)?;
    let bool_query = BooleanQuery::new(vec![(Occur::Must, query_ts), (Occur::Must, query_title)]);

    let top_docs = reader
        .searcher()
        .search(&bool_query, &TopDocs::with_limit(1))?;
    println!("top_docs: {:?}", top_docs.len());

    let mut index_writer = index.writer(15_000_000)?;
    index_writer.delete_query(Box::new(bool_query))?;
    index_writer.commit()?;
    reader.reload()?;
    Ok(())
}
/// Combine multiple queries into one BoolQuery
fn build_bool_query(
    query_parser: &QueryParser,
    op: Combiner,
    keys: Vec<&str>,
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
fn make_doc(index: &Index, doc: &KnownledgeDocument, now: &str) -> tantivy::Result<Document> {
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
    // Local::now().to_rfc3339()
    Local::now().format("%Y-%m-%dT%H:%M:%S%.3fZ").to_string()
    // Local::now().format(Rfc3339).to_string()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::BufRead;
    use std::io::BufReader;

    #[test]
    fn test_now() {
        let now = now();
        println!("now : {}", now);
        assert!(now.len() == 24);
        assert!(now.contains('-'));
        assert!(now.contains('T'));
        assert!(now.contains(':'));
        assert!(now.contains('.'));
    }
    #[test]
    fn test_all() {
        create_repository();
        load_and_search();
        delete_test();
    }
    fn create_repository() {
        let begin = std::time::Instant::now();
        let (index, index_reader) = create_index("index_test").unwrap();

        let file = std::fs::File::open("data.json").unwrap();
        let mut reader = BufReader::new(file);

        let mut line = String::new();

        let mut docs: Vec<KnownledgeDocument> = Vec::new();
        loop {
            if let Ok(n) = reader.read_line(&mut line) {
                // println!("{},{}", n, line);
                if n == 0 {
                    break;
                }

                if let Ok(doc) = serde_json::from_str::<KnownledgeDocument>(&*line) {
                    // add_doc(&index, data, &index_reader).unwrap(); //It cost expensively to commit the changes!!
                    docs.push(doc);
                }
                line.clear();
            }
        }
        add_doc_in_batch(&index, &index_reader, docs).unwrap();
        let search = index_reader.searcher();
        assert_eq!(9774, search.num_docs());
        let end = std::time::Instant::now();
        println!("create cost: {:?}", end.duration_since(begin));
    }
    fn load_and_search() {
        let begin = std::time::Instant::now();
        let (index, reader) = load_index("index_test").unwrap();
        let loaded = std::time::Instant::now();
        assert_eq!(
            0,
            query_title_body(&index, &reader, vec![], Combiner::OR, 10)
                .unwrap()
                .len()
        );
        assert_eq!(
            10,
            query_title_body(&index, &reader, vec!["儿童", "头痛"], Combiner::OR, 10)
            .unwrap()
            .len()
        );
        let res =
            query_title_body(&index, &reader, vec!["儿童", "头痛"], Combiner::AND, 10).unwrap();
        let query = std::time::Instant::now();
        println!("{:?}", res.get(0));
        assert_eq!(10, res.len());
        println!(
            "load cost: {:?} ; query cost: {:?}",
            loaded.duration_since(begin),
            query.duration_since(loaded)
        );

        let res2 = query_title(&index, &reader, "湿气", 10).unwrap();
        println!("{:?}", res2.get(0));
        assert_eq!(1, res2.len());
    }
    fn delete_test() {
        let begin = std::time::Instant::now();
        let (index, reader) = load_index("index_test").unwrap();

        delele_all(&index, &reader).unwrap();
        add_doc(
            &index,
            &reader,
            KnownledgeDocument {
                title: "我们一起去唱歌".to_string(),
                body: "天天向上".to_string(),
            },
        )
        .unwrap();
        let r = query_title(&index, &reader, "我们", 1).unwrap();
        assert_eq!(r.len(), 1);
        // println!("{:?}", r.get(0));
        let ts = &*r.get(0).unwrap().create_at;
        delete(&index, &reader, "我们一起去唱歌", ts).unwrap();
        let serach = reader.searcher();
        assert_eq!(serach.num_docs(), 0);
        let end = std::time::Instant::now();
        println!("total cost {:?}", end.duration_since(begin));
    }
}
