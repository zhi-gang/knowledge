use std::io::{BufRead, BufReader};

use knowledge::repository::*;


fn create_repository() {
    let begin = std::time::Instant::now();
    let (index, index_reader) = create_index("repository").unwrap();

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

pub fn main(){
    create_repository();
}