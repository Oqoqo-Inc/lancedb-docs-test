// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

use std::sync::Arc;

use arrow_array::{Int64Array, RecordBatch, RecordBatchIterator, StringArray};
use arrow_schema::{DataType, Field, Schema};
use futures_util::TryStreamExt;
// --8<-- [start:versioning_imports]
use lancedb::connect;
use lancedb::database::CreateTableMode;
// --8<-- [end:versioning_imports]

type BatchIter = RecordBatchIterator<
    std::vec::IntoIter<std::result::Result<RecordBatch, arrow_schema::ArrowError>>,
>;

fn quotes_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("author", DataType::Utf8, false),
        Field::new("quote", DataType::Utf8, false),
    ]))
}

fn create_quotes_batch(schema: Arc<Schema>) -> RecordBatch {
    let ids = Int64Array::from(vec![1, 2, 3]);
    let authors = StringArray::from(vec!["Richard", "Morty", "Richard"]);
    let quotes = StringArray::from(vec![
        "Wubba Lubba Dub Dub!",
        "Rick, what's going on?",
        "I turned myself into a pickle, Morty!",
    ]);

    RecordBatch::try_new(
        schema,
        vec![Arc::new(ids), Arc::new(authors), Arc::new(quotes)],
    )
    .unwrap()
}

fn quotes_to_reader(schema: Arc<Schema>) -> BatchIter {
    let batch = create_quotes_batch(schema.clone());
    RecordBatchIterator::new(vec![Ok(batch)].into_iter(), schema)
}

#[tokio::main]
async fn main() {
    let temp_dir = tempfile::tempdir().unwrap();
    let uri = temp_dir.path().to_str().unwrap();

    // --8<-- [start:versioning_basic_setup]
    // Connect to LanceDB
    let db = connect(uri).execute().await.unwrap();

    // Create a table with initial data
    let schema = quotes_schema();
    let table = db
        .create_table("quotes_versioning_example", quotes_to_reader(schema))
        .mode(CreateTableMode::Overwrite)
        .execute()
        .await
        .unwrap();
    // --8<-- [end:versioning_basic_setup]

    // --8<-- [start:versioning_check_initial_version]
    // View the initial version
    let versions = table.list_versions().await.unwrap();
    println!("Number of versions after creation: {}", versions.len());
    println!("Current version: {}", table.version().await.unwrap());
    // --8<-- [end:versioning_check_initial_version]

    // --8<-- [start:versioning_list_all_versions]
    // Let's see all versions
    let all_versions = table.list_versions().await.unwrap();
    for v in &all_versions {
        println!("Version {}, created at {:?}", v.version, v.timestamp);
    }
    // --8<-- [end:versioning_list_all_versions]

    // --8<-- [start:versioning_checkout_latest]
    // Go back to the latest version
    table.checkout_latest().await.unwrap();
    // --8<-- [end:versioning_checkout_latest]
}
