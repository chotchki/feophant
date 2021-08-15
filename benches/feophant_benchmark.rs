/*use std::sync::Arc;
use std::time::Duration;

use criterion::BenchmarkId;
use criterion::Criterion;
use criterion::{criterion_group, criterion_main};

use feophantlib::constants::Nullable;
use feophantlib::engine::io::row_formats::RowData;
use feophantlib::engine::io::FileManager;
use feophantlib::engine::io::RowManager;
use feophantlib::engine::objects::types::BaseSqlTypes;
use feophantlib::engine::objects::types::BaseSqlTypesMapper;
use feophantlib::engine::objects::Attribute;
use feophantlib::engine::objects::SqlTuple;
use feophantlib::engine::objects::Table;
use feophantlib::engine::transactions::TransactionId;
use futures::pin_mut;
use tempfile::TempDir;
use tokio::runtime::Builder;
use tokio_stream::StreamExt;

fn get_table() -> Arc<Table> {
    Arc::new(Table::new(
        uuid::Uuid::new_v4(),
        "test_table".to_string(),
        vec![
            Attribute::new(
                "header".to_string(),
                BaseSqlTypesMapper::Text,
                Nullable::NotNull,
                None,
            ),
            Attribute::new(
                "id".to_string(),
                BaseSqlTypesMapper::Uuid,
                Nullable::Null,
                None,
            ),
            Attribute::new(
                "header3".to_string(),
                BaseSqlTypesMapper::Text,
                Nullable::NotNull,
                None,
            ),
        ],
    ))
}

fn get_row(input: String) -> SqlTuple {
    SqlTuple(vec![
            Some(BaseSqlTypes::Text(input)),
            None,
            Some(BaseSqlTypes::Text("blah blah blah blah blah blah blah blah blah blah blah blah blah blah blah blah blah blah blah blah blah blah blah blah blah blah blah blah blah blah blah blah blah blah blah blah".to_string())),
        ])
}

// Here we have an async function to benchmark
async fn row_manager_mass_insert(row_count: usize) -> Result<(), Box<dyn std::error::Error>> {
    let tmp = TempDir::new()?;
    let tmp_dir = tmp.path().as_os_str().to_os_string();

    let table = get_table();
    let fm = Arc::new(FileManager::new(tmp_dir.clone())?);
    let rm = RowManager::new(fm);

    let tran_id = TransactionId::new(1);

    for i in 0..row_count {
        rm.clone()
            .insert_row(tran_id, table.clone(), get_row(i.to_string()))
            .await?;
    }

    drop(rm);

    //Now let's make sure they're really in the table, persisting across restarts
    let fm = Arc::new(FileManager::new(tmp_dir)?);
    let rm = RowManager::new(fm);

    pin_mut!(rm);
    let result_rows: Vec<RowData> = rm
        .clone()
        .get_stream(table.clone())
        .map(Result::unwrap)
        .collect()
        .await;

    assert_eq!(result_rows.len(), row_count);
    for i in 0..row_count {
        let sample_row = get_row(i.to_string());
        assert_eq!(result_rows[i].user_data, sample_row);
    }

    Ok(())
}

fn from_elem(c: &mut Criterion) {
    let rt = Builder::new_current_thread().build().unwrap();

    let row_count: usize = 50;

    c.bench_with_input(
        BenchmarkId::new("row_manager_mass_insert", row_count),
        &row_count,
        |b, &row_count| {
            // Insert a call to `to_async` to convert the bencher to async mode.
            // The timing loops are the same as with the normal bencher.
            b.to_async(&rt).iter(|| row_manager_mass_insert(row_count));
        },
    );
}

criterion_group!(benches, from_elem);
criterion_main!(benches);
*/
