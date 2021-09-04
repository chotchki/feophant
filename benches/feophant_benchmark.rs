use criterion::BenchmarkId;
use criterion::Criterion;
use criterion::{criterion_group, criterion_main};
use feophantlib::engine::get_row;
use feophantlib::engine::get_table;
use feophantlib::engine::io::row_formats::RowData;
use feophantlib::engine::io::FileManager;
use feophantlib::engine::io::LockCacheManager;
use feophantlib::engine::io::RowManager;
use feophantlib::engine::transactions::TransactionId;
use futures::pin_mut;
use std::sync::Arc;
use tempfile::TempDir;
use tokio::runtime::Builder;
use tokio_stream::StreamExt;

// Here we have an async function to benchmark
async fn row_manager_mass_insert(row_count: usize) -> Result<(), Box<dyn std::error::Error>> {
    let tmp = TempDir::new()?;
    let tmp_dir = tmp.path().as_os_str().to_os_string();

    let table = get_table();
    let fm = Arc::new(FileManager::new(tmp_dir.clone())?);
    let rm = RowManager::new(LockCacheManager::new(fm));

    let tran_id = TransactionId::new(1);

    for i in 0..row_count {
        rm.clone()
            .insert_row(tran_id, table.clone(), get_row(i.to_string()))
            .await?;
    }

    drop(rm);

    //Now let's make sure they're really in the table, persisting across restarts
    let fm = Arc::new(FileManager::new(tmp_dir)?);
    let rm = RowManager::new(LockCacheManager::new(fm));

    pin_mut!(rm);
    let result_rows: Vec<RowData> = rm
        .clone()
        .get_stream(table.clone())
        .map(Result::unwrap)
        .collect()
        .await;

    assert_eq!(result_rows.len(), row_count);
    result_rows
        .iter()
        .enumerate()
        .take(row_count)
        .map(|(i, row)| {
            let sample_row = get_row(i.to_string());
            assert_eq!(row.user_data, sample_row);
        })
        .for_each(drop);

    Ok(())
}

fn from_elem(c: &mut Criterion) {
    let rt = Builder::new_current_thread().build().unwrap();

    let row_count: usize = 500;

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
