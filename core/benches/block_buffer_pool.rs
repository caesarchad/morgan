#![feature(test)]
use rand;

extern crate test;

#[macro_use]
extern crate morgan;

use rand::seq::SliceRandom;
use rand::{thread_rng, Rng};
use morgan::block_buffer_pool::{get_tmp_ledger_path, BlockBufferPool};
use morgan::entry_info::{make_large_test_entries, make_tiny_test_entries, EntrySlice};
use morgan::packet::{Blob, BLOB_HEADER_SIZE};
use test::Bencher;

// Given some blobs and a ledger at ledger_path, benchmark writing the blobs to the ledger
fn bench_write_blobs(bench: &mut Bencher, blobs: &mut Vec<Blob>, ledger_path: &str) {
    let block_buffer_pool =
        BlockBufferPool::open_ledger_file(&ledger_path).expect("Expected to be able to open database ledger");

    let num_blobs = blobs.len();

    bench.iter(move || {
        for blob in blobs.iter_mut() {
            let index = blob.index();

            block_buffer_pool
                .place_info_obj_bytes(
                    blob.slot(),
                    index,
                    &blob.data[..BLOB_HEADER_SIZE + blob.size()],
                )
                .unwrap();

            blob.set_index(index + num_blobs as u64);
        }
    });

    BlockBufferPool::remove_ledger_file(&ledger_path).expect("Expected successful database destruction");
}

// Insert some blobs into the ledger in preparation for read benchmarks
fn setup_read_bench(
    block_buffer_pool: &mut BlockBufferPool,
    num_small_blobs: u64,
    num_large_blobs: u64,
    slot: u64,
) {
    // Make some big and small entries
    let mut entries = make_large_test_entries(num_large_blobs as usize);
    entries.extend(make_tiny_test_entries(num_small_blobs as usize));

    // Convert the entries to blobs, write the blobs to the ledger
    let mut blobs = entries.to_blobs();
    for (index, b) in blobs.iter_mut().enumerate() {
        b.set_index(index as u64);
        b.set_slot(slot);
    }
    block_buffer_pool
        .record_objs(&blobs)
        .expect("Expectd successful insertion of blobs into ledger");
}

// Write small blobs to the ledger
#[bench]
#[ignore]
fn bench_write_small(bench: &mut Bencher) {
    let ledger_path = get_tmp_ledger_path!();
    let num_entries = 32 * 1024;
    let entries = make_tiny_test_entries(num_entries);
    let mut blobs = entries.to_blobs();
    for (index, b) in blobs.iter_mut().enumerate() {
        b.set_index(index as u64);
    }
    bench_write_blobs(bench, &mut blobs, &ledger_path);
}

// Write big blobs to the ledger
#[bench]
#[ignore]
fn bench_write_big(bench: &mut Bencher) {
    let ledger_path = get_tmp_ledger_path!();
    let num_entries = 32 * 1024;
    let entries = make_large_test_entries(num_entries);
    let mut blobs = entries.to_blobs();
    for (index, b) in blobs.iter_mut().enumerate() {
        b.set_index(index as u64);
    }

    bench_write_blobs(bench, &mut blobs, &ledger_path);
}

#[bench]
#[ignore]
fn bench_read_sequential(bench: &mut Bencher) {
    let ledger_path = get_tmp_ledger_path!();
    let mut block_buffer_pool =
        BlockBufferPool::open_ledger_file(&ledger_path).expect("Expected to be able to open database ledger");

    // Insert some big and small blobs into the ledger
    let num_small_blobs = 32 * 1024;
    let num_large_blobs = 32 * 1024;
    let total_blobs = num_small_blobs + num_large_blobs;
    let slot = 0;
    setup_read_bench(&mut block_buffer_pool, num_small_blobs, num_large_blobs, slot);

    let num_reads = total_blobs / 15;
    let mut rng = rand::thread_rng();
    bench.iter(move || {
        // Generate random starting point in the range [0, total_blobs - 1], read num_reads blobs sequentially
        let start_index = rng.gen_range(0, num_small_blobs + num_large_blobs);
        for i in start_index..start_index + num_reads {
            let _ = block_buffer_pool.fetch_info_obj(slot, i as u64 % total_blobs);
        }
    });

    BlockBufferPool::remove_ledger_file(&ledger_path).expect("Expected successful database destruction");
}

#[bench]
#[ignore]
fn bench_read_random(bench: &mut Bencher) {
    let ledger_path = get_tmp_ledger_path!();
    let mut block_buffer_pool =
        BlockBufferPool::open_ledger_file(&ledger_path).expect("Expected to be able to open database ledger");

    // Insert some big and small blobs into the ledger
    let num_small_blobs = 32 * 1024;
    let num_large_blobs = 32 * 1024;
    let total_blobs = num_small_blobs + num_large_blobs;
    let slot = 0;
    setup_read_bench(&mut block_buffer_pool, num_small_blobs, num_large_blobs, slot);

    let num_reads = total_blobs / 15;

    // Generate a num_reads sized random sample of indexes in range [0, total_blobs - 1],
    // simulating random reads
    let mut rng = rand::thread_rng();
    let indexes: Vec<usize> = (0..num_reads)
        .map(|_| rng.gen_range(0, total_blobs) as usize)
        .collect();
    bench.iter(move || {
        for i in indexes.iter() {
            let _ = block_buffer_pool.fetch_info_obj(slot, *i as u64);
        }
    });

    BlockBufferPool::remove_ledger_file(&ledger_path).expect("Expected successful database destruction");
}

#[bench]
#[ignore]
fn bench_insert_data_blob_small(bench: &mut Bencher) {
    let ledger_path = get_tmp_ledger_path!();
    let block_buffer_pool =
        BlockBufferPool::open_ledger_file(&ledger_path).expect("Expected to be able to open database ledger");
    let num_entries = 32 * 1024;
    let entries = make_tiny_test_entries(num_entries);
    let mut blobs = entries.to_blobs();

    blobs.shuffle(&mut thread_rng());

    bench.iter(move || {
        for blob in blobs.iter_mut() {
            let index = blob.index();
            blob.set_index(index + num_entries as u64);
        }
        block_buffer_pool.record_objs(&blobs).unwrap();
    });

    BlockBufferPool::remove_ledger_file(&ledger_path).expect("Expected successful database destruction");
}

#[bench]
#[ignore]
fn bench_insert_data_blob_big(bench: &mut Bencher) {
    let ledger_path = get_tmp_ledger_path!();
    let block_buffer_pool =
        BlockBufferPool::open_ledger_file(&ledger_path).expect("Expected to be able to open database ledger");
    let num_entries = 32 * 1024;
    let entries = make_large_test_entries(num_entries);
    let mut shared_blobs = entries.to_shared_blobs();
    shared_blobs.shuffle(&mut thread_rng());

    bench.iter(move || {
        for blob in shared_blobs.iter_mut() {
            let index = blob.read().unwrap().index();
            block_buffer_pool.record_public_objs(vec![blob.clone()]).unwrap();
            blob.write().unwrap().set_index(index + num_entries as u64);
        }
    });

    BlockBufferPool::remove_ledger_file(&ledger_path).expect("Expected successful database destruction");
}
