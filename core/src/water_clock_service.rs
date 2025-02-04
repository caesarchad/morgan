//! The water_clock_service implements a system-wide clock to measure the passage of time
use crate::water_clock_recorder::WaterClockRecorder;
use crate::service::Service;
use core_affinity;
use morgan_interface::waterclock_config::WaterClockConfig;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::{self, sleep, Builder, JoinHandle};
use futures::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use std::io::Result;

pub struct WaterClockService {
    tick_producer: JoinHandle<()>,
}


pub const NUM_HASHES_PER_BATCH: u64 = 1;

impl WaterClockService {
    pub fn new(
        waterclock_recorder: Arc<Mutex<WaterClockRecorder>>,
        waterclock_config: &Arc<WaterClockConfig>,
        waterclock_exit: &Arc<AtomicBool>,
    ) -> Self {
        let waterclock_exit_ = waterclock_exit.clone();
        let waterclock_config = waterclock_config.clone();
        let tick_producer = Builder::new()
            .name("morgan-waterclock-service-tick_producer".to_string())
            .spawn(move || {
                if waterclock_config.hashes_per_tick.is_none() {
                    Self::sleepy_tick_producer(waterclock_recorder, &waterclock_config, &waterclock_exit_);
                } else {
                    if let Some(cores) = core_affinity::get_core_ids() {
                        core_affinity::set_for_current(cores[0]);
                    }
                    Self::tick_producer(waterclock_recorder, &waterclock_exit_);
                }
                waterclock_exit_.store(true, Ordering::Relaxed);
            })
            .unwrap();

        Self { tick_producer }
    }

    fn sleepy_tick_producer(
        waterclock_recorder: Arc<Mutex<WaterClockRecorder>>,
        waterclock_config: &WaterClockConfig,
        waterclock_exit: &AtomicBool,
    ) {
        while !waterclock_exit.load(Ordering::Relaxed) {
            sleep(waterclock_config.target_tick_duration);
            waterclock_recorder.lock().unwrap().tick();
        }
    }

    fn tick_producer(waterclock_recorder: Arc<Mutex<WaterClockRecorder>>, waterclock_exit: &AtomicBool) {
        let waterclock = waterclock_recorder.lock().unwrap().waterclock.clone();
        loop {
            if waterclock.lock().unwrap().hash(NUM_HASHES_PER_BATCH) {
                waterclock_recorder.lock().unwrap().tick();
                if waterclock_exit.load(Ordering::Relaxed) {
                    break;
                }
            }
        }
    }

}

impl Service for WaterClockService {
    type JoinReturnType = ();

    fn join(self) -> thread::Result<()> {
        self.tick_producer.join()
    }
}

pub fn write_u16frame<'stream, 'buf, 'c, TSocket>(
    mut stream: &'stream mut TSocket,
    buf: &'buf [u8],
) -> Result<()>
where
    'stream: 'c,
    'buf: 'c,
    TSocket: AsyncWrite + Unpin,
{
    

    Ok(())
}

pub fn write_u16frame_len<TSocket>(stream: &mut TSocket, len: u16) -> Result<()>
where
    TSocket: AsyncWrite + Unpin,
{
    let len = u16::to_be_bytes(len);

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::block_buffer_pool::{get_tmp_ledger_path, BlockBufferPool};
    use crate::genesis_utils::{create_genesis_block, GenesisBlockInfo};
    use crate::leader_arrange_cache::LeaderScheduleCache;
    use crate::water_clock_recorder::WorkingBank;
    use crate::result::Result;
    use crate::test_tx::test_tx;
    use morgan_runtime::bank::Bank;
    use morgan_interface::hash::hash;
    use morgan_interface::pubkey::Pubkey;
    use std::time::Duration;

    #[test]
    fn test_waterclock_service() {
        let GenesisBlockInfo { genesis_block, .. } = create_genesis_block(2);
        let bank = Arc::new(Bank::new(&genesis_block));
        let prev_hash = bank.last_blockhash();
        let ledger_path = get_tmp_ledger_path!();
        {
            let block_buffer_pool =
                BlockBufferPool::open_ledger_file(&ledger_path).expect("Expected to be able to open database ledger");
            let waterclock_config = Arc::new(WaterClockConfig {
                hashes_per_tick: Some(2),
                target_tick_duration: Duration::from_millis(42),
            });
            let (waterclock_recorder, entry_receiver) = WaterClockRecorder::new(
                bank.tick_height(),
                prev_hash,
                bank.slot(),
                Some(4),
                bank.ticks_per_slot(),
                &Pubkey::default(),
                &Arc::new(block_buffer_pool),
                &Arc::new(LeaderScheduleCache::new_from_bank(&bank)),
                &waterclock_config,
            );
            let waterclock_recorder = Arc::new(Mutex::new(waterclock_recorder));
            let exit = Arc::new(AtomicBool::new(false));
            let working_bank = WorkingBank {
                bank: bank.clone(),
                min_tick_height: bank.tick_height(),
                max_tick_height: std::u64::MAX,
            };

            let entry_producer: JoinHandle<Result<()>> = {
                let waterclock_recorder = waterclock_recorder.clone();
                let exit = exit.clone();

                Builder::new()
                    .name("morgan-waterclock-service-entry_producer".to_string())
                    .spawn(move || {
                        loop {
                            // send some data
                            let h1 = hash(b"hello world!");
                            let tx = test_tx();
                            let _ = waterclock_recorder
                                .lock()
                                .unwrap()
                                .record(bank.slot(), h1, vec![tx]);

                            if exit.load(Ordering::Relaxed) {
                                break Ok(());
                            }
                        }
                    })
                    .unwrap()
            };

            let waterclock_service = WaterClockService::new(waterclock_recorder.clone(), &waterclock_config, &exit);
            waterclock_recorder.lock().unwrap().set_working_bank(working_bank);

            // get some events
            let mut hashes = 0;
            let mut need_tick = true;
            let mut need_entry = true;
            let mut need_partial = true;

            while need_tick || need_entry || need_partial {
                for entry in entry_receiver.recv().unwrap().1 {
                    let entry = &entry.0;
                    if entry.is_tick() {
                        assert!(
                            entry.num_hashes <= waterclock_config.hashes_per_tick.unwrap(),
                            format!(
                                "{} <= {}",
                                entry.num_hashes,
                                waterclock_config.hashes_per_tick.unwrap()
                            )
                        );

                        if entry.num_hashes == waterclock_config.hashes_per_tick.unwrap() {
                            need_tick = false;
                        } else {
                            need_partial = false;
                        }

                        hashes += entry.num_hashes;

                        assert_eq!(hashes, waterclock_config.hashes_per_tick.unwrap());

                        hashes = 0;
                    } else {
                        assert!(entry.num_hashes >= 1);
                        need_entry = false;
                        hashes += entry.num_hashes;
                    }
                }
            }
            exit.store(true, Ordering::Relaxed);
            let _ = waterclock_service.join().unwrap();
            let _ = entry_producer.join().unwrap();
        }
        BlockBufferPool::remove_ledger_file(&ledger_path).unwrap();
    }
}
