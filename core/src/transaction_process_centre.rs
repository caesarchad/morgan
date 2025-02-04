//! The `tpu` module implements the Transaction Processing Unit, a
//! multi-stage transaction processing pipeline in software.

use crate::treasury_stage::BankingStage;
use crate::block_buffer_pool::BlockBufferPool;
use crate::propagate_stage::BroadcastStage;
use crate::node_group_info::NodeGroupInfo;
use crate::node_group_info_voter_listener::ClusterInfoVoteListener;
use crate::fetch_stage::FetchStage;
use crate::water_clock_recorder::{WaterClockRecorder, WorkingBankEntries};
use crate::service::Service;
use crate::signature_verify_stage::SigVerifyStage;
use morgan_interface::hash::Hash;
use morgan_interface::pubkey::Pubkey;
use std::net::UdpSocket;
use std::sync::atomic::AtomicBool;
use std::sync::mpsc::{channel, Receiver};
use std::sync::{Arc, Mutex, RwLock};
use std::thread;

pub struct Tpu {
    fetch_stage: FetchStage,
    sigverify_stage: SigVerifyStage,
    banking_stage: BankingStage,
    cluster_info_vote_listener: ClusterInfoVoteListener,
    broadcast_stage: BroadcastStage,
}

impl Tpu {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        id: &Pubkey,
        node_group_info: &Arc<RwLock<NodeGroupInfo>>,
        waterclock_recorder: &Arc<Mutex<WaterClockRecorder>>,
        entry_receiver: Receiver<WorkingBankEntries>,
        transactions_sockets: Vec<UdpSocket>,
        tpu_via_blobs_sockets: Vec<UdpSocket>,
        broadcast_socket: UdpSocket,
        sigverify_disabled: bool,
        block_buffer_pool: &Arc<BlockBufferPool>,
        exit: &Arc<AtomicBool>,
        genesis_blockhash: &Hash,
    ) -> Self {
        node_group_info.write().unwrap().set_leader(id);

        let (packet_sender, packet_receiver) = channel();
        let fetch_stage = FetchStage::new_with_sender(
            transactions_sockets,
            tpu_via_blobs_sockets,
            &exit,
            &packet_sender,
            &waterclock_recorder,
        );
        let (verified_sender, verified_receiver) = channel();

        let sigverify_stage =
            SigVerifyStage::new(packet_receiver, sigverify_disabled, verified_sender.clone());

        let (verified_vote_sender, verified_vote_receiver) = channel();
        let cluster_info_vote_listener = ClusterInfoVoteListener::new(
            &exit,
            node_group_info.clone(),
            sigverify_disabled,
            verified_vote_sender,
            &waterclock_recorder,
        );

        let banking_stage = BankingStage::new(
            &node_group_info,
            waterclock_recorder,
            verified_receiver,
            verified_vote_receiver,
        );

        let broadcast_stage = BroadcastStage::new(
            broadcast_socket,
            node_group_info.clone(),
            entry_receiver,
            &exit,
            block_buffer_pool,
            genesis_blockhash,
        );

        Self {
            fetch_stage,
            sigverify_stage,
            banking_stage,
            cluster_info_vote_listener,
            broadcast_stage,
        }
    }
}

impl Service for Tpu {
    type JoinReturnType = ();

    fn join(self) -> thread::Result<()> {
        let mut results = vec![];
        results.push(self.fetch_stage.join());
        results.push(self.sigverify_stage.join());
        results.push(self.cluster_info_vote_listener.join());
        results.push(self.banking_stage.join());
        let broadcast_result = self.broadcast_stage.join();
        for result in results {
            result?;
        }
        let _ = broadcast_result?;
        Ok(())
    }
}

pub fn camel_to_snake(text: &str) -> String {
    let mut out = String::with_capacity(text.len());
    let mut first = true;
    text.chars().for_each(|c| {
        if !first && c.is_uppercase() {
            out.push('_');
            out.extend(c.to_lowercase());
        } else if first {
            first = false;
            out.extend(c.to_lowercase());
        } else {
            out.push(c);
        }
    });
    out
}
