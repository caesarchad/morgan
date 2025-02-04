use crate::bank_client::BankClient;
use serde::Serialize;
use morgan_interface::client::SyncClient;
use morgan_interface::instruction::{AccountMeta, Instruction};
use morgan_interface::loader_instruction;
use morgan_interface::message::Message;
use morgan_interface::pubkey::Pubkey;
use morgan_interface::signature::{Keypair, KeypairUtil};
use morgan_interface::system_instruction;

pub fn load_program(
    bank_client: &BankClient,
    from_keypair: &Keypair,
    loader_pubkey: &Pubkey,
    program: Vec<u8>,
) -> Pubkey {
    let program_keypair = Keypair::new();
    let program_pubkey = program_keypair.pubkey();

    let instruction = system_instruction::create_account(
        &from_keypair.pubkey(),
        &program_pubkey,
        1,
        program.len() as u64,
        loader_pubkey,
    );
    bank_client
        .send_instruction(&from_keypair, instruction)
        .unwrap();

    let chunk_size = 256; // Size of chunk just needs to fit into tx
    let mut offset = 0;
    for chunk in program.chunks(chunk_size) {
        let instruction =
            loader_instruction::write(&program_pubkey, loader_pubkey, offset, chunk.to_vec());
        let message = Message::new_with_payer(vec![instruction], Some(&from_keypair.pubkey()));
        bank_client
            .send_message(&[from_keypair, &program_keypair], message)
            .unwrap();
        offset += chunk_size as u32;
    }

    let instruction = loader_instruction::finalize(&program_pubkey, loader_pubkey);
    let message = Message::new_with_payer(vec![instruction], Some(&from_keypair.pubkey()));
    bank_client
        .send_message(&[from_keypair, &program_keypair], message)
        .unwrap();

    program_pubkey
}

// Return an Instruction that invokes `program_id` with `data` and required
// a signature from `from_pubkey`.
pub fn create_invoke_instruction<T: Serialize>(
    from_pubkey: Pubkey,
    program_id: Pubkey,
    data: &T,
) -> Instruction {
    let account_metas = vec![AccountMeta::new(from_pubkey, true)];
    Instruction::new(program_id, data, account_metas)
}
