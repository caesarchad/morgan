use log::*;
use morgan_sdk::account::KeyedAccount;
use morgan_sdk::instruction::InstructionError;
use morgan_sdk::pubkey::Pubkey;
use morgan_sdk::morgan_entrypoint;

morgan_entrypoint!(entrypoint);
fn entrypoint(
    program_id: &Pubkey,
    keyed_accounts: &mut [KeyedAccount],
    data: &[u8],
    tick_height: u64,
) -> Result<(), InstructionError> {
    morgan_logger::setup();
    trace!("noop: program_id: {:?}", program_id);
    trace!("noop: keyed_accounts: {:#?}", keyed_accounts);
    trace!("noop: data: {:?}", data);
    trace!("noop: tick_height: {:?}", tick_height);
    Ok(())
}
