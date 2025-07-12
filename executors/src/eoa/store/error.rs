use crate::eoa::EoaExecutorStore;
use crate::eoa::store::TransactionStoreError;
use crate::eoa::store::atomic::AtomicEoaExecutorStore;

impl AtomicEoaExecutorStore {
    pub fn eoa_lock_lost_error(&self) -> TransactionStoreError {
        TransactionStoreError::LockLost {
            eoa: self.eoa(),
            chain_id: self.chain_id(),
            worker_id: self.worker_id().to_string(),
        }
    }
}

impl EoaExecutorStore {
    pub fn nonce_sync_required_error(&self) -> TransactionStoreError {
        TransactionStoreError::NonceSyncRequired {
            eoa: self.eoa,
            chain_id: self.chain_id,
        }
    }
}
