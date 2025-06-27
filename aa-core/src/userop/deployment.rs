use alloy::primitives::Address;
use engine_core::error::EngineError;
use std::time::Duration;

pub trait DeploymentCache: Send + Sync {
    /// Check if we know for certain that the account is deployed
    /// Returns Some(true) if definitely deployed, None if unknown
    fn is_deployed(
        &self,
        chain_id: u64,
        account_address: &Address,
    ) -> impl Future<Output = Option<bool>> + Send;
}

pub enum AcquireLockResult {
    Acquired,
    AlreadyLocked(String),
}

pub type LockId = String;

pub trait DeploymentLock: Send + Sync {
    /// Check if a deployment lock exists
    /// Returns Some(Duration) with time since lock was acquired if locked, None if not locked
    fn check_lock(
        &self,
        chain_id: u64,
        account_address: &Address,
    ) -> impl Future<Output = Option<(LockId, Duration)>> + Send;

    /// Try to acquire a deployment lock
    /// Returns true if successful, false if already locked
    fn acquire_lock(
        &self,
        chain_id: u64,
        account_address: &Address,
    ) -> impl Future<Output = Result<AcquireLockResult, EngineError>> + Send;

    /// Release a deployment lock
    fn release_lock(
        &self,
        chain_id: u64,
        account_address: &Address,
    ) -> impl Future<Output = Result<bool, EngineError>> + Send;
}

pub enum DeploymentStatus {
    /// Account is definitely deployed
    Deployed,

    /// Account is currently being deployed by another process
    BeingDeployed { stale: bool, lock_id: LockId },

    /// Account is not deployed
    NotDeployed,
}

// A generic deployment manager
pub struct DeploymentManager<C, L>
where
    C: DeploymentCache,
    L: DeploymentLock,
{
    pub cache: C,
    pub lock: L,
    pub stale_lock_threshold: Duration,
}

impl<C, L> DeploymentManager<C, L>
where
    C: DeploymentCache,
    L: DeploymentLock,
{
    pub fn new(cache: C, lock: L, stale_lock_threshold: Duration) -> Self {
        Self {
            cache,
            lock,
            stale_lock_threshold,
        }
    }

    /// Core function that implements the deployment check logic
    pub async fn check_deployment_status(
        &self,
        chain_id: u64,
        account_address: &Address,
        check_chain: impl std::future::Future<Output = Result<bool, EngineError>>,
    ) -> Result<DeploymentStatus, EngineError> {
        // 1. Check cache first for definitive "deployed" status
        if let Some(true) = self.cache.is_deployed(chain_id, account_address).await {
            return Ok(DeploymentStatus::Deployed);
        }

        // 2. Check for deployment lock
        if let Some((lock_id, locked_duration)) =
            self.lock.check_lock(chain_id, account_address).await
        {
            // Check if lock is stale
            let is_stale = locked_duration > self.stale_lock_threshold;

            // For stale locks, we'll check chain state
            if is_stale {
                let is_deployed = check_chain.await?;
                if is_deployed {
                    return Ok(DeploymentStatus::Deployed);
                }
            }

            // Either fresh lock or stale but not deployed
            return Ok(DeploymentStatus::BeingDeployed {
                stale: is_stale,
                lock_id,
            });
        }

        // 3. No lock exists, check chain state
        match check_chain.await? {
            true => Ok(DeploymentStatus::Deployed),
            false => Ok(DeploymentStatus::NotDeployed),
        }
    }
}
