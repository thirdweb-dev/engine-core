use alloy::primitives::Address;
use std::time::Duration;
use testcontainers::clients::Cli;
use testcontainers_modules::redis::Redis;
use twmq::redis::AsyncCommands;
use engine_aa_core::userop::deployment::{AcquireLockResult, DeploymentCache, DeploymentLock};
use engine_executors::external_bundler::deployment::{RedisDeploymentCache, RedisDeploymentLock};

#[tokio::test]
async fn test_redis_deployment_cache_new() {
    let docker = Cli::default();
    let redis_container = docker.run(Redis::default());
    let redis_url = format!("redis://127.0.0.1:{}/", redis_container.get_host_port_ipv4(6379));
    
    let client = twmq::redis::Client::open(redis_url).unwrap();
    let cache = RedisDeploymentCache::new(client).await.unwrap();
    
    // Test that cache was created successfully
    assert!(cache.conn().is_open());
}

#[tokio::test]
async fn test_redis_deployment_lock_new() {
    let docker = Cli::default();
    let redis_container = docker.run(Redis::default());
    let redis_url = format!("redis://127.0.0.1:{}/", redis_container.get_host_port_ipv4(6379));
    
    let client = twmq::redis::Client::open(redis_url).unwrap();
    let lock = RedisDeploymentLock::new(client).await.unwrap();
    
    // Test that lock was created successfully
    assert!(lock.conn().is_open());
}

#[tokio::test]
async fn test_deployment_cache_is_deployed() {
    let docker = Cli::default();
    let redis_container = docker.run(Redis::default());
    let redis_url = format!("redis://127.0.0.1:{}/", redis_container.get_host_port_ipv4(6379));
    
    let client = twmq::redis::Client::open(redis_url).unwrap();
    let cache = RedisDeploymentCache::new(client).await.unwrap();
    
    let chain_id = 1;
    let address = Address::from([0x42; 20]);
    
    // Test initial state (no cache entry)
    let result = cache.is_deployed(chain_id, &address).await;
    assert!(result.is_none());
    
    // Manually set deployed state
    let mut conn = cache.conn().clone();
    let cache_key = format!("deployment_cache:{}:{}", chain_id, address);
    let _: () = conn.set(&cache_key, "deployed").await.unwrap();
    
    // Test deployed state
    let result = cache.is_deployed(chain_id, &address).await;
    assert_eq!(result, Some(true));
    
    // Manually set not deployed state
    let _: () = conn.set(&cache_key, "not_deployed").await.unwrap();
    
    // Test not deployed state
    let result = cache.is_deployed(chain_id, &address).await;
    assert_eq!(result, Some(false));
    
    // Test invalid state
    let _: () = conn.set(&cache_key, "invalid").await.unwrap();
    let result = cache.is_deployed(chain_id, &address).await;
    assert!(result.is_none());
}

#[tokio::test]
async fn test_deployment_lock_acquire_and_release() {
    let docker = Cli::default();
    let redis_container = docker.run(Redis::default());
    let redis_url = format!("redis://127.0.0.1:{}/", redis_container.get_host_port_ipv4(6379));
    
    let client = twmq::redis::Client::open(redis_url).unwrap();
    let lock = RedisDeploymentLock::new(client).await.unwrap();
    
    let chain_id = 1;
    let address = Address::from([0x42; 20]);
    
    // Test initial state (no lock)
    let check_result = lock.check_lock(chain_id, &address).await;
    assert!(check_result.is_none());
    
    // Acquire lock
    let acquire_result = lock.acquire_lock(chain_id, &address).await.unwrap();
    assert!(matches!(acquire_result, AcquireLockResult::Acquired));
    
    // Check lock exists
    let check_result = lock.check_lock(chain_id, &address).await;
    assert!(check_result.is_some());
    
    let (lock_id, duration) = check_result.unwrap();
    assert!(!lock_id.is_empty());
    assert!(duration.as_secs() < 10); // Should be recent
    
    // Try to acquire again (should fail)
    let acquire_result = lock.acquire_lock(chain_id, &address).await.unwrap();
    match acquire_result {
        AcquireLockResult::AlreadyLocked(existing_lock_id) => {
            assert_eq!(existing_lock_id, lock_id);
        }
        _ => panic!("Expected AlreadyLocked result"),
    }
    
    // Release lock
    let release_result = lock.release_lock(chain_id, &address).await.unwrap();
    assert!(release_result); // Should return true (lock was deleted)
    
    // Check lock is gone
    let check_result = lock.check_lock(chain_id, &address).await;
    assert!(check_result.is_none());
    
    // Release non-existent lock
    let release_result = lock.release_lock(chain_id, &address).await.unwrap();
    assert!(!release_result); // Should return false (no lock to delete)
}

#[tokio::test]
async fn test_deployment_lock_different_addresses() {
    let docker = Cli::default();
    let redis_container = docker.run(Redis::default());
    let redis_url = format!("redis://127.0.0.1:{}/", redis_container.get_host_port_ipv4(6379));
    
    let client = twmq::redis::Client::open(redis_url).unwrap();
    let lock = RedisDeploymentLock::new(client).await.unwrap();
    
    let chain_id = 1;
    let address1 = Address::from([0x42; 20]);
    let address2 = Address::from([0x43; 20]);
    
    // Acquire lock for first address
    let acquire_result1 = lock.acquire_lock(chain_id, &address1).await.unwrap();
    assert!(matches!(acquire_result1, AcquireLockResult::Acquired));
    
    // Acquire lock for second address (should succeed)
    let acquire_result2 = lock.acquire_lock(chain_id, &address2).await.unwrap();
    assert!(matches!(acquire_result2, AcquireLockResult::Acquired));
    
    // Both locks should exist
    let check_result1 = lock.check_lock(chain_id, &address1).await;
    let check_result2 = lock.check_lock(chain_id, &address2).await;
    assert!(check_result1.is_some());
    assert!(check_result2.is_some());
    
    // Release first lock
    let release_result1 = lock.release_lock(chain_id, &address1).await.unwrap();
    assert!(release_result1);
    
    // Second lock should still exist
    let check_result1 = lock.check_lock(chain_id, &address1).await;
    let check_result2 = lock.check_lock(chain_id, &address2).await;
    assert!(check_result1.is_none());
    assert!(check_result2.is_some());
}

#[tokio::test]
async fn test_deployment_lock_different_chains() {
    let docker = Cli::default();
    let redis_container = docker.run(Redis::default());
    let redis_url = format!("redis://127.0.0.1:{}/", redis_container.get_host_port_ipv4(6379));
    
    let client = twmq::redis::Client::open(redis_url).unwrap();
    let lock = RedisDeploymentLock::new(client).await.unwrap();
    
    let chain_id1 = 1;
    let chain_id2 = 2;
    let address = Address::from([0x42; 20]);
    
    // Acquire lock for same address on different chains
    let acquire_result1 = lock.acquire_lock(chain_id1, &address).await.unwrap();
    let acquire_result2 = lock.acquire_lock(chain_id2, &address).await.unwrap();
    
    assert!(matches!(acquire_result1, AcquireLockResult::Acquired));
    assert!(matches!(acquire_result2, AcquireLockResult::Acquired));
    
    // Both locks should exist
    let check_result1 = lock.check_lock(chain_id1, &address).await;
    let check_result2 = lock.check_lock(chain_id2, &address).await;
    assert!(check_result1.is_some());
    assert!(check_result2.is_some());
}

#[tokio::test]
async fn test_deployment_cache_different_chains() {
    let docker = Cli::default();
    let redis_container = docker.run(Redis::default());
    let redis_url = format!("redis://127.0.0.1:{}/", redis_container.get_host_port_ipv4(6379));
    
    let client = twmq::redis::Client::open(redis_url).unwrap();
    let cache = RedisDeploymentCache::new(client).await.unwrap();
    
    let chain_id1 = 1;
    let chain_id2 = 2;
    let address = Address::from([0x42; 20]);
    
    // Set deployed state on chain 1
    let mut conn = cache.conn().clone();
    let cache_key1 = format!("deployment_cache:{}:{}", chain_id1, address);
    let _: () = conn.set(&cache_key1, "deployed").await.unwrap();
    
    // Set not deployed state on chain 2
    let cache_key2 = format!("deployment_cache:{}:{}", chain_id2, address);
    let _: () = conn.set(&cache_key2, "not_deployed").await.unwrap();
    
    // Check states are isolated
    let result1 = cache.is_deployed(chain_id1, &address).await;
    let result2 = cache.is_deployed(chain_id2, &address).await;
    
    assert_eq!(result1, Some(true));
    assert_eq!(result2, Some(false));
}

#[tokio::test]
async fn test_deployment_lock_pipeline_operations() {
    let docker = Cli::default();
    let redis_container = docker.run(Redis::default());
    let redis_url = format!("redis://127.0.0.1:{}/", redis_container.get_host_port_ipv4(6379));
    
    let client = twmq::redis::Client::open(redis_url).unwrap();
    let lock = RedisDeploymentLock::new(client).await.unwrap();
    
    let chain_id = 1;
    let address = Address::from([0x42; 20]);
    
    // Acquire lock first
    let acquire_result = lock.acquire_lock(chain_id, &address).await.unwrap();
    assert!(matches!(acquire_result, AcquireLockResult::Acquired));
    
    // Use pipeline to release lock
    let mut pipeline = twmq::redis::Pipeline::new();
    lock.release_lock_with_pipeline(&mut pipeline, chain_id, &address);
    
    let mut conn = lock.conn().clone();
    pipeline.execute_async(&mut conn).await.unwrap();
    
    // Check lock is gone
    let check_result = lock.check_lock(chain_id, &address).await;
    assert!(check_result.is_none());
}

#[tokio::test]
async fn test_deployment_cache_pipeline_operations() {
    let docker = Cli::default();
    let redis_container = docker.run(Redis::default());
    let redis_url = format!("redis://127.0.0.1:{}/", redis_container.get_host_port_ipv4(6379));
    
    let client = twmq::redis::Client::open(redis_url).unwrap();
    let lock = RedisDeploymentLock::new(client).await.unwrap();
    
    let chain_id = 1;
    let address = Address::from([0x42; 20]);
    
    // Use pipeline to update cache
    let mut pipeline = twmq::redis::Pipeline::new();
    lock.update_cache_with_pipeline(&mut pipeline, chain_id, &address, true);
    
    let mut conn = lock.conn().clone();
    pipeline.execute_async(&mut conn).await.unwrap();
    
    // Check cache was updated
    let cache_key = format!("deployment_cache:{}:{}", chain_id, address);
    let result: Option<String> = conn.get(&cache_key).await.unwrap();
    assert_eq!(result, Some("deployed".to_string()));
}

#[tokio::test]
async fn test_deployment_lock_and_cache_pipeline_operations() {
    let docker = Cli::default();
    let redis_container = docker.run(Redis::default());
    let redis_url = format!("redis://127.0.0.1:{}/", redis_container.get_host_port_ipv4(6379));
    
    let client = twmq::redis::Client::open(redis_url).unwrap();
    let lock = RedisDeploymentLock::new(client).await.unwrap();
    
    let chain_id = 1;
    let address = Address::from([0x42; 20]);
    
    // Acquire lock first
    let acquire_result = lock.acquire_lock(chain_id, &address).await.unwrap();
    assert!(matches!(acquire_result, AcquireLockResult::Acquired));
    
    // Use pipeline to release lock and update cache atomically
    let mut pipeline = twmq::redis::Pipeline::new();
    lock.release_lock_and_update_cache_with_pipeline(&mut pipeline, chain_id, &address, true);
    
    let mut conn = lock.conn().clone();
    pipeline.execute_async(&mut conn).await.unwrap();
    
    // Check lock is gone
    let check_result = lock.check_lock(chain_id, &address).await;
    assert!(check_result.is_none());
    
    // Check cache was updated
    let cache_key = format!("deployment_cache:{}:{}", chain_id, address);
    let result: Option<String> = conn.get(&cache_key).await.unwrap();
    assert_eq!(result, Some("deployed".to_string()));
}

#[tokio::test]
async fn test_deployment_lock_serialization() {
    let docker = Cli::default();
    let redis_container = docker.run(Redis::default());
    let redis_url = format!("redis://127.0.0.1:{}/", redis_container.get_host_port_ipv4(6379));
    
    let client = twmq::redis::Client::open(redis_url).unwrap();
    let lock = RedisDeploymentLock::new(client).await.unwrap();
    
    let chain_id = 1;
    let address = Address::from([0x42; 20]);
    
    // Acquire lock
    let acquire_result = lock.acquire_lock(chain_id, &address).await.unwrap();
    assert!(matches!(acquire_result, AcquireLockResult::Acquired));
    
    // Check lock data is properly serialized
    let lock_key = format!("deployment_lock:{}:{}", chain_id, address);
    let mut conn = lock.conn().clone();
    let lock_data: Option<String> = conn.get(&lock_key).await.unwrap();
    assert!(lock_data.is_some());
    
    let lock_data_str = lock_data.unwrap();
    let parsed: serde_json::Value = serde_json::from_str(&lock_data_str).unwrap();
    
    // Should contain lock_id and acquired_at fields
    assert!(parsed.get("lock_id").is_some());
    assert!(parsed.get("acquired_at").is_some());
    
    // lock_id should be a string
    assert!(parsed["lock_id"].is_string());
    
    // acquired_at should be a number
    assert!(parsed["acquired_at"].is_number());
}

#[tokio::test]
async fn test_deployment_cache_expiration() {
    let docker = Cli::default();
    let redis_container = docker.run(Redis::default());
    let redis_url = format!("redis://127.0.0.1:{}/", redis_container.get_host_port_ipv4(6379));
    
    let client = twmq::redis::Client::open(redis_url).unwrap();
    let lock = RedisDeploymentLock::new(client).await.unwrap();
    
    let chain_id = 1;
    let address = Address::from([0x42; 20]);
    
    // Use pipeline to update cache with expiration
    let mut pipeline = twmq::redis::Pipeline::new();
    lock.update_cache_with_pipeline(&mut pipeline, chain_id, &address, true);
    
    let mut conn = lock.conn().clone();
    pipeline.execute_async(&mut conn).await.unwrap();
    
    // Check cache key has TTL set
    let cache_key = format!("deployment_cache:{}:{}", chain_id, address);
    let ttl: i64 = conn.ttl(&cache_key).await.unwrap();
    
    // Should have TTL set (3600 seconds minus some time for execution)
    assert!(ttl > 3500 && ttl <= 3600);
}

#[tokio::test]
async fn test_deployment_lock_corrupted_data() {
    let docker = Cli::default();
    let redis_container = docker.run(Redis::default());
    let redis_url = format!("redis://127.0.0.1:{}/", redis_container.get_host_port_ipv4(6379));
    
    let client = twmq::redis::Client::open(redis_url).unwrap();
    let lock = RedisDeploymentLock::new(client).await.unwrap();
    
    let chain_id = 1;
    let address = Address::from([0x42; 20]);
    
    // Manually set corrupted lock data
    let lock_key = format!("deployment_lock:{}:{}", chain_id, address);
    let mut conn = lock.conn().clone();
    let _: () = conn.set(&lock_key, "invalid_json").await.unwrap();
    
    // Check lock should return None for corrupted data
    let check_result = lock.check_lock(chain_id, &address).await;
    assert!(check_result.is_none());
    
    // But acquiring should still work (will overwrite corrupted data)
    let acquire_result = lock.acquire_lock(chain_id, &address).await.unwrap();
    match acquire_result {
        AcquireLockResult::AlreadyLocked(lock_id) => {
            // If Redis SET NX fails, it should return the corrupted data as "unknown"
            assert_eq!(lock_id, "unknown");
        }
        AcquireLockResult::Acquired => {
            // This could happen if Redis SET NX succeeds (overwrites corrupted data)
        }
    }
}

#[tokio::test]
async fn test_multiple_deployment_locks_concurrent() {
    let docker = Cli::default();
    let redis_container = docker.run(Redis::default());
    let redis_url = format!("redis://127.0.0.1:{}/", redis_container.get_host_port_ipv4(6379));
    
    let client = twmq::redis::Client::open(redis_url).unwrap();
    let lock = RedisDeploymentLock::new(client).await.unwrap();
    
    let chain_id = 1;
    let address = Address::from([0x42; 20]);
    
    // Try to acquire the same lock concurrently
    let lock1 = lock.clone();
    let lock2 = lock.clone();
    
    let (result1, result2) = tokio::join!(
        lock1.acquire_lock(chain_id, &address),
        lock2.acquire_lock(chain_id, &address)
    );
    
    let result1 = result1.unwrap();
    let result2 = result2.unwrap();
    
    // One should succeed, one should fail
    let acquired_count = match (&result1, &result2) {
        (AcquireLockResult::Acquired, AcquireLockResult::AlreadyLocked(_)) => 1,
        (AcquireLockResult::AlreadyLocked(_), AcquireLockResult::Acquired) => 1,
        (AcquireLockResult::Acquired, AcquireLockResult::Acquired) => 2, // Should not happen
        (AcquireLockResult::AlreadyLocked(_), AcquireLockResult::AlreadyLocked(_)) => 0, // Should not happen
    };
    
    assert_eq!(acquired_count, 1, "Exactly one lock acquisition should succeed");
}