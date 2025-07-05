use redis::Commands;
use testcontainers::clients::Cli;
use testcontainers_modules::redis::Redis;
use twmq::redis::aio::ConnectionManager;
use engine_executors::transaction_registry::{TransactionRegistry, TransactionRegistryError};

#[tokio::test]
async fn test_transaction_registry_new() {
    let docker = Cli::default();
    let redis_container = docker.run(Redis::default());
    let redis_url = format!("redis://127.0.0.1:{}/", redis_container.get_host_port_ipv4(6379));
    
    let client = twmq::redis::Client::open(redis_url).unwrap();
    let conn_manager = ConnectionManager::new(client).await.unwrap();
    
    let registry = TransactionRegistry::new(conn_manager.clone(), Some("test_namespace".to_string()));
    
    // Test that the registry was created successfully
    assert!(registry.registry_key().contains("test_namespace:tx_registry"));
    
    // Test with None namespace
    let registry_no_namespace = TransactionRegistry::new(conn_manager, None);
    assert_eq!(registry_no_namespace.registry_key(), "tx_registry");
}

#[tokio::test]
async fn test_set_and_get_transaction_queue() {
    let docker = Cli::default();
    let redis_container = docker.run(Redis::default());
    let redis_url = format!("redis://127.0.0.1:{}/", redis_container.get_host_port_ipv4(6379));
    
    let client = twmq::redis::Client::open(redis_url).unwrap();
    let conn_manager = ConnectionManager::new(client).await.unwrap();
    let registry = TransactionRegistry::new(conn_manager, Some("test".to_string()));
    
    let tx_id = "test_tx_123";
    let queue_name = "test_queue";
    
    // Test setting transaction queue
    registry.set_transaction_queue(tx_id, queue_name).await.unwrap();
    
    // Test getting transaction queue
    let result = registry.get_transaction_queue(tx_id).await.unwrap();
    assert_eq!(result, Some(queue_name.to_string()));
    
    // Test getting non-existent transaction
    let result = registry.get_transaction_queue("nonexistent").await.unwrap();
    assert_eq!(result, None);
}

#[tokio::test]
async fn test_remove_transaction() {
    let docker = Cli::default();
    let redis_container = docker.run(Redis::default());
    let redis_url = format!("redis://127.0.0.1:{}/", redis_container.get_host_port_ipv4(6379));
    
    let client = twmq::redis::Client::open(redis_url).unwrap();
    let conn_manager = ConnectionManager::new(client).await.unwrap();
    let registry = TransactionRegistry::new(conn_manager, Some("test".to_string()));
    
    let tx_id = "test_tx_456";
    let queue_name = "test_queue";
    
    // First set the transaction
    registry.set_transaction_queue(tx_id, queue_name).await.unwrap();
    
    // Verify it exists
    let result = registry.get_transaction_queue(tx_id).await.unwrap();
    assert_eq!(result, Some(queue_name.to_string()));
    
    // Remove the transaction
    registry.remove_transaction(tx_id).await.unwrap();
    
    // Verify it's gone
    let result = registry.get_transaction_queue(tx_id).await.unwrap();
    assert_eq!(result, None);
}

#[tokio::test]
async fn test_pipeline_operations() {
    let docker = Cli::default();
    let redis_container = docker.run(Redis::default());
    let redis_url = format!("redis://127.0.0.1:{}/", redis_container.get_host_port_ipv4(6379));
    
    let client = twmq::redis::Client::open(redis_url).unwrap();
    let conn_manager = ConnectionManager::new(client).await.unwrap();
    let registry = TransactionRegistry::new(conn_manager.clone(), Some("test".to_string()));
    
    let tx_id = "test_tx_789";
    let queue_name = "test_queue";
    
    // Create a pipeline
    let mut pipeline = twmq::redis::Pipeline::new();
    
    // Add set command to pipeline
    registry.add_set_command(&mut pipeline, tx_id, queue_name);
    
    // Execute the pipeline
    let mut conn = conn_manager.clone();
    pipeline.execute_async(&mut conn).await.unwrap();
    
    // Verify the transaction was set
    let result = registry.get_transaction_queue(tx_id).await.unwrap();
    assert_eq!(result, Some(queue_name.to_string()));
    
    // Create another pipeline to remove
    let mut pipeline = twmq::redis::Pipeline::new();
    registry.add_remove_command(&mut pipeline, tx_id);
    
    // Execute the pipeline
    pipeline.execute_async(&mut conn).await.unwrap();
    
    // Verify the transaction was removed
    let result = registry.get_transaction_queue(tx_id).await.unwrap();
    assert_eq!(result, None);
}

#[tokio::test]
async fn test_multiple_transactions() {
    let docker = Cli::default();
    let redis_container = docker.run(Redis::default());
    let redis_url = format!("redis://127.0.0.1:{}/", redis_container.get_host_port_ipv4(6379));
    
    let client = twmq::redis::Client::open(redis_url).unwrap();
    let conn_manager = ConnectionManager::new(client).await.unwrap();
    let registry = TransactionRegistry::new(conn_manager, Some("test".to_string()));
    
    let transactions = vec![
        ("tx_1", "queue_1"),
        ("tx_2", "queue_2"),
        ("tx_3", "queue_3"),
    ];
    
    // Set multiple transactions
    for (tx_id, queue_name) in &transactions {
        registry.set_transaction_queue(tx_id, queue_name).await.unwrap();
    }
    
    // Verify all transactions exist
    for (tx_id, queue_name) in &transactions {
        let result = registry.get_transaction_queue(tx_id).await.unwrap();
        assert_eq!(result, Some(queue_name.to_string()));
    }
    
    // Remove one transaction
    registry.remove_transaction("tx_2").await.unwrap();
    
    // Verify only tx_2 was removed
    assert_eq!(registry.get_transaction_queue("tx_1").await.unwrap(), Some("queue_1".to_string()));
    assert_eq!(registry.get_transaction_queue("tx_2").await.unwrap(), None);
    assert_eq!(registry.get_transaction_queue("tx_3").await.unwrap(), Some("queue_3".to_string()));
}

#[tokio::test]
async fn test_error_handling() {
    // Test with invalid Redis connection
    let client = twmq::redis::Client::open("redis://invalid:6379/").unwrap();
    let conn_manager = ConnectionManager::new(client).await;
    
    // This should fail to connect
    assert!(conn_manager.is_err());
}

#[tokio::test]
async fn test_namespace_isolation() {
    let docker = Cli::default();
    let redis_container = docker.run(Redis::default());
    let redis_url = format!("redis://127.0.0.1:{}/", redis_container.get_host_port_ipv4(6379));
    
    let client = twmq::redis::Client::open(redis_url).unwrap();
    let conn_manager = ConnectionManager::new(client).await.unwrap();
    
    let registry1 = TransactionRegistry::new(conn_manager.clone(), Some("namespace1".to_string()));
    let registry2 = TransactionRegistry::new(conn_manager, Some("namespace2".to_string()));
    
    let tx_id = "shared_tx_id";
    let queue_name1 = "queue_1";
    let queue_name2 = "queue_2";
    
    // Set the same transaction ID in both namespaces
    registry1.set_transaction_queue(tx_id, queue_name1).await.unwrap();
    registry2.set_transaction_queue(tx_id, queue_name2).await.unwrap();
    
    // Verify they are isolated
    let result1 = registry1.get_transaction_queue(tx_id).await.unwrap();
    let result2 = registry2.get_transaction_queue(tx_id).await.unwrap();
    
    assert_eq!(result1, Some(queue_name1.to_string()));
    assert_eq!(result2, Some(queue_name2.to_string()));
    
    // Remove from one namespace
    registry1.remove_transaction(tx_id).await.unwrap();
    
    // Verify only removed from one namespace
    assert_eq!(registry1.get_transaction_queue(tx_id).await.unwrap(), None);
    assert_eq!(registry2.get_transaction_queue(tx_id).await.unwrap(), Some(queue_name2.to_string()));
}