use crate::error::TwmqError;
use std::sync::Arc;

/// Handle for a single worker that can be shut down gracefully
pub struct WorkerHandle<H: crate::DurableExecution> {
    pub join_handle: tokio::task::JoinHandle<Result<(), TwmqError>>,
    pub shutdown_tx: tokio::sync::oneshot::Sender<()>,
    pub queue: Arc<crate::Queue<H>>,
}

impl<H: crate::DurableExecution> WorkerHandle<H> {
    /// Shutdown this worker gracefully
    pub async fn shutdown(self) -> Result<(), TwmqError> {
        tracing::info!(
            "Initiating graceful shutdown of worker for queue: {}",
            self.queue.name()
        );

        // Signal shutdown to the worker
        if self.shutdown_tx.send(()).is_err() {
            tracing::warn!(
                "Worker for queue {} was already shutting down",
                self.queue.name()
            );
        }

        // Wait for worker to finish
        match self.join_handle.await {
            Ok(Ok(())) => {
                tracing::info!(
                    "Worker for queue {} shut down gracefully",
                    self.queue.name()
                );
                Ok(())
            }
            Ok(Err(e)) => {
                tracing::error!(
                    "Worker for queue {} shut down with error: {:?}",
                    self.queue.name(),
                    e
                );
                Err(e)
            }
            Err(e) => {
                tracing::error!(
                    "Worker task for queue {} panicked during shutdown: {:?}",
                    self.queue.name(),
                    e
                );
                Err(TwmqError::Runtime { message: format!("Worker panic: {}", e) })
            }
        }
    }
}

/// Handle for coordinating shutdown of multiple workers
pub struct ShutdownHandle {
    join_handles: Vec<tokio::task::JoinHandle<Result<(), TwmqError>>>,
    shutdown_txs: Vec<tokio::sync::oneshot::Sender<()>>,
}

impl ShutdownHandle {
    /// Create a new empty shutdown handle
    pub fn new() -> Self {
        Self {
            join_handles: Vec::new(),
            shutdown_txs: Vec::new(),
        }
    }

    /// Add a worker to be managed by this shutdown handle
    pub fn add_worker<H: crate::DurableExecution>(&mut self, worker: WorkerHandle<H>) {
        self.join_handles.push(worker.join_handle);
        self.shutdown_txs.push(worker.shutdown_tx);
    }

    /// Shutdown all workers gracefully
    pub async fn shutdown(self) -> Result<(), TwmqError> {
        let worker_count = self.join_handles.len();
        tracing::info!("Initiating graceful shutdown of {} workers", worker_count);

        // Send shutdown signals to all workers first
        for shutdown_tx in self.shutdown_txs {
            if shutdown_tx.send(()).is_err() {
                tracing::warn!("A worker was already shutting down");
            }
        }

        // Wait for all workers to complete

        let results = futures::future::join_all(self.join_handles).await;
        let mut errors = Vec::new();

        for (i, result) in results.into_iter().enumerate() {
            match result {
                Ok(Ok(())) => {
                    tracing::debug!("Worker {} shut down gracefully", i);
                }
                Ok(Err(e)) => {
                    tracing::error!("Worker {} shut down with error: {:?}", i, e);
                    errors.push(e);
                }
                Err(e) => {
                    let runtime_error = TwmqError::Runtime { message: format!("Worker {} panic: {}", i, e) };
                    tracing::error!("Worker {} task panicked during shutdown: {:?}", i, e);
                    errors.push(runtime_error);
                }
            }
        }

        if errors.is_empty() {
            tracing::info!("All {} workers shut down gracefully", worker_count);
            Ok(())
        } else {
            tracing::error!("{} workers had errors during shutdown", errors.len());
            // Return the first error, but all errors have been logged
            Err(errors.into_iter().next().unwrap())
        }
    }

    /// Get the number of workers managed by this handle
    pub fn worker_count(&self) -> usize {
        self.join_handles.len()
    }
}

impl Default for ShutdownHandle {
    fn default() -> Self {
        Self::new()
    }
}

// Convenience methods to make collecting workers easier
impl ShutdownHandle {
    /// Create a new shutdown handle with a single worker
    pub fn with_worker<H: crate::DurableExecution>(worker: WorkerHandle<H>) -> Self {
        let mut handle = Self::new();
        handle.add_worker(worker);
        handle
    }

    /// Add multiple workers at once
    pub fn add_workers<H: crate::DurableExecution>(
        &mut self,
        workers: impl IntoIterator<Item = WorkerHandle<H>>,
    ) {
        for worker in workers {
            self.add_worker(worker);
        }
    }

    /// Builder-style method to add a worker
    pub fn and_worker<H: crate::DurableExecution>(mut self, worker: WorkerHandle<H>) -> Self {
        self.add_worker(worker);
        self
    }
}
