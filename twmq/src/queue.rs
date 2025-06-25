use std::{marker::PhantomData, sync::Arc, time::Duration};

use redis::{Client, aio::ConnectionManager};

use crate::{DurableExecution, Queue, error::TwmqError};

#[derive(Clone, Debug)]
pub struct QueueOptions {
    pub max_success: usize,
    pub max_failed: usize,
    pub lease_duration: Duration,
    pub local_concurrency: usize,

    pub polling_interval: Duration,

    /// If true, always poll for jobs even if there are no available permits
    /// This is important, because polling is how delayed and timed out jobs are handled
    /// If you have a horiztonally scaled deployment, this can be set to the default of false
    /// But if there's only one node, you can set this to true to avoid the local concurrency from blocking queue housekeeping
    pub always_poll: bool,
}

impl Default for QueueOptions {
    fn default() -> Self {
        Self {
            max_success: 1000,
            max_failed: 10000,
            local_concurrency: 100,
            polling_interval: Duration::from_millis(100),
            lease_duration: Duration::from_secs(30),
            always_poll: false,
        }
    }
}

// Typestate markers for tracking builder state
pub struct NoRedis;
pub struct HasRedis;
pub struct NoName;
pub struct HasName;
pub struct NoHandler;
pub struct HasHandler;

enum RedisSource {
    Url(String),
    Client(Client),
    ConnectionManager(ConnectionManager),
}

// Builder with typestate pattern
pub struct QueueBuilder<H, R, N, Hn>
where
    H: DurableExecution,
{
    redis_source: Option<RedisSource>,
    name: Option<String>,
    options: Option<QueueOptions>,
    handler: Option<H>,
    _phantom: PhantomData<(R, N, Hn)>,
}

impl<H: DurableExecution> Default for QueueBuilder<H, NoRedis, NoName, NoHandler> {
    fn default() -> Self {
        Self::new()
    }
}

impl<H: DurableExecution> QueueBuilder<H, NoRedis, NoName, NoHandler> {
    /// Create a new queue builder
    pub fn new() -> Self {
        Self {
            redis_source: None,
            name: None,
            options: None,
            handler: None,
            _phantom: PhantomData,
        }
    }
}

// Redis configuration methods
impl<H: DurableExecution, N, Hn> QueueBuilder<H, NoRedis, N, Hn> {
    /// Set Redis connection from URL
    pub fn redis_url<S: Into<String>>(self, url: S) -> QueueBuilder<H, HasRedis, N, Hn> {
        QueueBuilder {
            redis_source: Some(RedisSource::Url(url.into())),
            name: self.name,
            options: self.options,
            handler: self.handler,
            _phantom: PhantomData,
        }
    }

    /// Set Redis connection from existing client
    pub fn redis_client(self, client: Client) -> QueueBuilder<H, HasRedis, N, Hn> {
        QueueBuilder {
            redis_source: Some(RedisSource::Client(client)),
            name: self.name,
            options: self.options,
            handler: self.handler,
            _phantom: PhantomData,
        }
    }

    /// Set Redis connection from existing connection manager
    pub fn redis_connection_manager(
        self,
        manager: ConnectionManager,
    ) -> QueueBuilder<H, HasRedis, N, Hn> {
        QueueBuilder {
            redis_source: Some(RedisSource::ConnectionManager(manager)),
            name: self.name,
            options: self.options,
            handler: self.handler,
            _phantom: PhantomData,
        }
    }
}

// Name configuration methods
impl<H: DurableExecution, R, Hn> QueueBuilder<H, R, NoName, Hn> {
    /// Set queue name
    pub fn name<S: Into<String>>(self, name: S) -> QueueBuilder<H, R, HasName, Hn> {
        QueueBuilder {
            redis_source: self.redis_source,
            name: Some(name.into()),
            options: self.options,
            handler: self.handler,
            _phantom: PhantomData,
        }
    }
}

// Handler configuration methods
impl<H: DurableExecution, R, N> QueueBuilder<H, R, N, NoHandler> {
    /// Set the handler
    pub fn handler(self, handler: H) -> QueueBuilder<H, R, N, HasHandler> {
        QueueBuilder {
            redis_source: self.redis_source,
            name: self.name,
            options: self.options,
            handler: Some(handler),
            _phantom: PhantomData,
        }
    }
}

// Options can be set at any time (it's optional)
impl<H: DurableExecution, R, N, Hn> QueueBuilder<H, R, N, Hn> {
    /// Set queue options (optional)
    pub fn options(self, options: QueueOptions) -> Self {
        Self {
            redis_source: self.redis_source,
            name: self.name,
            options: Some(options),
            handler: self.handler,
            _phantom: PhantomData,
        }
    }
}

// Build method is only available when all required fields are set
impl<H: DurableExecution> QueueBuilder<H, HasRedis, HasName, HasHandler> {
    /// Build the Queue (only available when all required fields are set)
    pub async fn build(self) -> Result<Queue<H>, TwmqError> {
        // We can safely unwrap because the typestate guarantees these are Some
        let redis_source = self.redis_source.unwrap();
        let name = self.name.unwrap();
        let handler = self.handler.unwrap();

        let redis = match redis_source {
            RedisSource::Url(url) => {
                let client = Client::open(url)?;
                client.get_connection_manager().await?
            }
            RedisSource::Client(client) => client.get_connection_manager().await?,
            RedisSource::ConnectionManager(manager) => manager,
        };

        Ok(Queue {
            redis,
            name,
            options: self.options.unwrap_or_default(),
            handler: Arc::new(handler),
        })
    }
}

impl<H: DurableExecution> Queue<H> {
    pub fn builder() -> QueueBuilder<H, NoRedis, NoName, NoHandler> {
        QueueBuilder::new()
    }
}
