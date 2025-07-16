use std::collections::HashMap;

use crate::eoa::{
    EoaExecutorStore, EoaTransactionRequest,
    store::{
        BorrowedTransaction, BorrowedTransactionData, NO_OP_TRANSACTION_ID,
        SubmittedNoopTransaction, SubmittedTransaction, SubmittedTransactionHydrated,
        TransactionStoreError, submitted::SubmittedTransactionDehydrated,
    },
};

pub trait Dehydrated<R> {
    type Hydrated;

    fn transaction_id(&self) -> &str;

    fn hydrate(self, required_data: R) -> Self::Hydrated;
}

#[derive(Debug, Clone)]
pub enum SubmittedTransactionHydrator {
    Noop,
    Real(EoaTransactionRequest),
}

impl Dehydrated<SubmittedTransactionHydrator> for SubmittedTransactionDehydrated {
    type Hydrated = SubmittedTransactionHydrated;

    fn transaction_id(&self) -> &str {
        &self.transaction_id
    }

    fn hydrate(self, required_data: SubmittedTransactionHydrator) -> SubmittedTransactionHydrated {
        match required_data {
            SubmittedTransactionHydrator::Noop => {
                SubmittedTransactionHydrated::Noop(SubmittedNoopTransaction {
                    nonce: self.nonce,
                    hash: self.hash,
                })
            }
            SubmittedTransactionHydrator::Real(request) => {
                SubmittedTransactionHydrated::Real(SubmittedTransaction {
                    data: self,
                    user_request: request,
                })
            }
        }
    }
}

impl Dehydrated<EoaTransactionRequest> for BorrowedTransactionData {
    type Hydrated = BorrowedTransaction;

    fn transaction_id(&self) -> &str {
        &self.transaction_id
    }

    fn hydrate(self, required_data: EoaTransactionRequest) -> BorrowedTransaction {
        BorrowedTransaction {
            data: self,
            user_request: required_data,
        }
    }
}

impl EoaExecutorStore {
    pub async fn hydrate_all<D>(
        &self,
        dehydrated: Vec<D>,
    ) -> Result<Vec<D::Hydrated>, TransactionStoreError>
    where
        D: Dehydrated<EoaTransactionRequest>,
    {
        let mut pipe = twmq::redis::pipe();

        for d in &dehydrated {
            pipe.hget(
                self.keys.transaction_data_key_name(d.transaction_id()),
                "user_request",
            );
        }

        let results: Vec<String> = pipe.query_async(&mut self.redis.clone()).await?;

        let mut hydrated = Vec::with_capacity(dehydrated.len());
        for (d, r) in dehydrated.into_iter().zip(results.iter()) {
            hydrated.push(d.hydrate(serde_json::from_str::<EoaTransactionRequest>(r)?));
        }

        Ok(hydrated)
    }

    pub async fn hydrate<D>(&self, dehydrated: D) -> Result<D::Hydrated, TransactionStoreError>
    where
        D: Dehydrated<EoaTransactionRequest>,
    {
        let mut pipe = twmq::redis::pipe();
        pipe.hget(
            self.keys
                .transaction_data_key_name(dehydrated.transaction_id()),
            "user_request",
        );
        let result: String = pipe.query_async(&mut self.redis.clone()).await?;
        Ok(dehydrated.hydrate(serde_json::from_str::<EoaTransactionRequest>(&result)?))
    }

    pub async fn hydrate_all_submitted<D>(
        &self,
        dehydrated: Vec<D>,
    ) -> Result<Vec<D::Hydrated>, TransactionStoreError>
    where
        D: Dehydrated<SubmittedTransactionHydrator>,
    {
        let mut pipe = twmq::redis::pipe();
        for d in &dehydrated {
            if d.transaction_id() == NO_OP_TRANSACTION_ID {
                continue;
            }

            pipe.hget(
                self.keys.transaction_data_key_name(d.transaction_id()),
                "user_request",
            );
        }

        let results: Vec<String> = pipe.query_async(&mut self.redis.clone()).await?;

        let id_to_eoa_request = results
            .into_iter()
            .map(|r| {
                let request = serde_json::from_str::<EoaTransactionRequest>(&r)?;
                Ok((request.transaction_id.clone(), request))
            })
            .collect::<Result<HashMap<String, EoaTransactionRequest>, TransactionStoreError>>()?;

        let mut hydrated = Vec::with_capacity(dehydrated.len());

        for d in dehydrated {
            let id = d.transaction_id();
            if id == NO_OP_TRANSACTION_ID {
                hydrated.push(d.hydrate(SubmittedTransactionHydrator::Noop));
                continue;
            }

            let request =
                id_to_eoa_request
                    .get(id)
                    .ok_or(TransactionStoreError::TransactionNotFound {
                        transaction_id: id.to_string(),
                    })?;

            hydrated.push(d.hydrate(SubmittedTransactionHydrator::Real(request.clone())));
        }

        Ok(hydrated)
    }
}
