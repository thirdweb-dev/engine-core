# Solana Program Interaction Endpoint - Technical Specification

## Overview

This document specifies the design and implementation plan for a new Solana program interaction endpoint (`/solana/program`) that provides high-level program invocation similar to how the EVM `contract_write.rs` endpoint works for smart contracts.

## Current State Analysis

### Existing Solana Infrastructure

1. **Current Transaction Flow**:
   - Endpoint: `/solana/transaction` 
   - Takes fully resolved `SolanaInstructionData[]` with:
     - `program_id`: Pubkey
     - `accounts`: Array of `SolanaAccountMeta` (pubkey, is_signer, is_writable)
     - `data`: Hex or Base64 encoded instruction data
     - `encoding`: Hex or Base64
   - Users must manually construct instruction data and account lists

2. **Execution Pipeline**:
   ```
   HTTP Request -> SendSolanaTransactionRequest -> ExecutionRouter.execute_solana() 
   -> SolanaExecutorJobData -> Queue -> SolanaExecutorWorker
   ```

3. **Key Components**:
   - `solana-core/`: Transaction types (`SolanaInstructionData`, `SolanaTransaction`)
   - `core/execution_options/solana.rs`: Request/response types
   - `server/routes/solana_transaction.rs`: HTTP handler
   - `executors/solana_executor/`: Worker that builds, signs, sends, confirms

### EVM Contract Write Pattern

1. **User Experience**:
   - Takes high-level parameters: `contract_address`, `method`, `params[]`, optional `abi`
   - Resolves function signature from ABI (fetched or provided)
   - Encodes parameters using type system
   - Converts to `InnerTransaction` with encoded call data
   - Handles multiple calls in one request

2. **Key Flow**:
   ```
   ContractWrite -> ContractCall.prepare_call() -> PreparedContractCall 
   -> InnerTransaction -> SendTransactionRequest -> Executor
   ```

3. **ABI Resolution**:
   - Fetches from thirdweb's contract verification service
   - Supports function overloading
   - Type-safe parameter encoding from JSON

## New Endpoint Design

### API Surface

```
POST /solana/program
```

#### Request Structure

```json
{
  "executionOptions": {
    "chainId": "solana:mainnet",
    "signerAddress": "9vNYXEehFV8V1jxzjH7Sv3BBtsYZ92HPKYP1stgNGHJE",
    "priorityFee": { "type": "auto" }
  },
  "instructions": [{
    "program": "675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8",
    "instruction": "swapBaseIn",
    "accounts": {
      "poolId": "58oQChx4yWmvKdwLLZzBi4ChoCc2fqCUWBkwMihLYQo2",
      "inputMint": "So11111111111111111111111111111111111111112",
      "outputMint": "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"
    },
    "args": {
      "amountIn": "1000000000",
      "minimumAmountOut": "150000000"
    },
    "idl": { /* optional IDL JSON */ }
  }],
  "webhookOptions": []
}
```

#### Response Structure

```json
{
  "result": {
    "transactionId": "uuid-v4",
    "chainId": "solana:mainnet",
    "signerAddress": "9vNYXEehFV8V1jxzjH7Sv3BBtsYZ92HPKYP1stgNGHJE"
  }
}
```

## Architecture Components

### 1. `solana-core/` Extension

New modules to add:

#### `solana-core/src/program.rs`

**Purpose**: High-level program call abstraction and instruction building

```rust
pub struct ProgramCall {
    pub program: Pubkey,
    pub instruction: String,
    pub accounts: HashMap<String, String>, // account name -> pubkey string
    pub args: HashMap<String, serde_json::Value>,
    pub idl: Option<Idl>,
}

pub struct PreparedProgramCall {
    pub instruction_data: SolanaInstructionData,
    pub resolved_accounts: Vec<ResolvedAccount>,
    pub instruction_name: String,
}

pub struct ResolvedAccount {
    pub name: String,
    pub pubkey: Pubkey,
    pub is_signer: bool,
    pub is_writable: bool,
    pub source: AccountSource, // Provided, Derived, System
}

pub enum AccountSource {
    Provided,      // User-supplied in request
    Derived,       // Derived from PDA or other accounts
    System,        // System programs (sysvar, etc)
}
```

#### `solana-core/src/idl.rs`

**Purpose**: IDL parsing and instruction resolution

```rust
pub struct Idl {
    pub instructions: Vec<IdlInstruction>,
    pub accounts: Vec<IdlAccount>,
    pub types: Vec<IdlType>,
}

pub struct IdlInstruction {
    pub name: String,
    pub accounts: Vec<IdlAccountConstraint>,
    pub args: Vec<IdlField>,
    pub discriminator: Option<Vec<u8>>,
}

pub struct IdlAccountConstraint {
    pub name: String,
    pub is_mut: bool,
    pub is_signer: bool,
    pub pda: Option<PdaSeeds>,
    pub relations: Vec<String>, // e.g., "authority" field points to signer
}

pub struct PdaSeeds {
    pub seeds: Vec<Seed>,
    pub program_id: Option<Pubkey>,
}

pub enum Seed {
    Constant(Vec<u8>),
    AccountKey { account: String },
    AccountField { account: String, field: String },
}
```

**Key Functions**:
- `parse_idl(json: &str) -> Result<Idl>`
- `find_instruction(&self, name: &str) -> Result<&IdlInstruction>`
- `derive_pda(seeds: &PdaSeeds, program_id: &Pubkey) -> Result<Pubkey>`

#### `solana-core/src/instruction_builder.rs`

**Purpose**: Encode instruction data from args

```rust
pub struct InstructionEncoder;

impl InstructionEncoder {
    /// Encode instruction data from JSON args using Borsh
    pub fn encode_instruction(
        instruction_name: &str,
        discriminator: Option<&[u8]>,
        args: &HashMap<String, serde_json::Value>,
        arg_schema: &[IdlField],
    ) -> Result<Vec<u8>>;
    
    /// Convert JSON value to Borsh-serializable type
    fn json_to_borsh_value(
        value: &serde_json::Value,
        type_def: &IdlType,
    ) -> Result<Vec<u8>>;
}
```

#### `solana-core/src/account_resolver.rs`

**Purpose**: Resolve accounts from constraints and user input

```rust
pub struct AccountResolver;

impl AccountResolver {
    /// Resolve all accounts for an instruction
    pub async fn resolve_accounts(
        instruction: &IdlInstruction,
        provided_accounts: &HashMap<String, String>,
        signer: &Pubkey,
        program_id: &Pubkey,
    ) -> Result<Vec<ResolvedAccount>>;
    
    /// Derive PDA account
    fn derive_pda_account(
        constraint: &IdlAccountConstraint,
        provided_accounts: &HashMap<String, String>,
        program_id: &Pubkey,
    ) -> Result<ResolvedAccount>;
    
    /// Inject system accounts (rent, clock, etc)
    fn inject_system_accounts(
        name: &str,
    ) -> Option<ResolvedAccount>;
}
```

#### `solana-core/src/builtin_programs.rs`

**Purpose**: Built-in knowledge of common programs

```rust
pub enum WellKnownProgram {
    System,
    Token,
    Token2022,
    AssociatedToken,
    Memo,
    ComputeBudget,
}

pub struct BuiltinProgramRegistry;

impl BuiltinProgramRegistry {
    /// Get program info by address or name
    pub fn get_program(identifier: &str) -> Option<ProgramInfo>;
    
    /// Get built-in IDL for known programs
    pub fn get_idl(program: WellKnownProgram) -> Idl;
    
    /// Create instruction for common operations
    pub fn build_transfer(from: Pubkey, to: Pubkey, lamports: u64) -> SolanaInstructionData;
    pub fn build_spl_transfer(from: Pubkey, to: Pubkey, amount: u64, mint: Pubkey) -> SolanaInstructionData;
    pub fn build_create_associated_token_account(payer: Pubkey, owner: Pubkey, mint: Pubkey) -> SolanaInstructionData;
}

pub struct ProgramInfo {
    pub name: String,
    pub program_id: Pubkey,
    pub well_known: Option<WellKnownProgram>,
}
```

### 2. Core Types (`core/execution_options/solana.rs`)

New request types:

```rust
#[derive(Serialize, Deserialize, Clone, Debug, utoipa::ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct SolanaProgramCall {
    /// Program address or name (e.g., "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA" or "spl-token")
    pub program: String,
    
    /// Instruction name (e.g., "transfer", "swapBaseIn")
    pub instruction: String,
    
    /// Named account mappings
    pub accounts: HashMap<String, String>,
    
    /// Instruction arguments
    pub args: HashMap<String, serde_json::Value>,
    
    /// Optional IDL for custom programs
    #[serde(skip_serializing_if = "Option::is_none")]
    pub idl: Option<serde_json::Value>,
}

#[derive(Serialize, Deserialize, Clone, Debug, utoipa::ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct SendSolanaProgramRequest {
    /// Idempotency key
    #[serde(default = "super::default_idempotency_key")]
    pub idempotency_key: String,
    
    /// List of program calls to execute
    pub params: Vec<SolanaProgramCall>,
    
    /// Solana execution options
    pub execution_options: SolanaExecutionOptions,
    
    /// Webhook options
    #[serde(default)]
    pub webhook_options: Vec<super::WebhookOptions>,
}
```

### 3. Server Route (`server/src/http/routes/solana_program.rs`)

New endpoint handler:

```rust
#[utoipa::path(
    post,
    operation_id = "sendSolanaProgram",
    path = "/solana/program",
    tag = "Solana",
    // ...
)]
pub async fn send_solana_program(
    State(state): State<EngineServerState>,
    SigningCredentialsExtractor(signing_credential): SigningCredentialsExtractor,
    EngineJson(request): EngineJson<SendSolanaProgramRequest>,
) -> Result<impl IntoResponse, ApiEngineError> {
    // 1. Prepare all program calls in parallel (like contract_write does)
    let prepare_futures = request.params.iter().map(|program_call| {
        program_call.prepare_call(state.signer_pubkey, &state.idl_cache)
    });
    
    let preparation_results = join_all(prepare_futures).await;
    
    // 2. Collect successful preparations and errors
    let mut instructions = Vec::new();
    let mut preparation_errors = Vec::new();
    
    for (index, prep_result) in preparation_results.iter().enumerate() {
        match prep_result {
            Ok(prepared) => instructions.push(prepared.instruction_data.clone()),
            Err(e) => preparation_errors.push((index, e.to_string())),
        }
    }
    
    // 3. Return errors if any
    if !preparation_errors.is_empty() {
        return Err(/* validation error with details */);
    }
    
    // 4. Create SendSolanaTransactionRequest
    let transaction_request = SendSolanaTransactionRequest {
        idempotency_key: request.idempotency_key,
        instructions,
        execution_options: request.execution_options,
        webhook_options: request.webhook_options,
    };
    
    // 5. Execute via existing solana pipeline
    let response = state
        .execution_router
        .execute_solana(transaction_request, signing_credential)
        .await?;
    
    Ok((StatusCode::ACCEPTED, Json(SuccessResponse::new(response))))
}
```

### 4. IDL Management

#### IDL Cache Service

```rust
pub struct IdlCache {
    cache: Arc<RwLock<HashMap<Pubkey, Idl>>>,
}

impl IdlCache {
    /// Get IDL from cache or fetch
    pub async fn get_idl(&self, program_id: &Pubkey) -> Result<Idl>;
    
    /// Fetch from on-chain IDL account (Anchor standard)
    async fn fetch_from_chain(program_id: &Pubkey, rpc_url: &str) -> Result<Idl>;
    
    /// Fetch from thirdweb IDL registry
    async fn fetch_from_registry(program_id: &Pubkey) -> Result<Idl>;
}
```

## Implementation Phases

### Phase 1: Core Infrastructure (Foundation)

**Goal**: Build the foundational types and basic instruction encoding

**Tasks**:
1. Create `solana-core/src/program.rs` with core types
2. Create `solana-core/src/idl.rs` with IDL types and parsing
3. Create `solana-core/src/instruction_builder.rs` for Borsh encoding
4. Add dependencies: `borsh`, `anchor-lang-idl` (or custom IDL parser)
5. Write unit tests for instruction encoding

**Deliverable**: Can parse IDL and encode instruction data from JSON args

### Phase 2: Account Resolution (Core Logic)

**Goal**: Implement account derivation and resolution

**Tasks**:
1. Create `solana-core/src/account_resolver.rs`
2. Implement PDA derivation from seeds
3. Implement account relationship resolution
4. Handle system accounts (rent, clock, etc)
5. Write integration tests

**Deliverable**: Can resolve accounts from IDL constraints and user input

### Phase 3: Built-in Programs (UX Enhancement)

**Goal**: Add support for common programs without IDL

**Tasks**:
1. Create `solana-core/src/builtin_programs.rs`
2. Define well-known program IDs (System, Token, Token-2022, etc)
3. Implement instruction builders for common operations:
   - System: transfer, create_account, allocate
   - SPL Token: transfer, approve, mint, burn, create_account
   - Associated Token: create, get_address
   - Memo: create memo
4. Add name-to-program-id mapping
5. Write tests for each built-in instruction

**Deliverable**: Can invoke system and SPL programs by name without IDL

### Phase 4: HTTP Endpoint (API Layer)

**Goal**: Expose the functionality via HTTP

**Tasks**:
1. Add types to `core/execution_options/solana.rs`
2. Create `server/src/http/routes/solana_program.rs`
3. Implement preparation pipeline with parallel calls
4. Add error handling and validation
5. Add OpenAPI documentation
6. Add route to router

**Deliverable**: Working `/solana/program` endpoint

### Phase 5: IDL Management (Advanced)

**Goal**: Automatic IDL fetching and caching

**Tasks**:
1. Create IDL cache service
2. Implement on-chain IDL fetching (Anchor standard)
3. Implement fallback to IDL registry
4. Add TTL-based cache invalidation
5. Add metrics for cache hits/misses

**Deliverable**: Programs can be called without providing IDL

### Phase 6: Advanced Features (Polish)

**Goal**: Handle edge cases and optimize

**Tasks**:
1. Support for:
   - Address lookup tables
   - Multiple instruction invocations in one transaction
   - Partial account derivation (user provides some, we derive others)
   - Custom serialization formats beyond Borsh
2. Error messages with helpful debugging info
3. Validation of account relationships
4. Support for instruction result passing between instructions
5. Comprehensive integration tests

## Technical Decisions

### 1. IDL Format

**Decision**: Use Anchor IDL format as the standard

**Rationale**:
- Most widely adopted format in Solana ecosystem
- Well-defined schema with account constraints and PDAs
- Large library of existing IDLs available
- Can be extended for non-Anchor programs

**Alternative considered**: Custom format - rejected due to ecosystem fragmentation

### 2. Instruction Data Encoding

**Decision**: Use Borsh as primary serialization format

**Rationale**:
- Standard in Solana/Anchor ecosystem
- Deterministic and space-efficient
- Well-supported Rust libraries
- Matches what Anchor uses for discriminators

**Fallback**: Allow custom encoding for special cases (pass through hex/base64)

### 3. Account Resolution Strategy

**Decision**: Hybrid approach - derive what we can, require rest from user

**Approach**:
1. System accounts (rent, clock) - auto-inject
2. PDA accounts with full seed info - auto-derive
3. PDAs with partial seeds - derive from user input
4. Regular accounts - require in `accounts` map
5. Program IDs - auto-inject

**Error Handling**: If we can't derive an account, fail with clear message about what's needed

### 4. Built-in vs IDL-based

**Decision**: Built-in for common programs, IDL for everything else

**Built-in programs**:
- System Program
- SPL Token & Token-2022
- Associated Token Program
- SPL Memo
- Compute Budget

**Rationale**:
- Better UX for common operations
- No IDL lookup overhead
- Type-safe builders with validation
- Can add sugar like automatic ATA creation

### 5. Parallel Processing

**Decision**: Follow EVM pattern - prepare all calls in parallel

**Rationale**:
- Better performance for multi-instruction requests
- Consistent with existing patterns
- Failed preparations don't block successful ones
- Clear error reporting per instruction

### 6. Dependency Management

**New Dependencies Required**:
```toml
[dependencies]
borsh = "1.5"
anchor-lang = { version = "0.30", optional = true, default-features = false }
anchor-spl = { version = "0.30", optional = true }
spl-token = { workspace = true }
spl-associated-token-account = { workspace = true }
```

## Error Handling

### Error Types

```rust
#[derive(Debug, thiserror::Error)]
pub enum SolanaProgramError {
    #[error("Program not found: {identifier}")]
    ProgramNotFound { identifier: String },
    
    #[error("Instruction not found: {instruction} in program {program}")]
    InstructionNotFound { program: String, instruction: String },
    
    #[error("Failed to parse IDL: {error}")]
    IdlParseError { error: String },
    
    #[error("Missing required account: {name}")]
    MissingAccount { name: String },
    
    #[error("Failed to derive PDA: {error}")]
    PdaDerivationError { error: String },
    
    #[error("Invalid argument '{arg}': {error}")]
    InvalidArgument { arg: String, error: String },
    
    #[error("Failed to encode instruction: {error}")]
    EncodingError { error: String },
    
    #[error("Account constraint violation: {constraint}")]
    ConstraintViolation { constraint: String },
}
```

### User-Facing Error Messages

Errors should be actionable:
```json
{
  "error": {
    "code": "MISSING_ACCOUNT",
    "message": "Missing required account: 'userTokenAccount'",
    "details": {
      "instruction": "transfer",
      "program": "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA",
      "requiredAccounts": ["source", "destination", "owner"],
      "providedAccounts": ["source", "destination"],
      "hint": "Please provide the 'userTokenAccount' address in the accounts object"
    }
  }
}
```

## Testing Strategy

### Unit Tests

1. **Instruction Encoding**:
   - Test each Borsh type encoding
   - Test discriminator prepending
   - Test complex nested types

2. **PDA Derivation**:
   - Test constant seeds
   - Test account-based seeds
   - Test bump seed finding

3. **Built-in Programs**:
   - Verify instruction data matches SDK
   - Verify account ordering
   - Test all variants

### Integration Tests

1. **End-to-End Program Calls**:
   - SPL Token transfer
   - Create associated token account
   - System program transfer
   - Custom program with IDL

2. **Account Resolution**:
   - Auto-derived PDA accounts
   - Mixed provided + derived
   - System account injection

3. **Error Cases**:
   - Missing accounts
   - Invalid arguments
   - Malformed IDL
   - Unknown program

### Performance Tests

1. Measure preparation time for:
   - Single instruction
   - 10 parallel instructions
   - Large instruction data
   - Complex PDA derivation

## Security Considerations

1. **IDL Validation**:
   - Validate IDL structure before use
   - Limit IDL size to prevent DoS
   - Sanitize account constraints

2. **Account Verification**:
   - Never auto-inject signer for non-payer accounts
   - Validate PDA derivation matches expected program
   - Warn on unusual account patterns

3. **Instruction Data**:
   - Validate argument types match schema
   - Limit instruction data size
   - Sanitize user input before encoding

4. **Rate Limiting**:
   - Apply same limits as EVM endpoints
   - Track IDL fetches separately
   - Cache aggressively to reduce RPC load

## Documentation

### API Documentation

1. **OpenAPI Spec**: Full schema with examples
2. **Usage Guide**: Step-by-step for common scenarios
3. **Error Reference**: All error codes with solutions
4. **IDL Format**: Specification for custom programs

### Code Documentation

1. **Public API**: Full rustdoc for all public types
2. **Architecture**: Module-level docs explaining flow
3. **Examples**: Code examples in docstrings
4. **Decision Records**: Why certain approaches were chosen

## Migration Path

### For Existing Users

Existing `/solana/transaction` endpoint remains unchanged. Users can:
1. Continue using low-level endpoint
2. Migrate to high-level endpoint for better UX
3. Mix both in same application

### For New Users

Recommend:
1. Use `/solana/program` for known programs
2. Fall back to `/solana/transaction` for edge cases
3. Provide IDL for custom programs

## Success Metrics

1. **Adoption**: % of Solana transactions using new endpoint
2. **Error Rate**: Should be < 5% preparation errors
3. **Performance**: <100ms preparation time for simple instructions
4. **Coverage**: Support top 20 Solana programs by TVL

## Open Questions

1. **IDL Registry**: Should we build our own or use existing?
2. **Custom Serialization**: How to handle non-Borsh programs?
3. **Versioning**: How to handle program upgrades?
4. **Composability**: Support for instruction result passing?
5. **Simulation**: Should we simulate before queueing?

## Appendix: Example Usage

### Example 1: SPL Token Transfer

```json
{
  "executionOptions": {
    "chainId": "solana:mainnet",
    "signerAddress": "9vNYXEehFV8V1jxzjH7Sv3BBtsYZ92HPKYP1stgNGHJE"
  },
  "instructions": [{
    "program": "spl-token",
    "instruction": "transfer",
    "accounts": {
      "source": "ABC...123",
      "destination": "DEF...456",
      "owner": "9vNYXEehFV8V1jxzjH7Sv3BBtsYZ92HPKYP1stgNGHJE"
    },
    "args": {
      "amount": "1000000"
    }
  }]
}
```

### Example 2: Raydium Swap with IDL

```json
{
  "executionOptions": {
    "chainId": "solana:mainnet",
    "signerAddress": "9vNYXEehFV8V1jxzjH7Sv3BBtsYZ92HPKYP1stgNGHJE",
    "priorityFee": { "type": "percentile", "percentile": 75 }
  },
  "instructions": [{
    "program": "675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8",
    "instruction": "swapBaseIn",
    "accounts": {
      "poolId": "58oQChx4yWmvKdwLLZzBi4ChoCc2fqCUWBkwMihLYQo2",
      "inputMint": "So11111111111111111111111111111111111111112",
      "outputMint": "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v",
      "userSourceTokenAccount": "...",
      "userDestinationTokenAccount": "..."
    },
    "args": {
      "amountIn": "1000000000",
      "minimumAmountOut": "150000000"
    },
    "idl": { /* Raydium IDL */ }
  }]
}
```

### Example 3: Multiple Instructions (Swap with ATA Creation)

```json
{
  "executionOptions": {
    "chainId": "solana:mainnet",
    "signerAddress": "9vNYXEehFV8V1jxzjH7Sv3BBtsYZ92HPKYP1stgNGHJE"
  },
  "instructions": [
    {
      "program": "associated-token",
      "instruction": "create",
      "accounts": {
        "mint": "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v",
        "owner": "9vNYXEehFV8V1jxzjH7Sv3BBtsYZ92HPKYP1stgNGHJE"
      },
      "args": {}
    },
    {
      "program": "raydium",
      "instruction": "swap",
      "accounts": { /* ... */ },
      "args": { /* ... */ }
    }
  ]
}
```

## Summary

This specification outlines a comprehensive approach to building a high-level Solana program interaction endpoint that:

1. **Mirrors EVM UX**: Provides same developer experience as contract_write
2. **Leverages IDL**: Uses Anchor IDL for type-safe instruction building
3. **Handles Common Cases**: Built-in support for System & SPL programs
4. **Extensible**: Supports custom programs via IDL
5. **Production-Ready**: Proper error handling, caching, and validation
6. **Performant**: Parallel preparation, caching, minimal RPC calls

The implementation follows existing patterns in the codebase (especially contract_write) while adapting to Solana's unique architecture (PDAs, Borsh encoding, account model).

