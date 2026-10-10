# Project — Claude Code Guidelines (Template)

A reusable template describing the architecture and engineering conventions Claude Code should follow when working on a new project. Copy this file to `CLAUDE.md` (or extend an existing one) and customize the project-specific sections.

---

## Project Overview

> **Customize this paragraph for your project.** Describe the runtime, the architectural style, and what the codebase emphasizes.

This project follows **Hexagonal Architecture (Ports & Adapters)** with Domain-Driven Design principles. The codebase emphasizes:

- **Clean separation of concerns** across Domain, Application, and Infrastructure layers
- **Testability** through dependency inversion and fake/integration implementations
- **Maintainability** through explicit naming and behavior-driven design
- **Scalability** through domain-driven structure

---

## Core Architecture Principles

### The Four Pillars

**YOU MUST** follow these principles when working with this codebase:

1. **Dependency Rule**: Dependencies point inward. Domain knows nothing about infrastructure.
2. **Behavior Over Implementation**: Design around what the system does, not how it does it.
3. **Explicit Over Clever**: Code should be obvious. If it needs a comment, it might need refactoring.
4. **Test What Matters**: Protect behaviors users care about, not implementation details.

### Layer Boundaries

```
┌─────────────────────────────────────────────────────────────┐
│                     INFRASTRUCTURE                          │
│  Controllers • Repositories • External Services • Validators│
│  ┌───────────────────────────────────────────────────────┐  │
│  │                   APPLICATION                          │  │
│  │           Use Cases • Application Services            │  │
│  │  ┌─────────────────────────────────────────────────┐  │  │
│  │  │                   DOMAIN                         │  │  │
│  │  │   Entities • Value Objects • Domain Services    │  │  │
│  │  │           (Pure code — No I/O)                  │  │  │
│  │  └─────────────────────────────────────────────────┘  │  │
│  └───────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────┘

Dependencies always point INWARD →
```

**CRITICAL**: Domain layer must be runnable without a database, API, or framework.

---

## Project Structure

> **Customize this for your project layout.** The structure below illustrates a domain-versioned monorepo with a shared library and one or more application entry points. Adapt names, paths, and aliases.

```
{shared-lib}/                                # Shared library — domain logic
  src/
    domains/{domain}-v{n}/                   # e.g., order-v1/, customer-v2/
      core/                                  # Domain + application layer (no I/O)
        entity.ts                            # {Name}V{n}Entity class
        entity.spec.ts                       # Behavior-driven entity tests
        entity.schema.ts                     # JSON Schema for entity validation
        types.ts                             # DTO interfaces (I{Name}V{n}Dto)
        errors.ts                            # Domain-specific errors
        factory.ts                           # Test/data factory
        database-repository.ts               # Abstract repository port (DTOs only)
        events-repository.ts                 # Abstract events port (when applicable)
        services/{service-name}/
          index.ts                           # Pure function service
          index.spec.ts
        use-cases/{action}/                  # e.g., create/, list/, update/, delete/
          index.ts                           # {Action}{Name}V{n}UseCase class
          index.spec.ts                      # Unit / integration tests
          input-dto.schema.ts                # JSON Schema for input DTO
          types.ts                           # Input/output types
          errors.ts                          # Use case errors
          fixtures.ts                        # Test fixtures (when needed)
      adapters/                              # Infrastructure implementations
        mongodb-database-repository.ts       # extends abstract DB repo
        mongodb-events-repository.ts         # extends abstract events repo
        express-{entity}.controller.ts       # HTTP handler
        {use-case}/                          # Per-use-case adapter tests
          controller.spec.ts
          controller.e2e.spec.ts

{api-app}/                                   # Application entry point + DI composition
  src/
    app.ts                                   # Manual DI: instantiates and wires use cases
    server.ts                                # Bootstrap entry
    config.ts                                # Env-driven configuration
```

**Key conventions:**
- Domain version lives in the *domain* folder name (e.g., `order-v2`, not `order/v2`).
- Use case folders are named by action only (`create/`, `list/`) — entity is implicit from the parent domain.
- Repository ports are **abstract classes** (not interfaces), defined in `core/`.
- DI is **manual** — register new use cases by editing the composition root.

---

## Naming Conventions

**YOU MUST** follow these naming conventions strictly:

### Files and Folders

| Type | Convention | Example |
|------|------------|---------|
| Domain folders | `{name}-v{n}` (kebab-case, version on the domain) | `order-v1/`, `customer-v2/` |
| Use case folders | `{verb}` (entity is implicit from parent domain) | `create/`, `list/`, `update/`, `delete/` |
| Service folders | `kebab-case` descriptive | `convert-{source}-to-{target}/` |
| JSON schemas | `{name}.schema.ts` | `entity.schema.ts`, `input-dto.schema.ts` |
| Repository port | `database-repository.ts` (abstract class in `core/`) | `core/database-repository.ts` |
| Events repository port | `events-repository.ts` (abstract class in `core/`) | `core/events-repository.ts` |
| Repository implementation | `{tech}-database-repository.ts` (in `adapters/`) | `adapters/mongodb-database-repository.ts` |
| Controllers | `{framework}-{entity}.controller.ts` | `express-orders.controller.ts` |
| Unit tests | Same name + `.spec.ts` | `entity.spec.ts`, `index.spec.ts` |
| E2E / integration tests | Same name + `.e2e.spec.ts` | `controller.e2e.spec.ts`, `index.e2e.spec.ts` |
| Factories | `factory.ts` (inside `core/`) | `order-v1/core/factory.ts` |

### Code Elements

| Element | Convention | Example |
|---------|------------|---------|
| Entities | `{Name}V{n}Entity` | `OrderV1Entity`, `CustomerV2Entity` |
| Repository ports (abstract) | `{Name}V{n}DatabaseRepository` | `OrdersV1DatabaseRepository` |
| Repository implementations | `{Tech}{Name}V{n}DatabaseRepository` | `MongoDbOrdersV1DatabaseRepository` |
| Events repository ports | `{Name}V{n}EventsRepository` | `OrdersV1EventsRepository` |
| Use Cases | `{Verb}{Name}V{n}UseCase` | `CreateOrderV1UseCase` |
| DTOs | `I{Name}V{n}Dto` | `IOrderV1Dto` |
| Use case input/output DTOs | `I{Action}{Name}V{n}UseCaseInputDto` / `OutputDto` | `ICreateOrderV1UseCaseInputDto` |
| JSON Schemas (variables) | `{name}V{n}{Kind}JsonSchema` | `createOrderV1InputDtoJsonSchema` |
| Functions | `camelCase`, verb-noun | `calculateTotal()`, `validateInput()` |
| Constants | `UPPER_SNAKE_CASE` | `MAX_RETRY_ATTEMPTS` |

---

## Layer Responsibilities

### Domain Layer (`{domain}/core/`)

**Purpose**: Express enterprise business rules that are true regardless of delivery mechanism or persistence.

**RULES**:
- NO imports from adapters
- NO framework dependencies
- NO I/O operations (no database, HTTP, file system)
- Pure code only
- Business logic lives here

**Contains**:
- **Entities** (`entity.ts`): `{Name}V{n}Entity` class with fluent `.validate().getDto()` chain
- **Entity JSON Schema** (`entity.schema.ts`): JSON Schema Draft-07 used by entity's `.validate()`
- **Types** (`types.ts`): DTO interfaces (`I{Name}V{n}Dto`), value objects, enums
- **Errors** (`errors.ts`): Domain-specific validation and business rule errors
- **Factory** (`factory.ts`): Test/data factory using `@faker-js/faker` + `json-schema-faker`
- **Repository ports** (`database-repository.ts`, `events-repository.ts`): Abstract classes — methods accept and return DTOs

### Application Layer (`{domain}/core/use-cases/`)

**Purpose**: Orchestrate entities, services, and repositories to perform application-specific business operations.

**RULES**:
- Depends on entities, services, and repository ports from the same domain
- NO direct infrastructure access (uses abstract repository classes)
- One use case = one business operation
- Validates input DTOs inline via `schemaValidator.validateOrReject(schema, dto)`
- Coordinates entities and services — doesn't implement business rules

**Contains** (per use case folder):
- **Use case** (`index.ts`): `{Verb}{Name}V{n}UseCase` class with single `execute()` method
- **Input JSON schema** (`input-dto.schema.ts`): JSON Schema for the use case input DTO
- **Types** (`types.ts`): Input/output DTO types
- **Errors** (`errors.ts`): Use case specific errors
- **Fixtures** (`fixtures.ts`): Test data (when needed)

### Domain Services (`{domain}/core/services/`)

**Purpose**: Encapsulate pure domain logic that operates across multiple entities.

**RULES**:
- **Pure Functions** — NO side effects
- Accept DTOs/entities as input (already fetched by use case)
- Return plain data structures
- NO I/O operations — no database, HTTP, file system
- NO dependencies on repositories or external services
- Easily testable with factory-created data

### Infrastructure Layer (`{domain}/adapters/`)

**Purpose**: Implement technical capabilities and connect the domain to the external world.

**RULES**:
- Extends abstract repository classes from `core/`
- Contains ALL external dependencies and framework code
- Handles technical concerns (persistence mapping, sessions/transactions, message envelopes)
- NEVER contains business logic
- Translates between external formats and domain DTOs

**Contains**:
- **Repository implementations**: extending the abstract classes from `core/`
- **Controllers**: thin HTTP handlers delegating to use cases
- **Producers/consumers**: wired via abstract messaging interfaces
- **External clients**: HTTP clients for third-party APIs

---

## Critical Patterns

### JSON Schema Validation Pattern

**Validate input DTOs and entity data using JSON Schema Draft-07 via AJV.** Validators are NOT constructor-injected — there's a singleton, called inline.

**Why JSON Schema?**
- Single source of truth: same schema drives runtime validation, OpenAPI docs, and `json-schema-faker` factories
- Built-in caching: AJV compiles schemas once, with TTL-based recompilation
- Rich error context: violations carry the offending property path, expected/actual values, and `additionalProperties` info

**Validator singleton:**

```typescript
import { schemaValidator } from '{path-to}/utils/validators/json-schema-validator';

// Throws SchemaValidationError on failure:
schemaValidator.validateOrReject(jsonSchema, data);
```

**Where schemas live:**

```
{domain}-v{n}/core/
├── entity.schema.ts                       # Schema for the entity DTO (used by entity.validate())
└── use-cases/{action}/input-dto.schema.ts # Schema for the use case input DTO
```

**Use case pattern (validate inline at the start of `execute`):**

```typescript
async execute(inputDto: ICreateOrderV1InputDto): Promise<IOrderV1Dto> {
  schemaValidator.validateOrReject(createOrderV1InputDtoJsonSchema, inputDto);
  // ... use case logic ...
}
```

**Errors thrown by the validator:**
- `SchemaValidationError` — input doesn't match the schema
- `InvalidJsonSchemaError` — the schema itself is malformed

### Entity Pattern

**Entities use a fluent validate-then-getDto chain.** The constructor accepts a single named-fields object, and entities expose `.validate()` and `.getDto()` methods.

```typescript
export class OrderV1Entity {
  private readonly dto: IOrderV1Dto;

  constructor({ orderInputData }: { orderInputData: IOrderV1Dto }) {
    this.dto = orderInputData;
  }

  validate(): this {
    schemaValidator.validateOrReject(orderV1EntityJsonSchema, this.dto);
    return this;
  }

  getDto(): IOrderV1Dto {
    return structuredClone(this.dto);
  }

  // Business logic methods return new entity instances (immutability)
  confirm(): OrderV1Entity {
    return new OrderV1Entity({ orderInputData: { ...this.dto, status: OrderStatus.CONFIRMED } });
  }
}
```

**Common usage:**

```typescript
const orderDto = new OrderV1Entity({ orderInputData })
  .validate()
  .getDto();
```

**Entities MUST:**
- Accept a single object argument in the constructor (named fields, not positional)
- Expose a chainable `.validate()` method that calls the JSON Schema validator and returns `this`
- Expose `.getDto()` to return an immutable copy of internal state
- Implement business logic methods that return new entities (immutability)
- Throw domain-specific errors for business rule violations

### Repository Pattern

**CRITICAL**: Repository methods MUST accept and return DTOs, NOT entities.

**Why DTOs, not Entities?**
- Repositories are infrastructure adapters — they shouldn't dictate entity construction
- DTOs are pure data structures — easier to serialize, cache, and transform
- Allows flexibility in entity instantiation across different contexts
- Repositories focus on data persistence, not business logic

**Repository ports are abstract classes in `core/`:**
```typescript
import { IOrderV1Dto } from './types';
import { RepositorySession } from '../../_shared/types';

export abstract class OrderV1DatabaseRepository {
  abstract findById(id: string): Promise<IOrderV1Dto | null>;
  abstract save(dto: IOrderV1Dto, session?: RepositorySession): Promise<IOrderV1Dto>;
  abstract findByCustomer(customerId: string): Promise<IOrderV1Dto[]>;
  abstract update(id: string, updates: Partial<IOrderV1Dto>, session?: RepositorySession): Promise<void>;
}
```

**BAD — Accepting/Returning Entities (NEVER do this):**
```typescript
export abstract class OrderV1DatabaseRepository {
  abstract findById(id: string): Promise<OrderV1Entity | null>;  // ❌ Returns entity
  abstract save(order: OrderV1Entity): Promise<void>;            // ❌ Accepts entity
}
```

**Implementations live in `adapters/`** and extend the abstract class with persistence-mapping plumbing.

**Use case usage:**

```typescript
async execute(inputDto: ICreateOrderV1UseCaseInputDto): Promise<IOrderV1Dto> {
  schemaValidator.validateOrReject(createOrderV1InputDtoJsonSchema, inputDto);

  const orderDto = new OrderV1Entity({
    orderInputData: {
      _id: randomUUID(),
      customerId: inputDto.customerId,
      items: inputDto.items,
      status: OrderStatus.PENDING,
      createdAt: new Date().toISOString(),
    },
  })
    .validate()
    .getDto();

  return this.orderDatabaseRepository.save(orderDto);
}
```

**Update flow:**

```typescript
async execute(inputDto: IConfirmOrderV1UseCaseInputDto): Promise<void> {
  const orderDto = await this.orderDatabaseRepository.findById(inputDto.orderId);
  if (!orderDto) throw new OrderNotFoundError(`Order ${inputDto.orderId} not found`);

  const confirmedDto = new OrderV1Entity({ orderInputData: orderDto })
    .validate()
    .confirm()
    .getDto();

  await this.orderDatabaseRepository.save(confirmedDto);
}
```

### Error Hierarchy

**Pattern: flexible error classes without fixed messages.**

```typescript
export class InvalidOrderV1Error extends Error {}
export class OrderTransitionError extends Error {}

throw new OrderTransitionError(
  `Cannot transition order from ${this.dto.status} to ${OrderStatus.CONFIRMED}`
);
```

**Categories:**
- **Domain Errors** — validation errors and business rule violations (thrown by entities/services)
- **Application Errors** — use-case-level errors (e.g., `InsufficientInventoryError`, `DuplicateOrderError`)

### Logging Best Practices

**Use cases MUST implement structured logging using an `ILog` type with the log-steps pattern.**

```typescript
export type ILog = {
  module: string;
  method?: string;
  steps: Array<{ message: string; data?: unknown }>;
  error?: unknown;
  requestStartTime?: Date;
  lastStepTime?: Date;
  totalProcessingDuration?: string;
  processingTime?: number;
};
```

#### Log Steps Pattern

**CRITICAL**: Log steps MUST describe **actions that have been executed**, NOT actions to be executed.

**BAD — Logging intentions (future tense):**
```typescript
log.steps.push({ message: 'Validating input dto' });  // ❌ About to do, not done
schemaValidator.validateOrReject(schema, inputDto);
```

**GOOD — Logging completed actions (past tense):**
```typescript
schemaValidator.validateOrReject(schema, inputDto);
log.steps.push({ message: 'Validated use case input dto.', data: inputDto });  // ✅ Action completed
```

#### Use Case Logging Pattern

**Every use case MUST follow this structure:**

```typescript
export class CreateOrderV1UseCase {
  async execute(inputDto: ICreateOrderV1InputDto): Promise<IOrderV1Dto> {
    const log: ILog = {
      module: CreateOrderV1UseCase.name,
      method: 'execute',
      steps: [],
      error: null,
    };

    try {
      schemaValidator.validateOrReject(createOrderV1InputDtoJsonSchema, inputDto);
      log.steps.push({ message: 'Validated use case input dto.', data: inputDto });

      const orderEntityDto = new OrderV1Entity({ orderInputData: inputDto })
        .validate()
        .getDto();
      log.steps.push({ message: 'Successfully generated an Order entity.' });

      const insertedOrder = await this.orderDatabaseRepository.save(orderEntityDto);
      log.steps.push({ message: `Order with id ${insertedOrder._id} was persisted.` });

      info(`Successfully created an Order with id ${insertedOrder._id}`, log);
      return insertedOrder;
    } catch (err) {
      log.error = err;
      log.steps.push({ message: `Error while creating OrderV1` });
      error('Error while trying to create OrderV1', log);
      throw err;
    }
  }
}
```

When delegating to private/helper methods, pass `log` down so they can append steps to the same log object.

#### Logging Rules

**ALWAYS:**
- Use the `ILog` type for all logging in use cases
- Initialize `log` at the top of `execute` with `module`, `method`, `steps: []`, `error: null`
- Push step entries as objects: `{ message: string; data?: unknown }`
- Log steps AFTER the action completes
- Include contextual identifiers (IDs, names) in step messages and the `data` field
- On success: call `info('msg', log)`
- On failure: set `log.error = err`, push a final error step, call `error('msg', log)`, then `throw`
- Pass `log` down to private/helper methods

**NEVER:**
- Use `console.log` directly in use cases or domain code
- Log sensitive data (passwords, tokens, PII)
- Log steps before the action executes
- Push raw strings to `log.steps` (must be objects with `message`)
- Skip error logging in catch blocks

### Authentication & Authorization

**Granular permission checks belong in the use case, not the controller.** The use case owns the business decision; the controller only forwards the token.

```typescript
const userHasPermission = await authCheck({
  originNamespace: 'order',
  action: 'cancel',
  namespaceToCheck: 'workspace',
  resourceId: order.workspaceId,
  token,
});

if (!userHasPermission) {
  throw new Forbidden('You do not have permission to cancel this order');
}
```

**Rules:**
- Always check **inside the use case**
- Pass the user's `token` through the input DTO
- Throw `Forbidden` from `http-errors` when the check fails

### Transactions

**Use a `RepositorySession` + `withTransaction` when a single use case must coordinate multiple repository writes atomically.**

```typescript
const session = (await getDBSession()) as unknown as RepositorySession;

try {
  await session.withTransaction(async () => {
    await this.orderDatabaseRepository.save(orderDto, session);
    await this.orderEventsRepository.save(orderDto, session);
    await this.auditDatabaseRepository.save(auditDto, session);
  });
} finally {
  await session.endSession();
}
```

**Rules:**
- All repository methods that participate in a transaction MUST accept an optional `session?: RepositorySession` argument and pass it through to the underlying driver
- Always call `session.endSession()` in a `finally` block
- Test infrastructure must support transactions (e.g., a MongoDB ReplicaSet)

### Feature Flags

**Feature flags are env-keyed and consumed via a `config()` helper.**

**Naming**: `{TICKET}_{FEATURE_NAME}_ENABLED` → exposed as `enable{FeatureName}`

```typescript
const { featureFlag: { enableOrderCancellationV2 } } = config();

if (enableOrderCancellationV2) {
  registerOrderCancellationV2Routes(app);
}
```

**Rules:**
- New non-trivial features SHOULD be gated by a flag named after their ticket
- Add the flag in `config.ts`, then read via `config().featureFlag.enableXxx`
- For pure domain code, prefer **constructor injection** of the flag value to avoid leaking infrastructure imports into the domain layer
- Remove the flag (and its dead code path) once the feature is fully rolled out

### Messaging Patterns (Kafka / Pub-Sub)

**Producer side** — use an events-repository pattern instead of calling the broker directly from use cases:

- Port: `core/events-repository.ts` → abstract class `{Name}V{n}EventsRepository`
- Impl: `adapters/{tech}-events-repository.ts`

**Consumer side** — wire topics in a single composition module via a `topicsSubscribers` map.

**Rules:**
- Use the events-repository pattern (not direct producer calls) inside use cases
- Consumer activation is gated by feature flags
- For atomic writes that span DB + broker, use a transactional producer

---

## Testing Strategy

### Test Organization — BEHAVIOR-DRIVEN

**CRITICAL**: Tests MUST describe BEHAVIORS, not methods.

**BAD — Method-focused (testing implementation):**
```typescript
describe('OrderV1Entity', () => {
  describe('constructor()', () => { });
  describe('confirm()', () => { });
  describe('cancel()', () => { });
});
```

**GOOD — Behavior-focused (testing business rules):**
```typescript
describe('OrderV1Entity', () => {
  describe('entity creation', () => {
    it('should create a valid order entity', () => { });
    it('should reject invalid order data', () => { });
  });

  describe('order confirmation workflow', () => {
    it('should confirm pending order', () => { });
    it('should reject confirmation of already confirmed order', () => { });
  });

  describe('validation rules', () => {
    it('should require at least one item', () => { });
    it('should require item quantity to be at least 1', () => { });
  });
});
```

### Integration Tests Over Fakes (CRITICAL PREFERENCE)

Always prefer integration tests with real infrastructure over unit tests with fake in-memory repositories.

**What we test:**
- ✅ **Entities** — Unit tests (pure domain logic, no database)
- ✅ **Use Cases** — Integration tests (with real database, broker, etc.)
- ✅ **Controllers** — Integration tests (with real infrastructure) or with fake use cases (only if the use case is already battle-tested)
- ✅ **Application Workflows** — Integration tests (end-to-end scenarios)
- ❌ **Repository Implementations** — NOT tested in isolation (tested through use cases)

**Benefits of integration tests:**
- Real database behavior (transactions, constraints, indexes)
- Catch query issues that fakes won't reveal
- Verify actual data persistence and retrieval
- Test real broker message handling
- Higher confidence in production behavior

**When to use fakes (sparingly):**
- External APIs you don't control (payment gateways, third-party services)
- Infrastructure too complex/expensive to set up in tests
- Rapid TDD feedback loops (still maintain integration tests in parallel)

**If you use fakes, you MUST also have integration tests that verify the same behavior with real implementations.**

#### Decision Matrix: Integration Test vs Fake

| Scenario | Use Integration Test | Use Fake |
|----------|---------------------|----------|
| Testing use cases | ✅ ALWAYS PREFERRED | ❌ Only if integration not feasible |
| Testing controllers | ✅ ALWAYS PREFERRED | ❌ Only if integration not feasible |
| Testing with database | ✅ REQUIRED | ❌ Avoid |
| Testing with broker | ✅ REQUIRED (use test containers) | ❌ Avoid |
| Testing external HTTP APIs | ⚠️ Consider test doubles | ✅ Acceptable |
| Testing repository implementations | ❌ Test through use cases | ❌ Not tested in isolation |
| Testing entities | ❌ Unit tests (no I/O) | ❌ Not applicable |
| CI/CD with test database | ✅ REQUIRED | ❌ Avoid |

#### Recommended Test Suite Structure

```
domains/orders/entities/order-v1/
├── entity.ts
└── entity.spec.ts                         # Unit tests (pure domain logic, no database)

domains/orders/use-cases/create-order-v1/
├── index.ts
├── index.e2e.spec.ts                      # PREFERRED — Test with real database
└── dtos.ts

domains/orders/adapters/
├── mongo-order-v1-repository.ts           # Not tested in isolation
├── express-orders-controller.ts
└── express-orders-controller.e2e.spec.ts  # Test with real infrastructure

test/fakes/
├── fake-order-v1-repository.ts            # Only if integration tests not feasible
└── fake-inventory-service.ts              # Fake external services only
```

### Factory Functions

**Use the in-domain `core/factory.ts` (built on `@faker-js/faker` + `json-schema-faker`) and apply overrides for what the test cares about:**

```typescript
const orderDto = orderV1Factory.generateOne({
  overrides: { items: [{ sku: 'SKU-001', quantity: 1, unitPrice: 100000 }] },
});

// Build an entity from the DTO when the test needs entity behavior:
const entity = new OrderV1Entity({ orderInputData: orderDto }).validate();
```

### Test Infrastructure

**For databases that support it (e.g., MongoDB ReplicaSet), provide an isolated server for each test run.** ReplicaSet is required so transactions work in tests.

```typescript
beforeAll(() => {
  memoryServerFactory = new {DB}MemoryServerFactory();
});

beforeEach(async () => {
  testUrl = await memoryServerFactory.create();
  await reloadDb(testUrl);

  orderDatabaseRepository = new {Tech}OrderV1DatabaseRepository(testUrl);
  useCase = new CreateOrderV1UseCase(orderDatabaseRepository);
});
```

**Rules:**
- One factory per `describe`; create a fresh URL in each `beforeEach`
- Always re-bootstrap (indexes/seed data) after creating a fresh URL
- Use real implementations (not fakes) — that's the whole point of an in-memory ReplicaSet

---

## TDD with Toyota Kata Mindset

**CRITICAL**: When implementing features using TDD, apply the Toyota Kata improvement pattern.

### The Four Questions

```
1. CURRENT CONDITION  — "Where are we now?"
2. TARGET CONDITION   — "Where do we want to be?"
3. OBSTACLES          — "What's preventing us from reaching the target?"
4. EXPERIMENT         — "What's the next small step to learn/progress?"
```

### Mapping to TDD

- **Current Condition (RED)** — write a failing test that describes the desired behavior. The failing test IS your current condition.
- **Target Condition (GREEN)** — define the smallest passing implementation; focus on THIS test only.
- **Obstacles** — identify them BEFORE writing code (knowledge gaps, design uncertainty, technical blockers, scope creep).
- **Experiment (REFACTOR)** — small, deliberate changes to improve design while keeping tests green.

### Workflow Template

```markdown
## TDD Cycle #N

### 1. Current Condition
- What exists now: [describe]
- Failing test: [test name]
- Gap identified: [what's missing]

### 2. Target Condition
- Desired behavior: [specific, measurable outcome]
- This cycle's scope: [what THIS test covers, nothing more]

### 3. Obstacles
- [ ] Obstacle 1: [description] → [how to address]
- [ ] Obstacle 2: [description] → [how to address]

### 4. Experiment
- Hypothesis: [what I expect to learn/achieve]
- Smallest step: [minimal code change]
- Result: [PASS/FAIL, what was learned]
```

### Key Principles

**ALWAYS:**
- Start each cycle by stating current and target conditions
- Identify obstacles BEFORE writing code
- Keep experiments small and reversible
- One behavior per cycle — resist scope creep

**NEVER:**
- Skip obstacle identification
- Write code without a failing test (except refactoring)
- Let target condition expand mid-cycle
- Ignore what you learned from failed experiments

---

## Anti-Patterns to Avoid

### DON'T: Anemic Domain Model
**BAD**: Data bags with no behavior, logic scattered in services
**GOOD**: Rich entities with business logic encapsulated

### DON'T: Infrastructure in Domain
**BAD**: Domain depends on Prisma, Express, etc.
**GOOD**: Domain defines abstract ports, infrastructure implements them

### DON'T: Repositories Working with Entities
**BAD**: Repository methods accepting or returning entities
**GOOD**: Repository methods work exclusively with DTOs; use cases construct entities

### DON'T: Business Logic in Controllers
**BAD**: Controllers doing validation, calculations, business rules
**GOOD**: Thin controllers that delegate to use cases and handle HTTP concerns

### DON'T: Over-Mocking
**BAD**: Mocking everything with Jest mocks, testing nothing
**GOOD**: Use integration tests with real infrastructure; fakes only when integration is not feasible

### DON'T: Catch-All Files
**BAD**: `utils.ts`, `helpers.ts`, `common.ts`
**GOOD**: `date-utils.ts`, `money-operations.ts`, `validation-helpers.ts`

---

## Coding Guidelines for Claude

### When Adding New Features

1. **Create Domain Core**:
   - [ ] Domain folder: `{domain}-v{n}/core/`
   - [ ] `entity.ts` — Entity class with fluent `.validate().getDto()` chain
   - [ ] `entity.schema.ts` — JSON Schema (Draft-07) for the entity DTO
   - [ ] `types.ts` — DTO interfaces (`I{Name}V{n}Dto`)
   - [ ] `errors.ts` — Domain-specific error classes
   - [ ] `factory.ts` — Test/data factory
   - [ ] `database-repository.ts` — Abstract repository class (port, DTO-only)
   - [ ] `events-repository.ts` — Abstract events repository (when applicable)

2. **Create Use Case**:
   - [ ] Folder: `{domain}-v{n}/core/use-cases/{action}/`
   - [ ] `index.ts` — `{Verb}{Name}V{n}UseCase` class with single `execute()`
   - [ ] `input-dto.schema.ts` — JSON Schema for the input DTO
   - [ ] `types.ts` — Input/output DTO types
   - [ ] `errors.ts` — Use case specific errors
   - [ ] Inside `execute()`: call `schemaValidator.validateOrReject(schema, inputDto)` first

3. **Create Services** (if needed):
   - [ ] Pure functions in `{domain}-v{n}/core/services/{service-name}/`
   - [ ] NO side effects, NO I/O

4. **Create Adapters**:
   - [ ] Repository implementation extending the abstract class from `core/`
   - [ ] Events repository implementation when applicable
   - [ ] Controller — thin handler delegating to the use case

5. **Wire DI**:
   - [ ] Register the new use case + dependencies in the composition root (manual DI)

6. **Create Tests** (behavior-driven):
   - [ ] `entity.spec.ts` — entity unit tests (use real JSON schemas)
   - [ ] `use-cases/{action}/index.spec.ts` — use case unit/integration tests
   - [ ] `adapters/{use-case}/controller.e2e.spec.ts` — E2E test with real infra + supertest

7. **Update API Documentation** (if applicable):
   - [ ] Update OpenAPI / GraphQL schema for new or modified endpoints
   - [ ] Update DTOs and parameters
   - [ ] Gate feature-flagged endpoints behind the appropriate flag

### When Modifying Code

**ALWAYS**:
- Read existing code first to understand patterns
- Follow existing naming conventions exactly
- Maintain layer boundaries (no domain importing infrastructure)
- Keep JSON Schemas in `*.schema.ts` files; call `schemaValidator.validateOrReject(...)` inline in use cases
- Write behavior-driven tests, not method-focused tests
- Prefer integration tests over fakes
- Keep services as pure functions with no side effects

**NEVER**:
- Put business logic in controllers or repositories
- Import infrastructure code into the domain layer
- Create anemic entities (data bags without behavior)
- Use global mutable state
- Skip tests for new functionality
- Create catch-all utility files

### When Modifying API Interface

**CRITICAL**: When you change the REST/GraphQL/RPC interface (routes, request/response schemas, parameters, headers, etc.), you **MUST** update the API documentation.

**Checklist for API changes:**
- [ ] Add/modify the endpoint definition in the API spec
- [ ] Update request body schemas for new/modified DTOs
- [ ] Update parameters for new/modified parameters
- [ ] Update responses for new/modified responses
- [ ] Verify examples match actual behavior
- [ ] If feature-flagged, gate the endpoint accordingly

### When Creating Tests

**ALWAYS**:
- Organize by behavior, not by method
- Use descriptive test names that explain the scenario
- Use Given-When-Then structure
- Use real JSON schemas in entity tests (validation is part of the entity's behavior)
- Prefer integration tests with real infrastructure over fakes
- Create factory functions with overrides pattern
- Test behaviors users care about, not implementation details

**NEVER**:
- Name describe blocks after method names
- Over-mock dependencies
- Test implementation details
- Skip contract tests for fakes

### Versioning Strategy

**When to increment versions:**
- Breaking schema changes → Create V2 entity
- Removed fields → Create V2, deprecate V1
- Changed business rules → Create V2 with new rules

**When to stay on current version:**
- New optional fields → Add to existing version
- Bug fixes → Stay on current version

---

## Repository-Specific Notes

### Path Aliases

Use a single root alias for cross-package imports (e.g., `@root/*` maps to the repo root) to avoid deep `../../../../` chains. Within the same package tree, relative imports are fine.

### Manual Dependency Injection

**There is no DI container.** Use cases and their dependencies are wired by hand in the composition root (e.g., `app.ts`).

**Adding a new use case requires:**
1. Importing the use case class and its repository implementations into the composition root
2. Constructing the repositories
3. Constructing the use case with those repositories
4. Passing the use case instance into the controller registration

**Rules:**
- Wiring code lives in the composition root, not in domain code
- Domain code MUST NOT import from the composition root or know about the container

### Git Workflow

**Branch naming**: `{TICKET}-{kebab-case-description}` (e.g., `PROJ-123-add-user-authentication`).

**Commit messages — Conventional Commits**: `{type}({TICKET}): {description}`
- Scope MUST be UPPER-CASE (the ticket key)
- Type MUST be lower-case
- Subject: no sentence-case, start-case, pascal-case, or upper-case; no full stop at end
- Header max length: 100 characters

**Allowed types**: `feat`, `fix`, `refactor`, `test`, `docs`, `chore`, `perf`, `build`, `ci`, `style`, `revert`, `config`, `adjust`, `debug`, `observe`

**Pull Requests**: Target the team's mainline branch (e.g., `staging`, `develop`). PR title follows the same format as commits.

### Code Review Standards

1. Is business logic in the right layer?
2. Are dependencies pointing inward?
3. Do repositories accept and return DTOs (not entities)?
4. Is the use case doing only one thing?
5. Are services pure functions with no side effects?
6. Are tests organized by behavior, not by method?
7. Does validation use a JSON Schema validator?
8. Can this be understood without reading other files?
9. Are versioned names consistent across the feature?

---

## Quick Reference

### Dependency Rule Verification

Ask yourself:
1. Can I run domain tests without a database? **Must be yes.**
2. Can I swap the DB implementation without touching use cases? **Must be yes.**
3. Can I add a new transport (e.g., GraphQL) without modifying domain? **Must be yes.**

### Test Quality Checklist
- [ ] Behavior-driven (NOT method-focused)
- [ ] Tests use Given-When-Then structure
- [ ] Test names explain scenarios in business terms
- [ ] Integration tests preferred for use cases / controllers
- [ ] Real validators in entity tests
- [ ] No mocking of internal classes
- [ ] Fakes have contract tests
- [ ] Factory functions with overrides pattern

---

## Insights Management System

Claude maintains two insight files for continuous learning and knowledge preservation across context boundaries.

### Overview

| File | Purpose | Capacity | Order |
|------|---------|----------|-------|
| `.claude/insights-short-term.md` | Latest learnings + current task | 100 insights | Newest first (by datetime) |
| `.claude/insights-long-term.md` | High-impact, evergreen insights | 100 insights | By relevance/impact |

### Triggers

| Trigger | Hook | Action |
|---------|------|--------|
| End of response | Stop | Add valuable learnings to short-term |
| Context at 99% | PreCompact | **CRITICAL**: Execute full memory protocol (see below) |

### Memory Protocol (PreCompact — 99% Context)

When the PreCompact hook fires, execute these steps IN ORDER:

1. **Save Current Task** — add to `.claude/insights-short-term.md` at TOP: `[YYYY-MM-DD HH:MM] TASK: <task description, max 100 chars>`
2. **Review & Promote Insights** — check short-term insights for any worth promoting to long-term
3. **Clear Context** — execute `/clear`
4. **Restore Memory** — read in order: `.claude/insights-long-term.md`, then `.claude/insights-short-term.md`
5. **Continue Task** — find your `TASK:` entry in short-term insights and resume

### Adding Insights

**Short-term**: Open `.claude/insights-short-term.md`, add at TOP. Format: `[YYYY-MM-DD HH:MM] <insight max 100 chars>`. If >100 entries, remove the oldest.

**Promotion to long-term**: If an insight has lasting value, add to `.claude/insights-long-term.md` with a rank, re-sort by impact. Format: `[01] [YYYY-MM-DD] <insight>`.

### What Makes a Good Insight?

**GOOD insights are:**
- Actionable (can be applied immediately)
- Concise (max 100 characters)
- Non-obvious (not already documented)
- Reusable (applies beyond the current task)

**BAD insights are:**
- Task-specific details ("Fixed bug in order service")
- Obvious statements ("Tests should pass")
- Too long or vague

### When NOT to Add Insights

- Skip if nothing genuinely valuable was learned
- Skip if the insight already exists
- Skip if it's already well-documented in this file

---

## Important Reminders

**CRITICAL RULES**:

1. **Domain layer is sacred** — NO infrastructure dependencies
2. **Validators are inline** — call the JSON Schema validator at the start of use cases
3. **Repositories work with DTOs** — NEVER accept or return entities
4. **Services are pure functions** — NO side effects, NO I/O
5. **Tests describe behaviors** — NOT methods
6. **Prefer integration tests** — fakes only when infeasible
7. **Entities return new instances** — Immutability pattern
8. **One use case, one operation** — Single responsibility
9. **Names follow conventions** — Versioned naming pattern

**When in doubt:**
- Read this file's relevant section
- Follow existing patterns in the codebase
- Ask clarifying questions before implementing

---

*This template ensures Claude Code understands and follows the established patterns, making it a productive contributor to any team that adopts these conventions.*