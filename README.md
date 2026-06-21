# Automated Card-to-Card Payment Reconciliation System

<p align="center">
  <img src="https://img.shields.io/badge/FastAPI-005571?style=flat-square&logo=fastapi" alt="FastAPI">
  <img src="https://img.shields.io/badge/PostgreSQL-316192?style=flat-square&logo=postgresql&logoColor=white" alt="PostgreSQL">
  <img src="https://img.shields.io/badge/Redis-DC382D?style=flat-square&logo=redis&logoColor=white" alt="Redis">
  <img src="https://img.shields.io/badge/Docker-2496ED?style=flat-square&logo=docker&logoColor=white" alt="Docker">
  <img src="https://img.shields.io/badge/Python-3776AB?style=flat-square&logo=python&logoColor=white" alt="Python">
</p>

> An asynchronous, event-driven backend service built to automate card-to-card payment verification without relying on traditional payment gateways.

---

## 🎯 Overview & The Problem

**This project automates the entire verification pipeline.**

In many payment workflows, users complete purchases through manual card-to-card transfers. Verifying these payments usually requires reviewing receipts or checking transactions manually, which becomes time-consuming, error-prone, and difficult to scale.

For every payment request, the system generates a unique amount that is temporarily reserved for a specific user. Once the transfer is completed, the incoming transaction is automatically identified, matched to the correct request, and the associated business workflow is executed without any manual intervention.

The result is a fully automated payment confirmation pipeline capable of activating services, subscriptions, or user accounts immediately after a successful transfer.

---

## 📸 System Flow

<p align="center">
  <img src="./docs/payment-flow.png" width="1000">
</p>

---

## 🗺️ System Architecture & Data Flow

The following sequence illustrates the complete lifecycle of a transaction from order creation to background verification:

```mermaid
graph TD
    User[👤 User] -->|1. Request Order| API[⚡ FastAPI Gateway]
    API -->|2. SMOVE Atomic Fetch| RedisSet[(🧠 Redis: free_amounts)]
    API -->|3. Save Pending Order| DB[(🐘 PostgreSQL)]

    User -->|4. Exact Card-to-Card Transfer| Bank[🏦 Bank System]

    Bank -->|5. Send SMS| Phone[📱 Sim Card / Android]

    Phone -->|6. Forward SMS Payload| API_Tx[📥 /transaction Webhook]

    API_Tx -->|7. XADD Stream Entry| Stream((🔥 Redis Streams))

    Stream -->|8. Fetch / XCLAIM| Worker[⚙️ Async Worker]

    Worker -->|9. Match Amount & Deduplicate| DB

    Worker -->|10. XACK Message| Stream
```

---

## 💡 How It Works (The Core Logic)

When traditional digital payment gateways are unavailable or impractical, businesses often rely on manual peer-to-peer card transfers. 
  
The primary challenge here is reliably identifying which user made a specific payment without manual receipt verification or automated bank webhooks.

### The Solution: Unique Amount Mapping

This project solves the problem by assigning a unique payment amount to every pending checkout request.

For example, if a service is priced within a configurable range, the system allocates a unique amount from that range and reserves it for a limited period.

When the user transfers that exact amount:

- The incoming bank notification is captured automatically.
- The transaction is matched to the correct order.
- The order is marked as paid.
- The requested service is activated.

All of this happens automatically without requiring human review.

---

## 🛠️ Core Implementation Details

### 1. High-Concurrency & Race Condition Prevention

To prevent duplicate amount allocation under heavy concurrent load, the system combines atomic Redis operations with database-level guarantees.

#### Memory Layer (Atomic Operations)

- Redis maintains the pool of available amounts.
- Active allocations are tracked separately.
- Amount assignment is performed atomically using `SMOVE`.

#### Database Layer (Safety Net)

A PostgreSQL Partial Unique Index guarantees uniqueness among active payment requests.

```sql
CREATE UNIQUE INDEX uq_pending_amount
ON payment_order (amount)
WHERE status = 'PENDING';
```

---

### 2. Idempotency & Double-Spending Protection

Incoming SMS notifications may occasionally be forwarded multiple times.

To prevent duplicate processing, transactions are deduplicated through a composite database constraint.

```python
__table_args__ = (
    UniqueConstraint(
        "sms_amount",
        "sms_inventory",
        "sms_date",
        "sms_time",
        name="uq_sms_amount_inventory_date_time"
    ),
)
```

---

### 3. Fault Tolerance & Guaranteed Message Delivery

The processing pipeline is built on Redis Streams and Consumer Groups.

#### Reliability Mechanisms

- Message acknowledgment
- Retry strategy
- Stale message recovery 
- Dead Letter Stream (DLQ)

These mechanisms ensure transactions are processed reliably even when workers fail unexpectedly.

---

### 4. Structured Logging

Structured JSON logging is implemented using Structlog.

Each transaction receives a unique `trace_id` which propagates through API requests, Redis Streams, and background workers, enabling end-to-end traceability.

---

### 5. Authentication & Session Management

In addition to payment processing, the system includes a complete authentication and session management layer designed to support long-lived user sessions while maintaining strong security guarantees.

The core challenge is maintaining seamless, long-lived sessions without exposing the system to token theft, replay attacks, or state inconsistencies. This is solved via JWT rotation, token families, and Redis-backed session recovery.

---

#### Authentication Layer

The API implements standard JWT-based authentication using short-lived Access Tokens and long-lived Refresh Tokens. User credentials are securely stored using BCrypt password hashing, and protected endpoints resolve identities through OAuth2 Bearer tokens.

---

#### Refresh Token Rotation

To ensure refresh tokens remain strictly single-use, every refresh operation invalidates the previous token and issues a completely new pair. This significantly mitigates the risk and impact of token leakage.

```text
┌──────────────┐
│ Refresh RT1  │
└──────┬───────┘
       │
       ▼
   [ Revoked ]
       │
       ▼
┌──────────────┐
│ Refresh RT2  │
└──────┬───────┘
       │
       ▼
┌──────────────┐
│ Access AT2   │
└──────────────┘
```

---

#### Replay Attack Detection

Refresh tokens are tracked within unique "token families." If a previously revoked token is reused, the system detects a potential replay attack and immediately invalidates the entire family, preventing unauthorized session extensions.

```text
Token Family A

RT1 ───▶ RT2 ───▶ RT3
 │
 │ Reused after revocation
 ▼
⚠ Potential Replay Attack

RT1 ✖
RT2 ✖
RT3 ✖
```

---

#### Grace Period & Race Condition Handling

> Handles concurrent refresh requests or network retries securely without breaking the session flow.

- Rotated token pairs are temporarily cached in Redis during a short grace period.
- Legitimate retry requests (due to network instability) receive the same token pair instead of triggering a false replay attack alarm.
- This balances user experience with strict security during concurrent token rotation.

```text
Client
 │
 ├── Refresh Request #1 ──▶ Success
 │
 └── Refresh Request #2 ──▶ Arrives Moments Later
                               │
                               ▼
                     Redis Grace Period Cache
                               │
                               ▼
                     Return Same Token Pair
```

---

#### Security Guarantees

- JWT-based Authentication
- Access & Refresh Token Architecture
- Refresh Token Rotation
- Token Family Tracking
- Replay Attack Detection
- Redis-backed Grace Period Recovery
- Database-backed Token Revocation
- BCrypt Password Hashing
---

## 📦 Tech Stack

| Technology | Purpose | Key Feature Implemented |
|------------|----------|-------------------------|
| FastAPI | Web Framework | Async APIs & Lifespan Management & Authentication |
| PostgreSQL | Relational Database | Partial Indexes & Constraints & Token Persistence |
| SQLAlchemy | ORM | AsyncSession |
| Redis | Broker & Cache | Streams & Atomic Operations & Grace Period Handling  |
| Structlog | Logging | Structured JSON Logs |
| Docker | Infrastructure | Containerized Environment |

---

## 🚀 Getting Started

### Environment Variables

Create a `.env` file:

```env
POSTGRES_USER=your_postgres_user
POSTGRES_PASSWORD=your_postgres_password
POSTGRES_DB=your_database_name
DATABASE_URL=postgresql+asyncpg://user:password@localhost:5432/dbname

REDIS_HOST=your_redis_host
REDIS_PORT=your_redis_port

SECRET_ACCESS_KEY=your_access_secret
SECRET_REFRESH_KEY=your_refresh_secret
```

### Run with Docker

```bash
docker compose up --build
```

---

## 🗺️ Roadmap
- [ ] Support multiple bank SMS formats
- [ ] Automatic expired payment handling
- [ ] Administrative dashboard for failed transactions
- [ ] Metrics & monitoring integration
- [ ] Multi-bank transaction processing support
