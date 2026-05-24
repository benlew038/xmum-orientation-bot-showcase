# 🦢 XMUM Orientation Management Bot

A scalable Telegram-based orientation management system built with Python, Google Sheets API, and asynchronous processing architecture to support large-scale university orientation operations.

This project was developed to streamline orientation workflows including attendance tracking, Atlantis coin management, facilitator communication, ranking systems, logging, and real-time operations management.

---

# 🚀 Features

- Telegram Bot Management System
- Real-time Atlantis Coin Management
- Group Data Management
- Automated Attendance System with OTP Verification
- Facilitator Messaging System
- Day 1 Ranking System
- Day 2 PK Battle System
- Undo Transaction System
- Google Sheets Database Integration
- Async Queue-Based Architecture
- Cache Optimization System
- Telegram Rate Limiting
- Bulk Batch Updates
- Logging & Audit System
- Role-Based Access Control (RBAC)
- Railway Cloud Deployment

---

# 🛠 Tech Stack

- Python 3
- python-telegram-bot
- asyncio
- Google Sheets API
- gspread
- python-dotenv
- Railway
- GitHub
- Telegram Bot API

---

# 📂 Project Structure

```text
Orientation-Management-Bot/
│
├── main.py
├── requirements.txt
├── Procfile
├── README.md
└── screenshots/
```

---

# ⚙️ Core System Architecture

## Async Queue System

The bot uses multiple asynchronous queues to improve scalability and reduce Telegram / Google Sheets API bottlenecks.

Implemented queues include:

- Coin Update Queue
- Attendance Queue
- Telegram Send Queue
- Group Update Queue
- Logging Queue

---

## Cache Optimization

To reduce excessive Google Sheets reads:

- AUTH cache
- Coins cache
- Attendance cache
- Logs cache
- Group info cache

The system automatically refreshes caches periodically for performance optimization.

---

## Telegram Rate Limiter

A custom token-bucket rate limiter is implemented to:

- Prevent Telegram flood limits
- Avoid API bans
- Stabilize large-scale broadcasts

---

# 👥 User Roles

The system supports role-based permissions:

| Role | Permissions |
|---|---|
| Advisor | Full system access |
| OC | Full operational access |
| HOF | Management access |
| HOGM | Game management access |
| Facilitators | Group management & attendance |
| Game Masters | Coin management |
| Game Test Accounts | Limited testing access |

---

# 💰 Atlantis Coin System

Features include:

- Manual coin editing
- Day 1 ranking rewards
- Day 2 PK rewards
- Undo transaction recovery
- Real-time coin synchronization
- Batch update optimization

---

# 📋 Attendance System

The attendance module includes:

- OTP verification
- Duplicate attendance prevention
- Batch attendance writing
- Attendance caching
- Session-based attendance control
- Retry & fallback handling

---

# 📩 Messaging System

The facilitator messaging module supports:

- Broadcast messaging
- Group-specific messaging
- Reply system
- Inline keyboard interactions
- Telegram queue rate control

---

# 🔐 Security Features

Sensitive credentials are protected using environment variables.

```env
BOT_TOKEN=your_telegram_bot_token
```

Additional security implementations:

- Environment variable handling
- Role-based access control
- Proxy account restrictions
- Session validation
- Anti-duplicate processing locks

---

# ⚡ Performance Optimizations

This project includes multiple production-level optimization strategies:

- Async concurrency control
- Semaphore throttling
- Batch Google Sheets updates
- Write queue buffering
- Cached reads
- Rate-limited Telegram sending
- Background cache warmup
- Retry & exponential backoff mechanisms

---

# ☁️ Deployment

This project supports deployment using:

- Railway
- GitHub automatic deployment
- Environment variable configuration

---

# ▶️ Installation

## Clone Repository

```bash
git clone https://github.com/yourusername/orientation-management-bot.git
cd orientation-management-bot
```

---

## Install Dependencies

```bash
pip install -r requirements.txt
```

---

## Configure Environment Variables

Create a `.env` file:

```env
BOT_TOKEN=your_telegram_bot_token
```

---

## Google Sheets Credentials

Place your Google Service Account credentials:

```text
service_account.json
```

inside the project root directory.

---

## Run Application

```bash
python main.py
```

---

# 📊 Key Functional Modules

## Atlantis Coin Management
- Real-time updates
- Transaction undo support
- Queue-based processing

## Attendance System
- OTP attendance
- Duplicate prevention
- Attendance caching

## Ranking System
- Day 1 ranking rewards
- Day 2 PK battle rewards

## Logging System
- Centralized audit logs
- Batched log writing
- Activity tracking

## Group Management
- Student count updates
- Group slogan updates
- Group location tracking

---

# 📸 Screenshots

## Telegram Bot Interface

_Add screenshots here_

---

## Atlantis Coin Management

_Add screenshots here_

---

## Attendance System

_Add screenshots here_

---

## Railway Deployment

_Add screenshots here_

---

# 📚 What I Learned

- Advanced asynchronous Python architecture
- Queue-based system design
- Telegram bot scalability handling
- Google Sheets API optimization
- Rate limiting systems
- Cache architecture
- Concurrent task processing
- Cloud deployment with Railway
- Role-based permission systems
- Real-time operational automation

---

# 💡 Future Improvements

- Migration from Google Sheets to PostgreSQL
- Web admin dashboard
- Docker containerization
- Analytics dashboard
- Multi-event support
- Redis caching layer
- WebSocket live updates
- Automated backup system

---

# 👨‍💻 Author

Developed as a large-scale operational automation project for university orientation management, focusing on scalability, concurrency, and real-time coordination systems.
