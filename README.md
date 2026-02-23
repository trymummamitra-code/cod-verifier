# COD Verification System - Backend

## Quick Start

```bash
# Install dependencies
pip install -r requirements.txt

# Copy environment template
cp .env.example .env

# Edit .env with your credentials
# Then run
python app.py
```

## Default Credentials

**Admin:**
- Email: admin@codverifier.com
- Password: admin123

**Callers (PIN login):**
- Caller 1: PIN 1111
- Caller 2: PIN 2111
- Caller 3: PIN 3111
- Caller 4: PIN 4111
- Caller 5: PIN 5111

## Features

- Multi-store Shopify integration (3 stores)
- Auto-distribution to callers
- 17 status dispositions
- Real-time dashboard
- Call logging
- SQLite database

## API Endpoints

See `app.py` for full list of endpoints.

Key endpoints:
- `POST /api/login` - Caller login (returns JWT)
- `GET /api/orders/queue/<caller_id>` - Get caller's queue
- `POST /api/orders/update-status` - Update order status
