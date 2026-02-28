"""
COD Verification System - Database Module
Supports both SQLite (local dev) and PostgreSQL (production)
"""

import sqlite3
import os
from datetime import datetime, timedelta
from contextlib import contextmanager

try:
    import psycopg2
    import psycopg2.extras
    POSTGRES_AVAILABLE = True
except ImportError:
    POSTGRES_AVAILABLE = False

DB_FILE = "cod_verifier.db"

class Database:
    def __init__(self, db_path=None, database_url=None):
        self.db_path = db_path or DB_FILE
        self.database_url = database_url or os.getenv('DATABASE_URL')
        self.is_postgres = bool(self.database_url and self.database_url.startswith('postgres'))
        
        if self.is_postgres and not POSTGRES_AVAILABLE:
            raise ImportError("psycopg2 not installed. Install with: pip install psycopg2-binary")
        
        print(f"✅ Database initialized successfully")
        if self.is_postgres:
            print(f"   Using PostgreSQL (Railway production)")
        else:
            print(f"   Using SQLite (local dev): {self.db_path}")
        
        self.init_database()
    
    @contextmanager
    def get_connection(self):
        """Context manager for database connections"""
        if self.is_postgres:
            conn = psycopg2.connect(self.database_url)
            conn.cursor_factory = psycopg2.extras.RealDictCursor
        else:
            conn = sqlite3.connect(self.db_path)
            conn.row_factory = sqlite3.Row
        
        try:
            yield conn
            conn.commit()
        except Exception as e:
            conn.rollback()
            raise e
        finally:
            conn.close()
    
    def convert_query(self, query):
        """Convert SQLite syntax to PostgreSQL when needed"""
        if not self.is_postgres:
            return query
        
        # Replace ? placeholders with %s for PostgreSQL
        query = query.replace('?', '%s')
        
        # Replace SQLite functions
        query = query.replace("datetime('now')", "CURRENT_TIMESTAMP")
        query = query.replace("AUTOINCREMENT", "")  # PostgreSQL uses SERIAL
        query = query.replace("INTEGER PRIMARY KEY AUTOINCREMENT", "SERIAL PRIMARY KEY")
        
        return query
    
    def init_database(self):
        """Initialize database schema"""
        with self.get_connection() as conn:
            c = conn.cursor()
            
            # Users table (callers + admin)
            query = '''
                CREATE TABLE IF NOT EXISTS users (
                    id SERIAL PRIMARY KEY,
                    name TEXT NOT NULL,
                    role TEXT NOT NULL CHECK(role IN ('caller', 'admin')),
                    pin TEXT,
                    email TEXT,
                    password_hash TEXT,
                    is_active INTEGER DEFAULT 1,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            '''
            c.execute(self.convert_query(query))
            
            # Shopify stores
            query = '''
                CREATE TABLE IF NOT EXISTS shopify_stores (
                    id SERIAL PRIMARY KEY,
                    name TEXT NOT NULL,
                    shop_url TEXT NOT NULL,
                    access_token TEXT NOT NULL,
                    is_active INTEGER DEFAULT 1
                )
            '''
            c.execute(self.convert_query(query))
            
            # Orders
            query = '''
                CREATE TABLE IF NOT EXISTS orders (
                    id SERIAL PRIMARY KEY,
                    order_id TEXT UNIQUE NOT NULL,
                    store_id INTEGER,
                    order_type TEXT NOT NULL CHECK(order_type IN ('cod', 'abandoned_cart')),
                    customer_name TEXT,
                    phone TEXT NOT NULL,
                    address TEXT,
                    pincode TEXT,
                    product_name TEXT,
                    price REAL,
                    qty INTEGER,
                    order_date TIMESTAMP,
                    status TEXT DEFAULT 'pending' CHECK(status IN ('pending', 'assigned', 'calling', 'confirmed', 'cancelled')),
                    final_status TEXT,
                    assigned_to INTEGER,
                    assigned_at TIMESTAMP,
                    completed_at TIMESTAMP,
                    attempts INTEGER DEFAULT 0,
                    is_whatsapp_queue INTEGER DEFAULT 0,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    edited_customer_name TEXT,
                    edited_phone TEXT,
                    edited_address TEXT,
                    edited_pincode TEXT,
                    edited_at TIMESTAMP,
                    shopify_order_number TEXT,
                    shopify_synced_at TIMESTAMP,
                    FOREIGN KEY (store_id) REFERENCES shopify_stores(id),
                    FOREIGN KEY (assigned_to) REFERENCES users(id)
                )
            '''
            c.execute(self.convert_query(query))
            
            # Add columns if they don't exist (migration for existing databases)
            try:
                c.execute('ALTER TABLE orders ADD COLUMN edited_customer_name TEXT')
            except:
                pass
            try:
                c.execute('ALTER TABLE orders ADD COLUMN edited_phone TEXT')
            except:
                pass
            try:
                c.execute('ALTER TABLE orders ADD COLUMN edited_address TEXT')
            except:
                pass
            try:
                c.execute('ALTER TABLE orders ADD COLUMN edited_pincode TEXT')
            except:
                pass
            try:
                c.execute('ALTER TABLE orders ADD COLUMN edited_at TIMESTAMP')
            except:
                pass
            try:
                c.execute('ALTER TABLE orders ADD COLUMN shopify_order_number TEXT')
            except:
                pass
            try:
                c.execute('ALTER TABLE orders ADD COLUMN shopify_synced_at TIMESTAMP')
            except:
                pass
            
            # Call logs
            query = '''
                CREATE TABLE IF NOT EXISTS call_logs (
                    id SERIAL PRIMARY KEY,
                    order_id INTEGER NOT NULL,
                    caller_id INTEGER NOT NULL,
                    phone_dialed TEXT,
                    call_start TIMESTAMP,
                    call_end TIMESTAMP,
                    call_duration INTEGER,
                    status TEXT,
                    recording_url TEXT,
                    notes TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (order_id) REFERENCES orders(id),
                    FOREIGN KEY (caller_id) REFERENCES users(id)
                )
            '''
            c.execute(self.convert_query(query))
            
            # Store assignments (daily)
            query = '''
                CREATE TABLE IF NOT EXISTS store_assignments (
                    id SERIAL PRIMARY KEY,
                    store_id INTEGER NOT NULL,
                    caller_id INTEGER NOT NULL,
                    assigned_date DATE NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (store_id) REFERENCES shopify_stores(id),
                    FOREIGN KEY (caller_id) REFERENCES users(id),
                    UNIQUE(store_id, caller_id, assigned_date)
                )
            '''
            c.execute(self.convert_query(query))
            
            # Recordings metadata
            query = '''
                CREATE TABLE IF NOT EXISTS recordings (
                    id SERIAL PRIMARY KEY,
                    call_log_id INTEGER NOT NULL,
                    file_name TEXT,
                    file_size_bytes INTEGER,
                    local_path TEXT,
                    gdrive_file_id TEXT,
                    gdrive_url TEXT,
                    uploaded_at TIMESTAMP,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (call_log_id) REFERENCES call_logs(id)
                )
            '''
            c.execute(self.convert_query(query))
            
            # Create indexes for performance
            c.execute(self.convert_query('CREATE INDEX IF NOT EXISTS idx_orders_status ON orders(status)'))
            c.execute(self.convert_query('CREATE INDEX IF NOT EXISTS idx_orders_assigned_to ON orders(assigned_to)'))
            c.execute(self.convert_query('CREATE INDEX IF NOT EXISTS idx_orders_order_id ON orders(order_id)'))
            c.execute(self.convert_query('CREATE INDEX IF NOT EXISTS idx_call_logs_order_id ON call_logs(order_id)'))
            c.execute(self.convert_query('CREATE INDEX IF NOT EXISTS idx_call_logs_caller_id ON call_logs(caller_id)'))
            c.execute(self.convert_query('CREATE INDEX IF NOT EXISTS idx_assignments_date ON store_assignments(assigned_date)'))
    
    # ============= USER MANAGEMENT =============
    
    def create_user(self, name, role, pin=None, email=None, password_hash=None):
        """Create a new user (caller or admin)"""
        with self.get_connection() as conn:
            c = conn.cursor()
            c.execute('''
                INSERT INTO users (name, role, pin, email, password_hash)
                VALUES (?, ?, ?, ?, ?)
            ''', (name, role, pin, email, password_hash))
            return c.lastrowid
    
    def get_user_by_pin(self, pin):
        """Get user by PIN (for caller login)"""
        with self.get_connection() as conn:
            c = conn.cursor()
            c.execute('SELECT * FROM users WHERE pin = ? AND is_active = 1', (pin,))
            return c.fetchone()
    
    def get_user_by_email(self, email):
        """Get user by email (for admin login)"""
        with self.get_connection() as conn:
            c = conn.cursor()
            c.execute('SELECT * FROM users WHERE email = ? AND is_active = 1', (email,))
            return c.fetchone()
    
    def get_all_callers(self):
        """Get all active callers"""
        with self.get_connection() as conn:
            c = conn.cursor()
            c.execute('SELECT * FROM users WHERE role = "caller" AND is_active = 1')
            return c.fetchall()
    
    # ============= SHOPIFY STORES =============
    
    def add_store(self, name, shop_url, access_token):
        """Add Shopify store"""
        with self.get_connection() as conn:
            c = conn.cursor()
            c.execute('''
                INSERT INTO shopify_stores (name, shop_url, access_token)
                VALUES (?, ?, ?)
            ''', (name, shop_url, access_token))
            return c.lastrowid
    
    def get_all_stores(self):
        """Get all active stores"""
        with self.get_connection() as conn:
            c = conn.cursor()
            c.execute('SELECT * FROM shopify_stores WHERE is_active = 1')
            return c.fetchall()
    
    def get_store_by_id(self, store_id):
        """Get store by ID"""
        with self.get_connection() as conn:
            c = conn.cursor()
            c.execute('SELECT * FROM shopify_stores WHERE id = ?', (store_id,))
            return c.fetchone()
    
    # ============= ORDERS =============
    
    def create_order(self, order_id, store_id, order_type, customer_name, phone, 
                    address, pincode, product_name, price, qty, order_date):
        """Create a new order"""
        with self.get_connection() as conn:
            c = conn.cursor()
            c.execute('''
                INSERT INTO orders (order_id, store_id, order_type, customer_name, phone,
                                  address, pincode, product_name, price, qty, order_date)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (order_id, store_id, order_type, customer_name, phone, address, 
                  pincode, product_name, price, qty, order_date))
            return c.lastrowid
    
    def get_order_by_id(self, order_id):
        """Get order by order_id string"""
        with self.get_connection() as conn:
            c = conn.cursor()
            c.execute('SELECT * FROM orders WHERE order_id = ?', (order_id,))
            return c.fetchone()
    
    def get_orders_by_status(self, status):
        """Get all orders with a specific status"""
        with self.get_connection() as conn:
            c = conn.cursor()
            c.execute('SELECT * FROM orders WHERE status = ?', (status,))
            return c.fetchall()
    
    def get_orders_for_caller(self, caller_id, status='assigned'):
        """Get orders assigned to a specific caller"""
        with self.get_connection() as conn:
            c = conn.cursor()
            c.execute('''
                SELECT * FROM orders 
                WHERE assigned_to = ? AND status = ?
                ORDER BY created_at ASC
            ''', (caller_id, status))
            return c.fetchall()
    
    def assign_order(self, order_id, caller_id):
        """Assign an order to a caller"""
        with self.get_connection() as conn:
            c = conn.cursor()
            c.execute('''
                UPDATE orders 
                SET assigned_to = ?, assigned_at = ?, status = 'assigned', updated_at = ?
                WHERE order_id = ?
            ''', (caller_id, datetime.now(), datetime.now(), order_id))
    
    def update_order_status(self, order_id, status, final_status=None):
        """Update order status"""
        with self.get_connection() as conn:
            c = conn.cursor()
            if final_status:
                c.execute('''
                    UPDATE orders 
                    SET status = ?, final_status = ?, updated_at = ?, completed_at = ?
                    WHERE order_id = ?
                ''', (status, final_status, datetime.now(), datetime.now(), order_id))
            else:
                c.execute('''
                    UPDATE orders 
                    SET status = ?, updated_at = ?
                    WHERE order_id = ?
                ''', (status, datetime.now(), order_id))
    
    def increment_attempts(self, order_id):
        """Increment call attempts for an order"""
        with self.get_connection() as conn:
            c = conn.cursor()
            c.execute('''
                UPDATE orders 
                SET attempts = attempts + 1, updated_at = ?
                WHERE order_id = ?
            ''', (datetime.now(), order_id))
    
    def update_order_edits(self, order_id, customer_name, phone, address, pincode, shopify_order_number=None):
        """Update order with edited details"""
        with self.get_connection() as conn:
            c = conn.cursor()
            c.execute('''
                UPDATE orders 
                SET edited_customer_name = ?, 
                    edited_phone = ?, 
                    edited_address = ?, 
                    edited_pincode = ?,
                    edited_at = ?,
                    shopify_order_number = COALESCE(?, shopify_order_number),
                    updated_at = ?
                WHERE order_id = ?
            ''', (customer_name, phone, address, pincode, datetime.now(), 
                  shopify_order_number, datetime.now(), order_id))
    
    def mark_shopify_synced(self, order_id):
        """Mark order as synced to Shopify"""
        with self.get_connection() as conn:
            c = conn.cursor()
            c.execute('''
                UPDATE orders 
                SET shopify_synced_at = ?
                WHERE order_id = ?
            ''', (datetime.now(), order_id))
    
    # ============= CALL LOGS =============
    
    def create_call_log(self, order_internal_id, caller_id, phone, call_start, 
                       call_end, call_duration, status, notes=None):
        """Create a call log entry"""
        with self.get_connection() as conn:
            c = conn.cursor()
            c.execute('''
                INSERT INTO call_logs (order_id, caller_id, phone_dialed, call_start, 
                                     call_end, call_duration, status, notes)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ''', (order_internal_id, caller_id, phone, call_start, call_end, 
                  call_duration, status, notes))
            return c.lastrowid
    
    def get_call_logs_for_order(self, order_id):
        """Get all call logs for an order"""
        with self.get_connection() as conn:
            c = conn.cursor()
            # First get internal ID
            order = self.get_order_by_id(order_id)
            if not order:
                return []
            c.execute('''
                SELECT * FROM call_logs 
                WHERE order_id = ?
                ORDER BY created_at DESC
            ''', (order['id'],))
            return c.fetchall()
    
    def get_call_logs_for_caller(self, caller_id, limit=100):
        """Get recent call logs for a caller"""
        with self.get_connection() as conn:
            c = conn.cursor()
            c.execute('''
                SELECT * FROM call_logs 
                WHERE caller_id = ?
                ORDER BY created_at DESC
                LIMIT ?
            ''', (caller_id, limit))
            return c.fetchall()
    
    # ============= STORE ASSIGNMENTS =============
    
    def create_assignment(self, store_id, caller_id, assigned_date):
        """Create a store-to-caller assignment"""
        with self.get_connection() as conn:
            c = conn.cursor()
            c.execute('''
                INSERT INTO store_assignments (store_id, caller_id, assigned_date)
                VALUES (?, ?, ?)
                ON CONFLICT (store_id, caller_id, assigned_date) DO NOTHING
            ''', (store_id, caller_id, assigned_date))
            return c.lastrowid
    
    def get_assignments_for_date(self, date):
        """Get all assignments for a specific date"""
        with self.get_connection() as conn:
            c = conn.cursor()
            c.execute('''
                SELECT sa.*, s.name as store_name, u.name as caller_name
                FROM store_assignments sa
                JOIN shopify_stores s ON sa.store_id = s.id
                JOIN users u ON sa.caller_id = u.id
                WHERE sa.assigned_date = ?
            ''', (date,))
            return c.fetchall()
    
    def get_stores_for_caller(self, caller_id, date):
        """Get stores assigned to a caller for a specific date"""
        with self.get_connection() as conn:
            c = conn.cursor()
            c.execute('''
                SELECT s.*
                FROM shopify_stores s
                JOIN store_assignments sa ON s.id = sa.store_id
                WHERE sa.caller_id = ? AND sa.assigned_date = ?
            ''', (caller_id, date))
            return c.fetchall()
    
    # ============= STATS & REPORTS =============
    
    def get_stats_for_caller(self, caller_id, date=None):
        """Get stats for a caller (optionally for a specific date)"""
        with self.get_connection() as conn:
            c = conn.cursor()
            if date:
                c.execute('''
                    SELECT 
                        COUNT(*) as total_calls,
                        SUM(CASE WHEN status LIKE '%confirm%' THEN 1 ELSE 0 END) as confirmed,
                        SUM(CASE WHEN status LIKE '%cancel%' THEN 1 ELSE 0 END) as cancelled,
                        SUM(CASE WHEN status NOT LIKE '%confirm%' AND status NOT LIKE '%cancel%' THEN 1 ELSE 0 END) as pending
                    FROM orders
                    WHERE assigned_to = ? AND DATE(updated_at) = ?
                ''', (caller_id, date))
            else:
                c.execute('''
                    SELECT 
                        COUNT(*) as total_calls,
                        SUM(CASE WHEN status LIKE '%confirm%' THEN 1 ELSE 0 END) as confirmed,
                        SUM(CASE WHEN status LIKE '%cancel%' THEN 1 ELSE 0 END) as cancelled,
                        SUM(CASE WHEN status NOT LIKE '%confirm%' AND status NOT LIKE '%cancel%' THEN 1 ELSE 0 END) as pending
                    FROM orders
                    WHERE assigned_to = ?
                ''', (caller_id,))
            return c.fetchone()
    
    def get_confirmed_cancelled_ids(self):
        """Get list of order IDs that are confirmed or cancelled"""
        with self.get_connection() as conn:
            c = conn.cursor()
            c.execute('''
                SELECT order_id FROM orders 
                WHERE status IN ('confirmed', 'cancelled')
            ''')
            return [row['order_id'] for row in c.fetchall()]


# Initialize database on import
if __name__ == "__main__":
    db = Database()
    print("Database initialized successfully!")
