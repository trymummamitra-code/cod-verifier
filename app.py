"""
COD Verification System - Main Flask Application
"""

from flask import Flask, render_template, request, jsonify, session, redirect, url_for
from werkzeug.security import generate_password_hash, check_password_hash
from werkzeug.utils import secure_filename
from functools import wraps
from datetime import datetime, timedelta
from dotenv import load_dotenv
import os
import jwt
import csv
import io

from database import Database
from shopify_api import MultiStoreManager
from shiprocket_api import ShiprocketAPI

# Load environment variables
load_dotenv()

app = Flask(__name__)
app.secret_key = os.getenv('FLASK_SECRET_KEY', 'default-secret-key')

# Initialize database
db = Database()

# Initialize Shopify multi-store manager
shopify_stores = [
    {
        'name': 'Indian Goods Hub',
        'shop_name': os.getenv('SHOPIFY_STORE_1_NAME'),
        'access_token': os.getenv('SHOPIFY_STORE_1_TOKEN')
    },
    {
        'name': 'Mummamitra',
        'shop_name': os.getenv('SHOPIFY_STORE_2_NAME'),
        'access_token': os.getenv('SHOPIFY_STORE_2_TOKEN')
    },
    {
        'name': 'Paaltubazaar',
        'shop_name': os.getenv('SHOPIFY_STORE_3_NAME'),
        'access_token': os.getenv('SHOPIFY_STORE_3_TOKEN')
    }
]

shopify_manager = MultiStoreManager(shopify_stores)

# Initialize Shiprocket API
shiprocket_api = ShiprocketAPI(
    email=os.getenv('SHIPROCKET_EMAIL', 'srreportpullapi@gmail.com'),
    password=os.getenv('SHIPROCKET_PASSWORD')
)

# ============= INITIALIZATION =============

def init_default_data():
    """Initialize default stores and users"""
    # Add Shopify stores if not exist
    stores = db.get_all_stores()
    if not stores:
        db.add_store('Indian Goods Hub', 'ec0171-b0.myshopify.com', 
                    os.getenv('SHOPIFY_STORE_1_TOKEN'))
        db.add_store('Mummamitra', '3ac858.myshopify.com',
                    os.getenv('SHOPIFY_STORE_2_TOKEN'))
        db.add_store('Paaltubazaar', '12ufpn-k8.myshopify.com',
                    os.getenv('SHOPIFY_STORE_3_TOKEN'))
        print("✅ Shopify stores initialized")
    
    # Create default admin user if not exist
    admin = db.get_user_by_email('admin@codverifier.com')
    if not admin:
        db.create_user(
            'Admin',
            'admin',
            email='admin@codverifier.com',
            password_hash=generate_password_hash('admin123')
        )
        print("✅ Default admin created (email: admin@codverifier.com, password: admin123)")
    
    # Create 5 default callers if not exist
    callers = db.get_all_callers()
    if not callers:
        for i in range(1, 6):
            db.create_user(
                f'Caller {i}',
                'caller',
                pin=f'{i}111'  # 1111, 2111, 3111, 4111, 5111
            )
        print("✅ 5 default callers created (PINs: 1111, 2111, 3111, 4111, 5111)")

# Initialize default data when module loads
init_default_data()

# ============= AUTHENTICATION =============

def login_required(f):
    """Decorator to require login"""
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if 'user_id' not in session:
            return redirect(url_for('login'))
        return f(*args, **kwargs)
    return decorated_function

def admin_required(f):
    """Decorator to require admin role"""
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if 'user_id' not in session or session.get('role') != 'admin':
            return jsonify({'error': 'Admin access required'}), 403
        return f(*args, **kwargs)
    return decorated_function

# ============= WEB ROUTES =============

@app.route('/')
def index():
    """Landing page"""
    if 'user_id' in session:
        if session.get('role') == 'admin':
            return redirect(url_for('dashboard'))
        else:
            return redirect(url_for('caller_queue'))
    return redirect(url_for('login'))

@app.route('/login', methods=['GET', 'POST'])
def login():
    """Login page (admin or caller)"""
    if request.method == 'POST':
        data = request.json if request.is_json else request.form
        
        # Check if it's PIN login (caller) or email/password (admin)
        pin = data.get('pin')
        email = data.get('email')
        password = data.get('password')
        
        if pin:
            # Caller login
            user = db.get_user_by_pin(pin)
            if user:
                session['user_id'] = user['id']
                session['name'] = user['name']
                session['role'] = user['role']
                
                if request.is_json:
                    return jsonify({'success': True, 'role': 'caller'})
                return redirect(url_for('caller_queue'))
            else:
                if request.is_json:
                    return jsonify({'error': 'Invalid PIN'}), 401
                return render_template('login.html', error='Invalid PIN')
        
        elif email and password:
            # Admin login
            user = db.get_user_by_email(email)
            if user and check_password_hash(user['password_hash'], password):
                session['user_id'] = user['id']
                session['name'] = user['name']
                session['role'] = user['role']
                
                if request.is_json:
                    return jsonify({'success': True, 'role': 'admin'})
                return redirect(url_for('dashboard'))
            else:
                if request.is_json:
                    return jsonify({'error': 'Invalid credentials'}), 401
                return render_template('login.html', error='Invalid credentials')
        
        else:
            if request.is_json:
                return jsonify({'error': 'Missing credentials'}), 400
            return render_template('login.html', error='Missing credentials')
    
    return render_template('login.html')

@app.route('/logout')
def logout():
    """Logout"""
    session.clear()
    return redirect(url_for('login'))

# ============= ADMIN ROUTES =============

@app.route('/dashboard')
@login_required
@admin_required
def dashboard():
    """Admin dashboard - real-time caller activity"""
    # Get all callers
    callers = db.get_all_callers()
    
    # Get stats for each caller
    caller_stats = []
    for caller in callers:
        stats = db.get_stats_for_caller(caller['id'], datetime.now().date())
        caller_stats.append({
            'id': caller['id'],
            'name': caller['name'],
            'stats': stats
        })
    
    # Get today's overall stats
    today = datetime.now().date()
    pending_orders = db.get_orders_by_status('pending')
    assigned_orders = db.get_orders_by_status('assigned')
    confirmed_orders = db.get_orders_by_status('confirmed')
    cancelled_orders = db.get_orders_by_status('cancelled')
    
    return render_template('dashboard.html',
                         callers=caller_stats,
                         pending=len(pending_orders),
                         assigned=len(assigned_orders),
                         confirmed=len(confirmed_orders),
                         cancelled=len(cancelled_orders))

@app.route('/store-assignment', methods=['GET', 'POST'])
@login_required
@admin_required
def store_assignment():
    """Store-to-caller assignment page"""
    if request.method == 'POST':
        data = request.json
        assignments = data.get('assignments', [])
        date = data.get('date', datetime.now().date())
        
        # Clear existing assignments for this date
        # TODO: Add method to clear assignments
        
        # Create new assignments
        for assignment in assignments:
            db.create_assignment(
                assignment['store_id'],
                assignment['caller_id'],
                date
            )
        
        return jsonify({'success': True})
    
    # GET request - show assignment form
    stores = db.get_all_stores()
    callers = db.get_all_callers()
    today_assignments = db.get_assignments_for_date(datetime.now().date())
    
    return render_template('store_assignment.html',
                         stores=stores,
                         callers=callers,
                         assignments=today_assignments)

@app.route('/api/debug/shopify-config')
@login_required
@admin_required
def debug_shopify_config():
    """Debug endpoint to check Shopify configuration"""
    config = []
    for i in range(1, 4):
        store_name = os.getenv(f'SHOPIFY_STORE_{i}_NAME')
        token = os.getenv(f'SHOPIFY_STORE_{i}_TOKEN')
        config.append({
            'store': i,
            'name': store_name,
            'has_token': bool(token),
            'token_preview': token[:20] + '...' if token else None
        })
    return jsonify(config)

@app.route('/fetch-orders', methods=['GET', 'POST'])
@login_required
@admin_required
def fetch_orders():
    """Fetch orders from Shopify/Shiprocket"""
    if request.method == 'POST':
        data = request.json
        source = data.get('source', 'shopify')  # 'shopify' or 'shiprocket'
        days = int(data.get('days', 10))
        
        if source == 'shopify':
            # Get excluded IDs (already confirmed/cancelled)
            exclude_ids = db.get_confirmed_cancelled_ids()
            
            # Fetch from all stores
            all_orders = shopify_manager.fetch_all_stores(days, exclude_ids)
            
            # Save to database
            total_new = 0
            for store_name, orders in all_orders.items():
                # Get store_id from database
                stores = db.get_all_stores()
                store = next((s for s in stores if s['name'] == store_name), None)
                
                if not store:
                    continue
                
                for order in orders:
                    try:
                        db.create_order(
                            order['order_id'],
                            store['id'],
                            'cod',
                            order['customer_name'],
                            order['phone'],
                            order['address'],
                            order['pincode'],
                            order['product_name'],
                            order['price'],
                            order['qty'],
                            order['order_date']
                        )
                        total_new += 1
                    except Exception as e:
                        # Skip duplicates
                        continue
            
            # Auto-distribute to callers
            distribute_orders()
            
            return jsonify({
                'success': True,
                'total_fetched': sum(len(orders) for orders in all_orders.values()),
                'new_orders': total_new
            })
        
        elif source == 'abandoned_cart':
            # Fetch abandoned carts from Shiprocket
            try:
                carts = shiprocket_api.fetch_abandoned_carts(days)
                
                if not carts:
                    return jsonify({
                        'success': True,
                        'total_fetched': 0,
                        'new_orders': 0,
                        'message': 'No abandoned carts found or endpoint not supported by Shiprocket'
                    })
                
                # Save to database
                total_new = 0
                stores = db.get_all_stores()
                
                for cart in carts:
                    try:
                        # Try to match store by name, or use first store as default
                        store = next((s for s in stores if s['name'].lower() in cart.get('store', '').lower()), stores[0])
                        
                        db.create_order(
                            f"CART-{cart['cart_id']}",
                            store['id'],
                            'abandoned_cart',
                            cart['customer_name'],
                            cart['phone'],
                            cart['address'],
                            cart['pincode'],
                            cart['product_name'],
                            cart['total_price'],
                            cart['qty'],
                            cart['created_at']
                        )
                        total_new += 1
                    except Exception as e:
                        # Skip duplicates or errors
                        continue
                
                # Auto-distribute to callers
                distribute_orders()
                
                return jsonify({
                    'success': True,
                    'total_fetched': len(carts),
                    'new_orders': total_new
                })
            except Exception as e:
                return jsonify({
                    'success': False,
                    'error': str(e)
                }), 500
    
    return render_template('fetch_orders.html')

def distribute_orders():
    """Auto-distribute pending orders to assigned callers"""
    today = datetime.now().date()
    assignments = db.get_assignments_for_date(today)
    
    if not assignments:
        return
    
    # Get all pending orders
    pending_orders = db.get_orders_by_status('pending')
    
    if not pending_orders:
        return
    
    # Group assignments by caller
    caller_assignments = {}
    for assignment in assignments:
        caller_id = assignment['caller_id']
        if caller_id not in caller_assignments:
            caller_assignments[caller_id] = []
        caller_assignments[caller_id].append(assignment['store_id'])
    
    # Distribute orders
    for order in pending_orders:
        # Find caller assigned to this order's store
        for caller_id, store_ids in caller_assignments.items():
            if order['store_id'] in store_ids:
                db.assign_order(order['order_id'], caller_id)
                break

@app.route('/orders-list')
@login_required
@admin_required
def orders_list():
    """View all orders with filtering"""
    # Get filter parameters
    order_type = request.args.get('type', '')
    store_id = request.args.get('store', '')
    status = request.args.get('status', '')
    
    # Get all stores for dropdown
    stores = db.get_all_stores()
    
    # Build query
    query = '''
        SELECT o.*, s.name as store_name, u.name as caller_name
        FROM orders o
        LEFT JOIN shopify_stores s ON o.store_id = s.id
        LEFT JOIN users u ON o.assigned_to = u.id
        WHERE 1=1
    '''
    params = []
    
    if order_type:
        query += ' AND o.order_type = ?'
        params.append(order_type)
    
    if store_id:
        query += ' AND o.store_id = ?'
        params.append(int(store_id))
    
    if status:
        query += ' AND o.status = ?'
        params.append(status)
    
    query += ' ORDER BY o.created_at DESC LIMIT 500'
    
    with db.get_connection() as conn:
        orders = conn.execute(query, params).fetchall()
    
    return render_template('orders_list.html',
                         orders=orders,
                         stores=stores,
                         total_orders=len(orders))

@app.route('/upload-carts', methods=['GET', 'POST'])
@login_required
@admin_required
def upload_carts():
    """Upload abandoned carts CSV"""
    if request.method == 'POST':
        # Check if file was uploaded
        if 'file' not in request.files:
            return jsonify({'success': False, 'error': 'No file uploaded'}), 400
        
        file = request.files['file']
        if file.filename == '':
            return jsonify({'success': False, 'error': 'No file selected'}), 400
        
        # Get default store if provided
        default_store_id = request.form.get('default_store')
        
        try:
            # Read CSV file
            stream = io.StringIO(file.stream.read().decode('utf-8'), newline=None)
            csv_reader = csv.DictReader(stream)
            
            # Parse rows
            rows = list(csv_reader)
            if not rows:
                return jsonify({'success': False, 'error': 'CSV file is empty'}), 400
            
            # Get stores for matching
            stores = db.get_all_stores()
            default_store = stores[0] if stores else None
            if default_store_id:
                default_store = next((s for s in stores if s['id'] == int(default_store_id)), default_store)
            
            # Process each row
            imported = 0
            skipped = 0
            preview_data = []
            
            for row in rows:
                try:
                    # Parse row data (flexible column matching)
                    order_data = parse_csv_row(row, default_store, stores)
                    
                    if not order_data:
                        skipped += 1
                        continue
                    
                    # Save to database
                    db.create_order(
                        order_data['order_id'],
                        order_data['store_id'],
                        'abandoned_cart',
                        order_data['customer_name'],
                        order_data['phone'],
                        order_data['address'],
                        order_data['pincode'],
                        order_data['product_name'],
                        order_data['price'],
                        order_data['qty'],
                        order_data['order_date']
                    )
                    imported += 1
                    preview_data.append(order_data)
                    
                except Exception as e:
                    # Skip duplicates or invalid rows
                    skipped += 1
                    continue
            
            # Auto-distribute to callers
            distribute_orders()
            
            return jsonify({
                'success': True,
                'total_rows': len(rows),
                'imported': imported,
                'skipped': skipped,
                'preview': preview_data[:10]
            })
            
        except Exception as e:
            return jsonify({
                'success': False,
                'error': str(e),
                'details': 'Failed to parse CSV. Check file format.'
            }), 500
    
    # GET request - show upload form
    stores = db.get_all_stores()
    return render_template('upload_carts.html', stores=stores)

def parse_csv_row(row: dict, default_store: dict, all_stores: list) -> dict:
    """Parse CSV row with flexible column matching"""
    # Normalize column names (lowercase, remove spaces)
    normalized = {k.lower().strip().replace(' ', '_'): v for k, v in row.items()}
    
    # Find columns (try multiple possible names)
    def get_value(*keys):
        for key in keys:
            if key in normalized and normalized[key]:
                return normalized[key].strip()
        return ''
    
    # Required fields
    order_id = get_value('order_id', 'id', 'order_number', 'cart_id', 'checkout_id')
    customer_name = get_value('customer_name', 'name', 'customer', 'buyer_name')
    phone = get_value('phone', 'mobile', 'contact', 'phone_number', 'customer_phone')
    product_name = get_value('product', 'product_name', 'item', 'product_title', 'sku')
    price = get_value('price', 'amount', 'total', 'total_price', 'value')
    
    # Validate required fields
    if not order_id or not phone:
        return None
    
    # Optional fields
    address = get_value('address', 'shipping_address', 'customer_address', 'delivery_address')
    pincode = get_value('pincode', 'zip', 'postal_code', 'pin')
    qty = get_value('qty', 'quantity', 'items', 'count')
    store_name = get_value('store', 'store_name', 'channel', 'source')
    order_date = get_value('date', 'created_at', 'order_date', 'timestamp')
    
    # Match store
    store = default_store
    if store_name:
        matched = next((s for s in all_stores if s['name'].lower() in store_name.lower()), None)
        if matched:
            store = matched
    
    # Parse numeric values
    try:
        price = float(price.replace(',', '').replace('₹', '').strip()) if price else 0.0
    except:
        price = 0.0
    
    try:
        qty = int(qty) if qty else 1
    except:
        qty = 1
    
    return {
        'order_id': f"CART-{order_id}" if not order_id.startswith('CART-') else order_id,
        'store_id': store['id'] if store else 1,
        'customer_name': customer_name or 'Unknown',
        'phone': phone,
        'address': address or 'No address',
        'pincode': pincode or '',
        'product_name': product_name or 'Abandoned Cart',
        'price': price,
        'qty': qty,
        'order_date': order_date or datetime.now().isoformat(),
        'store': store['name'] if store else 'Unknown'
    }

@app.route('/call-logs')
@login_required
@admin_required
def call_logs():
    """View all call logs"""
    # TODO: Implement call logs viewer
    return render_template('call_logs.html')

@app.route('/reports')
@login_required
@admin_required
def reports():
    """Reports page"""
    # TODO: Implement reports
    return render_template('reports.html')

# ============= CALLER ROUTES =============

@app.route('/caller/queue')
@login_required
def caller_queue():
    """Caller's order queue (for Android app)"""
    caller_id = session.get('user_id')
    
    # Get assigned orders
    orders = db.get_orders_for_caller(caller_id)
    
    # Convert to JSON-friendly format
    orders_list = []
    for order in orders:
        orders_list.append({
            'id': order['id'],
            'order_id': order['order_id'],
            'customer_name': order['customer_name'],
            'phone': order['phone'],
            'address': order['address'],
            'pincode': order['pincode'],
            'product_name': order['product_name'],
            'price': order['price'],
            'qty': order['qty'],
            'attempts': order['attempts']
        })
    
    if request.is_json:
        return jsonify({'orders': orders_list})
    
    return render_template('caller_queue.html', orders=orders_list)

# ============= API ENDPOINTS =============

@app.route('/api/orders/queue/<int:caller_id>')
def api_get_queue(caller_id):
    """API: Get order queue for caller (for Android app)"""
    orders = db.get_orders_for_caller(caller_id)
    
    orders_list = []
    for order in orders:
        orders_list.append({
            'id': order['id'],
            'order_id': order['order_id'],
            'customer_name': order['customer_name'],
            'phone': order['phone'],
            'address': order['address'],
            'pincode': order['pincode'],
            'product_name': order['product_name'],
            'price': order['price'],
            'qty': order['qty'],
            'attempts': order['attempts']
        })
    
    return jsonify({'orders': orders_list})

@app.route('/api/orders/by-status/<int:caller_id>')
def api_get_orders_by_status(caller_id):
    """API: Get orders filtered by status for caller (for Status Dashboard)"""
    status_filter = request.args.get('status', 'all')
    
    # Build query based on status filter
    query = '''
        SELECT o.*, c.name as caller_name
        FROM orders o
        LEFT JOIN users c ON o.assigned_to = c.id
        WHERE o.assigned_to = ?
    '''
    params = [caller_id]
    
    if status_filter == 'pending':
        query += " AND o.status = 'pending'"
    elif status_filter == 'confirmed':
        query += " AND o.status = 'confirmed'"
    elif status_filter == 'cancelled':
        query += " AND o.status = 'cancelled'"
    elif status_filter == 'retry':
        query += " AND o.status IN ('calling', 'assigned')"
    
    query += " ORDER BY o.updated_at DESC LIMIT 500"
    
    with db.get_connection() as conn:
        orders = conn.execute(query, params).fetchall()
    
    orders_list = []
    for order in orders:
        orders_list.append({
            'id': order['id'],
            'order_id': order['order_id'],
            'customer_name': order['customer_name'],
            'phone': order['phone'],
            'address': order['address'],
            'pincode': order['pincode'],
            'product_name': order['product_name'],
            'price': order['price'],
            'qty': order['qty'],
            'status': order['status'],
            'final_status': order['final_status'],
            'attempts': order['attempts'],
            'completed_at': order['completed_at'],
            'updated_at': order['updated_at']
        })
    
    return jsonify({
        'orders': orders_list,
        'total': len(orders_list),
        'filter': status_filter
    })

@app.route('/api/orders/update-status', methods=['POST'])
def api_update_status():
    """API: Update order status (called from Android app)"""
    data = request.json
    
    order_id = data.get('order_id')
    status = data.get('status')
    caller_id = data.get('caller_id')
    call_start = data.get('call_start')
    call_end = data.get('call_end')
    call_duration = data.get('call_duration', 0)
    
    # Validate status
    valid_statuses = [
        'confirm on call', 'confirm on whatsapp call', 
        'confirm on text', 'confirm on whatsapp text',
        'cancel on call', 'cancel on whatsapp call',
        'cancel on whatsapp text', 'cancel on text',
        'not received', 'line busy', 'call forwarded',
        'incoming not available', 'call declined', 'language barrier',
        'call not connected', 'seen no reply', 'undelivered',
        'number not on whatsapp'
    ]
    
    if status.lower() not in [s.lower() for s in valid_statuses]:
        return jsonify({'error': 'Invalid status'}), 400
    
    # Get order
    order = db.get_order_by_id(order_id)
    if not order:
        return jsonify({'error': 'Order not found'}), 404
    
    # Increment attempts
    db.increment_attempts(order_id)
    
    # Create call log
    db.create_call_log(
        order['id'],
        caller_id,
        order['phone'],
        call_start,
        call_end,
        call_duration,
        status
    )
    
    # Update order status based on disposition
    if 'confirm' in status.lower():
        db.update_order_status(order_id, 'confirmed', status)
    elif 'cancel' in status.lower():
        db.update_order_status(order_id, 'cancelled', status)
    else:
        # Retry status - put back in queue
        db.update_order_status(order_id, 'assigned', status)
    
    return jsonify({'success': True})

@app.route('/api/login', methods=['POST'])
def api_login():
    """API: Login endpoint for Android app"""
    data = request.json
    pin = data.get('pin')
    
    if not pin:
        return jsonify({'error': 'PIN required'}), 400
    
    user = db.get_user_by_pin(pin)
    if not user:
        return jsonify({'error': 'Invalid PIN'}), 401
    
    # Generate JWT token
    token = jwt.encode({
        'user_id': user['id'],
        'name': user['name'],
        'role': user['role'],
        'exp': datetime.utcnow() + timedelta(days=30)
    }, app.secret_key, algorithm='HS256')
    
    return jsonify({
        'success': True,
        'token': token,
        'user': {
            'id': user['id'],
            'name': user['name'],
            'role': user['role']
        }
    })

# ============= RUN =============

if __name__ == '__main__':
    port = int(os.getenv('PORT', 5001))
    app.run(host='0.0.0.0', port=port, debug=True)
