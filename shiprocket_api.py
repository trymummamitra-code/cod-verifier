"""
Shiprocket API Integration for COD Verification System
Handles: Authentication, Abandoned Cart Fetching
"""

import requests
import time
from datetime import datetime, timedelta
from typing import Dict, List

class ShiprocketAPI:
    BASE_URL = "https://apiv2.shiprocket.in/v1/external"
    
    def __init__(self, email: str, password: str):
        self.email = email
        self.password = password
        self.token = None
        self.token_expiry = None
    
    def _get_headers(self) -> Dict[str, str]:
        """Get headers with auth token"""
        if not self.token or (self.token_expiry and datetime.now() >= self.token_expiry):
            self.authenticate()
        return {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {self.token}"
        }
    
    def authenticate(self) -> bool:
        """Authenticate and get token"""
        url = f"{self.BASE_URL}/auth/login"
        payload = {"email": self.email, "password": self.password}
        
        try:
            response = requests.post(url, json=payload, timeout=30)
            if response.status_code == 200:
                data = response.json()
                self.token = data.get("token")
                self.token_expiry = datetime.now() + timedelta(days=9)
                print(f"✅ Shiprocket authenticated successfully")
                return True
            else:
                raise Exception(f"Authentication failed: {response.text}")
        except Exception as e:
            print(f"❌ Shiprocket authentication error: {e}")
            raise
    
    def _request(self, method: str, endpoint: str, params: Dict = None, retries: int = 3) -> Dict:
        """Make authenticated API request with retry logic"""
        url = f"{self.BASE_URL}/{endpoint}"
        
        for attempt in range(retries):
            try:
                headers = self._get_headers()
                
                if method == "GET":
                    response = requests.get(url, headers=headers, params=params, timeout=30)
                elif method == "POST":
                    response = requests.post(url, headers=headers, json=params, timeout=30)
                else:
                    raise ValueError(f"Unsupported method: {method}")
                
                if response.status_code == 200:
                    return response.json()
                elif response.status_code == 401:
                    # Token expired, re-authenticate
                    self.token = None
                    self.authenticate()
                    continue
                elif response.status_code == 429:
                    # Rate limited
                    wait_time = (attempt + 1) * 2
                    print(f"⏳ Rate limited. Waiting {wait_time}s...")
                    time.sleep(wait_time)
                    continue
                else:
                    print(f"❌ Shiprocket API error: {response.status_code} - {response.text}")
                    return None
            except Exception as e:
                print(f"❌ Request error (attempt {attempt + 1}): {e}")
                if attempt < retries - 1:
                    time.sleep(2)
                    continue
                return None
        
        return None
    
    def fetch_abandoned_carts(self, days: int = 7) -> List[Dict]:
        """
        Fetch abandoned carts/checkouts from Shiprocket
        
        Note: Shiprocket API endpoints to try (in order):
        1. /checkouts - Standard checkout endpoint
        2. /abandoned_carts - If they have a dedicated endpoint
        3. /cart_recovery - Alternative naming
        
        We'll start with the most common pattern and log errors for debugging
        """
        start_date = (datetime.now() - timedelta(days=days)).strftime('%Y-%m-%d')
        end_date = datetime.now().strftime('%Y-%m-%d')
        
        print(f"📥 Attempting to fetch abandoned carts from Shiprocket ({start_date} to {end_date})...")
        
        # Try different possible endpoints
        endpoints_to_try = [
            ('checkouts', {'created_from': start_date, 'created_to': end_date, 'status': 'abandoned'}),
            ('abandoned_carts', {'from_date': start_date, 'to_date': end_date}),
            ('cart_recovery', {'start_date': start_date, 'end_date': end_date}),
            ('orders', {'created_from': start_date, 'created_to': end_date, 'status': 'abandoned'}),
        ]
        
        for endpoint, params in endpoints_to_try:
            print(f"🔍 Trying endpoint: /{endpoint}")
            result = self._request('GET', endpoint, params=params)
            
            if result and isinstance(result, dict):
                # Check for common response structures
                if 'data' in result and result['data']:
                    print(f"✅ Found {len(result['data'])} abandoned carts from /{endpoint}")
                    return self._parse_carts(result['data'])
                elif 'checkouts' in result and result['checkouts']:
                    print(f"✅ Found {len(result['checkouts'])} abandoned carts from /{endpoint}")
                    return self._parse_carts(result['checkouts'])
                elif 'carts' in result and result['carts']:
                    print(f"✅ Found {len(result['carts'])} abandoned carts from /{endpoint}")
                    return self._parse_carts(result['carts'])
                elif 'orders' in result and result['orders']:
                    # Filter for abandoned status
                    abandoned = [o for o in result['orders'] if o.get('status') == 'abandoned']
                    if abandoned:
                        print(f"✅ Found {len(abandoned)} abandoned carts from /{endpoint}")
                        return self._parse_carts(abandoned)
                else:
                    print(f"⚠️ Endpoint /{endpoint} returned data but no recognizable cart structure")
            else:
                print(f"❌ Endpoint /{endpoint} returned no data or error")
        
        print("⚠️ No abandoned carts endpoint found. Shiprocket may not support this feature via API.")
        print("💡 Alternative: You may need to export abandoned carts from Shiprocket dashboard manually,")
        print("   or contact Shiprocket support for API documentation.")
        return []
    
    def _parse_carts(self, carts: List[Dict]) -> List[Dict]:
        """Parse abandoned cart data into standardized format"""
        parsed = []
        
        for cart in carts:
            try:
                # Try to extract common fields (structure may vary)
                parsed_cart = {
                    'cart_id': cart.get('id') or cart.get('checkout_id') or cart.get('cart_token'),
                    'customer_name': self._get_customer_name(cart),
                    'phone': self._get_phone(cart),
                    'email': cart.get('email') or cart.get('customer', {}).get('email'),
                    'address': self._get_address(cart),
                    'pincode': self._get_pincode(cart),
                    'product_name': self._get_product_name(cart),
                    'total_price': float(cart.get('total_price', 0) or cart.get('amount', 0) or 0),
                    'qty': self._get_total_qty(cart),
                    'created_at': cart.get('created_at') or cart.get('abandoned_at'),
                    'store': cart.get('channel_name') or cart.get('store_name') or 'Unknown'
                }
                
                # Only add if we have minimum required fields
                if parsed_cart['cart_id'] and parsed_cart['phone']:
                    parsed.append(parsed_cart)
            except Exception as e:
                print(f"⚠️ Failed to parse cart: {e}")
                continue
        
        return parsed
    
    def _get_customer_name(self, cart: Dict) -> str:
        """Extract customer name from various possible structures"""
        if cart.get('customer_name'):
            return cart['customer_name']
        
        customer = cart.get('customer', {})
        if customer:
            first = customer.get('first_name', '')
            last = customer.get('last_name', '')
            name = f"{first} {last}".strip()
            if name:
                return name
        
        billing = cart.get('billing_address', {})
        if billing:
            first = billing.get('first_name', '')
            last = billing.get('last_name', '')
            name = f"{first} {last}".strip()
            if name:
                return name
        
        return 'Unknown Customer'
    
    def _get_phone(self, cart: Dict) -> str:
        """Extract phone from various possible structures"""
        # Direct phone field
        if cart.get('phone'):
            return str(cart['phone'])
        
        # Customer phone
        customer = cart.get('customer', {})
        if customer.get('phone'):
            return str(customer['phone'])
        
        # Billing address phone
        billing = cart.get('billing_address', {})
        if billing.get('phone'):
            return str(billing['phone'])
        
        # Shipping address phone
        shipping = cart.get('shipping_address', {})
        if shipping.get('phone'):
            return str(shipping['phone'])
        
        return ''
    
    def _get_address(self, cart: Dict) -> str:
        """Extract address from cart"""
        shipping = cart.get('shipping_address', {}) or cart.get('billing_address', {})
        if not shipping:
            return 'No address'
        
        parts = [
            shipping.get('address'),
            shipping.get('address_2'),
            shipping.get('city'),
            shipping.get('state')
        ]
        return ', '.join([p for p in parts if p]) or 'No address'
    
    def _get_pincode(self, cart: Dict) -> str:
        """Extract pincode from cart"""
        shipping = cart.get('shipping_address', {}) or cart.get('billing_address', {})
        return str(shipping.get('pincode', '') or shipping.get('zip', ''))
    
    def _get_product_name(self, cart: Dict) -> str:
        """Extract first product name from cart"""
        items = cart.get('line_items', []) or cart.get('products', []) or cart.get('items', [])
        if items and len(items) > 0:
            first_item = items[0]
            return first_item.get('name', '') or first_item.get('title', '') or 'Unknown Product'
        return 'Cart Items'
    
    def _get_total_qty(self, cart: Dict) -> int:
        """Calculate total quantity in cart"""
        items = cart.get('line_items', []) or cart.get('products', []) or cart.get('items', [])
        total = sum(item.get('quantity', 0) or item.get('qty', 0) for item in items)
        return total or 1


# Test function
if __name__ == '__main__':
    # Test with WMS credentials
    api = ShiprocketAPI(
        email='srreportpullapi@gmail.com',
        password='OMF2**zX2crxtNM*5YmLYrTg#$7fw*Rx'
    )
    
    carts = api.fetch_abandoned_carts(days=7)
    print(f"\n📊 Total carts found: {len(carts)}")
    
    if carts:
        print("\n📋 Sample cart:")
        print(carts[0])
