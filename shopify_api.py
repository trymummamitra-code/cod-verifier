"""
COD Verification System - Shopify API Integration
Handles order pulling from 3 Shopify stores
"""

import requests
import time
from datetime import datetime, timedelta
from typing import List, Dict, Optional

class ShopifyAPI:
    def __init__(self, shop_name, access_token):
        """
        Initialize Shopify API client
        shop_name: Store name (e.g., 'ec0171-b0')
        access_token: Admin API access token
        """
        self.shop_name = shop_name
        self.access_token = access_token
        self.base_url = f"https://{shop_name}.myshopify.com/admin/api/2024-01"
        self.headers = {
            "X-Shopify-Access-Token": access_token,
            "Content-Type": "application/json"
        }
    
    def _make_request(self, endpoint, params=None, method='GET'):
        """Make API request with error handling"""
        url = f"{self.base_url}{endpoint}"
        
        try:
            if method == 'GET':
                response = requests.get(url, headers=self.headers, params=params, timeout=30)
            else:
                response = requests.post(url, headers=self.headers, json=params, timeout=30)
            
            response.raise_for_status()
            return response.json()
        
        except requests.exceptions.Timeout:
            print(f"❌ Shopify API Timeout for {self.shop_name}")
            raise Exception(f"Shopify API timeout for {self.shop_name}")
        except requests.exceptions.HTTPError as e:
            print(f"❌ Shopify HTTP Error for {self.shop_name}: {e}")
            print(f"Response: {e.response.text if e.response else 'No response'}")
            raise Exception(f"Shopify API error: {e}")
        except requests.exceptions.RequestException as e:
            print(f"❌ Shopify API Error for {self.shop_name}: {e}")
            raise Exception(f"Shopify API request failed: {e}")
    
    def fetch_cod_orders(self, days=10) -> List[Dict]:
        """
        Fetch COD orders from the last N days
        Returns list of order dictionaries
        """
        start_date = (datetime.now() - timedelta(days=days)).isoformat()
        
        params = {
            'status': 'any',  # Get all orders
            'created_at_min': start_date,
            'financial_status': 'pending',  # COD orders typically pending
            'limit': 250
        }
        
        all_orders = []
        page_info = None
        
        print(f"📥 Fetching COD orders from {self.shop_name} (last {days} days)...")
        
        while True:
            if page_info:
                params['page_info'] = page_info
            
            result = self._make_request('/orders.json', params=params)
            
            if not result or 'orders' not in result:
                break
            
            orders = result['orders']
            if not orders:
                break
            
            # Filter for COD orders
            cod_orders = [
                order for order in orders
                if self._is_cod_order(order)
            ]
            
            all_orders.extend(cod_orders)
            
            # Check for next page
            if len(orders) < 250:
                break
            
            # Get page_info from Link header (if available)
            # For simplicity, we'll break after first page for now
            # TODO: Implement pagination properly
            break
        
        print(f"✅ Found {len(all_orders)} COD orders from {self.shop_name}")
        return all_orders
    
    def _is_cod_order(self, order) -> bool:
        """Check if an order is Cash on Delivery"""
        # Check payment gateway names
        if order.get('payment_gateway_names'):
            for gateway in order['payment_gateway_names']:
                if 'cash' in gateway.lower() or 'cod' in gateway.lower():
                    return True
        
        # Check financial status (pending usually means COD)
        if order.get('financial_status') == 'pending':
            return True
        
        return False
    
    def parse_order(self, order) -> Dict:
        """
        Parse Shopify order into our format
        """
        customer = order.get('customer', {})
        shipping = order.get('shipping_address', {})
        line_items = order.get('line_items', [])
        
        # Get first line item details
        first_item = line_items[0] if line_items else {}
        
        # Calculate total quantity
        total_qty = sum(item.get('quantity', 0) for item in line_items)
        
        # Format address
        address_parts = [
            shipping.get('address1', ''),
            shipping.get('address2', ''),
            shipping.get('city', ''),
            shipping.get('province', '')
        ]
        address = ', '.join([p for p in address_parts if p])
        
        # Get phone (try multiple sources)
        phone = (
            customer.get('phone') or 
            shipping.get('phone') or 
            order.get('phone') or 
            ''
        )
        
        return {
            'order_id': order.get('name'),  # e.g., "#1001"
            'customer_name': f"{customer.get('first_name', '')} {customer.get('last_name', '')}".strip(),
            'phone': phone,
            'address': address,
            'pincode': shipping.get('zip', ''),
            'product_name': first_item.get('title', ''),
            'price': float(order.get('total_price', 0)),
            'qty': total_qty,
            'order_date': order.get('created_at')
        }
    
    def bulk_fetch_orders(self, days=10, exclude_ids: List[str] = None) -> List[Dict]:
        """
        Fetch and parse COD orders, excluding already processed ones
        """
        exclude_ids = exclude_ids or []
        
        raw_orders = self.fetch_cod_orders(days)
        parsed_orders = []
        
        for order in raw_orders:
            parsed = self.parse_order(order)
            
            # Skip if already processed
            if parsed['order_id'] in exclude_ids:
                continue
            
            parsed_orders.append(parsed)
        
        return parsed_orders


# Multi-store manager
class MultiStoreManager:
    def __init__(self, stores_config: List[Dict]):
        """
        Initialize with multiple stores
        stores_config: List of {name, shop_name, access_token}
        """
        self.stores = {}
        for store in stores_config:
            self.stores[store['name']] = ShopifyAPI(
                store['shop_name'],
                store['access_token']
            )
    
    def fetch_all_stores(self, days=10, exclude_ids: List[str] = None) -> Dict[str, List[Dict]]:
        """
        Fetch orders from all stores
        Returns dict: {store_name: [orders]}
        """
        all_orders = {}
        
        for store_name, api in self.stores.items():
            orders = api.bulk_fetch_orders(days, exclude_ids)
            all_orders[store_name] = orders
            time.sleep(0.5)  # Rate limiting between stores
        
        return all_orders
    
    def fetch_store(self, store_name: str, days=10, exclude_ids: List[str] = None) -> List[Dict]:
        """Fetch orders from a specific store"""
        if store_name not in self.stores:
            print(f"❌ Store not found: {store_name}")
            return []
        
        return self.stores[store_name].bulk_fetch_orders(days, exclude_ids)


# Convenience function for quick testing
def test_shopify_connection(shop_name, access_token):
    """Test Shopify API connection"""
    api = ShopifyAPI(shop_name, access_token)
    orders = api.fetch_cod_orders(days=1)
    
    if orders:
        print(f"✅ Connection successful! Found {len(orders)} recent COD orders")
        if orders:
            print(f"Sample order: {api.parse_order(orders[0])}")
        return True
    else:
        print("❌ Connection failed or no orders found")
        return False


if __name__ == "__main__":
    # Test with environment variables
    import os
    from dotenv import load_dotenv
    
    load_dotenv()
    
    # Test Store 1 (Indian Goods Hub)
    shop_name = os.getenv('SHOPIFY_STORE_1_NAME')
    token = os.getenv('SHOPIFY_STORE_1_TOKEN')
    
    if shop_name and token:
        print(f"Testing connection to {shop_name}...")
        test_shopify_connection(shop_name, token)
    else:
        print("❌ Missing credentials in .env file")
