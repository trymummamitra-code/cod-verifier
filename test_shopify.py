#!/usr/bin/env python3
"""Quick test to verify Shopify credentials"""
import os
from dotenv import load_dotenv
import requests

load_dotenv()

stores = [
    ('Store 1', os.getenv('SHOPIFY_STORE_1_NAME'), os.getenv('SHOPIFY_STORE_1_TOKEN')),
    ('Store 2', os.getenv('SHOPIFY_STORE_2_NAME'), os.getenv('SHOPIFY_STORE_2_TOKEN')),
    ('Store 3', os.getenv('SHOPIFY_STORE_3_NAME'), os.getenv('SHOPIFY_STORE_3_TOKEN')),
]

for name, shop, token in stores:
    print(f"\n{name}:")
    print(f"  Shop: {shop}")
    print(f"  Token: {token[:20]}..." if token else "  Token: MISSING")
    
    if shop and token:
        # Test API call
        url = f"https://{shop}.myshopify.com/admin/api/2024-01/shop.json"
        headers = {"X-Shopify-Access-Token": token}
        
        try:
            response = requests.get(url, headers=headers, timeout=10)
            if response.status_code == 200:
                shop_data = response.json()
                print(f"  ✅ Connected! Shop: {shop_data['shop']['name']}")
            else:
                print(f"  ❌ Error {response.status_code}: {response.text[:200]}")
        except Exception as e:
            print(f"  ❌ Connection failed: {e}")
