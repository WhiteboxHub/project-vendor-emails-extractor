import os
import requests
from dotenv import load_dotenv

def test_auth():
    load_dotenv()
    
    base_url = os.getenv('API_BASE_URL')
    email = os.getenv('API_EMAIL')
    password = os.getenv('API_PASSWORD')
    
    print(f"Testing Auth for: {email} at {base_url}")
    print(f"Password length: {len(password) if password else 0}")
    
    token_url = f"{base_url}/api/login"
    data = {
        "username": email,
        "password": password,
        "grant_type": "password"
    }
    
    try:
        response = requests.post(token_url, data=data)
        print(f"Status Code: {response.status_code}")
        
        if response.status_code == 200:
            print("Login Successful!")
            token = response.json().get('access_token')
            print(f"Token: {token[:10]}...")
            
            # Test 1: Check Field Names
            print("\n--- Testing Data Structure ---")
            api_url = f"{base_url}/api/raw-positions/"
            headers = {"Authorization": f"Bearer {token}"}
            params = {"status": "new", "limit": 1}
            
            resp = requests.get(api_url, headers=headers, params=params)
            if resp.status_code == 200:
                data = resp.json()
                results = data.get('results', data.get('data', [])) if isinstance(data, dict) else data
                if results and len(results) > 0:
                    first_item = results[0]
                    print(f"First Record Keys: {list(first_item.keys())}")
                    print(f"First Record Sample: {str(first_item)[:200]}")
                else:
                    print("No records found to inspect.")
            else:
                print(f"Failed to fetch records: {resp.status_code}")

            # Test 2: Probe Bulk Endpoints
            print("\n--- Probing Endpoints ---")
            endpoints_to_test = [
                ("/api/positions/bulk", "POST"),
                ("/api/positions/bulk/", "POST"),
                ("/api/positions", "POST"), # Maybe standard list create?
                ("/api/positions/", "POST"), 
                ("/api/raw-positions/mark-processed", "POST"),
                ("/api/raw-positions/bulk-update", "POST"),
            ]
            
            for ep, method in endpoints_to_test:
                url = f"{base_url}{ep}"
                print(f"Testing {method} {url}...")
                try:
                    # Send empty or dummy data just to check 404 vs 405 vs 400/200
                    r = requests.request(method, url, headers=headers, json={}) 
                    print(f"Result: {r.status_code}")
                except Exception as e:
                    print(f"Error: {e}")
            
        else:
            print("Login Failed!")
            print("Response Headers:", response.headers)
            try:
                print("Response Body:", response.json())
            except:
                print("Response Text:", response.text)
                
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    test_auth()
