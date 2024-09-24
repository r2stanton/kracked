import asyncio
import websockets
import json
import hmac
import base64
import hashlib
import time
import urllib.parse
import toml

with open("/home/alg/.api.toml", "r") as fil:
    data = toml.load(fil)

API_KEY = data['kraken_api']
API_SECRET = data['kraken_sec']
async def get_ws_token():
    endpoint = "/0/private/GetWebSocketsToken"
    url = "https://api.kraken.com" + endpoint
    nonce = str(int(time.time() * 1000))
    
    post_data = {
        "nonce": nonce,
    }
    
    post_data = urllib.parse.urlencode(post_data)
    
    # Create signature
    encoded = (str(nonce) + post_data).encode()
    message = endpoint.encode() + hashlib.sha256(encoded).digest()
    mac = hmac.new(base64.b64decode(API_SECRET), message, hashlib.sha512)
    sigdigest = base64.b64encode(mac.digest())
    
    headers = {
        'API-Key': API_KEY,
        'API-Sign': sigdigest.decode()
    }
    
    async with aiohttp.ClientSession() as session:
        async with session.post(url, data=post_data, headers=headers) as resp:
            if resp.status == 200:
                result = await resp.json()
                return result['result']['token']
            else:
                raise Exception(f"HTTP error: {resp.status}")

async def subscribe_to_ticker():
    token = await get_ws_token()
    uri = "wss://ws-auth.kraken.com"
    
    async with websockets.connect(uri) as websocket:
        await websocket.send(json.dumps({
            "event": "subscribe",
            "subscription": {
                "name": "ticker",
                "token": token
            },
            "pair": ["XBT/USD"]  # Example pair, you can change or add more
        }))
        
        while True:
            response = await websocket.recv()
            print(response)

async def main():
    await subscribe_to_ticker()

if __name__ == "__main__":
    asyncio.run(main())
