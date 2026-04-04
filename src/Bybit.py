import websockets
import json
import requests
import sys

sys.stdout.reconfigure(encoding="utf-8")

def getAllAssets(category = "spot", url = "https://api.bybit.com/v5/market/instruments-info"):
    """
    
     category	Product type. "spot","linear","inverse","option"
     
    """
    params = {"category": category}
    r = requests.get(url, params=params).json()
    
    
    

    return [
        s["symbol"]
        for s in r["result"]["list"]
        if s["status"] == "Trading"
    ]
    







async def getBidAskSpot(pairs, queue, publicEndPoint="wss://stream.bybit.com/v5/public/spot"):
    
    streams = [f"orderbook.1.{s}" for s in pairs]
    prices = {"Exchange": "Bybit", "data": {}}
    
    
    
    print(f"Connexion à Bybit WebSocket: {publicEndPoint}...", end="\n")

    async with websockets.connect(publicEndPoint) as ws:
        print(f"Connecté à Bybit avec l'endpoint {publicEndPoint}!", end="\n")
        subscribeMsg = {
            "op": "subscribe",
            "args": streams
        }

        await ws.send(json.dumps(subscribeMsg))

        async for message in ws:
            try:
                data = json.loads(message)

                if "data" not in data:
                    continue
                    
                payload = data["data"]
                
                
                print("------------------------------------------------------ Payload ------------------------------------------------------", end="\n")
                print(payload)
                
                time = data["ts"]
                symbol = payload["s"]
                ask = payload["b"][0][0]
                bid = payload["a"][0][0]
                
                prices["data"][symbol] = {}
                prices["data"][symbol]["t"] = time
                prices["data"][symbol]["a"] = ask
                prices["data"][symbol]["b"] = bid
                
                print("------------------------------------------------------ Prices mis à jour ------------------------------------------------------", end="\n")
                print(prices)
                await queue.put(prices)
            
            except websockets.exceptions.ConnectionClosed as e:
                print(f"Connexion fermée coté Bybit : {e.reason}")
                break
            except Exception as e:
                print(f"Erreur traitement message coté Bybit : {e}")





async def getBidAskPerpetuals(pairs, queue, publicEndPoint="wss://stream.bybit.com/v5/public/linear"):
    
    #streams = [f"orderbook.1.{s}" for s in pairs]
    streams = [f"tickers.{s}" for s in pairs]
    prices = {"Exchange": "Bybit", "data": {}}
    
    
    
    print(f"Connexion à Bybit WebSocket: {publicEndPoint}...", end="\n")

    async with websockets.connect(publicEndPoint) as ws:
        print(f"Connecté à Bybit avec l'endpoint {publicEndPoint}!", end="\n")
        subscribeMsg = {
            "op": "subscribe",
            "args": streams
        }

        await ws.send(json.dumps(subscribeMsg))

        async for message in ws:
            try:
                data = json.loads(message)

                if "data" not in data:
                    continue

                payload = data["data"]
                
                if data["type"] == "snapshot":
                    time = data["ts"]
                    symbol = payload["symbol"]

                    prices["data"][symbol] = {
                        "t": time,
                        "a": float(payload.get("ask1Price")),
                        "b": float(payload.get("bid1Price")),
                        "f_r": float(payload.get("fundingRate")),
                    }

                elif data["type"] == "delta":

                    time = data["ts"]
                    symbol = payload["symbol"]

                    # sécurité si jamais snapshot pas encore reçu
                    if symbol not in prices["data"]:
                        return

                    prices["data"][symbol]["t"] = time

                    if "ask1Price" in payload:
                        prices["data"][symbol]["a"] = float(payload["ask1Price"])

                    if "bid1Price" in payload:
                        prices["data"][symbol]["b"] = float(payload["bid1Price"])

                    if "fundingRate" in payload:
                        prices["data"][symbol]["f_r"] = float(payload["fundingRate"])

                #print("------------------------------------------------------ Prices ------------------------------------------------------", end="\n")
                #print(prices)
                
                await queue.put(prices.copy())
                
                """
                time = data["ts"]
                symbol = payload["symbol"]
                ask = payload["ask1Price"]
                bid = payload["bid1Price"]
                fundingRate = payload["fundingRate"]
                
                prices["data"][symbol] = {}
                prices["data"][symbol]["t"] = time
                prices["data"][symbol]["a"] = ask
                prices["data"][symbol]["b"] = bid
                prices["data"][symbol]["f_r"] = fundingRate
                
                #print("------------------------------------------------------ Prices mis à jour ------------------------------------------------------", end="\n")
                #print(prices)
                """
            except websockets.exceptions.ConnectionClosed as e:
                print(f"Connexion fermée coté Bybit : {e.reason}")
                break
            except Exception as e:
                print(f"Erreur traitement message Bybit : {e}")
                
                
                
                
async def bybitWsSpot(pairs, queue, wsPublicEndPoint = "wss://stream.bybit.com/v5/public/spot"):
    
        try:
            await getBidAskSpot(pairs, queue, wsPublicEndPoint)
        except Exception as e:
            print(f"Échec connexion {wsPublicEndPoint} : {e}!", end="\n")



async def bybitWsPerpetuals(pairs, queue, wsPublicEndPoint = "wss://stream.bybit.com/v5/public/linear"):
    
        try:
            await getBidAskPerpetuals(pairs, queue, wsPublicEndPoint)
        except Exception as e:
            print(f"Échec connexion {wsPublicEndPoint} : {e}!", end="\n")
