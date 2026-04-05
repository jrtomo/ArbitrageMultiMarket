import asyncio
import json
import websockets
import time
import sys
from binance.client import Client

sys.stdout.reconfigure(encoding="utf-8")

def getAllSpotAssets():
    client = Client()
    infos = client.get_exchange_info()
    
    allPairs = [
        s["symbol"].lower() # On met en minuscule car dans l'endpoint, tous les symboles doivent être en minuscule
        for s in infos["symbols"]
        if s["status"] == "TRADING" and s["isMarginTradingAllowed"] == True and s["isSpotTradingAllowed"] == True
    ]
    
    return allPairs # On peut aussi retourner la liste complète des symboles avec toutes les infos, mais pour l'instant on ne garde que les symboles tradables et marginables
    
    

def getAllPerpetualAssets():
    client = Client()

    info = client.futures_exchange_info()

    perpetualAssets = [
        s["symbol"].lower()
        for s in info["symbols"]
        if s["contractType"] == "PERPETUAL" and s["status"] == "TRADING"
    ]
    
    return perpetualAssets



async def getBidAskSpot(pairs, queue, publicEndPoint="wss://stream.binance.com:9443/stream?streams="):
        
    streams = "/".join(f"{p}@bookTicker" for p in pairs)
        
    urlStreams = publicEndPoint + streams
    prices = {"Exchange": "Binance", "data": {}}
    
    print(f"Connexion à Binance WebSocket: {publicEndPoint}...", end="\n")
    
    #print(urlStreams)


    async with websockets.connect(urlStreams, ping_interval=25) as ws:
    #async with websockets.connect("wss://stream.binance.com:9443/stream?streams=btcusdt@bookTicker", ping_interval=25) as ws:
        print(f"Connecté à Binance avec l'endpoint {publicEndPoint}!", end="\n")

        async for message in ws: # On écoute les messages du WebSocket en continue jusqu'à ce que la connexion soit fermée (remplace la boucle while True)
            #Equivalent à 
            # while True: 
            #   message = await ws.recv() mais plus propre et gère mieux les fermetures de connexion
            try:
                # Gestion ping Binance (payload binaire)
                if isinstance(message, bytes):
                    await ws.pong(message)
                    continue

                data = json.loads(message)
                
                payload = data.get("data")
                
                """
                Payload
                {
                    "u": 400900217,         // order book updateId
                    "s": "BNBUSDT",         // symbol
                    "b": "25.35190000",     // best bid price
                    "B": "31.21000000",     // best bid qty
                    "a": "25.36520000",     // best ask price
                    "A": "40.66000000"      // best ask qty
}
                
                """
                if not payload:
                    continue

                
                symbol = payload["s"]
                ask = float(payload["b"])
                bid = float(payload["a"])
                
                
                prices["data"][symbol] = {}
                prices["data"][symbol]["t"] = time.time_ns() // 1_000_000
                prices["data"][symbol]["a"] = ask
                prices["data"][symbol]["b"] = bid
                
                await queue.put(prices)
                
            except websockets.exceptions.ConnectionClosed as e:
                print(f"Connexion fermée coté Binance : {e.reason}")
                break
            except Exception as e:
                print(f"Erreur traitement message : {e}")
                
    

async def getBidAskPerpetuals(pairs, queue, publicEndPoint="wss://fstream.binance.com/ws/"):
        
    streams = "/".join(f"{p}@bookTicker" for p in pairs)
        
    urlStreams = publicEndPoint + streams
    prices = {"Exchange": "Binance", "data": {}}
    
    #print(urlStreams)
    
    print(f"Connexion à Binance WebSocket: {publicEndPoint}...", end="\n")

    await asyncio.sleep(1)
    async with websockets.connect(urlStreams, ping_interval=60, ping_timeout=60, close_timeout=30) as ws:
    #async with websockets.connect("wss://fstream.binance.com/ws/btcusdt@bookTicker", ping_interval=25) as ws:
        print(f"Connecté à Binance avec l'endpoint {publicEndPoint}!", end="\n")

        await asyncio.sleep(1)
        async for message in ws: # On écoute les messages du WebSocket en continue jusqu'à ce que la connexion soit fermée (remplace la boucle while True)
            #Equivalent à
            # while True:
            #   message = await ws.recv() mais plus propre et gère mieux les fermetures de connexion
            try:
                payload = json.loads(message)
                
                """
                payload
                {
                "e":"bookTicker",			// event type
                "u":400900217,     		// order book updateId
                "E": 1568014460893,  	// event time
                "T": 1568014460891,  	// transaction time
                "s":"BNBUSDT",     		// symbol
                "b":"25.35190000", 		// best bid price
                "B":"31.21000000", 		// best bid qty
                "a":"25.36520000", 		// best ask price
                "A":"40.66000000"  		// best ask qty
                }
                """
                if not payload:
                    continue
                
                symbol = payload["s"]
                ask = float(payload["a"])
                bid = float(payload["b"])
                timeMs = payload["T"]

                
                prices["data"][symbol] = {}
                prices["data"][symbol]["t"] = timeMs
                prices["data"][symbol]["a"] = ask
                prices["data"][symbol]["b"] = bid
                
                #print("------------------------------------------------------ Prices mis à jour ------------------------------------------------------", end="\n")
                #print(prices)
                
                await queue.put(prices.copy())
                
                
            except websockets.exceptions.ConnectionClosed as e:
                print(f"Connexion fermée coté Binance : {e.reason}")
                break
            except Exception as e:
                print(f"Erreur traitement message coté Binance : {e}")





async def binanceWsPerpetuals(pairs, queue, wsPublicEndPoint1="wss://fstream.binance.com/ws/"):
    

        try:
            await getBidAskPerpetuals(pairs, queue, publicEndPoint=wsPublicEndPoint1)
        except Exception as e:
            print(f"Échec connexion ou connexion interrompue {wsPublicEndPoint1} : {e}", end="\n")






async def binanceWsGetBidAskSpot(pairs, queue, wsPublicEndPoint1="wss://stream.binance.com:9443/stream?streams=", wsPublicEndPoint2="wss://stream.binance.com:443/stream?streams="):
    

    for url in (wsPublicEndPoint1, wsPublicEndPoint2):
        try:
            await getBidAskSpot(pairs, queue, publicEndPoint=url)
            break  # si la connexion se termine proprement, on sort
        except Exception as e:
            print(f"Échec connexion {url} : {e}", end="\n")
            print("Tentative URL suivante...\n")

    else:
        print(f"Impossible de se connecter aux WebSockets Binance")

