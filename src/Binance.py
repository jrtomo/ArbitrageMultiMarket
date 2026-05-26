import asyncio
import json
import websockets
import time
import sys
from binance.client import Client

sys.stdout.reconfigure(encoding="utf-8")


class Binance:
    
    API_KEY_LIVE = "1wbE7UZUIMAXJttM7WngfiktToMcfJBXmjX7wDke2viF0Ukg5y4ijdqk7FUAS3m5"
    API_SECRET_LIVE = "m96UUg4g9LLKXgyCAUqwbLHUQ6kjYPPC7NqQVMDHUT8oaHLNIhzYhyMU2QcZqXSt"
    
    
    API_KEY_TESTNET = "XiqvkLbkXrSt0y7h1AlYT0U3csQSU5o6CPgi4j9NLr94cRRuWSMlB7oF520ZHAxI"
    API_SECRET_TESTNET = "oI3FLyQwTrTiPkjA8tAwzbH2FXN9E5pBvmpS1ah1vsakUwRfFaRiJTu8x4rmQrSi"
    
    SPOT_WS_1 = "wss://stream.binance.com:9443/stream?streams="
    SPOT_WS_2 = "wss://stream.binance.com:443/stream?streams="
    PERPETUALS_WS = "wss://fstream.binance.com/public/stream?streams="
    PERPETUALS_WS_TESTNET = "wss://fstream.binancefuture.com/stream?streams="
    
    
    
    
    
    def __init__(self, queue, testNet=True):
        if testNet == False:
            self.client = Client(api_key=self.API_KEY_LIVE, api_secret=self.API_SECRET_LIVE)
        else:
            self.client = Client(api_key=self.API_KEY_TESTNET, api_secret=self.API_SECRET_TESTNET, testnet=True)
        self.queue = queue
        self.symbolsInfos = Binance.getAllPerpetualAssetsInfos()
        self.testNet = testNet
        
        self.client.funding_wallet()
    
    
    @staticmethod
    def getAllSpotAssets():
        infos = Client.get_exchange_info()
        
        allPairs = [
            s["symbol"].lower() # On met en minuscule car dans l'endpoint, tous les symboles doivent être en minuscule
            for s in infos["symbols"]
            if s["status"] == "TRADING" and s["isMarginTradingAllowed"] == True and s["isSpotTradingAllowed"] == True
        ]
        
        return allPairs # On peut aussi retourner la liste complète des symboles avec toutes les infos, mais pour l'instant on ne garde que les symboles tradables et marginables
        
        
    @staticmethod
    def getAllPerpetualAssets():
        client = Client()
        info = client.futures_exchange_info()


        # On recupère les actifs tradables, qui ont pour quote USDT, et qui on un notional minimum de 5 USDT
        perpetualAssets = [
            s["symbol"].lower()
            for s in info["symbols"]
            if s["contractType"] == "PERPETUAL" and s["status"] == "TRADING" and s["quoteAsset"] == "USDT" and "MARKET" in s["orderTypes"] and float(s["filters"][4]["notional"]) <= 5
        ]
        
        return perpetualAssets
    
    
    @staticmethod
    def getAllPerpetualAssetsInfos():
        client = Client()
        info = client.futures_exchange_info()
        
        data = info["symbols"]
        
        data = {t["symbol"]: t for t in data}

        """
        perpetualAssets = [
            s["symbol"].lower()
            for s in info["symbols"]
            if s["contractType"] == "PERPETUAL" and s["status"] == "TRADING"
        ]
        """
        return data
    
    
    def getSymbolMinQty(self, symbol):
        
        return float(self.symbolsInfos[symbol]["filters"][1]["minQty"])
    
    
    def getSymbolMinNotional(self, symbol):
        
        return float(self.symbolsInfos[symbol]["filters"][4]["notional"])
    
    def getQuantityPrecision(self, symbol):
        
        return int(self.symbolsInfos[symbol]["quantityPrecision"])
    
    def getSymbolTickSize(self, symbol):
        return float(self.symbolsInfos[symbol]["filters"][0]["tickSize"])
    
    def setAndGetQuantity(self, symbol, price, quoteQuantity):
        
        quantityByNominal = self.getSymbolMinNotional(symbol=symbol) / price + (2*self.getSymbolTickSize(symbol=symbol))
        quantityByMinQty = self.getSymbolMinQty(symbol=symbol)
        quantityByQuoteQuantity = quoteQuantity/price
        
        
        quantityMax = max(quantityByNominal, quantityByMinQty, quantityByQuoteQuantity)
        
        if quantityMax == quantityByQuoteQuantity:
            quantity = round(quantityByQuoteQuantity, self.getQuantityPrecision(symbol=symbol))
        else:
            quantity = 0
        
        #print(f"Quantity By Nominal : {quantityByNominal} --------------Quantity By Min Qty : {quantityByMinQty} --------- Quantity By Quote: {quantityByQuoteQuantity} -------------- Max: {quantityMax} ")
        return quantity
    

    #print(getAllPerpetualAssets())


    async def getBidAskSpot(self, pairs, publicEndPoint):
            
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
                    
                    await self.queue.put(prices)
                    
                except websockets.exceptions.ConnectionClosed as e:
                    print(f"Connexion fermée coté Binance : {e.reason}")
                    break
                except Exception as e:
                    print(f"Erreur traitement message : {e}")
                    
        

    async def getBidAskPerpetuals(self, pairs):
        
        if self.testNet == False:
            perpetualsWs = self.PERPETUALS_WS
        else:
            perpetualsWs = self.PERPETUALS_WS_TESTNET
        #pairs = pairs[:100] # Binance limite à 10 streams par connexion, on prend les 10 premiers de la liste (on peut aussi faire plusieurs connexions pour couvrir plus de pairs, mais pour l'instant on se limite à 10)
        streams = "/".join(f"{p}@bookTicker" for p in pairs)
        #print(len(pairs))
            
        urlStreams = perpetualsWs + streams
        #urlStreams = "wss://fstream.binance.com/public/stream?streams=btcusdt@bookTicker"
        prices = {"Exchange": "Binance", "data": {}}
        
        #print(urlStreams)

        if self.testNet == False:
            print(f"Binance Live: Connexion à Binance WebSocket: {perpetualsWs}...", end="\n")
        else:
            print(f"Binance Testnet: Connexion à Binance WebSocket: {perpetualsWs}...", end="\n")

        await asyncio.sleep(2)
        async with websockets.connect(urlStreams, ping_interval=30, ping_timeout=60) as ws:
        #async with websockets.connect("wss://fstream.binance.com/ws/btcusdt@bookTicker", ping_interval=25) as ws:
            print(f"Connecté à Binance avec l'endpoint {perpetualsWs}!", end="\n")

            await asyncio.sleep(2)
            async for message in ws: # On écoute les messages du WebSocket en continue jusqu'à ce que la connexion soit fermée (remplace la boucle while True)
                #Equivalent à
                # while True:
                #   message = await ws.recv() mais plus propre et gère mieux les fermetures de connexion
                try:
                    payload = json.loads(message)['data']
                    
                    #print("------------------------------------------------------ Payload ------------------------------------------------------", end="\n")
                    #print(payload)
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
                    
                    await self.queue.put(prices.copy())
                    
                    
                except websockets.exceptions.ConnectionClosed as e:
                    print(f"Connexion fermée coté Binance : {e.reason}")
                    break
                except Exception as e:
                    print(f"Erreur traitement message coté Binance : {e}")

    async def binanceWsPerpetuals(self, pairs):
        if self.testNet == False:
            perpetualsWs = self.PERPETUALS_WS
        else:
            perpetualsWs = self.PERPETUALS_WS_TESTNET
        
        try:
            await self.getBidAskPerpetuals(pairs)
        except Exception as e:
            print(f"Échec connexion ou connexion interrompue {perpetualsWs} : {e}")




    async def binanceWsGetBidAskSpot(self, pairs):
        

        for url in (self.SPOT_WS_1, self.SPOT_WS_2):
            try:
                await self.getBidAskSpot(pairs, publicEndPoint=url)
                break  # si la connexion se termine proprement, on sort
            except Exception as e:
                print(f"Échec connexion {url} : {e}", end="\n")
                print("Tentative URL suivante...\n")

        else:
            print(f"Impossible de se connecter aux WebSockets Binance")


        

    def marketBuyOrder(self, symbol, quantity):
        
        # X/USDT = price <=> 1X = priceUSDT so, aUSDT = a/price X
        #Calcul de la quantité à acheter/vendre pour un montant fixe
        #quantity = self.setAndGetQuantity(symbol=symbol, price=price, quoteQuantity=quoteQuantity )
        
        if quantity == 0:
            print("La quantité souhaitée est inférieure à la quantité minimale")
            sys.exit()
        
        try:
            print(f"Try : {time.time()}")
            order = self.client.futures_market_buy_order(
                symbol=symbol,
                quantity=str(quantity)
            )
            print(f"Try : {time.time()}")
            print(f"Binance: Ordre d'achat de {quantity} {symbol} exécuté : {order}")
            return order
        except Exception as e:
            print(f"Erreur lors de l'exécution de l'ordre : {e}")




    def marketSellOrder(self, symbol, quantity):
        
        #quantity = self.setAndGetQuantity(symbol=symbol, price=price, quoteQuantity=quoteQuantity )
        
        if quantity == 0:
            print("La quantité souhaitée est inférieure à la quantité minimale")
            sys.exit()
        
        try:
            print(f"Try : {time.time()}")
            order = self.client.futures_market_sell_order(
                symbol=symbol,
                quantity=str(quantity)
            )
            print(f"Try : {time.time()}")
            print(f"Binance: Ordre de vente de {quantity} {symbol} exécuté : {order}")
            return order
        except Exception as e:
            print(f"Erreur lors de l'exécution de l'ordre : {e}")

