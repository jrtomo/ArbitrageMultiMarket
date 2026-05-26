from asyncio import tasks
from pybit.unified_trading import HTTP
import time
import websockets
import json
import requests
import sys
import asyncio

sys.stdout.reconfigure(encoding="utf-8")


class Bybit:

    
    
    
    
    API_KEY_LIVE = ""
    API_SECRET_LIVE = ""
    
    
    API_KEY_TESTNET = ""
    API_SECRET_TESTNET = ""
    
    
    
    SPOT_WS = "wss://stream.bybit.com/v5/public/spot"
    PERPETUALS_WS = "wss://stream.bybit.com/v5/public/linear"
    PERPETUALS_WS_TESTNET = "wss://stream-testnet.bybit.com/v5/public/linear"
    
    REST_URL = "https://api.bytick.com"
    REST_URL_TESTNET = "https://api-testnet.bybit.com"
    
    
    def __init__(self, queue, testNet=True):
        if testNet == False:
            self.client = HTTP(api_key=self.API_KEY_LIVE, api_secret=self.API_SECRET_LIVE, testnet=False)
        else:
            self.client = HTTP(api_key=self.API_KEY_TESTNET, api_secret=self.API_SECRET_TESTNET, testnet=True)
        self.queue = queue
        self.symbolsInfos = Bybit.getAllPerpetualAssetsInfos()
        self.testNet = testNet


    @staticmethod
    def getAllAssets(category = "spot", url = "https://api.bybit.com/v5/market/instruments-info"):
        """
        category	Product type. "spot","linear","inverse","option"
        """
        
        params = {"category": category}
        r = requests.get(url, params=params).json()
        
        #print(r)

        return [
            s["symbol"]
            for s in r["result"]["list"]
            if s["contractType"] == "LinearPerpetual" and s["status"] == "Trading"  and s["quoteCoin"] == "USDT" and float(s["lotSizeFilter"]["minNotionalValue"]) <= 5
        ]
        
    
    
    @staticmethod
    def getAllPerpetualAssetsInfos(url = "https://api.bybit.com/v5/market/instruments-info"):
        
        params = {"category": "linear"}
        r = requests.get(url, params=params).json()
        
        data = r["result"]["list"]
        
        data = {t["symbol"]: t for t in data}

        return data

    @staticmethod
    def getPrecision(value):
        """
        Retourne le nombre de décimales significatives.
        """
        value_str = f"{value:.16f}".rstrip('0').rstrip('.')

        if '.' not in value_str:
            return 0

        return len(value_str.split('.')[1])


    def getSymbolMinQty(self, symbol):
        
        return float(self.symbolsInfos[symbol]["lotSizeFilter"]["minOrderQty"])
    
    
    def getSymbolMinNotional(self, symbol):
        
        return float(self.symbolsInfos[symbol]["lotSizeFilter"]["minNotionalValue"])
    
    def getSymbolQtyStep(self, symbol):
        
        return float(self.symbolsInfos[symbol]["lotSizeFilter"]["qtyStep"])
    
    def getQuantityPrecision(self, symbol):
        
        return max(
            self.getPrecision(self.getSymbolMinQty(symbol)),
            self.getPrecision(self.getSymbolQtyStep(symbol))
        )
    
    def getSymbolTickSize(self, symbol):
        return float(self.symbolsInfos[symbol]["priceFilter"]["tickSize"])
        


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
    
    async def getBidAskSpot(self, pairs):
        
        streams = [f"orderbook.1.{s}" for s in pairs]
        prices = {"Exchange": "Bybit", "data": {}}
        
        
        
        print(f"Connexion à Bybit WebSocket: {self.SPOT_WS}...", end="\n")

        async with websockets.connect(self.SPOT_WS) as ws:
            print(f"Connecté à Bybit avec l'endpoint {self.SPOT_WS}!", end="\n")
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
                    
                    
                    #print("------------------------------------------------------ Payload ------------------------------------------------------", end="\n")
                    #print(payload)
                    
                    time = data["ts"]
                    symbol = payload["s"]
                    ask = payload["b"][0][0]
                    bid = payload["a"][0][0]
                    
                    prices["data"][symbol] = {}
                    prices["data"][symbol]["t"] = time
                    prices["data"][symbol]["a"] = ask
                    prices["data"][symbol]["b"] = bid
                    
                    #print("------------------------------------------------------ Prices mis à jour ------------------------------------------------------", end="\n")
                    #print(prices)
                    await self.queue.put(prices)
                
                except websockets.exceptions.ConnectionClosed as e:
                    print(f"Connexion fermée coté Bybit : {e.reason}")
                    break
                except Exception as e:
                    print(f"Erreur traitement message coté Bybit : {e}")


    async def bybitWsSpot(self, pairs, queue):
        
            try:
                await self.getBidAskSpot(pairs, queue, self.SPOT_WS)
            except Exception as e:
                print(f"Échec connexion {self.SPOT_WS} : {e}!", end="\n")



    async def getBidAskPerpetuals(self, pairs):
        
        if self.testNet == False:
            perpetualsWs = self.PERPETUALS_WS
        else:
            perpetualsWs = self.PERPETUALS_WS_TESTNET
        
        streams = [f"tickers.{s}" for s in pairs]
        prices = {"Exchange": "Bybit", "data": {}}
        
        
        
        if self.testNet == False:
            print(f"Bybit Live: Connexion à Binance WebSocket: {perpetualsWs}...", end="\n")
        else:
            print(f"Bybit Testnet: Connexion à Binance WebSocket: {perpetualsWs}...", end="\n")

        async with websockets.connect(perpetualsWs) as ws:
            print(f"Connecté à Bybit avec l'endpoint {perpetualsWs}!", end="\n")
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
                    
                    #print("*********************************************************** Payload ***********************************************************", end="\n")
                    #print(payload)
                    if data["type"] == "snapshot":
                        time = data["ts"]
                        symbol = payload["symbol"]

                        prices["data"][symbol] = {
                            "t": time,
                            "a": float(payload.get("ask1Price")),
                            "b": float(payload.get("bid1Price")),
                            "f_r": float(payload.get("fundingRate")) if payload.get("fundingRate") != "" else 0,
                        }

                    elif data["type"] == "delta":

                        time = data["ts"]
                        symbol = payload["symbol"]

                        # sécurité si jamais snapshot pas encore reçu
                        if symbol not in prices["data"]:
                            return

                        prices["data"][symbol]["t"] = time

                        if "ask1Price" in payload and payload["ask1Price"] != "":
                            prices["data"][symbol]["a"] = float(payload["ask1Price"])

                        if "bid1Price" in payload and payload["bid1Price"] != "":
                            prices["data"][symbol]["b"] = float(payload["bid1Price"])

                        if "fundingRate" in payload and payload["fundingRate"] != "":
                            prices["data"][symbol]["f_r"] = float(payload["fundingRate"])

                    #print("------------------------------------------------------ Prices ------------------------------------------------------", end="\n")
                    #print(data)
                    
                    await self.queue.put(prices.copy())
                except websockets.exceptions.ConnectionClosed as e:
                    print(f"Connexion fermée coté Bybit : {e.reason}")
                    break
                except Exception as e:
                    print(f"Erreur traitement message Bybit : {e}")
                    
                    
                    
                    

    async def bybitWsPerpetuals(self, pairs):

            try:
                await self.getBidAskPerpetuals(pairs)
            except Exception as e:
                print(f"Échec connexion {self.PERPETUALS_WS} : {e}!", end="\n")

    """
    async def bybitWsPerpetuals(pairs, queue, wsPublicEndPoint = "wss://stream.bybit.com/v5/public/linear"):

            try:
                tasks = []

                for group in chunk_pairs(pairs):
                    tasks.append(
                        asyncio.create_task(getBidAskPerpetuals(group, queue))
                    )
                    await asyncio.sleep(1)  # petit délai pour éviter de submerger le serveur avec trop de connexions simultanées
                await asyncio.gather(*tasks)
            except Exception as e:
                print(f"Échec connexion {wsPublicEndPoint} : {e}!", end="\n")
                
                
    """
    
    
    def marketBuyOrder(self, symbol, quantity):
        
        # X/USDT = price <=> 1X = priceUSDT so, aUSDT = a/price X
        #Calcul de la quantité à acheter/vendre pour un montant fixe
        #quantity = self.setAndGetQuantity(symbol=symbol, price=price, quoteQuantity=quoteQuantity )
        
        #if quantity == 0:
        #    print("La quantité souhaitée est inférieure à la quantité minimale")

        
        try:
            print(f"Try : {time.time()}")
            order = self.client.place_order(category="linear", symbol=symbol, side="Buy", orderType="Market", qty=quantity)
            print(f"Try : {time.time()}")
            print(f"Bybit: Ordre d'achat de {quantity} {symbol} exécuté : {order}")
            return order
        except Exception as e:
            print(f"Erreur lors de l'exécution de l'ordre : {e}")
            
    
    def marketSellOrder(self, symbol, price, quantity):
        
        # X/USDT = price <=> 1X = priceUSDT so, aUSDT = a/price X
        #Calcul de la quantité à acheter/vendre pour un montant fixe
        
        #quantity = self.setAndGetQuantity(symbol=symbol, price=price, quoteQuantity=quoteQuantity )
        
        if quantity == 0:
            print("La quantité souhaitée est inférieure à la quantité minimale")
            return 0

        
        try:
            print(f"Try : {time.time()}")
            order = self.client.place_order(category="linear", symbol=symbol, side="Sell", orderType="Market", qty=quantity)
            print(f"Try : {time.time()}")
            print(f"Bybit: Ordre de vente de {quantity} {symbol} exécuté : {order}")
            return order
        except Exception as e:
            print(f"Erreur lors de l'exécution de l'ordre : {e}")
            return 0



    @staticmethod
    def chunk_pairs(pairs, size=10):
        for i in range(0, len(pairs), size):
            yield pairs[i:i + size]
            
