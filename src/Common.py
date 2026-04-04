from . import Binance
from . import Bybit
import asyncio

from datetime import datetime


def commonAssets(listOfListsAssets):
    if not listOfListsAssets:
        return []
    
    # On transforme la première liste en set
    intersection = set(listOfListsAssets[0])
    
    # On intersecte avec les suivantes
    for lst in listOfListsAssets[1:]:
        intersection &= set(lst)
    
    return list(intersection)




async def strategy(pairsBinance, pairsBybit, intersection, mappingBinance, mappingBybit, queue, ecartSeuil):
    
    binanceData = {}
    bybitData = {}

    # Lancement concurrent des WS
    taskBinance = asyncio.create_task(
        Binance.binanceWsPerpetuals(pairsBinance, queue)
    )
    
    taskBybit = asyncio.create_task(
        Bybit.bybitWsPerpetuals(pairsBybit, queue)
    )

    while True:
        prices = await queue.get()
        
        if prices["Exchange"] == "Binance":
            binanceData = prices["data"]
        elif prices["Exchange"] == "Bybit":
            bybitData = prices["data"]
            
        #print("------------------------------------------------------ Prices Binance ------------------------------------------------------")
        #print(binanceData)
        #print("------------------------------------------------------ Prices Bybit ------------------------------------------------------")
        #print(bybitData)
        
        #print("------------------------------------------------------ Intersection ------------------------------------------------------")
        #print(intersection)
        
        for pair in intersection:
            if mappingBinance[pair].upper() in binanceData and mappingBybit[pair] in bybitData:
                
                ecartAsk = abs(binanceData[mappingBinance[pair].upper()]['a'] - bybitData[mappingBybit[pair]]['a']) / binanceData[mappingBinance[pair].upper()]['a'] * 100
                ecartBid = abs(binanceData[mappingBinance[pair].upper()]['b'] - bybitData[mappingBybit[pair]]['b']) / binanceData[mappingBinance[pair].upper()]['b'] * 100
                
                #print(f"Écart Ask {ecartAsk}------------------------------ Écart Bid {ecartBid}")
                
                if ecartAsk > ecartSeuil and ecartBid > ecartSeuil: # Seuil de 2% pour les écarts d'ask et bid
                    print(f"{datetime.now().strftime("%H:%M:%S.%f")} Arbitrage potentiel pour {pair.upper()}: Time Binance {datetime.fromtimestamp(binanceData[mappingBinance[pair].upper()]['t']/1000)}......... Time Bybit {datetime.fromtimestamp(bybitData[mappingBybit[pair]]['t']/1000)} :  Écart Ask = {ecartAsk:.2f}%, Écart Bid = {ecartBid:.2f}%")

        queue.task_done()




"""

allPerpetualsAssetsBinance = Binance.getAllPerpetualAssets()
allPairsBybit = Bybit.getAllAssets("linear")

# Créer un mapping
mappingCryptoExchange = {p.replace("_", "").lower(): p for p in allPairsCryptoExchange}
mappingBybit = {p.lower(): p for p in allPairsBybit}

# Liste transformée
normalizedPairsCryptoExchange = list(mappingCryptoExchange.keys())
normalizedPairsBybit = list(mappingBybit.keys())


intersection = list(set(normalizedPairsCryptoExchange) & set(allPairsBinance) & set(normalizedPairsBybit))

#print(" --------------------------------------------------------- Intersection ---------------------------------------------------------")
#print(intersection)


# Revenir au format initial
intersectionCryptoExchangeFormat = [mappingCryptoExchange[p] for p in intersection]
intersectionBybitFormat = [mappingBybit[p] for p in intersection]

print(" --------------------------------------------------------- Intersection crypto exchange format ---------------------------------------------------------")
print(intersectionBybitFormat)


#allPairsCryptoExchange = [p.replace("_", "").lower() for p in allPairsCryptoExchange]
#intersection = list(set(allPairsBinance) & set(allPairsCryptoExchange))


async def strategy(pairsCryptoExchange, pairsBinance, pairsBybit, queue):
    
    binanceData = {}
    CryptoExchangeData = {}
    bybitData = {}

    # Lancement concurrent des WS
    taskCryptoExchange = asyncio.create_task(
        CryptoExchange.cryptoExchangeWs(pairsCryptoExchange, queue)
    )

    taskBinance = asyncio.create_task(
        Binance.binanceWs(pairsBinance, queue)
    )
    
    taskBybit = asyncio.create_task(
        Bybit.bybitWs(pairsBybit, queue)
    )

    while True:
        prices = await queue.get()
        
        
        if prices["Exchange"] == "Binance":
            binanceData = prices["data"]
        elif prices["Exchange"] == "Crypto.com":
            CryptoExchangeData = prices["data"]
        elif prices["Exchange"] == "Bybit":
            bybitData = prices["data"]
            
        #print("------------------------------------------------------ Prices Binance ------------------------------------------------------")
        #print(binanceData)
        #print("------------------------------------------------------ Prices Crypto.com ------------------------------------------------------")
        #print(CryptoExchangeData)
        
        for pair in intersection:
            if pair.upper() in binanceData and mappingCryptoExchange[pair] in CryptoExchangeData and mappingBybit[pair] in bybitData:
                
                min
                
                ecartAsk = abs(binanceData[pair.upper()]['a'] - CryptoExchangeData[mappingCryptoExchange[pair]]['a'])/binanceData[pair.upper()]['a'] * 100
                ecartBid = abs(binanceData[pair.upper()]['b'] - CryptoExchangeData[mappingCryptoExchange[pair]]['b'])/binanceData[pair.upper()]['b'] * 100
                
                
                if ecartAsk > 3 and ecartBid > 3: # Seuil de 2% pour les écarts d'ask et bid
                    print(f"{datetime.now().strftime("%H:%M:%S.%f")} Arbitrage potentiel pour {mappingCryptoExchange[pair]}: Time Binance {datetime.fromtimestamp(binanceData[pair.upper()]['t']/1000)}......... Time Crypto Exchange {datetime.fromtimestamp(CryptoExchangeData[mappingCryptoExchange[pair]]['t']/1000)} :  Écart Ask = {ecartAsk:.2f}%, Écart Bid = {ecartBid:.2f}%")

        queue.task_done()










#print(mapping)
#print("-------------------------------------------------------Pairs communs aux deux Exchanges-------------------------------------------------------")
#print(intersection)



#queue = asyncio.Queue()
#asyncio.run(strategy(intersectionCryptoExchangeFormat, intersection, queue))

























print("----------------------------------------------------------Binance Pairs-----------------------------------------------------------")
print(allPairsBinance)




print("-------------------------------------------------------Crypto.com Exchange Pairs-------------------------------------------------------")
print(allPairsCryptoExchange)






print("-------------------------------------------------------Pairs communs aux deux Exchanges-------------------------------------------------------")
print(intersection)

"""