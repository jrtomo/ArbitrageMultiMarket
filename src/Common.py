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
    

    while not taskBinance.done() and not taskBybit.done(): # Tant que les tâches sont actives (les WS sont connectés)
        prices = await queue.get()
        
        if prices["Exchange"] == "Binance":
            binanceData = prices["data"]
        elif prices["Exchange"] == "Bybit":
            bybitData = prices["data"]
        
        for pair in intersection:
            if mappingBinance[pair].upper() in binanceData and mappingBybit[pair] in bybitData:
                
                ecartAsk = abs(binanceData[mappingBinance[pair].upper()]['a'] - bybitData[mappingBybit[pair]]['a']) / binanceData[mappingBinance[pair].upper()]['a'] * 100
                ecartBid = abs(binanceData[mappingBinance[pair].upper()]['b'] - bybitData[mappingBybit[pair]]['b']) / binanceData[mappingBinance[pair].upper()]['b'] * 100
                
                
                if ecartAsk > ecartSeuil and ecartBid > ecartSeuil: # Seuil de 2% pour les écarts d'ask et bid
                    print(f"{datetime.now().strftime("%H:%M:%S.%f")} Arbitrage potentiel pour {pair.upper()}: Time Binance {datetime.fromtimestamp(binanceData[mappingBinance[pair].upper()]['t']/1000)}......... Time Bybit {datetime.fromtimestamp(bybitData[mappingBybit[pair]]['t']/1000)} :  Écart Ask = {ecartAsk:.2f}%, Écart Bid = {ecartBid:.2f}%")

        queue.task_done()


    print("Une des connexions WebSocket a été fermée, arrêt de la stratégie.")


