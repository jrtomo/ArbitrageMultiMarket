from .Binance import Binance
from .Bybit import Bybit
from . import Revolut
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




async def strategy(pairsBinance, pairsBybit, intersection, mappingBinance, mappingBybit, queue, ecartSeuil, quoteQuantity):
    
    binance = Binance(queue, testNet=False)
    bybit = Bybit(queue, testNet=False)
    
    binanceData = {}
    bybitData = {}

    # Lancement concurrent des WS
    taskBinance = asyncio.create_task(
        binance.binanceWsPerpetuals(pairsBinance)
    )
    
    taskBybit = asyncio.create_task(
        bybit.bybitWsPerpetuals(pairsBybit)
    )
    

    while not taskBinance.done() and not taskBybit.done(): # Tant que les tâches sont actives (les WS sont connectés)
        prices = await queue.get()
        
        if prices["Exchange"] == "Binance":
            binanceData = prices["data"]
        elif prices["Exchange"] == "Bybit":
            bybitData = prices["data"]
        
        #print("------------------------------------------------------ Prices Binance ------------------------------------------------------")
        #print(binanceData)
        #print("------------------------------------------------------ Prices Bybit ------------------------------------------------------")
        #print(bybitData)
        
        
        for pair in intersection:
            pair = pair.upper()
            if pair in binanceData and pair in bybitData:
                
                ecartAskAbs = abs(binanceData[pair]['a'] - bybitData[pair]['a']) / min(binanceData[pair]['a'], bybitData[pair]['a']) * 100
                ecartBidAbs = abs(binanceData[pair]['b'] - bybitData[pair]['b']) / min(binanceData[pair]['b'], bybitData[pair]['b']) * 100
                
                
                if ecartAskAbs > ecartSeuil and ecartBidAbs > ecartSeuil: # Seuil de 2% pour les écarts d'ask et bid
                    # Passer les deux ordres, achats sur l'exchange le moins cher et ventes sur l'exchange le plus cher
                    # Calculer le profit potentiel en fonction des frais de trading et des prix exécutés
                    ecartAsk = (binanceData[pair]['a'] - bybitData[pair]['a']) / min(binanceData[pair]['a'], bybitData[pair]['a']) * 100
                    ecartBid = (binanceData[pair]['b'] - bybitData[pair]['b']) / min(binanceData[pair]['b'], bybitData[pair]['b']) * 100

                    if ecartAsk > 0 and ecartBid > 0:
                        #Achat sur Bybit (sous évalué) et vente sur Binance (sur évalué)
                        quantity = setAndGetCommonQuantity(binance=binance, bybit=bybit, symbol=pair, binancePrice=binanceData[pair]['b'], bybitPrice=bybitData[pair]['a'], quoteQuantity=quoteQuantity)
                        if quantity != 0:
                            print("----------------------------------------------------------------------------------------------------------------------------------------------------------------------")
                            print("Achat sur Bybit (sous évalué) et vente sur Binance (sur évalué)")
                            #binanceOrder = binance.marketSellOrder(symbol=pair, quantity=quantity)
                            #bybitOrder = bybit.marketBuyOrder(symbol=pair, quantity=quantity) 
                            print(f"{datetime.now().strftime("%H:%M:%S.%f")} Arbitrage potentiel pour {pair}: Time Binance {datetime.fromtimestamp(binanceData[pair]['t']/1000)}......... Time Bybit {datetime.fromtimestamp(bybitData[pair]['t']/1000)} :  Écart Ask = {ecartAsk:.2f}%, Écart Bid = {ecartBid:.2f}%")

                        else:
                            print("----------------------------------------------------------------------------------------------------------------------------------------------------------------------")
                            print("Quantity = 0")
                            print(f"{datetime.now().strftime("%H:%M:%S.%f")} Arbitrage potentiel pour {pair}: Time Binance {datetime.fromtimestamp(binanceData[pair]['t']/1000)}......... Time Bybit {datetime.fromtimestamp(bybitData[pair]['t']/1000)} :  Écart Ask = {ecartAsk:.2f}%, Écart Bid = {ecartBid:.2f}%")
                            continue
                        
                    if ecartAsk < 0 and ecartBid < 0:
                        #Achat sur Binance (sous évalué) et vente sur Bybit (sur évalué)
                        quantity = setAndGetCommonQuantity(binance=binance, bybit=bybit, symbol=pair, binancePrice=binanceData[pair]['a'], bybitPrice=bybitData[pair]['b'], quoteQuantity=quoteQuantity)
                        if quantity != 0:
                            print("----------------------------------------------------------------------------------------------------------------------------------------------------------------------")
                            print("Achat sur Binance (sous évalué) et vente sur Bybit (sur évalué)")
                            #binanceOrder = binance.marketBuyOrder(symbol=pair, quantity=quantity)
                            #bybitOrder = bybit.marketSellOrder(symbol=pair, quantity=quantity)
                            print(f"{datetime.now().strftime("%H:%M:%S.%f")} Arbitrage potentiel pour {pair}: Time Binance {datetime.fromtimestamp(binanceData[pair]['t']/1000)}......... Time Bybit {datetime.fromtimestamp(bybitData[pair]['t']/1000)} :  Écart Ask = {ecartAsk:.2f}%, Écart Bid = {ecartBid:.2f}%")

                        else:
                            print("----------------------------------------------------------------------------------------------------------------------------------------------------------------------")
                            print("Quantity = 0")
                            print(f"{datetime.now().strftime("%H:%M:%S.%f")} Arbitrage potentiel pour {pair}: Time Binance {datetime.fromtimestamp(binanceData[pair]['t']/1000)}......... Time Bybit {datetime.fromtimestamp(bybitData[pair]['t']/1000)} :  Écart Ask = {ecartAsk:.2f}%, Écart Bid = {ecartBid:.2f}%")
                            continue
                    
                    
                    #print(f"Binance data : {binanceData[mappingBinance[pair].upper()]}")
                    #print(f"Bybit data : {bybitData[mappingBybit[pair]]}")
                    #print(f"{datetime.now().strftime("%H:%M:%S.%f")} Arbitrage potentiel pour {pair.upper()}: Time Binance {datetime.fromtimestamp(binanceData[mappingBinance[pair].upper()]['t']/1000)}......... Time Bybit {datetime.fromtimestamp(bybitData[mappingBybit[pair]]['t']/1000)} :  Écart Ask = {ecartAsk:.2f}%, Écart Bid = {ecartBid:.2f}%")
        
        queue.task_done()


    print("Une des connexions WebSocket a été fermée, arrêt de la stratégie.")





def setAndGetCommonQuantity(binance:Binance, bybit:Bybit, symbol, binancePrice, bybitPrice, quoteQuantity):
    
    binanceQuantity = binance.setAndGetQuantity(symbol=symbol, price=binancePrice, quoteQuantity=quoteQuantity)
    bybitQuantity = bybit.setAndGetQuantity(symbol=symbol, price=bybitPrice, quoteQuantity=quoteQuantity)
    
    if binanceQuantity == 0 or bybitQuantity == 0:
        print("Actif trop cher pour le capital d'investissement souhaité!")
        return 0
    else:
        return max(binanceQuantity, bybitQuantity)
