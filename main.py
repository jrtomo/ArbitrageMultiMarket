from src.Binance import Binance
from src.Bybit import Bybit
from src import Revolut
from src import Common
import asyncio



def main():
        
    #ecartSeuil = float(input("Entrez l'écart de prix minimum en pourcentage (ex: 0.5 pour 0.5%) : "))
    #nbreAssets = int(input("Entrez le nombre d'actifs à analyser (ex: 50. La puissance de traitement de votre machine doit être pris en compte): "))
    ecartSeuil = 3
    #nbreAssets = 10
    allPerpetualsAssetsBinance = Binance.getAllPerpetualAssets()
    allPerpetualsAssetsBybit = Bybit.getAllAssets("linear")

    mappingAllPerpetualsAssetsBinance = {p.lower(): p for p in allPerpetualsAssetsBinance}
    mappingAllPerpetualsAssetsBybit = {p.lower(): p for p in allPerpetualsAssetsBybit}


    # Listes formatées
    normalizedPerpetualAssetsBinance = list(mappingAllPerpetualsAssetsBinance.keys())
    normalizedPerpetualAssetsBybit = list(mappingAllPerpetualsAssetsBybit.keys())


    intersection = Common.commonAssets([normalizedPerpetualAssetsBinance, normalizedPerpetualAssetsBybit])
    intersection = intersection[:100]
    
    print(" ------------------------------------- Intersection ------------------------------------- ")
    print(len(intersection))
    
    
    #intersection = ['morphousdt', 'iostusdt', 'glmusdt', 'fluxusdt', 'fidausdt', 'heiusdt', 'pharosusdt', 'bomeusdt', 'naorisusdt', 'allousdt', 'moveusdt', 'ausdt', 'altusdt', 'auctionusdt', 'geniususdt', 'aliceusdt', 'eulusdt', '1000xecusdt', 'orderusdt', 'opusdt', 'deepusdt', 'rplusdt', 'melaniausdt', 'rsrusdt', 'avntusdt', 'jctusdt', 'mtlusdt', 'bsbusdt', 'eigenusdt', 'icntusdt', 'ognusdt', 'ffusdt', 'polusdt', 'avaxusdt', 'phausdt', 'plumeusdt', 'nilusdt', 'ptbusdt', 'kaiausdt', 'fogousdt', 'sapienusdt', 'berausdt', 'myxusdt', 'roninusdt', 'eduusdt', 'initusdt', 'maviausdt', 'aweusdt', 'bluaiusdt', 'icxusdt', 'partiusdt', 'algousdt', 'inxusdt', 'jasmyusdt', 'nxpcusdt', 'mirausdt', 'mmtusdt', 'arbusdt', 'bnbusdt', 'cakeusdt', 'grtusdt', 'astrusdt', 'espusdt', 'mocausdt', 'ariausdt', 'aixbtusdt', 'metusdt', 'imxusdt', 'meusdt', 'prlusdt', 'ethfiusdt', 'aevousdt', '1000flokiusdt', '2zusdt', 'memeusdt', 'asrusdt', 'injusdt', 'ensousdt', 'billusdt', 'alpineusdt', 'arkusdt', 'c98usdt', 'chipusdt', 'evaausdt', 'highusdt', 'mavusdt', '1000catusdt', 'clousdt', 'fluidusdt', 'bigtimeusdt', 'sagausdt', 'metisusdt', 'renderusdt', 'birbusdt', 'labusdt', 'mboxusdt', 'crvusdt', 'idusdt', 'mantausdt', 'pendleusdt']
    
    
    """
    drift = "driftusdt"
    if drift in intersection:
        print(f"{drift.upper()} est dans l'intersection", end="\n\n")
        intersection.remove(drift)
    """
    
    if not intersection:
        print("Aucun actif en commun trouvé entre Binance et Bybit.")
        return
    
    print(" ------------------------------------- Actifs analysés ------------------------------------- ")
    print(intersection, end="\n\n")


    

    # Revenir au format de données initial
    intersectionPerpetualAssetsBinanceFormat = [mappingAllPerpetualsAssetsBinance[p] for p in intersection]
    intersectionPerpetualAssetsBybitFormat = [mappingAllPerpetualsAssetsBybit[p] for p in intersection]



    asyncio.run(Common.strategy(intersectionPerpetualAssetsBinanceFormat, intersectionPerpetualAssetsBybitFormat, intersection, mappingAllPerpetualsAssetsBinance, mappingAllPerpetualsAssetsBybit, asyncio.Queue(), ecartSeuil, 6))
    
main()






