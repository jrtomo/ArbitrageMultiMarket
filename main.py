from src import Binance
from src import Bybit
from src import Common
import asyncio



def main():
        
    ecartSeuil = float(input("Entrez l'écart de prix minimum en pourcentage (ex: 0.5 pour 0.5%) : "))
    nbreAssets = int(input("Entrez le nombre d'actifs à analyser (ex: 50. La puissance de traitement de votre machine doit être pris en compte): "))
    #ecartSeuil = 5
    #nbreAssets = 50
    allPerpetualsAssetsBinance = Binance.getAllPerpetualAssets()
    allPerpetualsAssetsBybit = Bybit.getAllAssets("linear")

    mappingAllPerpetualsAssetsBinance = {p.lower(): p for p in allPerpetualsAssetsBinance}
    mappingAllPerpetualsAssetsBybit = {p.lower(): p for p in allPerpetualsAssetsBybit}


    # Listes formatées
    normalizedPerpetualAssetsBinance = list(mappingAllPerpetualsAssetsBinance.keys())
    normalizedPerpetualAssetsBybit = list(mappingAllPerpetualsAssetsBybit.keys())


    intersection = Common.commonAssets([normalizedPerpetualAssetsBinance, normalizedPerpetualAssetsBybit])
    intersection = intersection[:nbreAssets]
    
    print(" ------------------------------------- Actifs analysés ------------------------------------- ")
    print(intersection, end="\n\n")


    

    # Revenir au format de données initial
    intersectionPerpetualAssetsBinanceFormat = [mappingAllPerpetualsAssetsBinance[p] for p in intersection]
    intersectionPerpetualAssetsBybitFormat = [mappingAllPerpetualsAssetsBybit[p] for p in intersection]



    asyncio.run(Common.strategy(intersectionPerpetualAssetsBinanceFormat, intersectionPerpetualAssetsBybitFormat, intersection, mappingAllPerpetualsAssetsBinance, mappingAllPerpetualsAssetsBybit, asyncio.Queue(), ecartSeuil))
    
main()

"""
print(" ------------------------------------- Intersection ------------------------------------- ")
print(intersection)

print(" ------------------------------------- Intersection Binance ------------------------------------- ")
print(intersectionPerpetualAssetsBinanceFormat)


print(" ------------------------------------- Intersection Bybit ------------------------------------- ")
print(intersectionPerpetualAssetsBybitFormat)


print(" ------------------------------------- Mapping Binance ------------------------------------- ")
print(mappingAllPerpetualsAssetsBinance)


print(" ------------------------------------- Mapping Bybit ------------------------------------- ")
print(mappingAllPerpetualsAssetsBybit)
"""







