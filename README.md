# ArbitrageMultiMarket

## Description
Ce projet est un modèle permettant de détecter automatiquement des opportunités d’arbitrage entre contrats perpétuels sur les plateformes Binance et Bybit.

L’objectif est d’exploiter les différences de prix entre les contrats perpétuels de plusieurs exchanges afin d’identifier des opportunités de trading à faible risque, en temps réel.

## Informations importantes
Ces deux plateformes disposent d’au moins 369 actifs perpétuels en commun. Cela maximise les chances d’observer des opportunités d’arbitrage.

Cependant, ce modèle étant conçu en Python (langage de haut niveau), il est en mesure de traiter en continu environ 80 à 90 actifs simultanément.

Pour prendre en charge l’ensemble des actifs, il est nécessaire de passer de Python à un langage intermédiaire ou de bas niveau tel que C++.

## Fonctionnalités
- Connexion en temps réel via WebSocket
- Récupération des prix des contrats perpétuels
- Comparaison multi-marché (perpétuel Binance vs perpétuel Bybit)
- Détection d’opportunités d’arbitrage
- Alertes en temps réel
- Calcul des spreads et profit potentiel

## Stratégies d’arbitrage supportées

Arbitrage inter-exchange perpétuel
Acheter sur Binance Perpétuel et vendre sur Bybit Perpétuel (ou inversement)
Analyse du spread après prise en compte des frais et du funding rate

## Architecture
```
ArbitrageMultiMarket/
│
├── src/
│   ├── Binance/     # Module Binance : WebSocket (perp & spot), récupération des données
│   ├── Bybit/       # Module Bybit
│   ├── Common/      # Logique commune + stratégie d’arbitrage
│
└── main.py          # Point d’entrée
```
## Sources de données
API WebSocket Binance Perpétuel
API WebSocket Bybit Perpétuel
Order books perpétuels en temps réel


## Installations des packages pré-requis
pip install -r requirements.txt


## Utilisation
python main.py


## Prérequis
Python 3.13+
Connexion internet stable

## Exemple d’opportunité détectée

DRIFTUSDT-PERP

<img width="1211" height="150" alt="Screenshot 2026-04-04 131937" src="https://github.com/user-attachments/assets/5a4c0cb8-536c-44ad-8e95-047e20057728" />


Binance Perp (Ask) : 0.03710
Bybit Perp (Bid)   : 0.04044

Spread brut : +9.01%
Frais estimés : 0.20%
Profit net : +8.81%

## Risques
- Latence réseau
- Slippage
- Frais de transaction et funding rate
- Liquidité insuffisante
- Risque d’exécution partielle

## Sécurité
Ne jamais exposer vos clés API
Utiliser des permissions restreintes
Activer les protections IP

# Axes d'amélioration
- Implémenter une exécution atomique ou quasi simultanée
- Module de backtesting sur données historiques perpétuelles
- Ajout d’autres exchanges
- Machine learning pour filtrer les meilleures opportunités




## Licence

MIT License

## Disclaimer

Ce projet est à but éducatif uniquement.
Le trading de contrats perpétuels comporte des risques importants et peut entraîner des pertes.
