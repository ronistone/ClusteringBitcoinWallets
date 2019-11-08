from bs4 import BeautifulSoup
import requests
from multiprocessing import Pool

import re
import signal
import time

exchangesPages = ["/wallet/Huobi.com", "/wallet/Bittrex.com", "/wallet/Poloniex.com", "/wallet/BTC-e.com", "/wallet/BTC-e.com-old", "/wallet/Luno.com", "/wallet/LocalBitcoins.com", "/wallet/LocalBitcoins.com-old", "/wallet/Bitstamp.net", "/wallet/MercadoBitcoin.com.br", "/wallet/Cryptsy.com", "/wallet/Bitcoin.de", "/wallet/Cex.io", "/wallet/BtcTrade.com", "/wallet/YoBit.net", "/wallet/OKCoin.com", "/wallet/BTCC.com", "/wallet/BTCC.com-old2", "/wallet/BX.in.th", "/wallet/HitBtc.com", "/wallet/Kraken.com", "/wallet/MaiCoin.com", "/wallet/Bter.com", "/wallet/Bter.com-old2", "/wallet/Bter.com-cold", "/wallet/Hashnest.com", "/wallet/AnxPro.com", "/wallet/BitBay.net", "/wallet/CoinSpot.com.au", "/wallet/Bleutrade.com", "/wallet/Bitfinex.com", "/wallet/Bitfinex.com-old2", "/wallet/Matbea.com", "/wallet/Bit-x.com", "/wallet/VirWoX.com", "/wallet/Paxful.com", "/wallet/BitBargain.co.uk", "/wallet/SpectroCoin.com", "/wallet/CoinHako.com", "/wallet/Cavirtex.com", "/wallet/C-Cex.com", "/wallet/FoxBit.com.br", "/wallet/FoxBit.com.br-cold", "/wallet/FoxBit.com.br-cold-old", "/wallet/Vircurex.com", "/wallet/BitVC.com", "/wallet/Exmo.com", "/wallet/Btc38.com", "/wallet/CoinMotion.com", "/wallet/TheRockTrading.com", "/wallet/TheRockTrading.com-old", "/wallet/Igot.com", "/wallet/SimpleCoin.cz", "/wallet/SimpleCoin.cz-old2", "/wallet/SimpleCoin.cz-old4", "/wallet/FYBSG.com", "/wallet/BlockTrades.us", "/wallet/CampBX.com", "/wallet/CoinTrader.net", "/wallet/Bitcurex.com", "/wallet/Coinmate.io", "/wallet/Korbit.co.kr", "/wallet/Vaultoro.com", "/wallet/Exchanging.ir", "/wallet/796.com", "/wallet/HappyCoins.com", "/wallet/BtcMarkets.net", "/wallet/ChBtc.com", "/wallet/Coins-e.com", "/wallet/LiteBit.eu", "/wallet/UrduBit.com-cold", "/wallet/CoinCafe.com", "/wallet/BTradeAustralia.com", "/wallet/BTradeAustralia.com-incoming", "/wallet/MeXBT.com", "/wallet/Coinomat.com", "/wallet/OrderBook.net", "/wallet/LakeBTC.com", "/wallet/BitKonan.com", "/wallet/QuadrigaCX.com", "/wallet/Banx.io", "/wallet/Banx.io-old2", "/wallet/CleverCoin.com", "/wallet/Gatecoin.com", "/wallet/Indacoin.com", "/wallet/CoinArch.com", "/wallet/BitcoinVietnam.com.vn", "/wallet/CoinChimp.com", "/wallet/Cryptonit.net", "/wallet/Exchange-Credit.ru", "/wallet/Bitso.com", "/wallet/Coinimal.com", "/wallet/EmpoEX.com", "/wallet/Ccedk.com", "/wallet/UseCryptos.com", "/wallet/Coinbroker.io", "/wallet/Zyado.com", "/wallet/1Coin.com", "/wallet/Coingi.com", "/wallet/ExchangeMyCoins.com"]
BASE_URL = "https://www.walletexplorer.com"

WORKERS = len(exchangesPages)
WAIT_TIME = 1

class ParserExchange:

    def __init__(self, addressTable, exchangeTable):
        self.addressTable = addressTable
        self.exchangeTable = exchangeTable

    @staticmethod
    def init_worker():
        signal.signal(signal.SIGINT, signal.SIG_IGN)

    def map_all_address_to_exchange(self):
        pool = Pool(WORKERS)#, self.init_worker)

        pool.map(self.map_address_to_exchange, exchangesPages)

    def map_address_to_exchange(self, exchange):
        connect_db(self)
        response = requests.get(BASE_URL + exchange + "/addresses")

        if response and response.content and len(response.content):
            lastPage = self.find_last_page(response)
            print(lastPage)
            self.process_page(response.content, exchange)

            if lastPage > 1:
                for i in range(2, lastPage+1):
                    before = time.time()
                    response = requests.get(BASE_URL + exchange + "/addresses?page=" + str(i))
                    print(BASE_URL + exchange + "/addresses?page=" + i)
                    if response and response.content:
                        self.process_page(response.content, exchange)

                    diff = time.time() - before
                    if diff > 0 and diff < WAIT_TIME:
                        print("sleeping")
                        time.sleep(diff)

    def find_last_page(self, response):
        soup = BeautifulSoup(response.content, 'html.parser')
        pages = soup.find_all("div", {"class": "paging"})
        if pages and len(pages):
            links = pages[0].find_all('a')
            if links and len(links) > 1:
                # print(links)
                url = links[0].get('href') if links[0].text == 'Last' else links[1].get('href')
                url = "" if url is None else url
                if len(url) > 1:
                    # print(url)
                    url = url.split('page=')[1]
                    return int(url)
        return 0

    def process_page(self, content, exchangeName):
        soup = BeautifulSoup(content, 'html.parser')
        addresses = []
        for td in soup.find_all('td'):
            addresses.extend(td.find_all('a', attrs={'href': re.compile("^/address/")}))

        for address in addresses:
            find = self.addressTable.find_one({ 'address': address.text })
            if find:
                self.exchangeTable.insert({
                    'address': address.text,
                    'exchange': exchangeName
                })

def connect_db(objectClass):
    from pymongo import MongoClient

    client = MongoClient()

    databaseTeste = client['bitcoin-teste']
    databaseReal = client['bitcoin-cluster']
    objectClass.addressTable = databaseReal['AddressEntity']
    objectClass.exchangeTable = databaseTeste['exchange']

if __name__ == '__main__':


    parser = ParserExchange(None, None)
    parser.map_all_address_to_exchange()
