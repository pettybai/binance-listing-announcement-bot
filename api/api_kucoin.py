import base64
import hashlib
import hmac
import json
import requests
import threading
import time
import uuid

from websocket import create_connection

from logins import personal_keys
from utils import round_nearest, uprint

from api.api_general import GeneralAPI


class KucoinAPI(GeneralAPI):
    def __init__(self):
        super().__init__()
        self.api_url = 'https://api.kucoin.com'
        self.exch_name = 'Kucoin'
        self.session = requests.Session()

        self.api_secret = personal_keys[self.exch_name]['api_secret']
        self.api_key = personal_keys[self.exch_name]['api_key']
        self.api_passphrase = personal_keys[self.exch_name]['api_passphrase']

        self.passphrase = base64.b64encode(
                                hmac.new(self.api_secret.encode('utf-8'),
                                         self.api_passphrase.encode('utf-8'),
                                         hashlib.sha256).digest())
        # a set of pairs
        self.update_pairs()

        # a dictionary of symbol to name
        self.update_tokens()

        self.pairs_separator = '-'

        self.support_websocket = True
        self.has_token_fullnames = True

        self.valid_code_on_limit_order = '200000'

    def update_pairs(self):
        """
        Generate a set of pairs (like 'USDT-BTC') on the exchange, and a dict
        of specifications for each pair (step size, min and max order size)
        """
        method = 'GET'
        endpoint = '/api/v1/symbols'

        response = self.session.request(method,
                                        url=self.api_url + endpoint)

        if response.status_code != 200:
            uprint(f'[{self.exch_name}] ERROR: failure in request '
                   f'in `update_pairs` with message: {response.text}')
            return False

        resp_json = response.json()

        pairs = set()
        pairs_specs = dict()
        n_pairs = len(resp_json['data'])
        for i in range(n_pairs):
            pairs.add(resp_json['data'][i]['symbol'])
            pairs_specs[resp_json['data'][i]['symbol']] = {
                'baseIncrement': float(resp_json['data'][i]['baseIncrement']),
                'quoteIncrement': float(resp_json['data'][i]['quoteIncrement']),
                'priceIncrement': float(resp_json['data'][i]['priceIncrement'])
                }

        self.pairs = pairs
        self.pairs_specs = pairs_specs

    def update_tokens(self):
        """
        Generate a dictionary mapping token symbols to token names on the exchange
        """
        method = 'GET'
        endpoint = '/api/v1/currencies'

        response = self.session.request(method,
                                        url=self.api_url + endpoint)

        if response.status_code != 200:
            uprint(f'[{self.exch_name}] ERROR: failure in request '
                   f'in `update_tokens` with message: {response.text}')
            return False

        res = dict()
        resp_json = response.json()

        #TODO keep not only old name
        # careful that `currency` holds the historical token symbol,
        # while `name` holds the current token symbol
        n_tokens = len(resp_json['data'])
        for i in range(n_tokens):
            res[resp_json['data'][i]['currency']] = resp_json['data'][i]['fullName']

        self.listed_tokens = res

    def get_headers(self, full_endpoint, data_string=''):
        """
        Return a header needed to include in a request
        """
        now = int(time.time() * 1000)
        str_to_sign = str(now) + full_endpoint + data_string
        signature = base64.b64encode(hmac.new(self.api_secret.encode('utf-8'),
                                            str_to_sign.encode('utf-8'),
                                            hashlib.sha256).digest())
        headers = {
            'Content-Type': 'application/json',
            'KC-API-SIGN': signature,
            'KC-API-TIMESTAMP': str(now),
            'KC-API-KEY': self.api_key,
            'KC-API-PASSPHRASE': self.passphrase,
            'KC-API-KEY-VERSION': '2'
        }
        return headers

    def get_price_sell(self, token_sell, token_buy):
        """
        Denomination in the `token_buy`

        Get highest price we can sell `token_sell` at, i.e. highest bid
        """
        pair_name, good_order = self.find_pair_from_tokens(token_sell, token_buy)

        if pair_name is None:
            return None

        method = 'GET'
        endpoint = '/api/v1/market/orderbook/level1?symbol=' + pair_name

        response = self.session.request(method,
                                        url=self.api_url + endpoint)

        if response.status_code != 200:
            uprint(f'[{self.exch_name}: {pair_name}] ERROR: failure in request '
                   f'in `get_price_sell` with message: {response.text}')
            return False

        resp_json = response.json()

        if good_order:  # A/B with token_sell = A, token_buy = B
            return float(resp_json['data']['bestBid'])
        else:
            return 1 / float(resp_json['data']['bestAsk'])

    def order_limit(self, token_sell, token_buy, max_impact,
                    amount_sell=None, amount_buy=None, time_in_force='GTC'):
        pair_name, good_order = self.find_pair_from_tokens(token_sell, token_buy)

        if pair_name is None:
            return False

        if amount_sell is None and amount_buy is None:
            uprint(f'[{self.exch_name}: {pair_name}] No amount specified for '
                   f'the order.')
            return False

        if amount_sell and amount_buy:
            uprint(f'[{self.exch_name}: {pair_name}] Both sell/buy amounts were '
                   f'specified for the order.')
            return False

        # we want the denomination of `ref_price` to be in the base currency
        # this is the sell price of A for B in pair A/B (e.g. BTC/USDT),
        # denominated in B
        if good_order:
            ref_price = self.get_price_sell(token_sell=token_sell,
                                            token_buy=token_buy)
        else:
            ref_price = self.get_price_sell(token_sell=token_buy,
                                            token_buy=token_sell)

        (amount_sell,
         amount_buy,
         execution_price,
         base_amount,
         printing) = self._round_to_increment(
                                    pair_name,
                                    good_order,
                                    amount_sell,
                                    amount_buy,
                                    ref_price,
                                    max_impact=max_impact
                                    )

        method = 'POST'
        endpoint = '/api/v1/orders'

        data = {'clientOid': uuid.uuid4().hex,  # use random uuid
                'symbol': pair_name,
                'type': 'limit',
                'timeInForce': time_in_force}

        data['price'] = execution_price
        if good_order: # note A = token_sell, B = token_buy, pair A/B
            if amount_sell is not None:
                data['side'] = 'sell'
                data['size'] = base_amount
            elif amount_buy is not None:
                data['side'] = 'sell'
                data['size'] = base_amount
        else:
            if amount_sell is not None:
                data['side'] = 'buy'
                data['size'] = base_amount
            elif amount_buy is not None:
                data['side'] = 'buy'
                data['size'] = base_amount

        data_jsoned = json.dumps(data)

        headers = self.get_headers(full_endpoint=method + endpoint,
                                   data_string=data_jsoned)
        response = self.session.request(method,
                                        url=self.api_url + endpoint,
                                        data=data_jsoned,
                                        headers=headers)

        resp_json = response.json()

        uprint(printing)

        uprint(f'[{self.exch_name}: {pair_name}] Specs order (after rounding):\n'
               f'       good_order: {good_order}\n'
               f'       amount_sell: {amount_sell}\n'
               f'       amount_buy: {amount_buy}\n'
               f'       base_amount: {base_amount}\n'
               f'       execution_price: {execution_price}\n'
               f'       ref_price: {ref_price}\n)')

        json_pretty = json.dumps(data, separators=(',', ':'), indent=4)
        uprint(f'[{self.exch_name}: {pair_name}] data \n'
               f'   {json_pretty}')


        if response.status_code != 200:
            uprint(f'[{self.exch_name}: {pair_name}] ERROR: failure in request '
                   f'in `order_limit` with message: {resp_json["msg"]}')

        if resp_json['code'] != '200000':
            uprint(f'[{self.exch_name}: {pair_name}] FAILED '
                   f'({resp_json["code"]}: {resp_json})')
        else:
            uprint(f'[{self.exch_name}: {pair_name}] Maximum price set at '
                   f'{execution_price:.4f} (price per base currency)')
            uprint(f'[{self.exch_name}: {pair_name}] SUCCESS sell {token_sell}.')

            details = self.get_order_details(resp_json['data']['orderId'])['data']
            deal_size = float(details['dealSize'])  # base
            deal_funds = float(details['dealFunds'])  # quote
            deal_size_asked = float(details['size'])

            if deal_funds == 0 and deal_size == 0:
                uprint(f'[{self.exch_name}: {pair_name}] Empty fill for the pair'
                       f' {pair_name}, try to increase `max_impact` (reference '
                       f' price was {ref_price:.4f} in the base currency).')
            elif deal_size != deal_size_asked:
                uprint(f'[{self.exch_name}: {pair_name}] Partial fill of '
                       f'{deal_size:.4f} - {deal_funds:.4f} for the pair '
                       f'{pair_name} (asked {deal_size_asked} for the base).')
            else:
                uprint(f'[{self.exch_name}: {pair_name}] Complete fill of '
                       f'{deal_size:.4f} - {deal_funds:.4f} '
                       f'for the pair {pair_name}.')

        return resp_json

    def order_limit_max(self, token_sell, token_buy, max_impact,
                        time_in_force='GTC'):
        """
        Place order to sell `token_sell` and buy `token_buy`, with the maximum
        amount available.
        """

        balance, available = self.get_balance(token_sell)
        resp = self.order_limit(token_sell, token_buy, max_impact=max_impact,
                                amount_sell=available, time_in_force=time_in_force)

        return resp

    def get_order_details(self, order_id):
        method = 'GET'
        endpoint = '/api/v1/orders/' + order_id

        headers = self.get_headers(full_endpoint=method + endpoint)

        response = self.session.request(method,
                                        url=self.api_url + endpoint,
                                        headers=headers)
        resp_json = response.json()

        if response.status_code != 200:
            uprint(f'[{self.exch_name}] ERROR: failure in request in '
                   f'`get_order_details` with message: {resp_json["msg"]}')
        return resp_json

    def get_execution_price(self, order_response, denomination, second_token):
        details = self.get_order_details(order_response['data']['orderId'])

        pair_name, good_order = self.find_pair_from_tokens(second_token,
                                                           denomination)

        deal_size = float(details['data']['dealSize'])
        deal_funds = float(details['data']['dealFunds'])

        if deal_funds == 0 and deal_size == 0:
            return False

        if good_order:  # A/B where second_token = A, denomination = B
            res = deal_funds / deal_size
        else:
            res = deal_size / deal_funds

        return res

    def get_balance(self, token_symbol):
        """
        Return the balance for the given token, both total and available.

        Using `data` fails here.
        """

        method = 'GET'
        endpoint = '/api/v1/accounts?currency=' + token_symbol + '&type=trade'

        headers = self.get_headers(full_endpoint=method + endpoint,
                                   data_string='')
        response = self.session.request(method,
                                        url=self.api_url + endpoint,
                                        headers=headers)
        resp_json = response.json()

        if response.status_code != 200:
            uprint(f'[{self.exch_name}] ERROR: failure in request in '
                   f'`get_balance` with message: {resp_json["msg"]}')

        if token_symbol not in self.listed_tokens.keys():
            assert resp_json['data'] == []
            uprint(f'[{self.exch_name}] WARNING: token {token_symbol} is '
                    f'not listed.')
            balance = 0
            available = 0
        else:
            balance = float(resp_json['data'][0]['balance'])
            available = float(resp_json['data'][0]['available'])

        return balance, available


    def create_ws_handle(self):
        method = 'POST'
        endpoint = '/api/v1/bullet-public'
        res = self.session.request(method, url=self.api_url + endpoint)

        res_json = res.json()
        token = res_json['data']['token']
        ws_endpoint = res_json['data']['instanceServers'][0]['endpoint']
        connect_id = str(int(time.time() * 1000))

        address = f'{ws_endpoint}?token={token}&[connectId={connect_id}]'
        ws = create_connection(address)

        return ws

    def get_subscription_data_ws(self, pair):
        data = {
            'id': int(time.time() * 1000),
            'type': 'subscribe',
            'topic': f'/market/ticker:{pair}'
        }
        return str(data)


class KucoinPriceSellSocketThread(threading.Thread):
    def __init__(self, exch_api, token_symbol, current_price, event_new_price):
        threading.Thread.__init__(self)

        self.old_price = -1
        self.current_price = [current_price]
        self.exch_api = exch_api
        self.day_duration = 60 * 60 * 24 - 500  # 500 secondes before to be safe

        self.token_symbol = token_symbol
        self.pair, self.ordered = self.exch_api.find_pair_from_tokens('USDT',
                                                                      token_symbol)

        self.killer = threading.Event()
        self.event_new_price = event_new_price

        if self.ordered is None:
            uprint(f'ERROR: pair of USDT and {token_symbol} do not exist at'
                   f'this point although it should.')
            sys.exit(1)

    def run(self):
        ws_handle = self.exch_api.create_ws_handle()
        ws_handle.settimeout(1)  # so that `recv` doesn't take more than 1s

        data_subscription = self.exch_api.get_subscription_data_ws(self.pair)
        ws_handle.send(data_subscription)

        tps_start = time.time()
        tps = tps_start
        lock = threading.Lock()

        while True:
            try:
                received = ws_handle.recv()
                received_json = json.loads(received)
            except Exception as e:
                #uprint(f'Exception in GetPriceSellSocketThread: {e} (expected)')
                pass

            if 'topic' not in received_json.keys():
                uprint(f'WARNING: discard socket data {received_json}')
                continue

            # have the denomination in USDT
            if not self.ordered:
                self.current_price[0] = float(received_json['data']['bestBid'])
            else:
                self.current_price[0] = 1 / float(received_json['data']['bestAsk'])
            # uprint(f'Looped in SocketThread! ({self.current_price[0]})')

            if self.old_price != self.current_price[0]:
                with lock:
                    self.event_new_price.set()
                self.old_price = self.current_price[0]

            current_tps = time.time()
            # we need to ping to not loose connection
            if current_tps - tps > 8:
                ws_handle.ping()
                tps = current_tps

            # the token is valid only 24 hours
            if current_tps - tps_start > self.day_duration:
                ws_handle.close()
                ws_handle = self.exch_api.create_ws_handle()
                data_subscription = self.exch_api.get_subscription_data_ws(
                                                                    self.pair)
                ws_handle.send(data_subscription)
                tps_start = time.time()

            # to exist gracefully once the parent thread for this pair has done
            # its job
            if self.killer.is_set():
                break


if __name__ == '__main__':
    kucoin = KucoinAPI()


    """
    data = kucoin.get_ticker('USDT', 'ORBS')
    best_bid = float(data['data']['bestBid'])
    best_ask = float(data['data']['bestAsk'])
    max_impact = 0.001
    """

    #uprint(f'Expected max price: {best_bid * (1 + max_impact)}')
    #uprint(f'Best ask: {best_ask}')
##
    #print('USDT', 'ORBS')
    res = kucoin.order_limit('USDT', 'AAVE', max_impact=0.001,
                amount_sell=1.05, time_in_force='IOC')
##
    res = kucoin.order_limit_max('AAVE', 'USDT', max_impact=0.1, time_in_force='IOC')

    #execution_price = kucoin.get_execution_price(res, 'USDT', 'ORBS')
    ##

    print(json.dumps(kucoin.get_order_details(res['data']['orderId']),
                     indent=4))

