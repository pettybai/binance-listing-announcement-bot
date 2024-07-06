import base64
import hashlib
import hmac
import json
import requests
import time
import urllib
import uuid

from websocket import create_connection

from keys import personal_keys
from utils import keysort, round_nearest, uprint

from api.api_general import GeneralAPI

class MEXC_API(GeneralAPI):
    def __init__(self):
        super().__init__()
        self.api_url = 'https://www.mexc.com'
        self.exch_name = 'MEXC'
        self.session = requests.Session()

        self.update_pairs()
        self.update_tokens()

        self.secret_key = personal_keys[self.exch_name]['secret_key']
        self.access_key = personal_keys[self.exch_name]['access_key']

        self.pairs_separator = '_'

        self.support_websocket = False
        self.has_token_fullnames = True

        self.valid_code_on_limit_order = 200

    def update_pairs(self):
        """
        Generate a set of pairs (like 'USDT-BTC') on the exchange, and a dict
        of specifications for each pair (step size, min and max order size)
        """

        method = 'GET'
        endpoint = '/open/api/v2/market/symbols'

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
            #print(resp_json['data'][i])

            decimals = float(resp_json['data'][i]['quantity_scale'])
            if decimals == 0:
                precision = 0.01
            else:
                precision = 1 / 10**decimals

            decimals = float(resp_json['data'][i]['price_scale'])
            if decimals == 0:
                precision_price = 0.01
            else:
                precision_price = 1 / 10**decimals

            pairs_specs[resp_json['data'][i]['symbol']] = {
                'baseIncrement': precision,
                'quoteIncrement': precision,
                'priceIncrement': precision_price
                }

        self.pairs = pairs
        self.pairs_specs = pairs_specs

    def update_tokens(self):
        """
        Generate a dictionary mapping token symbols to token names on the exchange
        """
        method = 'GET'
        endpoint = '/open/api/v2/market/coin/list'

        response = self.session.request(method,
                                        url=self.api_url + endpoint)

        if response.status_code != 200:
            uprint(f'[{self.exch_name}] ERROR: failure in request '
                   f'in `update_tokens` with message: {response.text}')
            return False

        resp_json = response.json()
        res = dict()

        n_tokens = len(resp_json['data'])
        for i in range(n_tokens):
            res[resp_json['data'][i]['currency']] = resp_json['data'][i]['full_name']

        self.listed_tokens = res

    def get_headers(self, request_type, data_dict={}):
        """
        Return a header needed to include in a request
        """
        now = str(int(time.time() * 1000))

        if request_type == 'GET':
            data_dict = keysort(data_dict)
            data_string = urllib.parse.urlencode(data_dict)
        elif request_type == 'POST':
            data_string = json.dumps(data_dict, separators=(',', ':'))

        str_to_sign = self.access_key + now + data_string

        h_binary = hmac.new(self.secret_key.encode(),
                     str_to_sign.encode(),
                     hashlib.sha256).hexdigest()

        headers = {
            'ApiKey': self.access_key,
            'Request-Time': now,
            'Signature': h_binary,
            'Content-Type': 'application/json'
        }
        return headers


    def get_price_sell(self, token_sell, token_buy):
        """
        Denomination in the `token_buy`

        Get highest price we can sell `token_sell` at, i.e. highest bid
        """
        pair_name, good_order = self.find_pair_from_tokens(token_sell, token_buy)

        if pair_name is None:
            return None

        method = 'GET'
        endpoint = '/open/api/v2/market/ticker?symbol=' + pair_name

        response = self.session.request(method,
                                        url=self.api_url + endpoint)

        if response.status_code != 200:
            uprint(f'[{self.exch_name}: {pair_name}] ERROR: failure in request '
                   f'in `get_price_sell` with message: {response.text}')
            return False

        resp_json = response.json()

        if good_order:  # A/B with token_sell = A, token_buy = B
            return float(resp_json['data'][0]['bid'])
        else:
            return 1 / float(resp_json['data'][0]['ask'])


    def order_limit(self, token_sell, token_buy, max_impact,
                    amount_sell=None, amount_buy=None, time_in_force='GTC'):
        pair_name, good_order = self.find_pair_from_tokens(token_sell, token_buy)

        # adapt to mexc syntax
        if time_in_force == 'GTC':
            order_type = 'LIMIT_ORDER'
        elif time_in_force == 'IOC':
            order_type = 'IMMEDIATE_OR_CANCEL'
        else:
            raise ValueError(f'{time_in_force} as time_in_force is invalid.')

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
        endpoint = '/open/api/v2/order/place'

        data = {'client_order_id': uuid.uuid4().hex,  # use random uuid
                'symbol': pair_name,
                'order_type': order_type}

        data['price'] = execution_price
        if good_order: # note A = token_sell, B = token_buy, pair A/B
            if amount_sell is not None:
                data['trade_type'] = 'ASK'
                data['quantity'] = base_amount
        else:
                data['trade_type'] = 'BID'
                data['quantity'] = base_amount

        data_jsoned = json.dumps(data, separators=(',', ':'))

        headers = self.get_headers(request_type=method,
                                   data_dict=data)
        response = self.session.request(method,
                                        url=self.api_url + endpoint,
                                        data=data_jsoned,
                                        headers=headers)

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

        resp_json = response.json()

        if response.status_code != 200:
            uprint(f'[{self.exch_name}: {pair_name}] ERROR: failure in request '
                   f'in `order_limit` with message: {resp_json["msg"]}')

        if resp_json['code'] != 200:
            uprint(f'[{self.exch_name}: {pair_name}] FAILED '
                   f'({resp_json["code"]}: {resp_json})')
        else:
            uprint(f'[{self.exch_name}: {pair_name}] Maximum price set at '
                   f'{execution_price:.4f} (price per base currency)')
            uprint(f'[{self.exch_name}: {pair_name}] SUCCESS sell {token_sell}.')

            details = self.get_order_details(resp_json['data'])['data'][0]

            deal_size = float(details['deal_quantity'])  # base
            deal_funds = float(details['deal_amount'])  # quote
            deal_size_asked = float(details['quantity'])

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
        endpoint = '/open/api/v2/order/query?order_ids=' + order_id

        data = {'order_ids': order_id}
        headers = self.get_headers(request_type=method, data_dict=data)

        response = self.session.request(method,
                                        url=self.api_url + endpoint,
                                        headers=headers)

        resp_json = response.json()

        if response.status_code != 200:
            uprint(f'[{self.exch_name}] ERROR: failure in request in '
                   f'`get_order_details` with message: {resp_json["msg"]}')

        return resp_json

    def get_execution_price(self, order_response, denomination, second_token):
        details = self.get_order_details(order_response['data'])

        pair_name, good_order = self.find_pair_from_tokens(second_token,
                                                           denomination)

        deal_size = float(details['data'][0]['deal_quantity'])  # base
        deal_funds = float(details['data'][0]['deal_amount'])  # quote

        #TODO danger here? If zero filled?
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

        Using `data` fails here.
        """

        method = 'GET'
        endpoint = '/open/api/v2/account/info'

        headers = self.get_headers(request_type=method,
                                   data_dict={})

        response = self.session.request(method,
                                        url=self.api_url + endpoint,
                                        headers=headers)
        resp_json = response.json()

        if response.status_code != 200:
            uprint(f'[{self.exch_name}] ERROR: failure in request in '
                   f'`get_balance` with message: {resp_json["msg"]}')

        if token_symbol not in self.listed_tokens.keys():
            assert token_symbol not in resp_json['data']
            uprint(f'[{self.exch_name}] WARNING: token {token_symbol} is '
                    f'not listed.')
        if token_symbol not in resp_json['data']:
            balance = 0
            available = 0
        else:
            available = float(resp_json['data'][token_symbol]['available'])
            balance = float(resp_json['data'][token_symbol]['frozen'])
            balance += available

        return balance, available


if __name__ == '__main__':
    mexc = MEXC_API()

    ##
    res = mexc.order_limit('USDT', 'AAVE', max_impact=0.2,
                           amount_sell=8, time_in_force='IOC')
    ##

    order_id = res['data']

    details = mexc.get_order_details(order_id)

    ##

    price = mexc.get_execution_price(res, denomination='USDT', second_token='AAVE')
    ##
    res = mexc.order_limit_max('AAVE', 'USDT', max_impact=0.2,
                    time_in_force='IOC')
    ##
    res = mexc.order_limit_max('USDT', 'AAVE', max_impact=0.1,
                    time_in_force='IOC')