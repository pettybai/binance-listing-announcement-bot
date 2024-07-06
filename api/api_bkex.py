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

class BKEX_API(GeneralAPI):
    def __init__(self):
        super().__init__()
        self.api_url = 'https://api.bkex.com'
        self.exch_name = 'BKEX'
        self.session = requests.Session()

        self.update_pairs()
        self.update_tokens()

        self.secret_key = personal_keys[self.exch_name]['secret_key']
        self.access_key = personal_keys[self.exch_name]['access_key']

        self.pairs_separator = '_'

        self.support_websocket = False
        self.has_token_fullnames = False

        self.valid_code_on_limit_order = 0

    def update_pairs(self):
        method = 'GET'
        endpoint = '/v2/common/symbols'

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

            decimals = float(resp_json['data'][i]['volumePrecision'])
            if decimals == 0:
                precision = 0.01  # fallback to default value
            else:
                precision = 1 / 10**decimals

            decimals = float(resp_json['data'][i]['pricePrecision'])
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
        method = 'GET'
        endpoint = '/v2/common/currencys'

        response = self.session.request(method,
                                        url=self.api_url + endpoint)

        if response.status_code != 200:
            uprint(f'[{self.exch_name}] ERROR: failure in request '
                   f'in `update_tokens` with message: {response.text}')
            return False

        resp_json = response.json()
        res = dict()

        #TODO BKEX doesn't hold token full name, adapt bot accordingly
        n_tokens = len(resp_json['data'])
        for i in range(n_tokens):
            res[resp_json['data'][i]['currency']] = ''

        self.listed_tokens = res

    def get_headers(self, request_type, data_dict={}):
        now = str(int(time.time() * 1000))

        data_dict = keysort(data_dict)
        data_string = urllib.parse.urlencode(data_dict)

        str_to_sign = data_string

        signature = hmac.new(self.secret_key.encode('utf-8'),
                     str_to_sign.encode('utf-8'),
                     hashlib.sha256).hexdigest()

        headers = {
            'Cache-Control': 'no-cache',
            'Content-type': 'application/x-www-form-urlencoded',
            'X_ACCESS_KEY': self.access_key,
            'X_SIGNATURE': signature
        }
        return headers

    def get_price_sell(self, token_sell, token_buy):
        pair_name, good_order = self.find_pair_from_tokens(token_sell, token_buy)

        if pair_name is None:
            return None

        method = 'GET'
        endpoint = '/v2/q/depth?symbol=' + pair_name + '&depth=1'

        response = self.session.request(method,
                                        url=self.api_url + endpoint)

        if response.status_code != 200:
            uprint(f'[{self.exch_name}: {pair_name}] ERROR: failure in request '
                   f'in `get_price_sell` with message: {response.text}')
            return False

        resp_json = response.json()

        if good_order:  # A/B with token_sell = A, token_buy = B
            return float(resp_json['data']['bid'][0][0])
        else:
            return 1 / float(resp_json['data']['ask'][0][0])

        return resp

    def order_limit(self, token_sell, token_buy, max_impact,
                    amount_sell=None, amount_buy=None, time_in_force='GTC'):
        pair_name, good_order = self.find_pair_from_tokens(token_sell, token_buy)

        if time_in_force != 'GTC' and time_in_force != 'IOC':
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
        endpoint = '/v2/u/order/create'

        data = {'client_order_id': uuid.uuid4().hex,  # use random uuid
                'symbol': pair_name,
                'type': 'LIMIT'}

        data['price'] = execution_price
        if good_order: # note A = token_sell, B = token_buy, pair A/B
            if amount_sell is not None:
                data['direction'] = 'ASK'
                data['volume'] = base_amount
        else:
                data['direction'] = 'BID'
                data['volume'] = base_amount

        data_jsoned = json.dumps(data, separators=(',', ':'))

        headers = self.get_headers(request_type=method,
                                   data_dict=data)

        response = self.session.request(method,
                                        url=self.api_url + endpoint,
                                        data=data,
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

        if resp_json['code'] != 0:
            uprint(f'[{self.exch_name}: {pair_name}] FAILED '
                   f'({resp_json["code"]}: {resp_json})')
        else:
            uprint(f'[{self.exch_name}: {pair_name}] Maximum price set at '
                   f'{execution_price:.4f} (price per base currency)')
            uprint(f'[{self.exch_name}: {pair_name}] SUCCESS sell {token_sell}.')

            order_id = resp_json['data']
            if time_in_force == 'IOC':
                self.cancel_order(order_id)

            #TODO extremely ugly
            details = False
            while details == False:
                details = self.get_order_details(order_id, token_sell, token_buy)

            deal_size = details['dealVolume']  # base
            deal_average_price = details['dealAvgPrice']
            if deal_size != 0 and deal_average_price != 0:
                deal_funds = deal_size * deal_average_price
            else:
                deal_funds = 0  # quote
            deal_size_asked = base_amount

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
        balance, available = self.get_balance(token_sell)
        resp = self.order_limit(token_sell, token_buy, max_impact=max_impact,
                                amount_sell=available, time_in_force=time_in_force)

        return resp

    def cancel_order(self, order_id):
        method = 'POST'
        endpoint = '/v2/u/order/cancel'

        data = {'orderId': order_id}
        headers = self.get_headers(request_type=method, data_dict=data)

        response = self.session.request(method,
                                        url=self.api_url + endpoint,
                                        data=data,
                                        headers=headers)

        resp_json = response.json()

        if response.status_code != 200:
            uprint(f'[{self.exch_name}] ERROR: failure in request in cancel_order'
                   f' with message: {resp_json["msg"]}')
        elif resp_json['code'] == 7019:
            uprint(f'[{self.exch_name}] WARNING: Could not cancel order with id '
                   f'{order_id} in cancel_order because not find. The order may '
                   f'be completely filled already, or the id is wrong.')
        elif resp_json['code'] != 0:
            uprint(f'[{self.exch_name}] ERROR: Could not cancel order with id '
                   f'{order_id} in cancel_order with error code {resp_json["code"]}'
                   f' ({resp_json["msg"]})')
        else:
            uprint(f'[{self.exch_name}] SUCCESS: Cancelled order with id '
                   f'{order_id}.')

        return resp_json

    def get_order_details(self, order_id, token1, token2):
        method = 'GET'
        endpoint = '/v2/u/order/openOrder/detail'

        data = {'orderId': order_id}
        headers = self.get_headers(request_type=method, data_dict=data)

        response = self.session.request(method,
                                        url=self.api_url + endpoint,
                                        data=data,
                                        headers=headers)

        resp_json = response.json()
        #print(resp_json)
        if response.status_code != 200:
            uprint(f'[{self.exch_name}] ERROR: failure in request in '
                   f'`get_order_details` with message: {resp_json["msg"]}')
            return False
        if resp_json['code'] == 0:
            uprint(f'[{self.exch_name}] Order with id {order_id} unfinished '
                   f'in `get_order_details`.')
            return resp_json['data']

        # try in the history since the order is not open
        method2 = 'GET'
        endpoint2 = '/v2/u/order/historyOrders'

        pair_name, good_order = self.find_pair_from_tokens(token1, token2)

        if pair_name is None:
            return False

        data2 = {'symbol': pair_name}
        headers2 = self.get_headers(request_type=method2, data_dict=data2)

        response2 = self.session.request(method2,
                                         url=self.api_url + endpoint2,
                                         data=data2,
                                         headers=headers2)

        resp_json2 = response2.json()

        if response2.status_code != 200:
            uprint(f'[{self.exch_name}] ERROR: failure in request in '
                   f'`get_order_details` with message: {resp_json2["msg"]}')
            return False

        if resp_json2['code'] == 0:
            for order in resp_json2['data']['data'][:5]:
                if order_id == order['id']:
                    uprint(f'[{self.exch_name}] Order with id {order_id} '
                           f'finished in `get_order_details`.')
                    return order

        uprint(f'[{self.exch_name}] Order with id {order_id} not found in open '
               f'orders and history, are you sure the id is right?')

        return False

    def get_execution_price(self, order_response, denomination, second_token):
        details = self.get_order_details(order_response['data'],
                                         denomination,
                                         second_token)

        pair_name, good_order = self.find_pair_from_tokens(second_token,
                                                           denomination)

        deal_size = details['dealVolume']  # base
        deal_average_price = details['dealAvgPrice']
        if deal_size != 0 and deal_average_price != 0:
            deal_funds = deal_size * deal_average_price  # quote
        else:
            deal_funds = 0

        #TODO danger here? If zero filled?
        if deal_funds == 0 and deal_size == 0:
            return False

        if good_order:  # A/B where second_token = A, denomination = B
            res = deal_funds / deal_size
        else:
            res = deal_size / deal_funds

        return res

    def get_balance(self, token_symbol):
        method = 'GET'
        endpoint = '/v2/u/account/balance'

        data_dict = {'currencys': token_symbol}

        headers = self.get_headers(request_type=method,
                                   data_dict=data_dict)

        response = self.session.request(method,
                                        url=self.api_url + endpoint,
                                        data=data_dict,
                                        headers=headers)
        resp_json = response.json()

        if response.status_code != 200:
            uprint(f'[{self.exch_name}] ERROR: failure in request in '
                   f'`get_balance` with message: {resp_json["msg"]}')

        if token_symbol not in self.listed_tokens.keys():
            uprint(f'[{self.exch_name}] WARNING: token {token_symbol} is '
                    f'not listed.')
            balance = 0
            available = 0
        else:
            available = float(resp_json['data']['WALLET'][0]['available'])
            balance = float(resp_json['data']['WALLET'][0]['frozen'])
            balance += available

        return balance, available


if __name__ == '__main__':
    bkex = BKEX_API()

    ##
    res = bkex.order_limit('USDT', 'AAVE', amount_sell=12,
                           max_impact=0.1, time_in_force='IOC')
    ##
    res = bkex.order_limit_max('AAVE', 'USDT',
                               max_impact=0.05, time_in_force='IOC')

    ##
    myid = res['data']
    myidfake = '2021102400142479237060295'
    ##
    details = bkex.get_order_details(myid, 'USDT', 'AAVE')
    ##

    res = bkex.cancel_order(myid)
