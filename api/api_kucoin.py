import asyncio
import base64
import hashlib
import hmac
import json
import sys
import time
import uuid
from datetime import datetime, timedelta

import aiohttp
import websockets

from api.api_general import GeneralAPI
from logins import personal_keys
from utils import uprint


class KucoinAPI(GeneralAPI):
    def __init__(self):
        super().__init__()
        self.api_url = 'https://api.kucoin.com'
        self.exch_name = 'Kucoin'
        self.session = aiohttp.ClientSession()

        self.api_secret = personal_keys[self.exch_name]['api_secret']
        self.api_key = personal_keys[self.exch_name]['api_key']
        self.api_passphrase = personal_keys[self.exch_name]['api_passphrase']

        self.passphrase = base64.b64encode(
            hmac.new(self.api_secret.encode('utf-8'),
                     self.api_passphrase.encode('utf-8'),
                     hashlib.sha256).digest())
        # 创建任务来更新 pairs 和 tokens
        asyncio.create_task(self.update_pairs())
        asyncio.create_task(self.update_tokens())

        self.pairs_separator = '-'

        self.support_websocket = True
        self.has_token_fullnames = True

        self.valid_code_on_limit_order = '200000'

    async def update_pairs(self):
        """
        Generate a set of pairs (like 'USDT-BTC') on the exchange, and a dict
        of specifications for each pair (step size, min and max order size)
        """
        method = 'GET'
        endpoint = '/api/v1/symbols'

        async with self.session.request(method, url=self.api_url + endpoint) as response:
            if response.status != 200:
                uprint(f'[{self.exch_name}] ERROR: failure in request '
                       f'in `update_pairs` with message: {await response.text()}')
                return False

            resp_json = await response.json()

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

    async def update_tokens(self):
        """
        Generate a dictionary mapping token symbols to token names on the exchange
        """
        method = 'GET'
        endpoint = '/api/v1/currencies'

        async with self.session.request(method, url=self.api_url + endpoint) as response:
            if response.status != 200:
                uprint(f'[{self.exch_name}] ERROR: failure in request '
                       f'in `update_tokens` with message: {await response.text()}')
                return False

            res = dict()
            resp_json = await response.json()

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
        now = datetime.utcnow() + timedelta(hours=8)
        timestamp = int(now.timestamp() * 1000)
        str_to_sign = str(timestamp) + full_endpoint + data_string
        signature = base64.b64encode(hmac.new(self.api_secret.encode('utf-8'),
                                              str_to_sign.encode('utf-8'),
                                              hashlib.sha256).digest())
        headers = {
            'Content-Type': 'application/json',
            'KC-API-SIGN': signature.decode('utf-8'),
            'KC-API-TIMESTAMP': str(now),
            'KC-API-KEY': self.api_key,
            'KC-API-PASSPHRASE': self.passphrase.decode('utf-8'),
            'KC-API-KEY-VERSION': '2'
        }
        return headers

    async def get_price_sell(self, token_sell, token_buy):
        """
        Denomination in the `token_buy`

        Get highest price we can sell `token_sell` at, i.e. highest bid
        """
        pair_name, good_order = self.find_pair_from_tokens(token_sell, token_buy)

        if pair_name is None:
            return None

        method = 'GET'
        endpoint = '/api/v1/market/orderbook/level1?symbol=' + pair_name

        async with self.session.request(method, url=self.api_url + endpoint) as response:
            if response.status != 200:
                uprint(f'[{self.exch_name}: {pair_name}] ERROR: failure in request '
                       f'in `get_price_sell` with message: {await response.text()}')
                return False

            resp_json = await response.json()

            if good_order:  # A/B with token_sell = A, token_buy = B
                return float(resp_json['data']['bestBid'])
            else:
                return 1 / float(resp_json['data']['bestAsk'])

    async def order_limit(self, token_sell, token_buy, max_impact,
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
            ref_price = await self.get_price_sell(token_sell=token_sell,
                                                  token_buy=token_buy)
        else:
            ref_price = await self.get_price_sell(token_sell=token_buy,
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
        if good_order:  # note A = token_sell, B = token_buy, pair A/B
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
        async with self.session.request(method, url=self.api_url + endpoint,
                                        data=data_jsoned, headers=headers) as response:
            resp_json = await response.json()

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

            if response.status != 200:
                uprint(f'[{self.exch_name}: {pair_name}] ERROR: failure in request '
                       f'in `order_limit` with message: {resp_json["msg"]}')

            if resp_json['code'] != '200000':
                uprint(f'[{self.exch_name}: {pair_name}] FAILED '
                       f'({resp_json["code"]}: {resp_json})')
            else:
                uprint(f'[{self.exch_name}: {pair_name}] Maximum price set at '
                       f'{execution_price:.4f} (price per base currency)')
                uprint(f'[{self.exch_name}: {pair_name}] SUCCESS sell {token_sell}.')
            details = await self.get_order_details(resp_json['data']['orderId'])
            deal_size = float(details['data']['dealSize'])  # base
            deal_funds = float(details['data']['dealFunds'])  # quote
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

    async def order_limit_max(self, token_sell, token_buy, max_impact,
                              time_in_force='GTC'):
        """
        Place order to sell `token_sell` and buy `token_buy`, with the maximum
        amount available.
        """

        balance, available = await self.get_balance(token_sell)
        resp = await self.order_limit(token_sell, token_buy, max_impact=max_impact,
                                      amount_sell=available, time_in_force=time_in_force)

        return resp

    async def get_order_details(self, order_id):
        method = 'GET'
        endpoint = '/api/v1/orders/' + order_id

        headers = self.get_headers(full_endpoint=method + endpoint)

        async with self.session.request(method, url=self.api_url + endpoint,
                                        headers=headers) as response:
            resp_json = await response.json()

            if response.status != 200:
                uprint(f'[{self.exch_name}] ERROR: failure in request in '
                       f'`get_order_details` with message: {resp_json["msg"]}')
            return resp_json

    async def get_execution_price(self, order_response, denomination, second_token):
        details = await self.get_order_details(order_response['data']['orderId'])

        pair_name, good_order = self.find_pair_from_tokens(second_token, denomination)

        deal_size = float(details['data']['dealSize'])
        deal_funds = float(details['data']['dealFunds'])

        if deal_funds == 0 and deal_size == 0:
            return False

        if good_order:  # A/B where second_token = A, denomination = B
            res = deal_funds / deal_size
        else:
            res = deal_size / deal_funds

        return res

    async def get_balance(self, token_symbol):
        """
        Return the balance for the given token, both total and available.

        Using `data` fails here.
        """

        method = 'GET'
        endpoint = '/api/v1/accounts?currency=' + token_symbol + '&type=trade'

        headers = self.get_headers(full_endpoint=method + endpoint,
                                   data_string='')
        async with self.session.request(method, url=self.api_url + endpoint,
                                        headers=headers) as response:
            resp_json = await response.json()

            if response.status != 200:
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

    async def create_ws_handle(self):
        method = 'POST'
        endpoint = '/api/v1/bullet-public'
        async with aiohttp.ClientSession() as session:
            async with session.request(method, url=self.api_url + endpoint) as res:
                res_json = await res.json()
                token = res_json['data']['token']
                ws_endpoint = res_json['data']['instanceServers'][0]['endpoint']
                connect_id = str(int(time.time() * 1000))

                address = f'{ws_endpoint}?token={token}&[connectId={connect_id}]'
                ws = await websockets.connect(address)

        return ws

    def get_subscription_data_ws(self, pair):
        data = {
            'id': int(time.time() * 1000),
            'type': 'subscribe',
            'topic': f'/market/ticker:{pair}'
        }
        return json.dumps(data)


class KucoinPriceSellSocket:
    def __init__(self, exch_api, token_symbol, current_price, event_new_price):
        self.old_price = -1
        self.current_price = [current_price]
        self.exch_api = exch_api
        self.day_duration = 60 * 60 * 24 - 500  # 500 seconds before to be safe

        self.token_symbol = token_symbol
        self.pair, self.ordered = self.exch_api.find_pair_from_tokens('USDT',
                                                                      token_symbol)

        self.event_new_price = event_new_price

        if self.ordered is None:
            uprint(f'ERROR: pair of USDT and {token_symbol} do not exist at'
                   f'this point although it should.')
            sys.exit(1)

    async def run(self):
        ws_handle = await self.exch_api.create_ws_handle()
        data_subscription = self.exch_api.get_subscription_data_ws(self.pair)
        await ws_handle.send(data_subscription)

        tps_start = time.time()
        tps = tps_start
        lock = asyncio.Lock()

        try:
            while True:
                try:
                    received = await asyncio.wait_for(ws_handle.recv(), timeout=1)
                    received_json = json.loads(received)
                except asyncio.TimeoutError:
                    continue
                except websockets.ConnectionClosed:
                    break
                except Exception as e:
                    uprint(f'Exception in KucoinPriceSellSocket: {e}')
                    continue

                if 'topic' not in received_json:
                    uprint(f'WARNING: discard socket data {received_json}')
                    continue

                # have the denomination in USDT
                if not self.ordered:
                    self.current_price[0] = float(received_json['data']['bestBid'])
                else:
                    self.current_price[0] = 1 / float(received_json['data']['bestAsk'])

                if self.old_price != self.current_price[0]:
                    async with lock:
                        self.event_new_price.set()
                    self.old_price = self.current_price[0]

                current_tps = time.time()
                # we need to ping to not lose connection
                if current_tps - tps > 8:
                    # await ws_handle.ping()
                    tps = current_tps

                # the token is valid only 24 hours
                if current_tps - tps_start > self.day_duration:
                    await ws_handle.close()
                    ws_handle = await self.exch_api.create_ws_handle()
                    data_subscription = self.exch_api.get_subscription_data_ws(self.pair)
                    await ws_handle.send(data_subscription)
                    tps_start = time.time()
        except asyncio.CancelledError:
            pass
        finally:
            await ws_handle.close()


if __name__ == '__main__':
    kucoin = KucoinAPI()

    """
    data = kucoin.get_ticker('USDT', 'ORBS')
    best_bid = float(data['data']['bestBid'])
    best_ask = float(data['data']['bestAsk'])
    max_impact = 0.001
    """

    # uprint(f'Expected max price: {best_bid * (1 + max_impact)}')
    # uprint(f'Best ask: {best_ask}')
    ##
    # print('USDT', 'ORBS')
    res = kucoin.order_limit('USDT', 'AAVE', max_impact=0.001,
                             amount_sell=1.05, time_in_force='IOC')
    ##
    res = kucoin.order_limit_max('AAVE', 'USDT', max_impact=0.1, time_in_force='IOC')

    # execution_price = kucoin.get_execution_price(res, 'USDT', 'ORBS')
    ##

    print(json.dumps(kucoin.get_order_details(res['data']['orderId']),
                     indent=4))
