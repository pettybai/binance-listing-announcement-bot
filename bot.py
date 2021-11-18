from api.api_kucoin import KucoinAPI
from api.api_kucoin import KucoinPriceSellSocketThread

from api.api_bkex import BKEX_API
from api.api_mexc import MEXC_API

from find_token import RegexTitle

from utils import uprint

import hashlib
import json
import logging
import requests
import sys
import time
import threading

class RefreshAnnouncementsThread(threading.Thread):
    def __init__(self, exchs_apis, exchs_apis_sockets):
        threading.Thread.__init__(self)

        # can acces every 40 ms fine
        self.url = 'https://www.binance.com/en/support/announcement/c-48'

        # API url, with slower access
        self.second_url = 'https://www.binance.com/bapi/composite/v1/public/cms/article/catalog/list/query?catalogId=48&pageNo=1&pageSize=15'
        self.session = requests.Session()

        self.regex_title = RegexTitle()

        # this dictionary holds instances of classes
        self.exchs_apis = exchs_apis

        # beware this dictionary holds classes, not instances of classes
        self.exchs_apis_sockets = exchs_apis_sockets

    def run(self):
        checksum = ['', '']  #TODO modify in reality
        first = True
        title = ''
        new_title = ''
        titles = []
        ind = 0

        headers = {'Cache-Control': 'no-cache, no-store, public, must-revalidate, proxy-revalidate, max-age=0',
                   'Pragma': 'no-cache',
                   'Expires': '0'}

        uprint(f'Starting to loop into RefreshAnnouncementsThread.')
        while True:
            time.sleep(40/1000)
            #uprint('Looping in RefreshAnnouncementsThread!') #TODO remove
            ind += 1
            if ind == 13:  # every 200 ms is too low for the bapi, 500 ms OK?
                ind = 0
                current_url = self.second_url
                from_title = True
                index = 1
            else:
                current_url = self.url
                from_title = False
                index = 0
            try:
                r = self.session.get(current_url, headers=headers)
            except requests.exceptions.ConnectionError as e:
                uprint(e)
                self.session = requests.Session()
                uprint(f'Likely connection aborted in RefreshAnnouncementsThread'
                       f', start a new session.')

            r_text = r.text

            # uprint(f'{r.status_code}, {from_title}')
            if r.status_code != 200:
                uprint(current_url)
                uprint(r_text)
                time.sleep(5 * 60)  # hope after 5 min it will be fine
                uprint(f'Slept 5 minutes after wrong return code for Binance'
                       f' request response, resuming.')
                continue

            if from_title:
                try:
                    r_text = json.loads(r_text)['data']['articles'][0]['title']
                except Exception as e:
                    uprint(f'{e}, with r_text: {r_text}')

            newchecksum = hashlib.sha256(r_text.encode('utf-8')).hexdigest()

            if newchecksum != checksum[index]:
                checksum[index] = newchecksum

                (new_title,
                symbols,
                token_names) = self.regex_title.find_token(r_text,
                                                           from_title=from_title)

                # first loop
                if title == '':
                    title = new_title
                    titles.append(title)

                title = new_title

                if title in titles:
                    # uprint(f'({title})')
                    # uprint(f'-------------------- Passing as already treated.')
                    continue

                # from here, we are sure to have a new title
                uprint(f'[**** ALERT ****] New news detected on Binance '
                       f'announcements:\n        {title}')
                uprint(f'Detected corresponding symbols:\n'
                       f'        {symbols}')
                uprint(f'Detected corresponding token names:\n'
                       f'        {token_names}')

                titles.append(title)
                thread_handles = [[] for i in range(len(symbols))]
                for i in range(len(symbols)):
                    for exch_name in self.exchs_apis_sockets.keys():
                        thread = threading.Thread(
                                        target=react_on_announcement,
                                        args=(self.exchs_apis[exch_name],
                                              self.exchs_apis_sockets[exch_name],
                                              symbols[i],
                                              token_names[i],
                                              0.1,  # max_impact
                                              130))  # quantity sell (USDT)
                        thread.start()
                        thread_handles[i].append(thread)
                time.sleep(10)  # sleep a bit to release the GIL fully


class ExchangeRefreshThread(threading.Thread):
    def __init__(self, exch_api):
        threading.Thread.__init__(self)
        self.exch_api = exch_api
        self.lock = threading.Lock()

    def run(self):
        uprint(f'Starting to loop into ExchangeRefreshThread for'
               f' {self.exch_api.exch_name}.')
        while True:
            with self.lock:
                self.exch_api.refresh()
            time.sleep(60 * 60)  # refresh exchanges every hour
            uprint(f'ExchangeRefreshThread: Updated {self.exch_api.exch_name}')


def react_on_announcement(exch_api, exch_api_socket_class, token_symbol,
                          token_name, max_impact=-1, amount_sell=None):
    if token_symbol not in exch_api.listed_tokens.keys():
        uprint(f'[{exch_api.exch_name}: {token_symbol}] Buy FAILED (not listed)')
        return False

    # additional failsafe in case the external exchange stores the full token names
    exch_token_name = exch_api.listed_tokens[token_symbol].lower()
    if (exch_api.has_token_fullnames is True
        and exch_token_name.lower() not in token_name.lower()
        and token_name.lower() not in exch_token_name.lower()):
        uprint(f'[{exch_api.exch_name}: {token_symbol}] Buy FAILED (token listed,'
               f' but {exch_api.exch_name} token name {exch_token_name} does not '
               f'match with {token_name.lower()})')
        return False

    # max_impact compared to highest bid
    response = exch_api.order_limit(token_sell='USDT',
                                    token_buy=token_symbol,
                                    max_impact=max_impact,
                                    amount_sell=amount_sell,
                                    time_in_force='IOC'  # immediate or cancel
                                    )

    # failsafe in case the order failed
    if response['code'] != exch_api.valid_code_on_limit_order:
        uprint(f'[{exch_api.exch_name}: {token_symbol}] WARNING: The response '
               f'code to the original buy order is wrong, see raw response:\n'
               f'        {response}')
        return False

    ref_price = exch_api.get_price_sell(token_sell=token_symbol,
                                        token_buy='USDT')

    execution_price = exch_api.get_execution_price(response,
                                                   denomination='USDT',
                                                   second_token=token_symbol)

    if execution_price == False:
        uprint(f'[{exch_api.exch_name}: {token_symbol}] Empty buy. Abort the '
               f'reaction for this exchange and token.')
        return False

    uprint(f'[{exch_api.exch_name}: {token_symbol}] Reference price (highest '
           f'bid): {ref_price:.4f} USDT')
    uprint(f'[{exch_api.exch_name}: {token_symbol}] Order execution price: '
           f'{execution_price:.4f} USDT')

    floor_sell = 0.94 * ref_price
    ceil_sell = 2 * execution_price

    uprint(f'[{exch_api.exch_name}: {token_symbol}] Floor sell price: '
           f'{floor_sell:.4f} USDT')
    uprint(f'[{exch_api.exch_name}: {token_symbol}] Ceil sell price: '
           f'{ceil_sell:.4f} USDT')

    trailing_sell_price = floor_sell

    current_price = ref_price
    max_reached_price = current_price

    if exch_api.support_websocket is True:
        event_new_price = threading.Event()
        lock = threading.Lock()
        thread_price_feed = exch_api_socket_class(exch_api=exch_api,
                                                  token_symbol=token_symbol,
                                                  current_price=current_price,
                                                  event_new_price=event_new_price)
        thread_price_feed.start()

    start_time = time.time()
    while True:
        # let some time to the websocket to initialize, if it is supported
        if time.time() - start_time < 2:
            current_price = exch_api.get_price_sell(token_symbol, 'USDT')
        elif exch_api.support_websocket is False:
            time.sleep(0.5)
            current_price = exch_api.get_price_sell(token_symbol, 'USDT')
        else:
            while not event_new_price.is_set():
                event_new_price.wait(1)

            current_price = thread_price_feed.current_price[0]

            if not event_new_price.is_set():
                continue
            else:
                with lock:
                    event_new_price.clear()

        # uprint(f'Current price in main thread: {current_price}')

        if current_price is False:
            uprint(f'[{exch_api.exch_name}: {token_symbol}] Something went wrong '
                   f'in `get_price_sell`. Sleep and skip this loop.')
            time.sleep(1)

        if current_price > ceil_sell:
            uprint(f'[{exch_api.exch_name}: {token_symbol}] Current price '
                   f'{current_price:.4f} exceed the ceil sell '
                   f'price {ceil_sell:.4f}, selling at ~ a *2 profit.')
            response = exch_api.order_limit_max(token_sell=token_symbol,
                                                token_buy='USDT',
                                                max_impact=0.2,
                                                time_in_force='IOC'
                                                )
            if exch_api.support_websocket is True:
                thread_price_feed.killer.set()
                thread_price_feed.join()
            break

        if current_price < floor_sell:
            uprint(f'[{exch_api.exch_name}: {token_symbol}] Current price '
                   f'{current_price:.4f} under the floor sell '
                   f'price {floor_sell:.4f}, selling at a loss.')
            response = exch_api.order_limit_max(token_sell=token_symbol,
                                                token_buy='USDT',
                                                max_impact=0.2,
                                                time_in_force='IOC'
                                                )
            if exch_api.support_websocket is True:
                thread_price_feed.killer.set()
                thread_price_feed.join()
            break

        if (current_price < trailing_sell_price
            and trailing_sell_price > ref_price):
            uprint(f'[{exch_api.exch_name}: {token_symbol}] Current price '
                   f'{current_price:.4f} went under the trailing '
                   f'sell price {trailing_sell_price:.4f}, selling.')
            response = exch_api.order_limit_max(token_sell=token_symbol,
                                                token_buy='USDT',
                                                max_impact=0.2,
                                                time_in_force='IOC'
                                                )
            if exch_api.support_websocket is True:
                thread_price_feed.killer.set()
                thread_price_feed.join()
            break

        if current_price > max_reached_price:
            max_reached_price = current_price
            trailing_sell_price = max_reached_price * 0.9
            uprint(f'[{exch_api.exch_name}: {token_symbol}] New trailing sell '
                   f'price: {trailing_sell_price:.4f} (for '
                   f'current price {current_price:.4f})')


if __name__ == '__main__':
    uprint('Starting.')

    exchanges_apis = {'Kucoin' : KucoinAPI(),
                      'MEXC': MEXC_API(),
                      'BKEX': BKEX_API()
                     }

    exchanges_apis_sockets = {'Kucoin': KucoinPriceSellSocketThread,
                              'MEXC': None,
                              'BKEX': None}

    threads_refresh_apis = []
    for exch_name in exchanges_apis.keys():
        thread_exch_refresh = ExchangeRefreshThread(exchanges_apis[exch_name])
        thread_exch_refresh.start()
        threads_refresh_apis.append(thread_exch_refresh)

    thread_announcements_refresh = RefreshAnnouncementsThread(
                                        exchs_apis=exchanges_apis,
                                        exchs_apis_sockets=exchanges_apis_sockets)
    thread_announcements_refresh.start()