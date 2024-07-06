import asyncio
import hashlib
import json

import aiohttp
import aiohttp_socks

from api.api_kucoin import KucoinAPI, KucoinPriceSellSocket
from find_token import RegexTitle
from utils import uprint


class RefreshAnnouncements:
    def __init__(self, exchs_apis, exchs_apis_sockets):
        self.url = 'https://www.binance.com/en/support/announcement/c-48'
        self.second_url = 'https://www.binance.com/bapi/composite/v1/public/cms/article/catalog/list/query?catalogId=48&pageNo=1&pageSize=15'
        proxy_url = 'socks5://host.docker.internal:7897'
        self.connector = aiohttp_socks.ProxyConnector.from_url(proxy_url)
        timeout = aiohttp.ClientTimeout(total=10)
        self.session = aiohttp.ClientSession(connector=self.connector, timeout=timeout)
        self.regex_title = RegexTitle()
        self.exchs_apis = exchs_apis
        self.exchs_apis_sockets = exchs_apis_sockets

    async def close(self):
        await self.session.close()

    async def refresh_announcements(self):
        checksum = ['', '']
        title = ''
        titles = []
        ind = 0

        headers = {'Cache-Control': 'no-cache, no-store, public, must-revalidate, proxy-revalidate, max-age=0',
                   'Pragma': 'no-cache',
                   'Expires': '0'}

        uprint(f'开始循环刷新公告。')
        while True:
            await asyncio.sleep(0.04)
            ind += 1
            if ind == 13:
                ind = 0
                current_url = self.second_url
                from_title = True
                index = 1
            else:
                current_url = self.url
                from_title = False
                index = 0

            try:
                async with self.session.get(current_url, headers=headers) as r:
                    r_text = await r.text()
                    uprint(r_text)
            except asyncio.TimeoutError:
                uprint("请求超时")
            except aiohttp.ClientError as e:
                uprint(e)
                self.session = aiohttp.ClientSession(connector=self.connector)
                uprint(f'可能连接中断，重新启动会话。')
                continue

            if r.status != 200:
                uprint(current_url)
                uprint(r_text)
                await asyncio.sleep(5 * 60)
                uprint(f'返回码错误，休眠5分钟后继续。')
                continue

            if from_title:
                try:
                    r_text = json.loads(r_text)['data']['articles'][0]['title']
                except Exception as e:
                    uprint(f'{e}, 响应文本: {r_text}')

            newchecksum = hashlib.sha256(r_text.encode('utf-8')).hexdigest()

            if newchecksum != checksum[index]:
                checksum[index] = newchecksum

                new_title, symbols, token_names = self.regex_title.find_token(r_text, from_title=from_title)

                if title == '':
                    title = new_title
                    titles.append(title)

                title = new_title

                if title in titles:
                    continue

                uprint(f'[**** 警报 ****] Binance 公告中检测到新新闻：\n        {title}')
                uprint(f'检测到的符号：\n        {symbols}')
                uprint(f'检测到的代币名称：\n        {token_names}')

                titles.append(title)

                tasks = []
                for i in range(len(symbols)):
                    for exch_name in self.exchs_apis_sockets.keys():
                        task = react_on_announcement(self.exchs_apis[exch_name], self.exchs_apis_sockets[exch_name],
                                                     symbols[i], token_names[i], 0.1, 130)
                        tasks.append(task)
                await asyncio.gather(*tasks)
                await asyncio.sleep(10)


class ExchangeRefresh:
    def __init__(self, exch_api):
        self.exch_api = exch_api

    async def refresh_exchange(self):
        uprint(f'开始循环刷新交易所 {self.exch_api.exch_name}。')
        while True:
            await self.exch_api.refresh()
            await asyncio.sleep(3600)
            uprint(f'更新交易所 {self.exch_api.exch_name}')


async def react_on_announcement(exch_api, exch_api_socket_class, token_symbol, token_name, max_impact=-1,
                                amount_sell=None):
    if token_symbol not in exch_api.listed_tokens.keys():
        uprint(f'[{exch_api.exch_name}: {token_symbol}] 购买失败（未列出）')
        return False

    exch_token_name = exch_api.listed_tokens[token_symbol].lower()
    if exch_api.has_token_fullnames and (
            exch_token_name.lower() not in token_name.lower() and token_name.lower() not in exch_token_name.lower()):
        uprint(f'[{exch_api.exch_name}: {token_symbol}] 购买失败（代币名称不匹配）')
        return False

    response = await exch_api.order_limit(token_sell='USDT', token_buy=token_symbol, max_impact=max_impact,
                                          amount_sell=amount_sell, time_in_force='IOC')

    if response['code'] != exch_api.valid_code_on_limit_order:
        uprint(f'[{exch_api.exch_name}: {token_symbol}] 警告：原始买单响应码错误，原始响应：\n        {response}')
        return False

    ref_price = await exch_api.get_price_sell(token_sell=token_symbol, token_buy='USDT')
    execution_price = await exch_api.get_price_sell(token_sell=token_symbol, token_buy='USDT')

    if not execution_price:
        uprint(f'[{exch_api.exch_name}: {token_symbol}] 空买单。中止此交易所和代币的反应。')
        return False

    uprint(f'[{exch_api.exch_name}: {token_symbol}] 参考价格（最高买价）：{ref_price:.4f} USDT')
    uprint(f'[{exch_api.exch_name}: {token_symbol}] 订单执行价格：{execution_price:.4f} USDT')

    floor_sell = 0.94 * ref_price
    ceil_sell = 2 * execution_price

    uprint(f'[{exch_api.exch_name}: {token_symbol}] 最低卖价：{floor_sell:.4f} USDT')
    uprint(f'[{exch_api.exch_name}: {token_symbol}] 最高卖价：{ceil_sell:.4f} USDT')

    trailing_sell_price = floor_sell
    current_price = ref_price
    max_reached_price = current_price

    if exch_api.support_websocket:
        event_new_price = asyncio.Event()
        price_socket = exch_api_socket_class(exch_api, token_symbol, current_price, event_new_price)
        asyncio.create_task(price_socket.run())

    start_time = asyncio.get_event_loop().time()
    while True:
        if asyncio.get_event_loop().time() - start_time < 2:
            current_price = await exch_api.get_price_sell(token_symbol, 'USDT')
        elif not exch_api.support_websocket:
            await asyncio.sleep(0.5)
            current_price = await exch_api.get_price_sell(token_symbol, 'USDT')
        else:
            await event_new_price.wait()
            current_price = price_socket.current_price[0]
            event_new_price.clear()

        if not current_price:
            uprint(f'[{exch_api.exch_name}: {token_symbol}] 获取卖价时出错，休眠并跳过此循环。')
            await asyncio.sleep(1)
            continue

        if current_price > ceil_sell:
            uprint(
                f'[{exch_api.exch_name}: {token_symbol}] 当前价格 {current_price:.4f} 超过最高卖价 {ceil_sell:.4f}，以约2倍利润出售。')
            response = await exch_api.order_limit_max(token_sell=token_symbol, token_buy='USDT', max_impact=0.2,
                                                      time_in_force='IOC')
            if exch_api.support_websocket:
                price_socket.killer.set()
            break

        if current_price < floor_sell:
            uprint(
                f'[{exch_api.exch_name}: {token_symbol}] 当前价格 {current_price:.4f} 低于最低卖价 {floor_sell:.4f}，亏损出售。')
            response = await exch_api.order_limit_max(token_sell=token_symbol, token_buy='USDT', max_impact=0.2,
                                                      time_in_force='IOC')
            if exch_api.support_websocket:
                price_socket.killer.set()
            break

        if current_price < trailing_sell_price and trailing_sell_price > ref_price:
            uprint(
                f'[{exch_api.exch_name}: {token_symbol}] 当前价格 {current_price:.4f} 低于追踪卖价 {trailing_sell_price:.4f}，出售。')
            response = await exch_api.order_limit_max(token_sell=token_symbol, token_buy='USDT', max_impact=0.2,
                                                      time_in_force='IOC')
            if exch_api.support_websocket:
                price_socket.killer.set()
            break

        if current_price > max_reached_price:
            max_reached_price = current_price
            trailing_sell_price = max_reached_price * 0.9
            uprint(
                f'[{exch_api.exch_name}: {token_symbol}] 新的追踪卖价：{trailing_sell_price:.4f} （当前价格 {current_price:.4f}）')


async def main():
    uprint('启动程序。')

    exchanges_apis = {
        'Kucoin': KucoinAPI(),
        # 'MEXC': MEXC_API(),
        # 'BKEX': BKEX_API()
    }

    exchanges_apis_sockets = {
        'Kucoin': KucoinPriceSellSocket,
        # 'MEXC': None,
        # 'BKEX': None
    }

    refresh_tasks = []
    for exch_name, exch_api in exchanges_apis.items():
        refresh_task = ExchangeRefresh(exch_api).refresh_exchange()
        refresh_tasks.append(refresh_task)

    refresh_announcements = RefreshAnnouncements(exchanges_apis, exchanges_apis_sockets).refresh_announcements()
    refresh_tasks.append(refresh_announcements)

    await asyncio.gather(*refresh_tasks)


if __name__ == '__main__':
    asyncio.run(main())
