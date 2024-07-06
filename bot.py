import asyncio
import hashlib
import json

import aiohttp
import aiohttp_socks

from api.api_kucoin import KucoinAPI, KucoinPriceSellSocket
from react import react_on_announcement
from regex_title import RegexTitle
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

        self.checksum = ['', '']
        self.title = ''
        self.titles = []
        self.ind = 0

    def react_announcement(self, symbols, token_names):
        for i in range(len(symbols)):
            for exch_name in self.exchs_apis_sockets.keys():
                task = react_on_announcement(self.exchs_apis[exch_name], self.exchs_apis_sockets[exch_name],
                                             symbols[i], token_names[i], 0.1, 130)
                asyncio.create_task(task)

    async def get_announcement(self):
        headers = {'Cache-Control': 'no-cache, no-store, public, must-revalidate, proxy-revalidate, max-age=0',
                   'Pragma': 'no-cache',
                   'Expires': '0'}

        await asyncio.sleep(0.04)
        self.ind += 1
        if self.ind == 13:
            self.ind = 0
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
        except aiohttp.ClientError as e:
            uprint(e)
            self.session = aiohttp.ClientSession(connector=self.connector)
            uprint(f'可能连接中断，重新启动会话。')

        if r.status != 200:
            uprint(current_url)
            uprint(r_text)
            await asyncio.sleep(5 * 60)
            uprint(f'返回码错误，休眠5分钟后继续。')

        if from_title:
            try:
                r_text = json.loads(r_text)['data']['articles'][0]['title']
            except Exception as e:
                uprint(f'{e}, 响应文本: {r_text}')

        newchecksum = hashlib.sha256(r_text.encode('utf-8')).hexdigest()

        if newchecksum != self.checksum[index]:
            self.checksum[index] = newchecksum
            return True, r_text, from_title

        return False, r_text, from_title

    async def run(self):
        uprint(f'开始循环刷新公告。')
        while True:
            is_new, r_text, from_title = await self.get_announcement()
            if is_new:
                new_title, symbols, token_names = self.regex_title.find_token(r_text, from_title=from_title)
                if self.title == '':
                    self.title = new_title
                    self.titles.append(self.title)

                self.title = new_title

                if self.title in self.titles:
                    continue

                uprint(f'[**** 警报 ****] Binance 公告中检测到新新闻：\n        {self.title}')
                uprint(f'检测到的符号：\n        {symbols}')
                uprint(f'检测到的代币名称：\n        {token_names}')

                self.react_announcement(symbols, token_names)
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

    refresh_announcements = RefreshAnnouncements(exchanges_apis, exchanges_apis_sockets).run()
    refresh_tasks.append(refresh_announcements)

    await asyncio.gather(*refresh_tasks)


if __name__ == '__main__':
    asyncio.run(main())
