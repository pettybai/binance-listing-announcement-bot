import asyncio

from utils import uprint


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
    execution_price = await exch_api.get_execution_price(response, denomination='USDT', second_token=token_symbol)

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

    event_new_price = asyncio.Event()
    if exch_api.support_websocket:
        price_socket = exch_api_socket_class(exch_api, token_symbol, current_price, event_new_price)
        price_socket_task = asyncio.create_task(price_socket.run())

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
                price_socket_task.cancel()
            break

        if current_price < floor_sell:
            uprint(
                f'[{exch_api.exch_name}: {token_symbol}] 当前价格 {current_price:.4f} 低于最低卖价 {floor_sell:.4f}，亏损出售。')
            response = await exch_api.order_limit_max(token_sell=token_symbol, token_buy='USDT', max_impact=0.2,
                                                      time_in_force='IOC')
            if exch_api.support_websocket:
                price_socket_task.cancel()
            break

        if current_price < trailing_sell_price and trailing_sell_price > ref_price:
            uprint(
                f'[{exch_api.exch_name}: {token_symbol}] 当前价格 {current_price:.4f} 低于追踪卖价 {trailing_sell_price:.4f}，出售。')
            response = await exch_api.order_limit_max(token_sell=token_symbol, token_buy='USDT', max_impact=0.2,
                                                      time_in_force='IOC')
            if exch_api.support_websocket:
                price_socket_task.cancel()
            break

        if current_price > max_reached_price:
            max_reached_price = current_price
            trailing_sell_price = max_reached_price * 0.9
            uprint(
                f'[{exch_api.exch_name}: {token_symbol}] 新的追踪卖价：{trailing_sell_price:.4f} （当前价格 {current_price:.4f}）')
