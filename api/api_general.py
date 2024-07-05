from utils import round_nearest, uprint

class GeneralAPI():
    def __init__(self):
        # a set of pairs
        self.pairs, self.pairs_specs = [], []

        # a dictionary of symbol to name
        self.listed_tokens = []

    async def refresh(self):
        await self.update_pairs()
        await self.update_tokens()

    async def update_pairs(self):
        """
        Generate a set of pairs (like 'USDT-BTC') on the exchange, and a dict
        of specifications for each pair (step size, min and max order size)
        """
        raise NotImplementedError

    async def update_tokens(self):
        """
        Generate a dictionary mapping token symbols to names on the exchange
        """
        raise NotImplementedError

    def get_headers(self):
        """
        Return a header needed to include in a request
        """
        raise NotImplementedError

    async def get_price_sell(self):
        """
        Denomination in the `token_buy`

        Get highest price we can sell `token_sell` at, i.e. highest bid
        """
        raise NotImplementedError

    async def order_limit(self):
        raise NotImplementedError

    async def order_limit_max(self):
        """
        Place order to sell `token_sell` and buy `token_buy`, with the maximum
        amount available.
        """
        raise NotImplementedError

    async def get_order_details(self):
        raise NotImplementedError

    async def get_execution_price(self):
        """
        Get the average price of an order from its response.
        """

        raise NotImplementedError

    async def get_balance(self):
        """
        Return the balance for the given token, both total and available.
        """
        raise NotImplementedError

    async def create_ws_handle(self):
        raise NotImplementedError

    def get_subscription_data_ws(self):
        raise NotImplementedError

    def find_pair_from_tokens(self, token1, token2):
        """
        Return the pair name from two symbols, and None if a pair doesn't exist
        """
        sol = token1 + self.pairs_separator + token2
        if sol in self.pairs:
            return sol, True
        else:
            sol = token2 + self.pairs_separator + token1
            if sol in self.pairs:
                return sol, False
            else:
                uprint(f'[{self.exch_name}] No pair for {token1} and {token2}.')
                return None, None

    def _round_to_increment(self, pair_name, good_order,
                            amount_sell=None, amount_buy=None, ref_price=None,
                            max_impact=None):
        """
        Round to nearest increment multiplier, lower.
        """
        if amount_sell is None and amount_buy is None:
            return False, 'No amount specified for the rounding.'

        if pair_name not in self.pairs:
            return False, f'No such pair as {pair_name}.'

        base_amount = None

        specs = self.pairs_specs[pair_name]

        printing = (f'[{self.exch_name}: pair_name] Specs:\n'
                    f'       baseIncrement: {specs["baseIncrement"]}\n'
                    f'       quoteIncrement: {specs["quoteIncrement"]}\n'
                    f'       priceIncrement: {specs["priceIncrement"]}\n')

        # `ref_price` is provided in the case of a limit order
        if good_order:
            if amount_sell:
                increment = specs['baseIncrement']
                amount_sell = round_nearest(amount_sell, increment)

                if ref_price:
                    increment = specs['priceIncrement']
                    execution_price = ref_price * (1 - max_impact)
                    execution_price = round_nearest(execution_price, increment)

                    base_amount = amount_sell

            elif amount_buy:
                increment = specs['quoteIncrement']
                amount_buy = round_nearest(amount_buy, increment)

                if ref_price:
                    increment = specs['priceIncrement']
                    execution_price = ref_price * (1 - max_impact)
                    execution_price = round_nearest(execution_price, increment)

                    increment = specs['baseIncrement']
                    base_amount = round_nearest(amount_buy / ref_price,
                                                increment)

        else:
            execution_price = ref_price * (1 + max_impact)
            if amount_sell:
                increment = specs['quoteIncrement']
                amount_sell = round_nearest(amount_sell, increment)

                if ref_price:
                    increment = specs['priceIncrement']
                    execution_price = ref_price * (1 + max_impact)
                    execution_price = round_nearest(execution_price, increment)

                    increment = specs['baseIncrement']
                    base_amount = round_nearest(amount_sell / ref_price,
                                                increment)
            elif amount_buy:
                increment = specs['baseIncrement']
                amount_buy = round_nearest(amount_buy, increment)

                if ref_price:
                    increment = specs['priceIncrement']
                    execution_price = ref_price * (1 + max_impact)
                    execution_price = round_nearest(execution_price, increment)

                    base_amount = amount_buy

        return (amount_sell, amount_buy, execution_price, base_amount, printing)
