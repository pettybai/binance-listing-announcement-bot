import re

from utils import uprint

class RegexTitle():
    def __init__(self):
        self.announcement_positive_flags = ['launchpool',
                                            'launchpad',
                                            'binance will list',
                                            'binance lists',
                                            'innovation zone']

        self.announcement_negative_flags = ['leveraged token',
                                            'stock token',
                                            'binance completes',
                                            'stable coin',
                                            'wrapped']

        self.banned_symbols = ['btc', 'usd']

        self.re1 = r'(?<=will list ).+?(?= \()'
        self.re2 = r'(?<=and ).+?(?= \()'

    def find_token(self, text, test_mode=False, from_title=False):

        if not from_title:
            matches = re.search(r'(?<="title":").+?(?=",")',
                                text).group()

            matches_good = matches.encode().decode('unicode-escape')
        else:
            matches_good = text

        if test_mode is True:
            matches_good = "Binance Will List hehe (HEHE) in the Innovation Zone and Aave (AAVE) here"

        likely_listing_announcement = False

        if any(flag in matches_good.lower()
            for flag in self.announcement_positive_flags):
            likely_listing_announcement = True

        if any(flag in matches_good.lower()
            for flag in self.announcement_negative_flags):
            likely_listing_announcement = False

        # Look for banned symbols between parenthesis
        for symbol in self.banned_symbols:
            matches_sym = re.findall("\([a-z]*" + symbol + "[a-z]*\)",
                                    matches_good.lower())
            if len(matches_sym) != 0:
                likely_listing_announcement = False

        if "introducing the " in matches_good.lower():
            re3 = r'(?<=introducing the ).+?(?= \()'
        else:
            re3 = r'(?<=introducing ).+?(?= \()'

        generic_re = re.compile("(%s|%s|%s)" % (self.re1, self.re2, re3))
        token_names = generic_re.findall(matches_good.lower())

        symbols = re.findall(r"(?<=\()[A-Z0-9]*(?=\))", matches_good)

        return matches_good, symbols, token_names

if __name__ == '__main__':
    import requests

    url = "https://www.binance.com/en/support/announcement/c-48"
    r = requests.get(url)

    parser = RegexTitle()

    uprint(parser.find_token(r.text))