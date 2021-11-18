import collections
import math
import sys

from datetime import datetime

def round_nearest(x, a):
    return round(int(x / a) * a, -int(math.floor(math.log10(a))))


def uprint(text, end='\n', tps=True):
    if tps:
        tps = datetime.utcnow().strftime('%m-%d %H:%M:%S.%f')[:-3]
        print(tps + ':', text, end=end)
    else:
        print(text, end=end)

    sys.stdout.flush()
    sys.stderr.flush()

def keysort(dictionary):
        return collections.OrderedDict(sorted(dictionary.items(),
                                       key=lambda t: t[0]))


if __name__ == '__main__':
    mydict = {'hehe': 4,
              'this': 'here',
              'ahi': 14}

    sorted_keys_dict = keysort(mydict)
    print(sorted_keys_dict)