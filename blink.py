# Copyright © Aidan Allen - All Rights Reserved
# Unauthorized copying of this project, via any medium is strictly prohibited
# Proprietary and confidential
# Written by <blink@aaix.dev>, 29 Dec 2023
from collections import OrderedDict


class CacheDict(OrderedDict):
    'Limit size, evicting the least recently looked-up key when full'
    # from https://docs.python.org/3/library/collections.html#collections.OrderedDict#OrderedDict

    def __init__(self, maxsize=128, *args, **kwds):
        self.maxsize = maxsize
        super().__init__(*args, **kwds)

    def __getitem__(self, key):
        value = super().__getitem__(key)
        self.move_to_end(key)
        return value

    def __setitem__(self, key, value):
        if key in self:
            self.move_to_end(key)
        super().__setitem__(key, value)
        if len(self) > self.maxsize:
            oldest = next(iter(self))
            del self[oldest]
