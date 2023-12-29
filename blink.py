# Copyright © Aidan Allen - All Rights Reserved
# Unauthorized copying of this project, via any medium is strictly prohibited
# Proprietary and confidential
# Written by <blink@aaix.dev>, 29 Dec 2023


import datetime
import json
from random import Random as RAND
import discord
from discord.ext import commands
import functools
import asyncio
from aiohttp import ClientSession
import time
from collections import OrderedDict
from queue import PriorityQueue
from typing import Callable, List, Union
import re


urlregex = re.compile(r"^(http:\/\/www\.|https:\/\/www\.|http:\/\/|https:\/\/)[a-z0-9]+([\-\.]{1}[a-z0-9]+)*\.[a-z]{2,5}(:[0-9]{1,5})?(\/.*)?$")


# lists used for alphabet conversion
# could use ascii_lowercase but what benefit ?
eng = ['a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l',
       'm', 'n', 'o', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z']
conversion = [
    ["𝔞", "𝔟", "𝔠", "𝔡", "𝔢", "𝔣", "𝔤", "𝔥", "𝔦", "𝔧", "𝔨", "𝔩", "𝔪", "𝔫", "𝔬",
        "𝔭", "𝔮", "𝔯", "𝔰", "𝔱", "𝔲", "𝔳", "𝔴", "𝔵", "𝔶", "𝔷"],  # ascii + 119997
    ['𝖆', '𝖇', '𝖈', '𝖉', '𝖊', '𝖋', '𝖌', '𝖍', '𝖎', '𝖏', '𝖐', '𝖑', '𝖒', '𝖓', '𝖔',
        '𝖕', '𝖖', '𝖗', '𝖘', '𝖙', '𝖚', '𝖛', '𝖜', '𝖝', '𝖞', '𝖟'],  # ascii + 120101
    ['𝓪', '𝓫', '𝓬', '𝓭', '𝓮', '𝓯', '𝓰', '𝓱', '𝓲', '𝓳', '𝓴', '𝓵', '𝓶', '𝓷', '𝓸',
        '𝓹', '𝓺', '𝓻', '𝓼', '𝓽', '𝓾', '𝓿', '𝔀', '𝔁', '𝔂', '𝔃'],  # ascii + 119945
    ['𝒶', '𝒷', '𝒸', '𝒹', '𝑒', '𝒻', '𝑔', '𝒽', '𝒾', '𝒿', '𝓀', '𝓁', '𝓂', '𝓃', '𝑜', '𝓅',
        '𝓆', '𝓇', '𝓈', '𝓉', '𝓊', '𝓋', '𝓌', '𝓍', '𝓎', '𝓏']  # ascii + 119893 or 119789
]


# unbelivabley simple class
class Timer:
    """Context manager that will store time since created since initialised"""
    def __enter__(self): # called using the with keyword
        self.start = time.perf_counter()
        return self

    def __exit__(self, *args):
        # Given exception information when manager is closed
        # too bad that isnt our job so we can ignore
        pass

    @property
    def time(self):
        """Return the time since the timer was created"""
        return time.perf_counter() - self.start


class DBCache():
    def __init__(self, db, identifier: str, statement: str, values: tuple):
        self.db = db # Pooled database connection
        self.identifier = identifier # The unique ID of the cache
        self.statement = statement # SQL Query to update cache
        self.values = values # Values to use to query
        self._value = None # Internal value
        self._current = False # If data is current

    def __repr__(self):
        return f"<In memory DB cache - {self.statement}, {self.values}>"

    async def _set_value(self):
        """Fetch data from database"""
        self._value = await self.db.fetchrow(self.statement, *self.values)
        self._current = True

    async def __aenter__(self):
        if not self._current: # Update when accessed from a context manager
            await self.update()
        return self

    async def __aexit__(*args):
        # exceptions are still not our problem
        return

    @property
    def value(self):
        return self._value

    def invalidate(self):
        self._current = False

    async def update(self):
        # MUST INVALIDATE SO CURRENT IS NOT TRUE WHILE FETCHING FROM DB
        # prevents silly race conditions and stuff
        self.invalidate()
        await self._set_value()

    async def bot_invalidate(self, bot):
        """Tell other clusters that this cache has been modified"""
        await bot.invalidate_cache(self.identifier)


def _rolecheck(name, term, checks: List[Callable]) -> int:
    """Refer to role search algorithm in design"""
    confidences = set()

    for confidence, func in enumerate(checks): # Run the checks before doing expensive computation
        if func(name, term):
            confidences.add(confidence)

    if 0 in confidences: # If we have a direct match return before doing expensive computation
        return 0

    for alphabet in conversion: # Support fancy text generator by replacing a-z with 'fancytext'
        check = term
        for x in range(0, 26): # str.translate cannot be used because it doesnt support non ascii
            check = check.replace(eng[x], alphabet[x])
        for confidence, func in enumerate(checks):
            if func(name, check):
                confidences.add(confidence)

    if confidences:
        return min(confidences) # Lower is more confident as it used an earlier check
    else:
        return -1 # no match found


async def searchrole(roles: list, term: str) -> discord.Role:
    """Custom role search for discord.py"""
    loop = asyncio.get_event_loop()

    matches = PriorityQueue()

    # our list of checks, the index is the confidence, lower is better
    checks = [
        (lambda name, term: name == term),
        (lambda name, term: name.startswith(term)),
        (lambda name, term: term in name)
    ]

    for r in roles: # These must be run in executor because they are potentially expensive to compute and would block the event loop
        # funtools.partial turns f(x) into g() where x is stored inside g
        # we can use this to consume our arguments into a function that can be called by the event loop without needing our arguments
        confidence = await loop.run_in_executor(None, functools.partial(_rolecheck, r.name.lower(), term.lower(), checks))
        if confidence == 0:
            return r
        elif confidence > 0:
            matches.put((confidence, r))

    if not matches.empty():
        return matches.get()[1] # we want the role not the (confidence, role) tuple

    # by default we return None here, indicating a role was not found


def ordinal(n: int):
    """Turns an int into its ordinal (1 -> 1st)"""
    return f"{n}{'tsnrhtdd'[(n//10!=1)*(n%10<4)*n%10::4]}"

    # refer to algorithms design section

    # n//10 gives the number of 10s n is divisible by
    # we make this a bool by comparing it to 1
    # if it is not 1 then the bool will be false or a 0 int
    # this causes all numbers between 10-19 to have a as 0
    # giving the 'th' result

    # we then find the remainder after dividing by 10
    # and check if its less than 4
    # n%10<4 returns a bool and if false the int 0
    # meaning any numbers greater than or equal to 4
    # have b being 0, returning 'th' when subscripted

    # finally n%10 returns the remainder after dividing
    # by 10, this is the last digit of the n
    # this causes c to be n

    # multiplying all 3 of these together, we get
    # x = a*b*c
    # x = 1-3 for the any n where it ends in 1-3 (b*c)
    # except in 10-19 where we multiply x by 0 (a)
    # to x = 0

    # the string 'tsnrhtdd' is then subscripted [x::4]
    # this returns the letter at index x and x+5
    # 0 --> t, h
    # 1 --> s, t
    # 2 --> n, d
    # 3 --> r, d


# Deprecated configuration only used in clustering
# could be moved but these are not volatile like other config settings
class Config():
    @classmethod
    def newguilds(self):
        return int(702201857606549646)

    @classmethod
    def errors(self):
        return 702201821615358004

    @classmethod
    def startup(self):
        return 702705386557276271

    @classmethod
    def warns(self):
        return 722131357136060507


def prettydelta(seconds):
    """Function to turn seconds into days minutes hours"""
    seconds = int(seconds)
    days, seconds = divmod(seconds, 86400)
    hours, seconds = divmod(seconds, 3600)
    minutes, seconds = divmod(seconds, 60)
    if days > 0:
        return '%dd %dh %dm %ds' % (days, hours, minutes, seconds)
    elif hours > 0:
        return '%dh %dm %ds' % (hours, minutes, seconds)
    elif minutes > 0:
        return '%dm %ds' % (minutes, seconds)
    else:
        return '%ds' % (seconds,)


def prand(spice: float, uid: int, start: int, stop: int, inverse: bool = False):
    """Seeded random"""
    b = uid * spice
    rng = RAND(x=(b))
    return rng.randint(start, stop)


# MUSIC ERRORS
class NoChannelProvided(commands.CommandError):
    """Error raised when no suitable voice channel was supplied."""
    pass


class IncorrectChannelError(commands.CommandError):
    """Error raised when commands are issued outside of the players session channel."""
    pass


# Raised to propogate up when something goes wrong
class SilentWarning(Exception):
    """Error for backing out of tasks with a warning"""
    pass


# Cog class provides default attributes, registers to bot and also cleans up clientsessions
class Cog(commands.Cog):
    def __init__(self, bot, identifier: str):
        self.bot = bot
        self.identifier = identifier
        bot._cogs.register(self, self.identifier) # Allow easy access during runtime code evaluation

    def cog_unload(self): # Called when a cog is unloaded
        self.bot._cogs.unregister(self.identifier)
        if hasattr(self, "session") and isinstance(self.session, ClientSession):
            self.bot.loop.create_task(self.session.close())


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


# class for command context
class Ctx(commands.Context):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        if self.guild and hasattr(self.bot, "wavelink"): # Attach player if music running
            self.player = self.bot.wavelink.players.get(self.guild.id)

    def __repr__(self):
        return f"<Blink context, author={self.author}, guild={self.guild}, message={self.message}>"

    @property
    def clean_prefix(self):
        """Returns the prefix used and will parse a id into a username"""
        # basically turns <@692738917236998154> into @blink

        # technically we should always have a guild because we dont recieve
        # dm messages by choice but checked here in case of cache discrepancy
        user = self.guild.me if self.guild else self.bot.user # displayname
        pattern = re.compile(r"<@!?%s>" % user.id) # literal basic regex
        return pattern.sub("@%s" % user.display_name.replace('\\', r'\\'), self.prefix)

    async def send(self, *args, **kwargs):
        if self.message.reference: # Reference message that the original message referenced when responding
            self.message.reference.fail_if_not_exists = False
            if not kwargs.get("reference"):
                kwargs["reference"] = self.message.reference
                kwargs["mention_author"] = False
        return await super().send(*args, **kwargs)


class CogStorage:
    """Dummy object used as an attribute to store each individual cog"""#
    # accessed with obj.cogname
    # eg _cogs.logging
    # this is really just basic python

    def __dir__(self):
        return sorted([a for a in super().__dir__() if not ((a.startswith("__") and a.endswith("__")) or a in ["register", "unregister"])])

    def __len__(self):
        return len(dir(self))

    def register(self, obj: object, identifier: str):
        setattr(self, identifier, obj)

    def unregister(self, identifer: str):
        delattr(self, identifer)


class ServerCache(DBCache):
    """Subclass of DBCache for guild data, parses json"""
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.exists = False

    async def _set_value(self):
        await super()._set_value()
        # deserialse our data into json
        # we may also be fetching a dead entry so we
        # use exists to determine if the record is real
        if self._value:
            self._value = json.loads(self._value['data'])
            self.exists = True
        else:
            self.exists = False
            self.value = {}

    async def save(self, guild_id: int, bot):
        """Save the dictionary to the database"""
        # guild_id and bot could be class attrs but if we have them here'
        # it makes sure when writing code we know what and where we are writing
        # and not to do tomfoolery
        await bot.DB.execute("UPDATE guilds SET data=$1 WHERE id=$2", json.dumps(self.value), guild_id)
        await self.bot_invalidate(bot)

    @property
    def value(self):
        return self._value

    @value.setter
    def value(self, other):
        self._value = other


class UrlConverter(commands.Converter):
    """Convertor to parse URLs from a string or text"""
    async def convert(self, ctx, argument):
        argument = argument.strip()
        if urlregex.match(argument):
            if len(argument) > 1000:
                raise commands.BadArgument("Urls must be at most 1000 characters")
            return argument
        else:
            if ctx.message.attachments:
                return ctx.message.attachments[0].url
            raise commands.BadArgument("String could not be interpereted as a url")


class SpotifyApiResponseError(Exception):
    def __init__(self, status, *args: object) -> None:
        super().__init__(*args)
        self.status = status


class SpotifyData():
    __slots__ = ("title", "artists", "album", "track_id", "started_at", "ends_at", "icon_url", "colour")

    def __init__(self, data: Union[dict, discord.Spotify]):
        if isinstance(data, discord.Spotify):
            self.init_sp(data)
        elif isinstance(data, dict):
            self.init_dict(data)
        else:
            raise TypeError(f"Expected either dict or discord.Spotify got {type(data)}")
        self.colour = 0x1db954

    def init_sp(self, spotify: discord.Spotify):
        self.title = spotify.title
        self.artists = spotify.artist
        self.album = spotify.album
        self.track_id = spotify.track_id
        self.started_at = spotify.created_at
        self.ends_at = spotify.end
        self.icon_url = spotify.album_cover_url

    def init_dict(self, payload: dict):
        track = payload['item']
        album = track['album']
        artists = track['artists']
        self.title = track['name']
        self.artists = "; ".join(a['name'] for a in artists)
        self.album = album['name']
        self.track_id = track['id']
        self.started_at = datetime.datetime.fromtimestamp(payload['timestamp'] / 1000, tz=datetime.timezone.utc)
        self.ends_at = self.started_at + datetime.timedelta(milliseconds=track['duration_ms'])
        self.icon_url = album['images'][0]['url']
