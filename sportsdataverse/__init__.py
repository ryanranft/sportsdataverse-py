from importlib.metadata import version, PackageNotFoundError

try:
    __version__ = version("sportsdataverse")
except PackageNotFoundError:
    __version__ = "unknown"

from sportsdataverse.cfb import *
from sportsdataverse.mbb import *
from sportsdataverse.nba import *
from sportsdataverse.nfl import *
from sportsdataverse.nhl import *
from sportsdataverse.wbb import *
from sportsdataverse.wnba import *

# from sportsdataverse.mlb import *
