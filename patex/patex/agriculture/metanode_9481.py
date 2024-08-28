import pandas as pd

from patex.helpers.globals import Globals
from patex.helpers import *


def metanode_9481(port_01, port_02, port_03, port_04, port_05, port_06, port_07):

    module_name = 'agriculture'
    port_2 = pd.concat([port_02, port_03])
    port = pd.concat([port_04, port_05])
    port = pd.concat([port_2, port])
    port_2 = pd.concat([port_01, port])
    port = pd.concat([port_06, port_07])
    port = pd.concat([port_2, port])

    return port


