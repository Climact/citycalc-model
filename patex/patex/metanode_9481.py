import pandas as pd

from patex.nodes.globals import Globals
from patex.nodes import *


def metanode_9481(port_01, port_02, port_03, port_04, port_05, port_06, port_07):

    module_name = 'agriculture'
    port_2 = pd.concat([port_02, port_03.set_index(port_03.index.astype(str) + '_dup')])
    port = pd.concat([port_04, port_05.set_index(port_05.index.astype(str) + '_dup')])
    port = pd.concat([port_2, port.set_index(port.index.astype(str) + '_dup')])
    port_2 = pd.concat([port_01, port.set_index(port.index.astype(str) + '_dup')])
    port = pd.concat([port_06, port_07.set_index(port_07.index.astype(str) + '_dup')])
    port = pd.concat([port_2, port.set_index(port.index.astype(str) + '_dup')])

    return port


