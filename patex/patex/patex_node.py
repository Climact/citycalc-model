import pandas as pd

from patex.nodes.node import Globals, PythonNode, FlowVars
from patex.nodes import *

from .metanode_9094 import metanode_9094
from .metanode_9096 import metanode_9096
from .metanode_9109 import metanode_9109
from .metanode_9097 import metanode_9097
from .metanode_9099 import metanode_9099
from .metanode_9100 import metanode_9100
from .metanode_1 import metanode_1
from .metanode_9000 import metanode_9000
from .metanode_9098 import metanode_9098
from .metanode_9102 import metanode_9102
from .metanode_9076 import metanode_9076
from .metanode_9101 import metanode_9101
from .metanode_9053 import metanode_9053


def patex_node():
    # Select parameters


    # Import data for corresponding ambition levels


    # Macro-Economy
    # DISCONNECTED FOR THE MOMENT !!
    # Deleted => should be set again
    # (but inside each modules
    # instead)


    # Import lever positions


    # Visualize outputs


    # DB
    # Il manque les DB de :
    # - Air Quality
    # - Minerals
    # - Macro-eco
    # => A rajouter quand on veut ces modules (cfr version antérieure à la release v14.1)


    # Select Countries 
    # to run


    # Visualize outputs


    # Visualize outputs


    # Visualize outputs


    # Visualize outputs


    # Visualize outputs


    # Visualize outputs


    # Visualize outputs


    # Visualize outputs


    # Export to database and to google sheet


    # Visualize outputs




    # Lifestyle

    # Lifestyle module
    out_9094_1, _, out_9094_3, out_9094_4, out_9094_5, out_9094_6, out_9094_7, _, _ = metanode_9094()

    # Import historical and future time series for all levers

    # Transport

    # Transport module
    out_9096_1, out_9096_2, out_9096_3, _, _, _, out_9096_7, _, out_9096_9, _ = metanode_9096(port_01=out_9094_4)

    # Buildings

    # Buildings module
    out_9109_1, _, out_9109_3, out_9109_4, out_9109_5, out_9109_6, _ = metanode_9109(port_01=out_9094_3)

    # Agriculture

    # Agriculture module
    out_9097_1, _, out_9097_3, out_9097_4, out_9097_5, out_9097_6, out_9097_7, out_9097_8, _, out_9097_10, out_9097_11 = metanode_9097(port_01=out_9094_6)

    # Industry

    # Industry module
    out_9099_1, _, out_9099_3, out_9099_4, out_9099_5, out_9099_6, _, out_9099_8, out_9099_9 = metanode_9099(port_01=out_9094_5, port_02=out_9109_3, port_03=out_9096_2, port_04=out_9097_3)

    # Power Supply

    # Power supply module
    out_9100_1, _, out_9100_3, out_9100_4, out_9100_5, _, out_9100_7, out_9100_8 = metanode_9100(port_01=out_9096_3, port_02=out_9109_4, port_03=out_9099_4, port_04=out_9097_5)

    # Water

    out_1_1, _ = metanode_1(port_01=out_9094_7, port_02=out_9099_8, port_03=out_9097_11, port_04=out_9100_8)

    # Energy calculation: RES share and Sankey

    # Wrond input data: it should come from the powersupply module (output to TPE) using the final-energy-demand

    out_9000_1, out_9000_2 = metanode_9000(port_02=out_9096_3, port_03=out_9097_5, port_04=out_9099_4, port_05=out_9100_1, port_01=out_9109_4)

    # Land use

    # Land-Use module (module-name = agriculture)
    out_9098_1, _, out_9098_3, out_9098_4, out_9098_5 = metanode_9098(port_01=out_9097_4)

    # Air Quality

    out_9102_1, _ = metanode_9102(port_02=out_9096_9, port_01=out_9109_6, port_03=out_9097_8, port_04=out_9098_5, port_05=out_9099_6, port_06=out_9100_5)

    # BioEnergy


    out_9098_3 = column_filter(df=out_9098_3, columns_to_drop=[])
    # Bioenergy Balance module (module-name = agriculture)
    out_9076_1, _, out_9076_3 = metanode_9076(port_02=out_9097_6, port_01=out_9099_3, port_04=out_9100_4, port_03=out_9098_3)

    # Scope 2/3

    out_9101_1 = metanode_9101(port_01=out_9100_7, port_02=out_9099_9, port_03=out_9097_10, port_04=out_9076_3)

    # EMISSIONS

    out_9053_1, out_9053_2 = metanode_9053(port_02=out_9096_7, port_04=out_9097_7, port_01=out_9109_5, port_03=out_9099_5, port_05=out_9098_4, port_06=out_9100_3)

    return {
        'node_9109_out_1': out_9109_1,
        'node_9076_out_1': out_9076_1,
        'node_9096_out_1': out_9096_1,
        'node_9100_out_1': out_9100_1,
        'node_9097_out_1': out_9097_1,
        'node_9099_out_1': out_9099_1,
        'node_9098_out_1': out_9098_1,
        'node_9094_out_1': out_9094_1,
        'node_9053_out_1': out_9053_1,
        'node_9053_out_2': out_9053_2,
        'node_9000_out_1': out_9000_1,
        'node_9000_out_2': out_9000_2,
        'node_9101_out_1': out_9101_1,
        'node_1_out_1': out_1_1,
        'node_9102_out_1': out_9102_1,
    }


