import pandas as pd

from patex.nodes.globals import Globals
from patex.nodes import *


# output
# =
# CCS
def metanode_9621(port_01, port_02, port_03):
    # Note :
    # 
    # When no CC, division is made by 0 leading to missing value.
    # 
    # If we have CCS (CC - remaining demand has heavy chance to be negatif), we should attribute this surplus of CCS to a sector.
    # 
    # We decide here to attribute all to refineries (by default). So if msising value, they are set to 1 in refineries and 0 in others sectors


    # Note :
    # 
    # It should not be frequent to have too less CO2 captured. That would means H2 demand is exploding !
    # If CCS < 0 : we keep it like this (message below grahs indicateing there is a lack between demand and supply)
    # 
    # In the future : could be a KPIs or alert in top of the website !



    module_name = 'electricity_supply'

    # E-FUELS Production

    # Carbon used (CCU)
    # The carbon used comes from the sub-module "CCU (Efuels and co)". It gives the CO2 demand. These demand could be furnished by DAC or Carbon Capture.
    # 
    # This demand include CO2 need for :
    # 	- Power production (efuels)
    # 	- Industry (feedstock, process)

    # CCU [Mt] 
    CCU_Mt = use_variable(input_table=port_02, selected_variable='CCU[Mt]')

    # CC Balance
    # 1. We compute CC left after CCU
    # 2.a. If remaining CC >= 0 => We store it (CCS)
    # 2.b. If remaining CC < 0 => We raise warning

    # Group by  Country, Years, gaes (SUM)
    CCU_Mt = group_by_dimensions(df=CCU_Mt, groupby_dimensions=['Country', 'Years', 'gaes'], aggregation_method='Sum')

    # [PRODUCTION] Carbon captured (CC) and Direct air capture (DAC)
    # This carbon comes from industrial process and power production (CC) or DAC.
    # It could be used (in industry process / efuels production) (=CCU) or could be stored (=CCS).

    # direct-air-capture [Mt] 
    direct_air_capture_Mt = use_variable(input_table=port_03, selected_variable='direct-air-capture[Mt]')
    # Group by  Country, Years, gaes (SUM)
    direct_air_capture_Mt = group_by_dimensions(df=direct_air_capture_Mt, groupby_dimensions=['Country', 'Years', 'gaes'], aggregation_method='Sum')
    # remaining-CCU[Mt] (correspond to remaining CCU after DAC) = CCU[Mt] - direct-air-capture[Mt]
    remaining_CCU_Mt = mcd(input_table_1=direct_air_capture_Mt, input_table_2=CCU_Mt, operation_selection='y - x', output_name='remaining-CCU[Mt]')
    # CC [Mt] from Power and other sectors
    CC_Mt = use_variable(input_table=port_01, selected_variable='CC[Mt]')
    # Group by  Country, Years, sector, gaes (SUM)
    CC_Mt = group_by_dimensions(df=CC_Mt, groupby_dimensions=['Country', 'Years', 'gaes', 'sector'], aggregation_method='Sum')
    # Group by  Country, Years, gaes (SUM)
    CC_Mt_2 = group_by_dimensions(df=CC_Mt, groupby_dimensions=['Country', 'Years', 'gaes'], aggregation_method='Sum')
    # CC-proportion[-] = CC[Mt] (by sector) / CC[Mt] (total)
    CC_proportion = mcd(input_table_1=CC_Mt, input_table_2=CC_Mt_2, operation_selection='x / y', output_name='CC-proportion[-]')
    # Include sector = refineries
    CC_proportion_2 = CC_proportion.loc[CC_proportion['sector'].isin(['refineries'])].copy()
    CC_proportion_excluded = CC_proportion.loc[~CC_proportion['sector'].isin(['refineries'])].copy()
    # Set 0 (no CC) (others than refineries)
    CC_proportion_excluded = missing_value(df=CC_proportion_excluded, DTS_DT_O=[['org.knime.core.data.def.IntCell', 'org.knime.base.node.preproc.pmml.missingval.handlers.DoNothingMissingCellHandlerFactory'], ['org.knime.core.data.def.StringCell', 'org.knime.base.node.preproc.pmml.missingval.handlers.DoNothingMissingCellHandlerFactory'], ['org.knime.core.data.def.DoubleCell', 'org.knime.base.node.preproc.pmml.missingval.handlers.FixedDoubleValueMissingCellHandlerFactory']], FixedValue='0.0')
    # Set 1 (no CC) (refineries)
    CC_proportion = missing_value(df=CC_proportion_2, DTS_DT_O=[['org.knime.core.data.def.IntCell', 'org.knime.base.node.preproc.pmml.missingval.handlers.DoNothingMissingCellHandlerFactory'], ['org.knime.core.data.def.StringCell', 'org.knime.base.node.preproc.pmml.missingval.handlers.DoNothingMissingCellHandlerFactory'], ['org.knime.core.data.def.DoubleCell', 'org.knime.base.node.preproc.pmml.missingval.handlers.FixedDoubleValueMissingCellHandlerFactory']], FixedValue='1.0')
    CC_proportion = pd.concat([CC_proportion, CC_proportion_excluded.set_index(CC_proportion_excluded.index.astype(str) + '_dup')])
    # CCS[Mt] = CC[Mt] - remainning-CCU[Mt]
    CCS_Mt = mcd(input_table_1=CC_Mt_2, input_table_2=remaining_CCU_Mt, operation_selection='x - y', output_name='CCS[Mt]')
    # CCS[Mt] (replace) (detailled by sector) = CCS[Mt] * CC-proportion[-]
    CCS_Mt = mcd(input_table_1=CC_proportion, input_table_2=CCS_Mt, operation_selection='x * y', output_name='CCS[Mt]')

    def helper_7313(input_table) -> pd.DataFrame:
        # Copy input to output
        output_table = input_table.copy()
        
        mask = (output_table['CCS[Mt]'] < 0)
        ## Create new table with refineries values
        output_table_refineries = output_table.loc[mask, :]
        output_table_refineries['sector'] = "refineries"
        ## Set previous values to 0
        output_table.loc[mask, 'CCS[Mt]'] = 0
        ## Concat tables
        output_table = pd.concat([output_table, output_table_refineries], ignore_index=True)
        return output_table
    # Force negative values to be attributed to refineries
    out_7313_1 = helper_7313(input_table=CCS_Mt)
    # Group by all dimensions (sum) (remove duplicates)
    out_7313_1 = group_by_dimensions(df=out_7313_1, groupby_dimensions=['Country', 'Years', 'gaes', 'sector'], aggregation_method='Sum')

    # Formating data for other modules + Pathway Explorer

    # Carbon capture storage (CCS)

    # CCS [Mt]
    CCS_Mt = use_variable(input_table=out_7313_1, selected_variable='CCS[Mt]')

    return CCS_Mt


