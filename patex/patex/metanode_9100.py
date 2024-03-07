import pandas as pd

from patex.nodes.globals import Globals
from patex.nodes import *

from .metanode_7248 import metanode_7248
from .metanode_6428 import metanode_6428
from .metanode_6429 import metanode_6429
from .metanode_6430 import metanode_6430
from .metanode_7083 import metanode_7083
from .metanode_9763 import metanode_9763
from .metanode_7084 import metanode_7084
from .metanode_6432 import metanode_6432
from .metanode_9621 import metanode_9621
from .metanode_9702 import metanode_9702
from .metanode_6469 import metanode_6469
from .metanode_6480 import metanode_6480
from .metanode_6501 import metanode_6501
from .metanode_6514 import metanode_6514


# Power supply module
def metanode_9100(port_01, port_02, port_03, port_04):
    # Calibration RATES


    # Energy demand [TWh] from all previous sectors


    # Cal rate


    # Concatenate of a same variable
    # coming from differents sources
    # 
    # Here : energy-demand [TWh]


    # Concatenate of a same variable
    # coming from differents sources
    # 
    # Here : energy-demand [TWh]


    # Concatenate of a same variable
    # coming from differents sources
    # 
    # Here : energy-imported[TWh]


    # Concatenate of a same variable
    # coming from differents sources
    # 
    # Here : energy-demand [TWh]


    # Concatenate of a same variable
    # coming from differents sources
    # 
    # Here : energy-demand [TWh]


    # Concatenate of a same variable
    # coming from differents sources
    # 
    # Here : energy-production[TWh]


    # CO2 capture [Mt] from all previous sectors (CC)
    # For the moment, only industry provides CO2 ; no need to concatenate data from differents sectors then.


    # Concatenate of a same variable
    # coming from differents sources
    # 
    # Here : energy-imported[TWh]


    # Concatenate of a same variable
    # coming from differents sources
    # 
    # Here : cal-rate[%]


    # CO2 demand [Mt] from all previous sectors (CC)
    # For the moment, only industry demands CO2 ; no need to concatenate data from differents sectors then.


    # Concatenate of a same variable
    # coming from differents sources
    # 
    # Here : final-energy-costs[MEUR]


    # Concatenate of a same variable
    # coming from differents sources
    # 
    # Here : energy-demand[TWh]


    # Concatenate of a same variable
    # coming from differents sources
    # 
    # Here : capex-opex


    # Concatenate of a same variable
    # coming from differents sources
    # 
    # Here : energy-production[TWh]


    # Concatenate of a same variable
    # coming from differents sources
    # 
    # Here : cal-rate[%]


    # Concatenate of a same variable
    # coming from differents sources
    # 
    # Here : energy-imported[TWh]


    # Concatenate of a same variable
    # coming from differents sources
    # 
    # Here : fuel-price[MEUR/TWh]


    # Concatenate of a same variable
    # coming from differents sources
    # 
    # Here : energy-demand [TWh]


    # Concatenate of a same variable
    # coming from differents sources
    # 
    # Here : energy-demand[TWh]


    # Concatenate of a same variable
    # coming from differents sources
    # 
    # Here : final-energy-costs[MEUR]


    # Concatenate of a same variable
    # coming from differents sources
    # 
    # Here : capex-opex


    # Concatenate of a same variable
    # coming from differents sources
    # 
    # Here : energy-production[TWh]


    # Concatenate of a same variable
    # coming from differents sources
    # 
    # Here : energy-imported[TWh]


    # Concatenate of a same variable
    # coming from differents sources
    # 
    # Here : fuel-price[MEUR/TWh]


    # Concatenate of a same variable
    # coming from differents sources
    # 
    # Here : energy-demand[TWh]


    # Concatenate of a same variable
    # coming from differents sources
    # 
    # Here : final-energy-costs[MEUR]


    # Concatenate of a same variable
    # coming from differents sources
    # 
    # Here : capex-opex


    # Concatenate of a same variable
    # coming from differents sources
    # 
    # Here : energy-production[TWh]


    # Concatenate of a same variable
    # coming from differents sources
    # 
    # Here : fuel-price[MEUR/TWh]


    # Concatenate of a same variable
    # coming from differents sources
    # 
    # Here : energy-demand[TWh]


    # Concatenate of a same variable
    # coming from differents sources
    # 
    # Here : final-energy-costs[MEUR]


    # Concatenate of a same variable
    # coming from differents sources
    # 
    # Here : emissions[Mt]


    # Concatenate of a same variable
    # coming from differents sources
    # 
    # Here : energy-demand[TWh]


    # Concatenate of a same variable
    # coming from differents sources
    # 
    # Here : fuel-price[MEUR/TWh]


    # Concatenate of a same variable
    # coming from differents sources
    # 
    # Here : emissions[Mt]


    # Concatenate of a same variable
    # coming from differents sources
    # 
    # Here : emissions[Mt]


    # Concatenate of a same variable
    # coming from differents sources
    # 
    # Here : emissions[Mt]


    # Concatenate of a same variable
    # coming from differents sources
    # 
    # Here : primary-energy-demand[TWh]


    # Concatenate of a same variable
    # coming from differents sources
    # 
    # Here : emissions[Mt]


    # Concatenate of a same variable
    # coming from differents sources
    # 
    # Here : primary-energy-demand[TWh]


    # Change for CO2 Value project :
    # 
    # - Vérifier que c'est fonctionnel une fois qu'on a les bonnes valeurs dans CAPEX/OPEX data
    # - Les inclure en tant qu'output ici et dans l'interface / graphes


    # QUICK FIX


    # For : Air Quality
    # 
    # - Energy demand (for power production)


    # Split : primary energy carrier and non-primary
    # => Split should be reviewed ? or coming from external data ?


    # Split : sector Heat between heat-only and CHP


    # Concatenate all :
    # - liquid-syn fuels
    # - gaseous-ff fuels
    # - liquid-bio fuels
    # - liquid-ff and liquid-ff-oil


    # Concatenate all :
    # - liquid-syn fuels
    # - gaseous-ff fuels
    # - liquid-bio fuels
    # - liquid-ff and liquid-ff-oil


    # Aggregate all liquid-syn together



    module_name = 'electricity_supply'

    # Formating data for other modules + Pathway Explorer

    # For : Pathway Explorer
    # - Import and exports

    # Aggregation table for energy-carrier
    out_9358_1 = pd.DataFrame(columns=['energy-carrier', 'energy-carrier-agg'], data=[['gaseous-syn', 'gaseous-synfuels'], ['liquid-syn', 'liquid-synfuels'], ['liquid-syn-diesel', 'liquid-synfuels'], ['liquid-syn-gasoline', 'liquid-synfuels'], ['liquid-syn-kerosene', 'liquid-synfuels'], ['liquid-syn-marinefueloil', 'liquid-synfuels'], ['heat', 'heat'], ['hydrogen', 'hydrogen'], ['electricity', 'electricity']])

    # For : Minerals (and Pathway Explorer)
    # - Primary-energy-demand : from refineries / electricity
    # - New-capacity for electricity production

    out_6982_1 = pd.DataFrame(columns=['new_name', 'old_name', 'column1'], data=[['new-capacities_elec-plant-with-solid-bio-waste[GW]', 'elc_new-capacity_RES_bio_mass[GW]', 'NaN'], ['new-capacities_RES-geothermal[GW]', 'elc_new-capacity_RES_other_geothermal[GW]', 'NaN'], ['new-capacities_RES-hydroelectric[GW]', 'elc_new-capacity_RES_other_hydroelectric[GW]', 'NaN'], ['new-capacities_RES-marine[GW]', 'elc_new-capacity_RES_other_marine[GW]', 'NaN'], ['new-capacities_RES-solar-csp[GW]', 'elc_new-capacity_RES_solar_csp[GW]', 'NaN'], ['new-capacities_RES-wind-offshore[GW]', 'elc_new-capacity_RES_wind_offshore[GW]', 'NaN'], ['new-capacities_RES-wind-onshore[GW]', 'elc_new-capacity_RES_wind_onshore[GW]', 'NaN'], ['new-capacities_elec-plant-with-gas[GW]', 'elc_new-capacity_fossil_gas[GW]', 'NaN'], ['new-capacities_elec-plant-with-liquid[GW]', 'elc_new-capacity_fossil_oil[GW]', 'NaN'], ['new-capacities_elec-plant-with-nuclear[GW]', 'elc_new-capacity_nuclear[GW]', 'NaN'], ['new-capacities_elec-plant-with-solid-coal[GW]', 'elc_new-capacity_fossil_coal[GW]', 'NaN']])

    # For : Pathway Explorer
    # - Energy-demand from sector linked to power supply (heat / hydrogen / efuels / electricity / fossil fuels) and others sectors

    # Direct use
    out_6652_1 = pd.DataFrame(columns=['sector', 'direct-use'], data=[['bld', 'for-sector'], ['tra', 'for-sector'], ['ind', 'for-sector'], ['amm', 'for-sector'], ['agr', 'for-sector'], ['lus', 'for-sector'], ['electricity', 'for-power-prod'], ['heat', 'for-power-prod'], ['efuels', 'for-power-prod'], ['hydrogen', 'for-power-prod'], ['refineries', 'for-power-prod'], ['DAC', 'for-power-prod'], ['losses', 'for-power-prod']])

    # For : Minerals
    # - Primary-energy-demand : from refineries / electricity
    # - New-capacity for electricity production

    out_6548_1 = pd.DataFrame(columns=['new_name', 'old_name'], data=[['energy-demand-total_gaseous-ff[TWh]', 'fos_primary-demand_gas[TWh]'], ['energy-demand-total_liquid-ff[TWh]', 'fos_primary-demand_oil[TWh]'], ['energy-demand-total_solid-ff[TWh]', 'fos_primary-demand_coal[TWh]']])

    # Adapt data from other module => pivot and co

    # Agriculture
    out_7479_1 = port_04

    # Additional demand for electricity [TWh] due to :
    # - Losses
    # - Refineries demand

    # Add losses to energy demand

    # HTS network-losses [TWh]
    network_losses_TWh = import_data(trigram='elc', variable_name='network-losses', variable_type='HTS')
    # HTS network-losses-additional [TWh]
    network_losses_additional_TWh = import_data(trigram='elc', variable_name='network-losses-additional', variable_type='HTS')

    # Add demand for refineries to energy demand

    # OTS (only) energy-demand [TWh] (electricity demand for refineries)
    energy_demand_TWh = import_data(trigram='elc', variable_name='energy-demand', variable_type='OTS (only)')
    # Same as last available year
    energy_demand_TWh = add_missing_years(df_data=energy_demand_TWh)
    # energy-demand[TWh] = network-losses[TWh] + network-losses-additional[TWh]
    energy_demand_TWh_2 = mcd(input_table_1=network_losses_TWh, input_table_2=network_losses_additional_TWh, operation_selection='x + y', output_name='energy-demand[TWh]')
    # sector = losses
    energy_demand_TWh_2['sector'] = "losses"
    energy_demand_TWh = pd.concat([energy_demand_TWh_2, energy_demand_TWh.set_index(energy_demand_TWh.index.astype(str) + '_dup')])
    # Industry
    out_7478_1 = port_03

    # CO2 production (Carbon Capture)

    # CC [Mt]
    CC_Mt = use_variable(input_table=out_7478_1, selected_variable='CC[Mt]')
    # sector = ind
    CC_Mt['sector'] = "ind"

    # Initial energy-demand [TWh]

    # CC [Mt]
    CC_Mt = export_variable(input_table=CC_Mt, selected_variable='CC[Mt]')

    # Energy demand

    # CC balance (CCS) / CCU and directe-air-capture

    # CC [Mt] from other sectors
    CC_Mt = use_variable(input_table=CC_Mt, selected_variable='CC[Mt]')

    # Energy demand
    # Note : CCU (carbon capture use) is considered as energy-demand where CO2 = energy-carrier

    # energy-demand [TWh]
    energy_demand_TWh_2 = use_variable(input_table=out_7478_1, selected_variable='energy-demand[TWh]')
    # sector = ind
    energy_demand_TWh_2['sector'] = "ind"
    out_5843_1 = pd.concat([energy_demand_TWh_2, out_7479_1.set_index(out_7479_1.index.astype(str) + '_dup')])

    # CO2 demand (Carbon Capture Use)

    # CCU [Mt]
    CCU_Mt = use_variable(input_table=out_7478_1, selected_variable='CCU[Mt]')
    # sector = ind
    CCU_Mt['sector'] = "ind"
    # CCU [Mt]
    CCU_Mt = export_variable(input_table=CCU_Mt, selected_variable='CCU[Mt]')
    # Buildings
    out_7477_1 = port_02
    # sector = bld
    out_7477_1['sector'] = "bld"
    out_7477_1 = out_7477_1.loc[~out_7477_1['energy-carrier'].isin(['ambiant'])].copy()
    # Transport
    out_5965_1 = port_01
    # sector = tra
    out_5965_1['sector'] = "tra"
    out_1 = pd.concat([out_5965_1, out_7477_1.set_index(out_7477_1.index.astype(str) + '_dup')])
    out_1 = pd.concat([out_1, out_5843_1.set_index(out_5843_1.index.astype(str) + '_dup')])
    out_6998_1 = pd.concat([out_1, energy_demand_TWh.set_index(energy_demand_TWh.index.astype(str) + '_dup')])
    # energy-demand [TWh]
    energy_demand_TWh = export_variable(input_table=out_6998_1, selected_variable='energy-demand[TWh]')
    out_7248_1, out_7248_2 = metanode_7248(port_01=energy_demand_TWh)

    # CHP / Heat :
    # - energy demand [TWh]
    # - energy production [TWh]
    # - CAPEX / OPEX [MEUR]

    out_6428_1, out_6428_2, out_6428_3, out_6428_4, out_6428_5, out_6428_6, out_6428_7, out_6428_8 = metanode_6428(port_01=out_7248_1)

    # Carbon Capture
    # 
    # = Concatenate carbon capture from all power sectors (heat / efuels / hydrogen / eletricity / fossil fuels) ; then will be used (CCU) or storage (CCS)
    # 
    # = Concatenate carbon capture primary energy demand (elec)

    # primary-energy-demand [TWh]
    primary_energy_demand_TWh = use_variable(input_table=out_6428_8, selected_variable='primary-energy-demand[TWh]')

    # Carbon Capture
    # 
    # = Concatenate carbon capture from all power sectors (heat / efuels / hydrogen / eletricity / fossil fuels) ; then will be used (CCU) or storage (CCS)
    # 
    # = Concatenate carbon capture primary energy demand (elec)
    # 
    # => No carbon capture in CCU (Efuels) ; only DAC and CCU

    # primary-energy-demand [TWh]
    primary_energy_demand_TWh = use_variable(input_table=primary_energy_demand_TWh, selected_variable='primary-energy-demand[TWh]')

    # Carbon Capture
    # 
    # = Concatenate carbon capture from all power sectors (heat / efuels / hydrogen / eletricity / fossil fuels) ; then will be used (CCU) or storage (CCS)
    # 
    # = Concatenate carbon capture primary energy demand (elec)
    # 
    # => No capture from hydrogen

    # primary-energy-demand [TWh]
    primary_energy_demand_TWh = use_variable(input_table=primary_energy_demand_TWh, selected_variable='primary-energy-demand[TWh]')

    # Carbon Capture
    # 
    # = Concatenate carbon capture from all power sectors (heat / efuels / hydrogen / eletricity / fossil fuels) ; then will be used (CCU) or storage (CCS)
    # 
    # = Concatenate carbon capture primary energy demand (elec)

    # primary-energy-demand [TWh]
    primary_energy_demand_TWh = use_variable(input_table=primary_energy_demand_TWh, selected_variable='primary-energy-demand[TWh]')
    # CC [Mt]
    CC_Mt_2 = use_variable(input_table=out_6428_7, selected_variable='CC[Mt]')
    # CC [Mt]
    CC_Mt_2 = use_variable(input_table=CC_Mt_2, selected_variable='CC[Mt]')
    # CC [Mt]
    CC_Mt_2 = use_variable(input_table=CC_Mt_2, selected_variable='CC[Mt]')
    # CC [Mt]
    CC_Mt_3 = use_variable(input_table=CC_Mt_2, selected_variable='CC[Mt]')
    # capex-opex [MEUR]
    out_6428_3 = column_filter(df=out_6428_3, pattern='^.*$')

    # Capex / Opex
    # 
    # = Concatenate costs from all power sectors (heat / efuels / hydrogen / eletricity / fossil fuels)

    # capex-opex [MEUR]
    out_6428_3 = column_filter(df=out_6428_3, pattern='^.*$')

    # Primary energy demand
    # 
    # = Sectors demand (except heat demand) + primary energy-carrier needed for heat production

    # primary-energy-demand [TWh]
    primary_energy_demand_TWh_2 = use_variable(input_table=out_6428_4, selected_variable='primary-energy-demand[TWh]')
    # Group by  country, years, primary-energy-carrier, sector, way-of-production
    primary_energy_demand_TWh_2 = group_by_dimensions(df=primary_energy_demand_TWh_2, groupby_dimensions=['Country', 'Years', 'way-of-production', 'primary-energy-carrier', 'sector'], aggregation_method='Sum')
    # primary-energy-demand to energy-demand  and  primary-energy-carrier to energy-carrier
    out_5959_1 = primary_energy_demand_TWh_2.rename(columns={'primary-energy-carrier': 'energy-carrier', 'primary-energy-demand[TWh]': 'energy-demand[TWh]'})
    # energy-imported [TWh]
    energy_imported_TWh = export_variable(input_table=out_6428_1, selected_variable='energy-imported[TWh]')

    # Energy Import
    # 
    # = Concatenate energy import from all power sectors (heat / efuels / hydrogen / eletricity / fossil fuels)

    # energy-imported [TWh]
    energy_imported_TWh_2 = use_variable(input_table=energy_imported_TWh, selected_variable='energy-imported[TWh]')
    # Calibration (primary energy demand & emissions)
    out_6428_6 = column_filter(df=out_6428_6, columns_to_drop=[])
    # energy-production [TWh]
    energy_production_TWh = export_variable(input_table=out_6428_2, selected_variable='energy-production[TWh]')

    # Energy Production
    # 
    # = Concatenate energy production from all power sectors (heat / efuels / hydrogen / eletricity / fossil fuels)

    # energy-production [TWh]
    energy_production_TWh_2 = use_variable(input_table=energy_production_TWh, selected_variable='energy-production[TWh]')

    # Emissions
    # 
    # = Concatenate emissions from all power sectors (heat / efuels / hydrogen / eletricity / fossil fuels)

    # emissions [Mt]
    emissions_Mt = use_variable(input_table=out_6428_5, selected_variable='emissions[Mt]')

    # Emissions
    # 
    # = Concatenate emissions from all power sectors (heat / efuels / hydrogen / eletricity / fossil fuels)
    # 
    # No emissions for Efuels (only CCUS)

    # emissions [Mt]
    emissions_Mt = use_variable(input_table=emissions_Mt, selected_variable='emissions[Mt]')

    # Emissions
    # 
    # = Concatenate emissions from all power sectors (heat / efuels / hydrogen / eletricity / fossil fuels)
    # 
    # The is no emissions for Hydrogen ??!!

    # emissions [Mt]
    emissions_Mt = use_variable(input_table=emissions_Mt, selected_variable='emissions[Mt]')

    # Emissions
    # 
    # = Concatenate emissions from all power sectors (heat / efuels / hydrogen / eletricity / fossil fuels)

    # emissions [Mt]
    emissions_Mt = use_variable(input_table=emissions_Mt, selected_variable='emissions[Mt]')
    # energy-demand [TWh]
    energy_demand_TWh = use_variable(input_table=out_7248_1, selected_variable='energy-demand[TWh]')
    out_5960_1 = pd.concat([out_5959_1, energy_demand_TWh.set_index(energy_demand_TWh.index.astype(str) + '_dup')])
    # If missing txt set to "" (way-of-prod for sector = "")
    out_5960_1 = missing_value(df=out_5960_1, dimension_rx='^.*\\[.*•\\]$', DTS_DT_O=[['org.knime.core.data.def.IntCell', 'org.knime.base.node.preproc.pmml.missingval.handlers.DoNothingMissingCellHandlerFactory'], ['org.knime.core.data.def.StringCell', 'org.knime.base.node.preproc.pmml.missingval.handlers.FixedStringValueMissingCellHandlerFactory'], ['org.knime.core.data.def.DoubleCell', 'org.knime.base.node.preproc.pmml.missingval.handlers.DoNothingMissingCellHandlerFactory']], FixedValue='')

    # Primary energy demand
    # 
    # = Sectors demand (except heat /efuels demand) + primary energy-carrier needed for heat /efuels production

    # Energy demand [TWh]
    energy_demand_TWh = use_variable(input_table=out_5960_1, selected_variable='energy-demand[TWh]')

    # Carbon Capture Use (CCU) :
    # - energy demand [TWh]
    # - energy production [TWh]
    # - CAPEX / OPEX [MEUR]

    out_6429_1, out_6429_2, out_6429_3, out_6429_4, out_6429_5, out_6429_6 = metanode_6429(port_01=out_5960_1, port_02=CCU_Mt)
    # capex-opex [MEUR]
    out_6429_3 = column_filter(df=out_6429_3, pattern='^.*$')
    # capex-opex [MEUR]
    out_6429_3 = column_filter(df=out_6429_3, pattern='^.*$')
    # Node 5876
    out_3 = pd.concat([out_6428_3, out_6429_3.set_index(out_6429_3.index.astype(str) + '_dup')])
    # Set to 0
    out_3 = missing_value(df=out_3, DTS_DT_O=[['org.knime.core.data.def.IntCell', 'org.knime.base.node.preproc.pmml.missingval.handlers.DoNothingMissingCellHandlerFactory'], ['org.knime.core.data.def.StringCell', 'org.knime.base.node.preproc.pmml.missingval.handlers.DoNothingMissingCellHandlerFactory'], ['org.knime.core.data.def.DoubleCell', 'org.knime.base.node.preproc.pmml.missingval.handlers.FixedDoubleValueMissingCellHandlerFactory']], FixedValue='0.0')

    # Capex / Opex
    # 
    # = Concatenate costs from all power sectors (heat / efuels / hydrogen / eletricity / fossil fuels)

    # capex-opex [MEUR]
    out_3 = column_filter(df=out_3, pattern='^Country$|^Years$|^sector$|^.*$')

    # Carbon Capture Use (CCU) and Direct air Capture (DAC)

    # CCU [Mt]
    CCU_Mt = use_variable(input_table=out_6429_5, selected_variable='CCU[Mt]')
    # CCU [Mt]
    CCU_Mt = use_variable(input_table=CCU_Mt, selected_variable='CCU[Mt]')
    # emissions [Mt]
    out_9635_1 = CCU_Mt.rename(columns={'CCU[Mt]': 'emissions[Mt]'})
    # Add emissions-or-capture (=CCU)
    out_9635_1['emissions-or-capture'] = "CCU"
    # primary-energy-demand [TWh]
    primary_energy_demand_TWh_2 = use_variable(input_table=out_6429_4, selected_variable='primary-energy-demand[TWh]')
    # Group by  country, years,  primary-energy-carrier, sector, way-of-production
    primary_energy_demand_TWh_2 = group_by_dimensions(df=primary_energy_demand_TWh_2, groupby_dimensions=['Country', 'Years', 'way-of-production', 'primary-energy-carrier', 'sector'], aggregation_method='Sum')
    # primary-energy-demand to energy-demand  and  primary-energy-carrier to energy-carrier
    out_5983_1 = primary_energy_demand_TWh_2.rename(columns={'primary-energy-carrier': 'energy-carrier', 'primary-energy-demand[TWh]': 'energy-demand[TWh]'})
    out_5982_1 = pd.concat([out_5983_1, energy_demand_TWh.set_index(energy_demand_TWh.index.astype(str) + '_dup')])

    # Hydrogen :
    # - energy demand [TWh]
    # - energy production [TWh]
    # - CAPEX / OPEX [MEUR]

    out_6430_1, out_6430_2, out_6430_3, out_6430_4 = metanode_6430(port_01=out_5982_1)
    # capex-opex [MEUR]
    out_6430_3 = column_filter(df=out_6430_3, pattern='^.*$')
    # capex-opex [MEUR]
    out_6430_3 = column_filter(df=out_6430_3, pattern='^.*$')
    # Node 5876
    out_3 = pd.concat([out_3, out_6430_3.set_index(out_6430_3.index.astype(str) + '_dup')])
    # Set to 0
    out_3 = missing_value(df=out_3, DTS_DT_O=[['org.knime.core.data.def.IntCell', 'org.knime.base.node.preproc.pmml.missingval.handlers.DoNothingMissingCellHandlerFactory'], ['org.knime.core.data.def.StringCell', 'org.knime.base.node.preproc.pmml.missingval.handlers.DoNothingMissingCellHandlerFactory'], ['org.knime.core.data.def.DoubleCell', 'org.knime.base.node.preproc.pmml.missingval.handlers.FixedDoubleValueMissingCellHandlerFactory']], FixedValue='0.0')

    # Capex / Opex
    # 
    # = Concatenate costs from all power sectors (heat / efuels / hydrogen / eletricity / fossil fuels)

    # capex-opex [MEUR]
    out_3 = column_filter(df=out_3, pattern='^Country$|^Years$|^sector$|^..*$')

    # Primary energy demand
    # 
    # = Sectors demand (except heat / efuels / hydrogen demand) + primary energy-carrier needed for heat / efuels / hydrogen production

    # primary-energy-demand [TWh]
    primary_energy_demand_TWh_2 = use_variable(input_table=out_6430_4, selected_variable='primary-energy-demand[TWh]')
    # Group by  country, years, primary-energy-carrier, sector, way-of-prod
    primary_energy_demand_TWh_2 = group_by_dimensions(df=primary_energy_demand_TWh_2, groupby_dimensions=['Country', 'Years', 'way-of-production', 'primary-energy-carrier', 'sector'], aggregation_method='Sum')
    # primary-energy-demand to energy-demand  and  primary-energy-carrier to energy-carrier
    out_6034_1 = primary_energy_demand_TWh_2.rename(columns={'primary-energy-carrier': 'energy-carrier', 'primary-energy-demand[TWh]': 'energy-demand[TWh]'})
    # energy-imported [TWh]
    energy_imported_TWh = export_variable(input_table=out_6430_1, selected_variable='energy-imported[TWh]')

    # Energy Import
    # 
    # = Concatenate energy import from all power sectors (heat / efuels / hydrogen / eletricity / fossil fuels)

    # energy-imported [TWh]
    energy_imported_TWh = use_variable(input_table=energy_imported_TWh, selected_variable='energy-imported[TWh]')
    # energy-production [TWh]
    energy_production_TWh = export_variable(input_table=out_6430_2, selected_variable='energy-production[TWh]')

    # Energy Production
    # 
    # = Concatenate energy production from all power sectors (heat / efuels / hydrogen / eletricity / fossil fuels)

    # energy-production [TWh]
    energy_production_TWh = use_variable(input_table=energy_production_TWh, selected_variable='energy-production[TWh]')
    # Energy demand [TWh]
    energy_demand_TWh = use_variable(input_table=out_5982_1, selected_variable='energy-demand[TWh]')
    out_6036_1 = pd.concat([out_6034_1, energy_demand_TWh.set_index(energy_demand_TWh.index.astype(str) + '_dup')])
    out_7083_1, out_7083_2 = metanode_7083(port_01=out_6036_1)
    # energy-demand [TWh]
    energy_demand_TWh = export_variable(input_table=out_7083_2, selected_variable='energy-demand[TWh]')

    # Primary energy demand
    # 
    # = Sectors demand (except heat / efuels / hydrogen / elec demand) + primary energy-carrier needed for heat / efuels / hydrogen / elec production
    # 
    # AJOUTER les RES à la demand primaire ??

    # Energy demand [TWh]
    energy_demand_TWh_2 = use_variable(input_table=energy_demand_TWh, selected_variable='energy-demand[TWh]')
    # Calibration (energy-demand)
    cal_rate_energy_demand_TWh = export_variable(input_table=out_7083_1, selected_variable='cal_rate_energy-demand[TWh]')

    # Cal rate
    # 
    # = Concatenate cal-rate

    # Calibration (energy-demand)
    cal_rate_energy_demand_TWh = use_variable(input_table=cal_rate_energy_demand_TWh, selected_variable='cal_rate_energy-demand[TWh]')
    # energy-imported [TWh]
    energy_imported_TWh_3 = export_variable(input_table=out_6429_1, selected_variable='energy-imported[TWh]')
    # energy-imported [TWh]
    energy_imported_TWh_3 = use_variable(input_table=energy_imported_TWh_3, selected_variable='energy-imported[TWh]')
    # Node 5876
    energy_imported_TWh_2 = pd.concat([energy_imported_TWh_2, energy_imported_TWh_3.set_index(energy_imported_TWh_3.index.astype(str) + '_dup')])
    # energy-imported [TWh]
    energy_imported_TWh_2 = use_variable(input_table=energy_imported_TWh_2, selected_variable='energy-imported[TWh]')
    # Node 5876
    energy_imported_TWh = pd.concat([energy_imported_TWh_2, energy_imported_TWh.set_index(energy_imported_TWh.index.astype(str) + '_dup')])

    # Energy Import
    # 
    # = Concatenate energy import from all power sectors (heat / efuels / hydrogen / eletricity / fossil fuels)

    # energy-imported [TWh]
    energy_imported_TWh_2 = use_variable(input_table=energy_imported_TWh, selected_variable='energy-imported[TWh]')
    # direct-air-capture [Mt]
    direct_air_capture_Mt = use_variable(input_table=out_6429_6, selected_variable='direct-air-capture[Mt]')
    # direct-air-capture [Mt]
    direct_air_capture_Mt = use_variable(input_table=direct_air_capture_Mt, selected_variable='direct-air-capture[Mt]')
    # emissions [Mt]
    out_9636_1 = direct_air_capture_Mt.rename(columns={'direct-air-capture[Mt]': 'emissions[Mt]'})
    # Add emissions-or-capture (=DAC)
    out_9636_1['emissions-or-capture'] = "DAC"
    out_1 = pd.concat([out_9635_1, out_9636_1.set_index(out_9636_1.index.astype(str) + '_dup')])
    # energy-production [TWh]
    energy_production_TWh_3 = export_variable(input_table=out_6429_2, selected_variable='energy-production[TWh]')
    # energy-production [TWh]
    energy_production_TWh_3 = use_variable(input_table=energy_production_TWh_3, selected_variable='energy-production[TWh]')
    # Node 5876
    energy_production_TWh_2 = pd.concat([energy_production_TWh_2, energy_production_TWh_3.set_index(energy_production_TWh_3.index.astype(str) + '_dup')])
    # energy-production [TWh]
    energy_production_TWh_2 = use_variable(input_table=energy_production_TWh_2, selected_variable='energy-production[TWh]')
    # Node 5876
    energy_production_TWh_2 = pd.concat([energy_production_TWh_2, energy_production_TWh.set_index(energy_production_TWh.index.astype(str) + '_dup')])

    # Electricity :
    # - energy demand [TWh]
    # - energy production [TWh] - excluding production from CHP
    # - CAPEX / OPEX [MEUR]

    out_9763_1, out_9763_2, out_9763_3, out_9763_4, out_9763_5, out_9763_6, out_9763_7, out_9763_8, out_9763_9, out_9763_10, out_9763_11, out_9763_12, out_9763_13, out_9763_14 = metanode_9763(port_01=energy_production_TWh_2, port_02=energy_demand_TWh)
    # emissions [Mt]
    emissions_Mt_2 = use_variable(input_table=out_9763_7, selected_variable='emissions[Mt]')
    # Node 5876
    emissions_Mt = pd.concat([emissions_Mt_2, emissions_Mt.set_index(emissions_Mt.index.astype(str) + '_dup')])

    # Emissions
    # 
    # = Concatenate emissions from all power sectors (heat / efuels / hydrogen / eletricity / fossil fuels)

    # emissions [Mt]
    emissions_Mt = use_variable(input_table=emissions_Mt, selected_variable='emissions[Mt]')

    # For : Pathway Explorer
    # - Backup Capacity [GW]

    # backup-capacity [GW]
    backup_capacity_GW = use_variable(input_table=out_9763_12, selected_variable='backup-capacity[GW]')
    # CC [Mt]
    CC_Mt_2 = use_variable(input_table=out_9763_8, selected_variable='CC[Mt]')
    CC_Mt_2 = pd.concat([CC_Mt_2, CC_Mt_3.set_index(CC_Mt_3.index.astype(str) + '_dup')])

    # Carbon Capture
    # 
    # = Concatenate carbon capture from all power sectors (heat / efuels / hydrogen / eletricity / fossil fuels) ; then will be used (CCU) or storage (CCS)

    # CC [Mt]
    CC_Mt_3 = use_variable(input_table=CC_Mt_2, selected_variable='CC[Mt]')
    # primary-energy-demand [TWh]
    primary_energy_demand_TWh_2 = use_variable(input_table=out_9763_13, selected_variable='primary-energy-demand[TWh]')
    primary_energy_demand_TWh = pd.concat([primary_energy_demand_TWh_2, primary_energy_demand_TWh.set_index(primary_energy_demand_TWh.index.astype(str) + '_dup')])
    # primary-energy-demand [TWh]
    primary_energy_demand_TWh = use_variable(input_table=primary_energy_demand_TWh, selected_variable='primary-energy-demand[TWh]')
    # primary-energy-demand [TWh]
    primary_energy_demand_TWh_2 = use_variable(input_table=out_9763_6, selected_variable='primary-energy-demand[TWh]')
    # Group by  country, years, primary-energy-carrier, sector, way-of-prod
    primary_energy_demand_TWh_2 = group_by_dimensions(df=primary_energy_demand_TWh_2, groupby_dimensions=['Country', 'Years', 'way-of-production', 'primary-energy-carrier', 'sector'], aggregation_method='Sum')
    # primary-energy-demand to energy-demand  and  primary-energy-carrier to energy-carrier
    out_6228_1 = primary_energy_demand_TWh_2.rename(columns={'primary-energy-carrier': 'energy-carrier', 'primary-energy-demand[TWh]': 'energy-demand[TWh]'})
    # Node 5876
    out_6222_1 = pd.concat([out_6228_1, energy_demand_TWh_2.set_index(energy_demand_TWh_2.index.astype(str) + '_dup')])
    out_7084_1, out_7084_2 = metanode_7084(port_01=out_6222_1)
    # energy-demand [TWh]
    energy_demand_TWh = export_variable(input_table=out_7084_2, selected_variable='energy-demand[TWh]')

    # Fossil Fuels :
    # energy demand [TWh]

    out_6432_1, out_6432_2, out_6432_3, out_6432_4, out_6432_5, out_6432_6, out_6432_7, out_6432_8, out_6432_9 = metanode_6432(port_01=energy_demand_TWh)
    # capex-opex [MEUR]
    out_6432_5 = column_filter(df=out_6432_5, pattern='^Country$|^Years$|^sector$|^.*capex.*$|^.*opex.*$|^cost-user$')

    # Capex / Opex
    # 
    # = Concatenate costs from all power sectors (heat / efuels / hydrogen / eletricity / fossil fuels)

    # capex-opex [MEUR]
    out_6432_5 = column_filter(df=out_6432_5, pattern='^.*$')

    # Primary energy demand
    # 
    # = Sectors demand (except heat / efuels / hydrogen / elec / refineries demand) + primary energy-carrier needed for heat / efuels / hydrogen / elec production / refineries
    # Note : Here, we only consider primary energy carrier needed to produce refined fossil fuels (top : refineries). The rest of the fossil fuel demand come from previous computations.

    # primary-energy-demand [TWh]
    primary_energy_demand_TWh_2 = use_variable(input_table=out_6432_6, selected_variable='primary-energy-demand[TWh]')
    # Group by  country, years, primary-energy-carrier, sector, way-of-prod
    primary_energy_demand_TWh_2 = group_by_dimensions(df=primary_energy_demand_TWh_2, groupby_dimensions=['Country', 'Years', 'way-of-production', 'primary-energy-carrier', 'energy-carrier-category', 'sector'], aggregation_method='Sum')
    # primary-energy-demand to energy-demand  and  primary-energy-carrier to energy-carrier
    out_6438_1 = primary_energy_demand_TWh_2.rename(columns={'primary-energy-carrier': 'energy-carrier', 'primary-energy-demand[TWh]': 'energy-demand[TWh]'})
    # Calibration (net energy production, total gross production & emissions)
    out_6432_1 = column_filter(df=out_6432_1, columns_to_drop=[])

    # Cal rate
    # 
    # = Concatenate cal-rate

    # Calibration (net energy production, total gross production & emissions)
    out_6432_1 = column_filter(df=out_6432_1, columns_to_drop=[])
    # primary-energy-demand [TWh]
    primary_energy_demand_TWh_2 = use_variable(input_table=out_6432_9, selected_variable='primary-energy-demand[TWh]')
    primary_energy_demand_TWh = pd.concat([primary_energy_demand_TWh_2, primary_energy_demand_TWh.set_index(primary_energy_demand_TWh.index.astype(str) + '_dup')])
    # primary-energy-demand [TWh]  only for CC
    primary_energy_demand_TWh = use_variable(input_table=primary_energy_demand_TWh, selected_variable='primary-energy-demand[TWh]')
    # Group by  way-of-production (sum)
    primary_energy_demand_TWh = group_by_dimensions(df=primary_energy_demand_TWh, groupby_dimensions=['Country', 'Years', 'way-of-production'], aggregation_method='Sum')
    # Rename variable to CC-energy-demand -by-way-of-prod[TWh]
    out_9666_1 = primary_energy_demand_TWh.rename(columns={'primary-energy-demand[TWh]': 'CC-energy-demand-by-way-of-prod[TWh]'})
    # CC [Mt]
    CC_Mt_2 = use_variable(input_table=out_6432_8, selected_variable='CC[Mt]')
    # Node 5876
    CC_Mt_2 = pd.concat([CC_Mt_2, CC_Mt_3.set_index(CC_Mt_3.index.astype(str) + '_dup')])
    # CC [Mt] from Power
    CC_Mt_2 = use_variable(input_table=CC_Mt_2, selected_variable='CC[Mt]')

    # Costs

    # Capex / Opex for Carbon Capture (in Power)

    # CP Global Warming Potential GWP
    clt_gwp = import_data(trigram='clt', variable_name='clt_gwp', variable_type='CP')
    # Switch variable to double
    gwp_100 = math_formula(df=clt_gwp, convert_to_int=False, replaced_column='gwp-100[-]', splitted='$gwp-100[-]$')
    CC_Mt = pd.concat([CC_Mt_2, CC_Mt.set_index(CC_Mt.index.astype(str) + '_dup')])
    # output = CCS
    out_9621_1 = metanode_9621(port_02=CCU_Mt, port_03=direct_air_capture_Mt, port_01=CC_Mt)
    # emissions [Mt]
    out_9634_1 = out_9621_1.rename(columns={'CCS[Mt]': 'emissions[Mt]'})
    # Add emissions-or-capture (=CCS)
    out_9634_1['emissions-or-capture'] = "CCS"
    out_1 = pd.concat([out_9634_1, out_1.set_index(out_1.index.astype(str) + '_dup')])
    # emissions [Mt]
    out_9649_1 = CC_Mt.rename(columns={'CC[Mt]': 'emissions[Mt]'})
    # Add emissions-or-capture (=CC)
    out_9649_1['emissions-or-capture'] = "CC"
    out_1 = pd.concat([out_9649_1, out_1.set_index(out_1.index.astype(str) + '_dup')])
    # emissions [Mt]
    emissions_Mt_2 = use_variable(input_table=out_6432_7, selected_variable='emissions[Mt]')
    # Node 5876
    emissions_Mt = pd.concat([emissions_Mt_2, emissions_Mt.set_index(emissions_Mt.index.astype(str) + '_dup')])

    # GAES Emissions
    # 
    # = Emissions from sub-sector (refineries, electricity, ...) + non-modelised emissions
    # => Compute % of fugitive and others (CO2 transport and coal manufacturing) based on Refineries only - gaes by gaes
    # => Apply % of emissions

    # emissions [Mt]
    emissions_Mt = use_variable(input_table=emissions_Mt, selected_variable='emissions[Mt]')
    # Keep  sector = refineries
    emissions_Mt_2 = emissions_Mt.loc[emissions_Mt['sector'].isin(['refineries'])].copy()
    # Group by  Country, Years, gaes (SUM)
    emissions_Mt_2 = group_by_dimensions(df=emissions_Mt_2, groupby_dimensions=['Country', 'Years', 'gaes'], aggregation_method='Sum')
    # OTS (only) power-gaes-additional [Gt]
    power_gaes_additional_Gt = import_data(trigram='elc', variable_name='power-gaes-additional', variable_type='OTS (only)')
    # Convert Gt to Mt
    power_gaes_additional_Mt = power_gaes_additional_Gt.drop(columns='power-gaes-additional[Gt]').assign(**{'power-gaes-additional[Mt]': power_gaes_additional_Gt['power-gaes-additional[Gt]'] * 1000.0})
    # ratio-emissions[%] = power-gaes-additional[Mt] / emissions[Mt]
    ratio_emissions_percent = mcd(input_table_1=emissions_Mt_2, input_table_2=power_gaes_additional_Mt, operation_selection='y / x', output_name='ratio-emissions[%]')
    # Set to 0
    ratio_emissions_percent = missing_value(df=ratio_emissions_percent, DTS_DT_O=[['org.knime.core.data.def.IntCell', 'org.knime.base.node.preproc.pmml.missingval.handlers.DoNothingMissingCellHandlerFactory'], ['org.knime.core.data.def.StringCell', 'org.knime.base.node.preproc.pmml.missingval.handlers.DoNothingMissingCellHandlerFactory'], ['org.knime.core.data.def.DoubleCell', 'org.knime.base.node.preproc.pmml.missingval.handlers.FixedDoubleValueMissingCellHandlerFactory']], FixedValue='0.0')
    # Same as last available year
    ratio_emissions_percent = add_missing_years(df_data=ratio_emissions_percent)
    # emissions[Mt] (replace) = ratio-emissions[%] * emissions[Mt]
    emissions_Mt_2 = mcd(input_table_1=emissions_Mt_2, input_table_2=ratio_emissions_percent, operation_selection='x * y', output_name='emissions[Mt]')
    emissions_Mt_2 = pd.concat([emissions_Mt, emissions_Mt_2.set_index(emissions_Mt_2.index.astype(str) + '_dup')])

    # Set ETS / non-ETS status

    # RCP power-ets-share [%] (from technology)
    power_ets_share_percent = import_data(trigram='tec', variable_name='power-ets-share', variable_type='RCP')
    # Convert Unit % to - (* 0.01)
    power_ets_share_ = power_ets_share_percent.drop(columns='power-ets-share[%]').assign(**{'power-ets-share[-]': power_ets_share_percent['power-ets-share[%]'] * 0.01})
    # emissions[Mt] (replace) = emissions[Mt] * power-ets-share[-]  LEFT Join If missing ; set to 1
    emissions_Mt = mcd(input_table_1=out_1, input_table_2=power_ets_share_, operation_selection='x * y', output_name='emissions[Mt]', fill_value_bool='Left [x] Outer Join', fill_value=1.0)

    def helper_9210(input_table) -> pd.DataFrame:
        # Copy input to output
        output_table = input_table.copy()
        
        # CCS is hard-coded as non-ETS
        mask_ccs = (output_table['emissions-or-capture']=="CCS")
        output_table.loc[mask_ccs, 'ets-or-not'] = "ETS"
        
        # Missing value => set non-ETS
        mask = (output_table['ets-or-not'].isna())
        output_table.loc[mask, 'ets-or-not'] = "non-ETS"
        return output_table
    # If missing value for ets-or-not set non-ETS (default value)  EXCEPTION FOR CCS HARD-CODED as ETS
    out_9210_1 = helper_9210(input_table=emissions_Mt)

    # For : Climate
    # - CC / DAC / CCS / CCU

    # emissions [Mt]
    emissions_Mt = use_variable(input_table=out_9210_1, selected_variable='emissions[Mt]')
    # Exclude CC and DAC (they remain positive !)
    emissions_Mt_excluded = emissions_Mt.loc[emissions_Mt['emissions-or-capture'].isin(['CC', 'DAC'])].copy()
    emissions_Mt = emissions_Mt.loc[~emissions_Mt['emissions-or-capture'].isin(['CC', 'DAC'])].copy()
    # Set NEGATIVE VALUES
    emissions_Mt['emissions[Mt]'] = -1.0*emissions_Mt['emissions[Mt]']
    emissions_Mt = pd.concat([emissions_Mt, emissions_Mt_excluded.set_index(emissions_Mt_excluded.index.astype(str) + '_dup')])
    # emissions[Mt] (replace) = emissions[Mt] * power-ets-share[-]  LEFT Join If missing ; set to 1
    emissions_Mt_2 = mcd(input_table_1=emissions_Mt_2, input_table_2=power_ets_share_, operation_selection='x * y', output_name='emissions[Mt]', fill_value_bool='Left [x] Outer Join', fill_value=1.0)

    def helper_9209(input_table) -> pd.DataFrame:
        # Copy input to output
        output_table = input_table.copy()
        
        # Missing value => set non-ETS
        mask = (output_table['ets-or-not'].isna())
        output_table.loc[mask, 'ets-or-not'] = "non-ETS"
        return output_table
    # If missing value for ets-or-not set non-ETS (default value)
    out_9209_1 = helper_9209(input_table=emissions_Mt_2)

    # KPI's (require extra computation)

    # Carbon intensity of heat and electricity generation
    # 
    # Unit : gCO2 / KWh
    # Assumption : 75% of CHP emissions are linked to electricity production ; the 25% remaining are linked to heat production. This balance accounts for the fact it is more easy (efficient) to do heat than electricity.

    # emissions [Mt]
    emissions_Mt_2 = use_variable(input_table=out_9209_1, selected_variable='emissions[Mt]')

    # For : Climate
    # - Emissions

    # emissions [Mt]
    emissions_Mt_3 = use_variable(input_table=emissions_Mt_2, selected_variable='emissions[Mt]')
    # Add emissions-or-capture
    emissions_Mt_3['emissions-or-capture'] = "emissions"
    emissions_Mt = pd.concat([emissions_Mt_3, emissions_Mt.set_index(emissions_Mt.index.astype(str) + '_dup')])
    # Keep way-of-prod = CHP
    emissions_Mt_3 = emissions_Mt_2.loc[emissions_Mt_2['way-of-production'].isin(['CHP'])].copy()
    emissions_Mt_excluded = emissions_Mt_2.loc[~emissions_Mt_2['way-of-production'].isin(['CHP'])].copy()
    # x 0.25 (we consider that 25% of CHP emissions are linked to heat production)
    emissions_Mt_2 = emissions_Mt_3.assign(**{'emissions[Mt]': emissions_Mt_3['emissions[Mt]']*0.25})
    # sector -> electricity
    emissions_Mt_3['sector'] = "electricity"
    # x 0.75 (we consider that 75% of CHP emissions are linked to electricity production)
    emissions_Mt_3['emissions[Mt]'] = emissions_Mt_3['emissions[Mt]']*0.75
    emissions_Mt_2 = pd.concat([emissions_Mt_3, emissions_Mt_2.set_index(emissions_Mt_2.index.astype(str) + '_dup')])
    emissions_Mt_2 = pd.concat([emissions_Mt_2, emissions_Mt_excluded.set_index(emissions_Mt_excluded.index.astype(str) + '_dup')])
    # Group by Country, Years, gaes, way-of-prod (sum)
    emissions_Mt_2 = group_by_dimensions(df=emissions_Mt_2, groupby_dimensions=['Country', 'Years', 'gaes', 'sector'], aggregation_method='Sum')
    # emissions[MtCO2eq] = emissions[Mt] *  gwp[-]
    emissions_MtCO2eq = mcd(input_table_1=emissions_Mt_2, input_table_2=gwp_100, operation_selection='x * y', output_name='emissions[MtCO2eq]')
    # Group by country, years, sector (sum)
    emissions_MtCO2eq = group_by_dimensions(df=emissions_MtCO2eq, groupby_dimensions=['Country', 'Years', 'sector'], aggregation_method='Sum')
    # energy-imported [TWh]
    energy_imported_TWh = export_variable(input_table=out_6432_3, selected_variable='energy-imported[TWh]')

    # Energy Import
    # 
    # = Concatenate energy import from all power sectors (heat / efuels / hydrogen / eletricity / fossil fuels)

    # energy-imported [TWh]
    energy_imported_TWh = use_variable(input_table=energy_imported_TWh, selected_variable='energy-imported[TWh]')

    # For : Pathway Explorer
    # - Energy-production (gross) from sector linked to power supply (heat / hydrogen / efuels / electricity / fossil fuels) and others sectors
    # - Energy-production (net) : for electricity and refineries sector

    # net-energy-production [TWh] FROM refineries
    net_energy_production_TWh = use_variable(input_table=out_6432_4, selected_variable='net-energy-production[TWh]')
    # Group by  country, years, way-of-prod
    net_energy_production_TWh = group_by_dimensions(df=net_energy_production_TWh, groupby_dimensions=['Country', 'Years', 'way-of-production'], aggregation_method='Sum')
    # net-energy-production-by- way-of-prod [TWh]  graph 92 (5f Bottom) and graph 14 (1h Bottom)
    out_9546_1 = net_energy_production_TWh.rename(columns={'net-energy-production[TWh]': 'net-energy-production-by-way-of-prod[TWh]'})
    # energy-production [TWh]
    energy_production_TWh = export_variable(input_table=out_6432_2, selected_variable='energy-production[TWh]')

    # Energy Production
    # 
    # = Concatenate energy production from all power sectors (heat / efuels / hydrogen / eletricity / fossil fuels)

    # energy-production [TWh]
    energy_production_TWh = use_variable(input_table=energy_production_TWh, selected_variable='energy-production[TWh]')
    # energy-demand [TWh]
    energy_demand_TWh = use_variable(input_table=energy_demand_TWh, selected_variable='energy-demand[TWh]')
    # Group by  country, years, sector, way-of-prod
    energy_demand_TWh = group_by_dimensions(df=energy_demand_TWh, groupby_dimensions=['Country', 'Years', 'way-of-production', 'energy-carrier', 'sector'], aggregation_method='Sum')
    out_6439_1 = pd.concat([out_6438_1, energy_demand_TWh.set_index(energy_demand_TWh.index.astype(str) + '_dup')])

    # Material Footprint (fossil fuel) per capita

    # energy-demand [TWh]
    energy_demand_TWh = use_variable(input_table=out_6439_1, selected_variable='energy-demand[TWh]')
    # OTS/FTS population[cap]
    population_cap = import_data(trigram='lfs', variable_name='population')
    # Keep only energy-carrier fossil fuels
    energy_demand_TWh_2 = energy_demand_TWh.loc[energy_demand_TWh['energy-carrier'].isin(['gaseous-ff', 'gaseous-ff-natural', 'liquid-ff', 'liquid-ff-crudeoil', 'liquid-ff-diesel', 'liquid-ff-gasoline', 'liquid-ff-kerosene', 'liquid-ff-marinefueloil', 'liquid-ff-oil', 'solid-ff-coal'])].copy()
    # Group by  country, years
    energy_demand_TWh_2 = group_by_dimensions(df=energy_demand_TWh_2, groupby_dimensions=['Country', 'Years'], aggregation_method='Sum')
    # ff-demand-per-cap[TWh/cap] = energy-demand[TWh] (ff) / population[cap]
    ff_demand_per_cap_TWh_per_cap = mcd(input_table_1=energy_demand_TWh_2, input_table_2=population_cap, operation_selection='x / y', output_name='ff-demand-per-cap[TWh/cap]')
    # Convert Unit Gpkm to MWh/cap
    ff_demand_per_cap_MWh_per_cap = ff_demand_per_cap_TWh_per_cap.drop(columns='ff-demand-per-cap[TWh/cap]').assign(**{'ff-demand-per-cap[MWh/cap]': ff_demand_per_cap_TWh_per_cap['ff-demand-per-cap[TWh/cap]'] * 1000000.0})
    # ff-demand-per-cap [MWh/cap]
    ff_demand_per_cap_MWh_per_cap = export_variable(input_table=ff_demand_per_cap_MWh_per_cap, selected_variable='ff-demand-per-cap[MWh/cap]')
    # Keep only sector = eletrcicty, heat, hydrogen, efuels refineries
    energy_demand_TWh_2 = energy_demand_TWh.loc[energy_demand_TWh['sector'].isin(['refineries', 'heat', 'efuels', 'electricity', 'hydrogen'])].copy()
    # Module = Air Quality
    energy_demand_TWh_2 = column_filter(df=energy_demand_TWh_2, pattern='^.*$')
    # Keep sector = agr, bld, ind, tran
    energy_demand_TWh_3 = energy_demand_TWh.loc[energy_demand_TWh['sector'].isin(['agr', 'bld', 'ind', 'tra'])].copy()

    # Share of electricity/fossil fuels/biomass/alternative fuels of total final energy consumption
    # 
    # Unit : %

    # Group by Country, Years,  vectors (sum)
    energy_demand_TWh_5 = group_by_dimensions(df=energy_demand_TWh_3, groupby_dimensions=['Country', 'Years', 'energy-carrier'], aggregation_method='Sum')
    # Keep electricity
    energy_demand_TWh_4 = energy_demand_TWh_5.loc[energy_demand_TWh_5['energy-carrier'].isin(['RES-electricity-hp', 'electricity'])].copy()
    # Group by Country, Years (sum)
    energy_demand_TWh_4 = group_by_dimensions(df=energy_demand_TWh_4, groupby_dimensions=['Country', 'Years'], aggregation_method='Sum')
    # Keep fossil fuels
    energy_demand_TWh_6 = energy_demand_TWh_5.loc[energy_demand_TWh_5['energy-carrier'].isin(['gaseous-ff', 'gaseous-ff-natural', 'liquid-ff', 'liquid-ff-crudeoil', 'liquid-ff-diesel', 'liquid-ff-gasoline', 'liquid-ff-kerosene', 'liquid-ff-marinefueloil', 'liquid-ff-oil', 'solid-ff-coal'])].copy()
    # Group by Country, Years (sum)
    energy_demand_TWh_6 = group_by_dimensions(df=energy_demand_TWh_6, groupby_dimensions=['Country', 'Years'], aggregation_method='Sum')
    # Keep biomass
    energy_demand_TWh_7 = energy_demand_TWh_5.loc[energy_demand_TWh_5['energy-carrier'].isin(['gaseous-bio', 'liquid-bio', 'liquid-bio-diesel', 'liquid-bio-gasoline', 'liquid-bio-kerosene', 'liquid-bio-marinefueloil', 'solid-biomass', 'solid-waste-res'])].copy()
    # Group by Country, Years (sum)
    energy_demand_TWh_7 = group_by_dimensions(df=energy_demand_TWh_7, groupby_dimensions=['Country', 'Years'], aggregation_method='Sum')
    # Keep H2 + e-fuels
    energy_demand_TWh_5 = energy_demand_TWh_5.loc[energy_demand_TWh_5['energy-carrier'].isin(['gaseous-syn', 'hydrogen', 'liquid-syn', 'liquid-syn-diesel', 'liquid-syn-gasoline', 'liquid-syn-kerosene', 'liquid-syn-marinefueloil'])].copy()
    # Group by Country, Years (sum)
    energy_demand_TWh_5 = group_by_dimensions(df=energy_demand_TWh_5, groupby_dimensions=['Country', 'Years'], aggregation_method='Sum')
    # Group by Country, Years (sum)
    energy_demand_TWh_3 = group_by_dimensions(df=energy_demand_TWh_3, groupby_dimensions=['Country', 'Years'], aggregation_method='Sum')
    # alternative-fuels-share [%]
    total_fec_alternative_fuels_share_percent = mcd(input_table_1=energy_demand_TWh_5, input_table_2=energy_demand_TWh_3, operation_selection='x / y', output_name='total-fec-alternative-fuels-share[%]')
    # biomass-share [%]
    total_fec_biomass_share_percent = mcd(input_table_1=energy_demand_TWh_7, input_table_2=energy_demand_TWh_3, operation_selection='x / y', output_name='total-fec-biomass-share[%]')
    total_fec_share_percent = pd.concat([total_fec_biomass_share_percent, total_fec_alternative_fuels_share_percent.set_index(total_fec_alternative_fuels_share_percent.index.astype(str) + '_dup')])
    # ff-share [%]
    total_fec_ff_share_percent = mcd(input_table_1=energy_demand_TWh_6, input_table_2=energy_demand_TWh_3, operation_selection='x / y', output_name='total-fec-ff-share[%]')
    # electricity-share [%]
    total_fec_electricity_share_percent = mcd(input_table_1=energy_demand_TWh_4, input_table_2=energy_demand_TWh_3, operation_selection='x / y', output_name='total-fec-electricity-share[%]')
    total_fec_share_percent_2 = pd.concat([total_fec_electricity_share_percent, total_fec_ff_share_percent.set_index(total_fec_ff_share_percent.index.astype(str) + '_dup')])
    total_fec_share_percent = pd.concat([total_fec_share_percent_2, total_fec_share_percent.set_index(total_fec_share_percent.index.astype(str) + '_dup')])

    # For : Bioenergy module
    # - Bio-energy demand (from sector linked to power production)

    # energy-demand [TWh]
    energy_demand_TWh_3 = use_variable(input_table=energy_demand_TWh, selected_variable='energy-demand[TWh]')
    # Group by  country, years, energy-carrier, sector
    energy_demand_TWh_4 = group_by_dimensions(df=energy_demand_TWh_3, groupby_dimensions=['Country', 'Years', 'energy-carrier', 'sector'], aggregation_method='Sum')
    # Keep only energy-carrier electricity
    energy_demand_TWh_5 = energy_demand_TWh_4.loc[energy_demand_TWh_4['energy-carrier'].isin(['electricity'])].copy()
    energy_demand_TWh_5 = energy_demand_TWh_5.loc[~energy_demand_TWh_5['sector'].isin(['losses', 'refineries'])].copy()
    # Group by  country, years
    energy_demand_TWh_5 = group_by_dimensions(df=energy_demand_TWh_5, groupby_dimensions=['Country', 'Years'], aggregation_method='Sum')
    # Same as in Pathway Explorer
    out_6542_1 = energy_demand_TWh_5.rename(columns={'energy-demand[TWh]': 'elc_electricity-demand_total[TWh]'})
    # Unit conversion TWh to GWh
    elc_electricity_demand_total_GWh = out_6542_1.drop(columns='elc_electricity-demand_total[TWh]').assign(**{'elc_electricity-demand_total[GWh]': out_6542_1['elc_electricity-demand_total[TWh]'] * 1000.0})
    # Keep only energy-carrier fossil fuels
    energy_demand_TWh_5 = energy_demand_TWh_4.loc[energy_demand_TWh_4['energy-carrier'].isin(['gaseous-ff', 'gaseous-ff-natural', 'liquid-ff', 'liquid-ff-crudeoil', 'liquid-ff-diesel', 'liquid-ff-gasoline', 'liquid-ff-kerosene', 'liquid-ff-marinefueloil', 'liquid-ff-oil', 'solid-ff-coal'])].copy()
    energy_demand_TWh_5 = energy_demand_TWh_5.loc[~energy_demand_TWh_5['sector'].isin(['refineries'])].copy()

    # Pivot

    out_6544_1, _, _ = pivoting(df=energy_demand_TWh_5, agg_dict={'energy-demand[TWh]': 'sum'}, column_name_option='Pivot name+Aggregation name', column_name_policy='Keep original name(s)', list_group_columns=['Country', 'Years'], list_pivots=['energy-carrier'])
    out_6545_1 = missing_value_column_filter(df=out_6544_1, missing_threshold=0.9, type_of_pattern='Manual')
    # Same as in Pathway Explorer
    out_6545_1 = column_rename_regex(df=out_6545_1, search_string='(.*)\\+(.*)\\[.*', replace_string='$2_$1[TWh]')
    out_7056_1 = tree_merge_groups(df=out_6545_1, new_column_name='energy-demand-total', aggregation_method='Sum', unit='TWh', aggregation_pattern='energy-demand_(.*-ff).*\\[.*')

    def helper_6549(input_table_1, input_table_2) -> pd.DataFrame:
        # Input tables
        corr_table = input_table_1.copy()
        output_table = input_table_2.copy()
        
        # Create dict with old_new_name
        dict_names = {}
        for row in corr_table.iterrows():
            old_name = row[1]["old_name"]
            new_name = row[1]["new_name"]
            dict_names[new_name] = old_name
        
        # For col in columns > if col correspond to new name => get in dict for rename
        dict_rename = {}
        for col in output_table.head():
            for i in dict_names.keys():
                if i == col:
                    dict_rename[i] = dict_names[i]
        
        output_table = output_table.rename(columns = dict_rename)
        return output_table
    # rename based on table creator
    out_6549_1 = helper_6549(input_table_1=out_6548_1, input_table_2=out_7056_1)
    out_7062_1 = joiner(df_left=out_6549_1, df_right=elc_electricity_demand_total_GWh, joiner='inner', left_input=['Country', 'Years'], right_input=['Country', 'Years'])
    # Keep only energy-carrier linked to bioenergy
    energy_demand_TWh_4 = energy_demand_TWh_4.loc[energy_demand_TWh_4['energy-carrier'].isin(['gaseous-bio', 'liquid-bio', 'liquid-bio-diesel', 'liquid-bio-gasoline', 'liquid-bio-kerosene', 'liquid-bio-marinefueloil', 'solid-biomass', 'solid-waste', 'solid-waste-nonres', 'solid-waste-res'])].copy()
    # Module = Bioenergy
    energy_demand_TWh_4 = column_filter(df=energy_demand_TWh_4, pattern='^.*$')
    # energy-demand [TWh]
    energy_demand_TWh_5 = use_variable(input_table=energy_demand_TWh_3, selected_variable='energy-demand[TWh]')
    # LEFT JOIN Add direct-use
    out_6651_1 = joiner(df_left=energy_demand_TWh_5, df_right=out_6652_1, joiner='inner', left_input=['sector'], right_input=['sector'])
    # Keep energy-carrier = electricity, hydrogen, heat, gaseous-syn, liquid-syn-.* (non-primary)
    out_6651_1_2 = out_6651_1.loc[out_6651_1['energy-carrier'].isin(['gaseous-syn', 'electricity', 'heat', 'hydrogen', 'liquid-syn', 'liquid-syn-diesel', 'liquid-syn-gasoline', 'liquid-syn-kerosene', 'liquid-syn-marinefueloil'])].copy()
    out_6651_1_excluded = out_6651_1.loc[~out_6651_1['energy-carrier'].isin(['gaseous-syn', 'electricity', 'heat', 'hydrogen', 'liquid-syn', 'liquid-syn-diesel', 'liquid-syn-gasoline', 'liquid-syn-kerosene', 'liquid-syn-marinefueloil'])].copy()
    # primary-type = primary
    out_6651_1_excluded['primary-type'] = "primary"
    # primary-type = non-primary
    out_6651_1 = out_6651_1_2.assign(**{'primary-type': "non-primary"})
    out_6651_1 = pd.concat([out_6651_1, out_6651_1_excluded.set_index(out_6651_1_excluded.index.astype(str) + '_dup')])

    # Split : ETS / non-ETS

    # energy-demand[TWh] (replace) = energy-demand[TWh] * power-ets-share[-]  LEFT Join If missing ; set to 1
    energy_demand_TWh_6 = mcd(input_table_1=out_6651_1, input_table_2=power_ets_share_, operation_selection='x * y', output_name='energy-demand[TWh]', fill_value_bool='Left [x] Outer Join', fill_value=1.0)

    def helper_9495(input_table) -> pd.DataFrame:
        # Copy input to output
        output_table = input_table.copy()
        
        # Missing value => set non-ETS
        mask = (output_table['ets-or-not'].isna())
        output_table.loc[mask, 'ets-or-not'] = "non-ETS"
        return output_table
    # If missing value for ets-or-not set non-ETS (default value)
    out_9495_1 = helper_9495(input_table=energy_demand_TWh_6)
    # Exclude sector = heat
    out_9495_1_excluded = out_9495_1.loc[out_9495_1['sector'].isin(['heat'])].copy()
    out_9495_1 = out_9495_1.loc[~out_9495_1['sector'].isin(['heat'])].copy()
    # Split way-of-prod Top : heat-plant Bottom : CHP
    out_9495_1_excluded_2 = out_9495_1_excluded.loc[out_9495_1_excluded['way-of-production'].isin(['heat-plant'])].copy()
    out_9495_1_excluded_excluded = out_9495_1_excluded.loc[~out_9495_1_excluded['way-of-production'].isin(['heat-plant'])].copy()
    # force sector to be equaled to heat-only
    out_9495_1_excluded = out_9495_1_excluded_2.assign(**{'sector': "heat-only"})
    # force sector to be equaled to heat-CHP
    out_9495_1_excluded_excluded['sector'] = "heat-CHP"
    out_9495_1_excluded = pd.concat([out_9495_1_excluded, out_9495_1_excluded_excluded.set_index(out_9495_1_excluded_excluded.index.astype(str) + '_dup')])
    out_9495_1 = pd.concat([out_9495_1, out_9495_1_excluded.set_index(out_9495_1_excluded.index.astype(str) + '_dup')])
    # Exclude energy-carrier = liquid-syn-.*
    out_9495_1_excluded = out_9495_1.loc[out_9495_1['energy-carrier'].isin(['liquid-syn', 'liquid-syn-diesel', 'liquid-syn-gasoline', 'liquid-syn-kerosene', 'liquid-syn-marinefueloil'])].copy()
    out_9495_1 = out_9495_1.loc[~out_9495_1['energy-carrier'].isin(['liquid-syn', 'liquid-syn-diesel', 'liquid-syn-gasoline', 'liquid-syn-kerosene', 'liquid-syn-marinefueloil'])].copy()
    # force energy-carrier to be equaled to liquid-syn
    out_9495_1_excluded['energy-carrier'] = "liquid-syn"
    # Exclude energy-carrier = liquid-bio-.*
    out_9495_1_excluded_2 = out_9495_1.loc[out_9495_1['energy-carrier'].isin(['liquid-bio', 'liquid-bio-diesel', 'liquid-bio-gasoline', 'liquid-bio-kerosene', 'liquid-bio-marinefueloil'])].copy()
    out_9495_1 = out_9495_1.loc[~out_9495_1['energy-carrier'].isin(['liquid-bio', 'liquid-bio-diesel', 'liquid-bio-gasoline', 'liquid-bio-kerosene', 'liquid-bio-marinefueloil'])].copy()
    # force energy-carrier to be equaled to liquid-bio
    out_9495_1_excluded_2['energy-carrier'] = "liquid-bio"
    out_9495_1_excluded = pd.concat([out_9495_1_excluded_2, out_9495_1_excluded.set_index(out_9495_1_excluded.index.astype(str) + '_dup')])
    # Exclude energy-carrier = gaseous-ff-.*
    out_9495_1_excluded_2 = out_9495_1.loc[out_9495_1['energy-carrier'].isin(['gaseous-ff', 'gaseous-ff-natural'])].copy()
    out_9495_1 = out_9495_1.loc[~out_9495_1['energy-carrier'].isin(['gaseous-ff', 'gaseous-ff-natural'])].copy()
    # force energy-carrier to be equaled to gaseous-ff
    out_9495_1_excluded_2['energy-carrier'] = "gaseous-ff"
    out_9495_1_excluded = pd.concat([out_9495_1_excluded_2, out_9495_1_excluded.set_index(out_9495_1_excluded.index.astype(str) + '_dup')])
    # Exclude energy-carrier = liquid-ff and liquid-ff-oil
    out_9495_1_excluded_2 = out_9495_1.loc[out_9495_1['energy-carrier'].isin(['liquid-ff-oil', 'liquid-ff'])].copy()
    out_9495_1 = out_9495_1.loc[~out_9495_1['energy-carrier'].isin(['liquid-ff-oil', 'liquid-ff'])].copy()
    # force energy-carrier to be equaled to liquid-ff-oil
    out_9495_1_excluded_2['energy-carrier'] = "liquid-ff-oil"
    out_9495_1_excluded = pd.concat([out_9495_1_excluded_2, out_9495_1_excluded.set_index(out_9495_1_excluded.index.astype(str) + '_dup')])
    out_9495_1 = pd.concat([out_9495_1, out_9495_1_excluded.set_index(out_9495_1_excluded.index.astype(str) + '_dup')])
    out_9495_1_2 = out_9495_1.loc[~out_9495_1['sector'].isin(['refineries'])].copy()
    # Include energy-carrier = fossil fuels  (only fossil fuels are demanded to refineries)
    out_9495_1_2 = out_9495_1_2.loc[out_9495_1_2['energy-carrier'].isin(['solid-ff-coal', 'liquid-ff-diesel', 'liquid-ff-gasoline', 'liquid-ff-kerosene', 'liquid-ff-marinefueloil', 'liquid-ff-oil', 'gaseous-ff'])].copy()
    # Group by  country, years, energy-carrier
    out_9495_1_2 = group_by_dimensions(df=out_9495_1_2, groupby_dimensions=['Country', 'Years', 'energy-carrier'], aggregation_method='Sum')
    # refineries-energy-demand-by-energy-carrier[TWh] graphes 91 (5f Top)
    out_9558_1 = out_9495_1_2.rename(columns={'energy-demand[TWh]': 'refineries-energy-demand-by-energy-carrier[TWh]'})
    out_9495_1_2 = out_9495_1.loc[~out_9495_1['sector'].isin(['efuels', 'hydrogen', 'electricity', 'heat-only', 'heat-CHP'])].copy()
    # Group by  country, years, energy-carrier (fuels demand for sectors detailled by fuels)
    out_9495_1_3 = group_by_dimensions(df=out_9495_1_2, groupby_dimensions=['Country', 'Years', 'energy-carrier'], aggregation_method='Sum')
    # energy-demand-by-energy-carrier[TWh] graphe 6 (1d Bottom)
    out_9525_1 = out_9495_1_3.rename(columns={'energy-demand[TWh]': 'energy-demand-by-energy-carrier[TWh]'})
    # Group by  country, years, sector, direct-use (fuels demand detailled by sector)
    out_9495_1_2 = group_by_dimensions(df=out_9495_1_2, groupby_dimensions=['Country', 'Years', 'sector', 'direct-use'], aggregation_method='Sum')
    # energy-demand-by-direct-use-and-sector[TWh] graphe 5 (1d Top)
    out_9524_1 = out_9495_1_2.rename(columns={'energy-demand[TWh]': 'energy-demand-by-direct-use-and-sector[TWh]'})
    # Keep direct-use = for-power-prod  (energy demand for sector)
    out_9495_1_2 = out_9495_1.loc[out_9495_1['direct-use'].isin(['for-power-prod'])].copy()
    # Group by  country, years, energy-carrier, ets-or-not
    out_9495_1_3 = group_by_dimensions(df=out_9495_1_2, groupby_dimensions=['Country', 'Years', 'energy-carrier', 'ets-or-not'], aggregation_method='Sum')
    # energy-demand-by-energy-carrier-and-ets[TWh] graphes 9 et 10 (1f Top and Bottom)
    out_9526_1 = out_9495_1_3.rename(columns={'energy-demand[TWh]': 'energy-demand-by-energy-carrier-and-ets[TWh]'})
    # Group by  country, years, ets-or-not (all power-prod as a same "sector-agg")
    out_9495_1_2 = group_by_dimensions(df=out_9495_1_2, groupby_dimensions=['Country', 'Years', 'ets-or-not'], aggregation_method='Sum')
    # energy-demand-by-ets[TWh] graphes 7 et 8 (1e Top and Bottom)
    out_9527_1 = out_9495_1_2.rename(columns={'energy-demand[TWh]': 'energy-demand-by-ets[TWh]'})
    # Keep primary-type = primary  (Primary energy demand)
    out_9495_1_2 = out_9495_1.loc[out_9495_1['primary-type'].isin(['primary'])].copy()
    out_9495_1_excluded = out_9495_1.loc[~out_9495_1['primary-type'].isin(['primary'])].copy()
    # Group by  country, years, sector
    out_9495_1 = group_by_dimensions(df=out_9495_1_2, groupby_dimensions=['Country', 'Years', 'sector'], aggregation_method='Sum')
    # primary-energy-demand-by-sector[TWh] graphe 13 (1h Top)
    out_9530_1 = out_9495_1.rename(columns={'energy-demand[TWh]': 'primary-energy-demand-by-sector[TWh]'})
    # Keep energy-carrier = electricity (Electricity demand)
    out_9495_1_excluded_2 = out_9495_1_excluded.loc[out_9495_1_excluded['energy-carrier'].isin(['electricity'])].copy()
    out_9495_1_excluded_excluded = out_9495_1_excluded.loc[~out_9495_1_excluded['energy-carrier'].isin(['electricity'])].copy()
    # Group by  country, years, energy-carrier, direct-use (heat, H2, synfuels demand)
    out_9495_1_excluded_excluded = group_by_dimensions(df=out_9495_1_excluded_excluded, groupby_dimensions=['Country', 'Years', 'energy-carrier', 'direct-use'], aggregation_method='Sum')
    # energy-demand-by-direct-use-and-energy-carrier[TWh] graphe 93 (5g Top)
    out_9528_1 = out_9495_1_excluded_excluded.rename(columns={'energy-demand[TWh]': 'energy-demand-by-direct-use-and-energy-carrier[TWh]'})
    out_1 = pd.concat([out_9528_1, out_9666_1.set_index(out_9666_1.index.astype(str) + '_dup')])
    # Keep energy-carrier = hydrogen (hydrogen demand)
    out_9495_1_excluded_excluded = out_9495_1_excluded_excluded.loc[out_9495_1_excluded_excluded['energy-carrier'].isin(['hydrogen'])].copy()

    # For hydrogen : 
    # We want to split de demand according of the use of hydrogen (for power production (efuels) or for sector)

    # Group by  country, years, energy-carrier (sum for-power-prod and for-sector)
    out_9495_1_excluded_excluded_2 = group_by_dimensions(df=out_9495_1_excluded_excluded, groupby_dimensions=['Country', 'Years', 'energy-carrier'], aggregation_method='Sum')
    # pct-direct-use[%] = energy-demand[TWh] / SUM(energy-demand[TWh])
    pct_direct_use_percent = mcd(input_table_1=out_9495_1_excluded_excluded_2, input_table_2=out_9495_1_excluded_excluded, operation_selection='y / x', output_name='pct-direct-use[%]')
    # Rename energy-carrier to sector
    out_9454_1 = pct_direct_use_percent.rename(columns={'energy-carrier': 'sector'})
    # Group by  country, years, sector, energy-carrier (electricity demand)
    out_9495_1_excluded = group_by_dimensions(df=out_9495_1_excluded_2, groupby_dimensions=['Country', 'Years', 'energy-carrier', 'sector'], aggregation_method='Sum')
    # energy-demand[TWh] = energy-demand[TWh] * pct-direct-use[%]  LEFT JOIN If missing => set 1
    energy_demand_TWh_6 = mcd(input_table_1=out_9495_1_excluded, input_table_2=out_9454_1, operation_selection='x * y', output_name='energy-demand[TWh]', fill_value_bool='Left [x] Outer Join', fill_value=1.0)
    # Split direct-use between for-power and for-sectors
    energy_demand_TWh_7 = energy_demand_TWh_6.loc[energy_demand_TWh_6['direct-use'].isin(['for-sector'])].copy()
    energy_demand_TWh_excluded = energy_demand_TWh_6.loc[~energy_demand_TWh_6['direct-use'].isin(['for-sector'])].copy()
    # Split direct-use between for-power and for-sectors
    energy_demand_TWh_excluded_2 = energy_demand_TWh_excluded.loc[energy_demand_TWh_excluded['direct-use'].isin(['for-power-prod'])].copy()
    energy_demand_TWh_excluded_excluded = energy_demand_TWh_excluded.loc[~energy_demand_TWh_excluded['direct-use'].isin(['for-power-prod'])].copy()
    # Switch sector from hydrogen to hydrogen-for-power
    energy_demand_TWh_excluded = energy_demand_TWh_excluded_2.assign(**{'sector': "hydrogen-for-power-prod"})
    energy_demand_TWh_excluded = pd.concat([energy_demand_TWh_excluded, energy_demand_TWh_excluded_excluded.set_index(energy_demand_TWh_excluded_excluded.index.astype(str) + '_dup')])
    # Switch sector from hydrogen to hydrogen-for-sectors
    energy_demand_TWh_6 = energy_demand_TWh_7.assign(**{'sector': "hydrogen-for-sector"})
    energy_demand_TWh_6 = pd.concat([energy_demand_TWh_6, energy_demand_TWh_excluded.set_index(energy_demand_TWh_excluded.index.astype(str) + '_dup')])
    # Remove direct-use dimension
    energy_demand_TWh_6 = column_filter(df=energy_demand_TWh_6, columns_to_drop=['direct-use'])
    # elec-demand-by-energy-carrier- and-sector[TWh] graphe 87 (5d Top)
    out_9529_1 = energy_demand_TWh_6.rename(columns={'energy-demand[TWh]': 'elec-demand-by-energy-carrier-and-sector[TWh]'})
    out_1 = pd.concat([out_9529_1, out_1.set_index(out_1.index.astype(str) + '_dup')])
    # Keep direct-use = for power-prod  (Primary energy demand for energy production)
    out_9495_1 = out_9495_1_2.loc[out_9495_1_2['direct-use'].isin(['for-power-prod'])].copy()
    # Group by  country, years, sector
    out_9495_1 = group_by_dimensions(df=out_9495_1, groupby_dimensions=['Country', 'Years', 'sector'], aggregation_method='Sum')
    # primary-energy-demand-for-power-by-sector[TWh] graphe 86 (5c Top)
    out_9531_1 = out_9495_1.rename(columns={'energy-demand[TWh]': 'primary-energy-demand-for-power-by-sector[TWh]'})
    out_1 = pd.concat([out_9531_1, out_1.set_index(out_1.index.astype(str) + '_dup')])
    out_1 = pd.concat([out_9530_1, out_1.set_index(out_1.index.astype(str) + '_dup')])
    out_1 = pd.concat([out_9558_1, out_1.set_index(out_1.index.astype(str) + '_dup')])
    out_1 = pd.concat([out_9527_1, out_1.set_index(out_1.index.astype(str) + '_dup')])
    out_1 = pd.concat([out_9526_1, out_1.set_index(out_1.index.astype(str) + '_dup')])
    out_1 = pd.concat([out_9525_1, out_1.set_index(out_1.index.astype(str) + '_dup')])
    out_1 = pd.concat([out_9524_1, out_1.set_index(out_1.index.astype(str) + '_dup')])
    # Keep sector = agr, bld, ind, tran
    energy_demand_TWh_6 = energy_demand_TWh_3.loc[energy_demand_TWh_3['sector'].isin(['agr', 'bld', 'ind', 'tra'])].copy()

    # Electrification of the economy
    # 
    # % of electricity used in final energy consumption

    # Group by  way-of-prod
    energy_demand_TWh_7 = group_by_dimensions(df=energy_demand_TWh_6, groupby_dimensions=['Country', 'Years', 'energy-carrier', 'sector'], aggregation_method='Sum')
    # Bottom:  electrycity (energy carrier)
    energy_demand_TWh_excluded = energy_demand_TWh_7.loc[energy_demand_TWh_7['energy-carrier'].isin(['electricity'])].copy()
    # Group by  energy-carrier
    energy_demand_TWh_excluded = group_by_dimensions(df=energy_demand_TWh_excluded, groupby_dimensions=['Country', 'Years', 'sector'], aggregation_method='Sum')
    # Group by  energy-carrier
    energy_demand_TWh_7 = group_by_dimensions(df=energy_demand_TWh_7, groupby_dimensions=['Country', 'Years', 'sector'], aggregation_method='Sum')
    # electrification[%] = energy-demand[TWh] (elec only) / energy-demand[TWh] (total)
    electrification_percent = mcd(input_table_1=energy_demand_TWh_excluded, input_table_2=energy_demand_TWh_7, operation_selection='x / y', output_name='electrification[%]')
    # Group by  way-of-prod
    energy_demand_TWh_6 = group_by_dimensions(df=energy_demand_TWh_6, groupby_dimensions=['Country', 'Years', 'energy-carrier'], aggregation_method='Sum')
    # Group by  energy-carrier
    energy_demand_TWh_7 = group_by_dimensions(df=energy_demand_TWh_6, groupby_dimensions=['Country', 'Years', 'sector'], aggregation_method='Sum')
    # Bottom:  electrycity (energy carrier)
    energy_demand_TWh_excluded = energy_demand_TWh_6.loc[energy_demand_TWh_6['energy-carrier'].isin(['electricity'])].copy()
    # Group by  energy-carrier
    energy_demand_TWh_excluded = group_by_dimensions(df=energy_demand_TWh_excluded, groupby_dimensions=['Country', 'Years', 'sector'], aggregation_method='Sum')
    # electrification[%] = energy-demand[TWh] (elec only) / energy-demand[TWh] (total)
    electrification_percent_2 = mcd(input_table_1=energy_demand_TWh_excluded, input_table_2=energy_demand_TWh_7, operation_selection='x / y', output_name='electrification[%]')
    # KPI
    electrification_percent = pd.concat([electrification_percent_2, electrification_percent.set_index(electrification_percent.index.astype(str) + '_dup')])
    # electrification [%]
    electrification_percent = export_variable(input_table=electrification_percent, selected_variable='electrification[%]')

    # Primary energy costs
    # 
    # = Primary energy-demand x primary energy-costs

    # Apply primary costs levers 
    # 
    # Determine the costs of primary energy-carrier

    # OTS / FTS primary-energy-costs [EUR/MWh] from TEC
    primary_energy_costs_EUR_per_MWh = import_data(trigram='tec', variable_name='primary-energy-costs')
    # primary-costs[MEUR] = energy-demand[TWh] * primary-energy-costs[EUR/MWh]  Left Outer Join Set 0 if missing
    primary_costs_MEUR = mcd(input_table_1=energy_demand_TWh, input_table_2=primary_energy_costs_EUR_per_MWh, operation_selection='x * y', output_name='primary-costs[MEUR]', fill_value_bool='Left [x] Outer Join')
    # primary-costs to final-energy-costs
    out_7306_1 = primary_costs_MEUR.rename(columns={'primary-costs[MEUR]': 'final-energy-costs[MEUR]'})
    out_7306_1 = out_7306_1.loc[~out_7306_1['energy-carrier'].isin(['gaseous-syn', 'electricity', 'heat', 'hydrogen', 'liquid-syn', 'liquid-syn-diesel', 'liquid-syn-gasoline', 'liquid-syn-kerosene', 'liquid-syn-marinefueloil'])].copy()

    # Final Energy Costs
    # 
    # = Concatenate final energy costs

    # final-energy-costs [MEUR]
    final_energy_costs_MEUR_3 = use_variable(input_table=out_7306_1, selected_variable='final-energy-costs[MEUR]')

    # Fuel Costs (Electricity)

    # primary-costs [MEUR]
    primary_costs_MEUR = use_variable(input_table=primary_costs_MEUR, selected_variable='primary-costs[MEUR]')
    # Keep only sector electricity
    primary_costs_MEUR_2 = primary_costs_MEUR.loc[primary_costs_MEUR['sector'].isin(['electricity'])].copy()
    # Group by  country, years, way-of-prod
    primary_costs_MEUR_2 = group_by_dimensions(df=primary_costs_MEUR_2, groupby_dimensions=['Country', 'Years', 'way-of-production'], aggregation_method='Sum')

    # Add Final Energy Costs to Primary-Costs

    # Group by  country, years, sector
    primary_costs_MEUR = group_by_dimensions(df=primary_costs_MEUR, groupby_dimensions=['Country', 'Years', 'sector'], aggregation_method='Sum')
    # primary-energy-costs to fuel-price
    out_7394_1 = primary_energy_costs_EUR_per_MWh.rename(columns={'primary-energy-costs[EUR/MWh]': 'fuel-price[EUR/MWh]'})
    # EUR/MWh to MEUR/TWh
    fuel_price_MEUR_per_TWh = out_7394_1.drop(columns='fuel-price[EUR/MWh]').assign(**{'fuel-price[MEUR/TWh]': out_7394_1['fuel-price[EUR/MWh]'] * 1.0})

    # Fuel Price
    # 
    # = Concatenate fuel price

    # fuel-price [MEUR/TWh]
    fuel_price_MEUR_per_TWh = use_variable(input_table=fuel_price_MEUR_per_TWh, selected_variable='fuel-price[MEUR/TWh]')
    # Keep only energy-carrier electricity
    energy_demand_TWh_6 = energy_demand_TWh.loc[energy_demand_TWh['energy-carrier'].isin(['electricity'])].copy()
    # Keep only energy-carrier hydrogen
    energy_demand_TWh_7 = energy_demand_TWh_3.loc[energy_demand_TWh_3['energy-carrier'].isin(['hydrogen'])].copy()
    # Keep only energy-carrier syn fuels
    energy_demand_TWh_3 = energy_demand_TWh_5.loc[energy_demand_TWh_5['energy-carrier'].isin(['gaseous-syn', 'liquid-syn', 'liquid-syn-diesel', 'liquid-syn-gasoline', 'liquid-syn-kerosene', 'liquid-syn-marinefueloil'])].copy()

    # Fuel Costs (Heat / CHP)

    # energy-demand [TWh]
    energy_demand_TWh_5 = use_variable(input_table=energy_demand_TWh_5, selected_variable='energy-demand[TWh]')
    # Keep only energy-carrier heat
    energy_demand_TWh_8 = energy_demand_TWh_5.loc[energy_demand_TWh_5['energy-carrier'].isin(['heat'])].copy()
    # cal-rate [%]
    cal_rate_energy_demand_TWh_2 = export_variable(input_table=out_7084_1, selected_variable='cal_rate_energy-demand[TWh]')
    # cal-rate [%]
    cal_rate_energy_demand_TWh_2 = use_variable(input_table=cal_rate_energy_demand_TWh_2, selected_variable='cal_rate_energy-demand[TWh]')
    out_7429_1 = pd.concat([cal_rate_energy_demand_TWh_2, out_6432_1.set_index(out_6432_1.index.astype(str) + '_dup')])

    # For : Scope 2/3
    # - Energy imported => we split the import by sector according to their energy demand

    # energy-demand [TWh]
    energy_demand_TWh_5 = use_variable(input_table=out_6222_1, selected_variable='energy-demand[TWh]')
    # Group by  country, years, energy-carrier, sector
    energy_demand_TWh_9 = group_by_dimensions(df=energy_demand_TWh_5, groupby_dimensions=['Country', 'Years', 'energy-carrier', 'sector'], aggregation_method='Sum')
    # Group by  country, years, energy-carrier
    energy_demand_TWh_5 = group_by_dimensions(df=energy_demand_TWh_5, groupby_dimensions=['Country', 'Years', 'energy-carrier'], aggregation_method='Sum')
    # sector-share[%] = energy-demand[TWh] by sector / energy-demand[TWh] total
    sector_share_percent = mcd(input_table_1=energy_demand_TWh_9, input_table_2=energy_demand_TWh_5, operation_selection='x / y', output_name='sector-share[%]')
    # Set to 0
    sector_share_percent = missing_value(df=sector_share_percent, DTS_DT_O=[['org.knime.core.data.def.IntCell', 'org.knime.base.node.preproc.pmml.missingval.handlers.DoNothingMissingCellHandlerFactory'], ['org.knime.core.data.def.StringCell', 'org.knime.base.node.preproc.pmml.missingval.handlers.DoNothingMissingCellHandlerFactory'], ['org.knime.core.data.def.DoubleCell', 'org.knime.base.node.preproc.pmml.missingval.handlers.FixedDoubleValueMissingCellHandlerFactory']], FixedValue='0.0')
    # Top : energy-carrier  = gaseous fossil
    sector_share_percent_2 = sector_share_percent.loc[sector_share_percent['energy-carrier'].isin(['gaseous-ff', 'gaseous-ff-natural'])].copy()
    sector_share_percent_excluded = sector_share_percent.loc[~sector_share_percent['energy-carrier'].isin(['gaseous-ff', 'gaseous-ff-natural'])].copy()
    # Top : energy-carrier  = liquid fossil
    sector_share_percent_excluded_2 = sector_share_percent_excluded.loc[sector_share_percent_excluded['energy-carrier'].isin(['liquid-ff', 'liquid-ff-diesel', 'liquid-ff-gasoline', 'liquid-ff-kerosene', 'liquid-ff-marinefueloil', 'liquid-ff-oil'])].copy()
    sector_share_percent_excluded_excluded = sector_share_percent_excluded.loc[~sector_share_percent_excluded['energy-carrier'].isin(['liquid-ff', 'liquid-ff-diesel', 'liquid-ff-gasoline', 'liquid-ff-kerosene', 'liquid-ff-marinefueloil', 'liquid-ff-oil'])].copy()
    # Top : energy-carrier  = solid fossil
    sector_share_percent_excluded_excluded_2 = sector_share_percent_excluded_excluded.loc[sector_share_percent_excluded_excluded['energy-carrier'].isin(['solid-ff-coal'])].copy()
    sector_share_percent_excluded_excluded_excluded = sector_share_percent_excluded_excluded.loc[~sector_share_percent_excluded_excluded['energy-carrier'].isin(['solid-ff-coal'])].copy()
    # Top : energy-carrier  = liquid-syn-.*
    sector_share_percent_excluded_excluded_excluded_2 = sector_share_percent_excluded_excluded_excluded.loc[sector_share_percent_excluded_excluded_excluded['energy-carrier'].isin(['liquid-syn-diesel', 'liquid-syn-gasoline', 'liquid-syn-kerosene', 'liquid-syn-marinefueloil'])].copy()
    sector_share_percent_excluded_excluded_excluded_excluded = sector_share_percent_excluded_excluded_excluded.loc[~sector_share_percent_excluded_excluded_excluded['energy-carrier'].isin(['liquid-syn-diesel', 'liquid-syn-gasoline', 'liquid-syn-kerosene', 'liquid-syn-marinefueloil'])].copy()
    # force energy-carrier = "liquid-syn"
    sector_share_percent_excluded_excluded_excluded = sector_share_percent_excluded_excluded_excluded_2.assign(**{'energy-carrier': "liquid-syn"})
    # force energy-carrier = "solid-ff"
    sector_share_percent_excluded_excluded = sector_share_percent_excluded_excluded_2.assign(**{'energy-carrier': "solid-ff"})
    # force energy-carrier = "liquid-ff"
    sector_share_percent_excluded = sector_share_percent_excluded_2.assign(**{'energy-carrier': "liquid-ff"})
    # force energy-carrier = "gaseous-ff"
    sector_share_percent = sector_share_percent_2.assign(**{'energy-carrier': "gaseous-ff"})
    sector_share_percent = pd.concat([sector_share_percent, sector_share_percent_excluded.set_index(sector_share_percent_excluded.index.astype(str) + '_dup')])
    sector_share_percent = pd.concat([sector_share_percent, sector_share_percent_excluded_excluded.set_index(sector_share_percent_excluded_excluded.index.astype(str) + '_dup')])
    sector_share_percent = pd.concat([sector_share_percent, sector_share_percent_excluded_excluded_excluded.set_index(sector_share_percent_excluded_excluded_excluded.index.astype(str) + '_dup')])
    sector_share_percent = pd.concat([sector_share_percent, sector_share_percent_excluded_excluded_excluded_excluded.set_index(sector_share_percent_excluded_excluded_excluded_excluded.index.astype(str) + '_dup')])
    # Sum on all dimensions to avoid duplicates
    sector_share_percent = group_by_dimensions(df=sector_share_percent, groupby_dimensions=['Country', 'Years', 'energy-carrier', 'sector'], aggregation_method='Sum')

    # For : Pathway Explorer
    # - Capacity-factor

    # capacity-factor [%]
    capacity_factor_percent = use_variable(input_table=out_9763_11, selected_variable='capacity-factor[%]')
    # net-energy-production [TWh] FROM electricity  (does not account for eletricity production with CHP)
    net_energy_production_TWh = use_variable(input_table=out_9763_2, selected_variable='net-energy-production[TWh]')
    # Group by  country, years, way-of-prod, energy-carrier, primary-energy-carrier
    net_energy_production_TWh = group_by_dimensions(df=net_energy_production_TWh, groupby_dimensions=['Country', 'Years', 'energy-carrier', 'way-of-production', 'primary-energy-carrier'], aggregation_method='Sum')
    # net-energy-production-by- carrier-primary- carrier-way-of-prod[TWh]
    out_9543_1 = net_energy_production_TWh.rename(columns={'net-energy-production[TWh]': 'net-energy-production-by-carrier-primary-carrier-way-of-prod[TWh]'})
    # Split direct-use between for-power and for-sectors
    out_9543_1_2 = out_9543_1.loc[out_9543_1['primary-energy-carrier'].isin(['gaseous-bio', 'gaseous-ff-natural', 'liquid-bio', 'liquid-ff', 'solid-biomass', 'solid-waste-nonres', 'solid-waste-res', 'solid-ff-coal'])].copy()
    out_9543_1_excluded = out_9543_1.loc[~out_9543_1['primary-energy-carrier'].isin(['gaseous-bio', 'gaseous-ff-natural', 'liquid-bio', 'liquid-ff', 'solid-biomass', 'solid-waste-nonres', 'solid-waste-res', 'solid-ff-coal'])].copy()
    # Add primary-carrier as carrier
    out_9543_1_excluded['primary-energy-carrier'] = "carrier"
    out_9543_1 = pd.concat([out_9543_1_2, out_9543_1_excluded.set_index(out_9543_1_excluded.index.astype(str) + '_dup')])
    # energy-imported[TWh]
    energy_imported_TWh_3 = export_variable(input_table=out_9763_3, selected_variable='energy-imported[TWh]')
    # energy-imported [TWh]
    energy_imported_TWh_3 = use_variable(input_table=energy_imported_TWh_3, selected_variable='energy-imported[TWh]')
    # Node 5876
    energy_imported_TWh_2 = pd.concat([energy_imported_TWh_2, energy_imported_TWh_3.set_index(energy_imported_TWh_3.index.astype(str) + '_dup')])
    # energy-imported [TWh]
    energy_imported_TWh_2 = use_variable(input_table=energy_imported_TWh_2, selected_variable='energy-imported[TWh]')
    # Node 5876
    energy_imported_TWh = pd.concat([energy_imported_TWh_2, energy_imported_TWh.set_index(energy_imported_TWh.index.astype(str) + '_dup')])
    # If missing txt set to "" (energy-carrier for refineries and energy-carrier-categories for other tahn syn and refineries)
    energy_imported_TWh = missing_value(df=energy_imported_TWh, dimension_rx='^.*\\[.*•\\]$', DTS_DT_O=[['org.knime.core.data.def.IntCell', 'org.knime.base.node.preproc.pmml.missingval.handlers.DoNothingMissingCellHandlerFactory'], ['org.knime.core.data.def.StringCell', 'org.knime.base.node.preproc.pmml.missingval.handlers.FixedStringValueMissingCellHandlerFactory'], ['org.knime.core.data.def.DoubleCell', 'org.knime.base.node.preproc.pmml.missingval.handlers.DoNothingMissingCellHandlerFactory']], FixedValue='')
    # energy-imported [TWh]
    energy_imported_TWh = use_variable(input_table=energy_imported_TWh, selected_variable='energy-imported[TWh]')
    # energy-exported[TWh] = abs(energy-imported) if val < 0 else : 0
    energy_exported_TWh = energy_imported_TWh.copy()
    mask = energy_exported_TWh['energy-imported[TWh]'] < 0
    energy_exported_TWh.loc[mask, 'energy-exported[TWh]'] =  abs(energy_exported_TWh.loc[mask, 'energy-imported[TWh]'])
    energy_exported_TWh.loc[~mask, 'energy-exported[TWh]'] =  0
    # Remove imported var
    energy_exported_TWh = column_filter(df=energy_exported_TWh, pattern='^((?!energy-imported\[TWh\]).)*$')
    # Group by  country, years, sector, energy-carrier-category
    energy_exported_TWh_2 = group_by_dimensions(df=energy_exported_TWh, groupby_dimensions=['Country', 'Years', 'sector', 'energy-carrier-category'], aggregation_method='Sum')
    # Keep refineries  sector
    energy_exported_TWh_2 = energy_exported_TWh_2.loc[energy_exported_TWh_2['sector'].isin(['refineries'])].copy()
    # Rename variable to fossil-energy-exported[TWh]
    out_9350_1 = energy_exported_TWh_2.rename(columns={'energy-exported[TWh]': 'fossil-energy-exported[TWh]'})
    # Group by  country, years, energy-carrier
    energy_exported_TWh = group_by_dimensions(df=energy_exported_TWh, groupby_dimensions=['Country', 'Years', 'energy-carrier'], aggregation_method='Sum')
    # add aggregation columns
    out_9586_1 = joiner(df_left=energy_exported_TWh, df_right=out_9358_1, joiner='inner', left_input=['energy-carrier'], right_input=['energy-carrier'])
    # Remove energy-carrier
    out_9586_1 = column_filter(df=out_9586_1, columns_to_drop=['energy-carrier'])
    # energy-carrier-agg to energy-carrier
    out_9353_1 = out_9586_1.rename(columns={'energy-carrier-agg': 'energy-carrier'})
    # Group by  country, years, energy-carrier
    out_9353_1 = group_by_dimensions(df=out_9353_1, groupby_dimensions=['Country', 'Years', 'energy-carrier'], aggregation_method='Sum')
    # Rename variable to elec-energy-exported[TWh]
    out_9355_1 = out_9353_1.rename(columns={'energy-exported[TWh]': 'elec-energy-exported[TWh]'})
    out_1_3 = pd.concat([out_9350_1, out_9355_1.set_index(out_9355_1.index.astype(str) + '_dup')])
    # energy-import[TWh] = val if > 0 else : 0
    mask = energy_imported_TWh['energy-imported[TWh]'] < 0
    energy_imported_TWh.loc[mask, 'energy-imported[TWh]'] =  0
    energy_imported_TWh.loc[~mask, 'energy-imported[TWh]'] =  energy_imported_TWh.loc[~mask, 'energy-imported[TWh]']
    # energy-import[TWh] Group by  country, years, energy-carrier, energy-carrier-category
    energy_imported_TWh_3 = group_by_dimensions(df=energy_imported_TWh, groupby_dimensions=['Country', 'Years', 'energy-carrier', 'energy-carrier-category'], aggregation_method='Sum')
    # Top : energy-carrier-categry = gaseous, liquid, solid
    energy_imported_TWh_2 = energy_imported_TWh_3.loc[energy_imported_TWh_3['energy-carrier-category'].isin(['gaseous', 'liquid', 'solid'])].copy()
    energy_imported_TWh_excluded = energy_imported_TWh_3.loc[~energy_imported_TWh_3['energy-carrier-category'].isin(['gaseous', 'liquid', 'solid'])].copy()
    # Top : energy-carrier = liquid-syn-.*
    energy_imported_TWh_3 = energy_imported_TWh_2.loc[energy_imported_TWh_2['energy-carrier'].isin(['liquid-syn-diesel', 'liquid-syn-gasoline', 'liquid-syn-kerosene', 'liquid-syn-marinefueloil'])].copy()
    energy_imported_TWh_excluded_2 = energy_imported_TWh_2.loc[~energy_imported_TWh_2['energy-carrier'].isin(['liquid-syn-diesel', 'liquid-syn-gasoline', 'liquid-syn-kerosene', 'liquid-syn-marinefueloil'])].copy()
    # Exclude energy-carrier = .*-syn
    energy_imported_TWh_excluded_excluded = energy_imported_TWh_excluded_2.loc[energy_imported_TWh_excluded_2['energy-carrier'].isin(['gaseous-syn', 'liquid-syn'])].copy()
    energy_imported_TWh_excluded_2 = energy_imported_TWh_excluded_2.loc[~energy_imported_TWh_excluded_2['energy-carrier'].isin(['gaseous-syn', 'liquid-syn'])].copy()
    energy_imported_TWh_excluded_2 = string_manipulation(df=energy_imported_TWh_excluded_2, expression='join($energy-carrier-category$, "-ff")', var_name='energy-carrier')
    energy_imported_TWh_2 = string_manipulation(df=energy_imported_TWh_3, expression='join("liquid-syn")', var_name='energy-carrier')
    energy_imported_TWh_2 = pd.concat([energy_imported_TWh_excluded_2, energy_imported_TWh_2.set_index(energy_imported_TWh_2.index.astype(str) + '_dup')])
    energy_imported_TWh_2 = pd.concat([energy_imported_TWh_2, energy_imported_TWh_excluded_excluded.set_index(energy_imported_TWh_excluded_excluded.index.astype(str) + '_dup')])
    energy_imported_TWh_2 = pd.concat([energy_imported_TWh_2, energy_imported_TWh_excluded.set_index(energy_imported_TWh_excluded.index.astype(str) + '_dup')])
    # Sum on all dimensions to avoid duplicates
    energy_imported_TWh_2 = group_by_dimensions(df=energy_imported_TWh_2, groupby_dimensions=['Country', 'Years', 'energy-carrier', 'energy-carrier-category'], aggregation_method='Sum')
    # Remove energy-carrier-category
    energy_imported_TWh_2 = column_filter(df=energy_imported_TWh_2, columns_to_drop=['energy-carrier-category'])
    # energy-imported[TWh] = energy-import[TWh] * sector-share[%]
    energy_imported_TWh_2 = mcd(input_table_1=energy_imported_TWh_2, input_table_2=sector_share_percent, operation_selection='x * y', output_name='energy-imported[TWh]', fill_value_bool='Left [x] Outer Join', fill_value=1.0)
    # Module = Scope 2 and 3
    energy_imported_TWh_2 = column_filter(df=energy_imported_TWh_2, pattern='^.*$')
    # Group by  country, years, energy-carrier
    energy_imported_TWh_3 = group_by_dimensions(df=energy_imported_TWh, groupby_dimensions=['Country', 'Years', 'energy-carrier'], aggregation_method='Sum')
    # add aggregation columns
    out_9585_1 = joiner(df_left=energy_imported_TWh_3, df_right=out_9358_1, joiner='inner', left_input=['energy-carrier'], right_input=['energy-carrier'])
    # Remove energy-carrier
    out_9585_1 = column_filter(df=out_9585_1, columns_to_drop=['energy-carrier'])
    # energy-carrier-agg to energy-carrier
    out_9346_1 = out_9585_1.rename(columns={'energy-carrier-agg': 'energy-carrier'})
    # Group by  country, years, energy-carrier
    out_9346_1 = group_by_dimensions(df=out_9346_1, groupby_dimensions=['Country', 'Years', 'energy-carrier'], aggregation_method='Sum')
    # Rename variable to elec-energy-imported[TWh]
    out_9348_1 = out_9346_1.rename(columns={'energy-imported[TWh]': 'elec-energy-imported[TWh]'})
    # Group by  country, years, sector, energy-carrier-category
    energy_imported_TWh = group_by_dimensions(df=energy_imported_TWh, groupby_dimensions=['Country', 'Years', 'sector', 'energy-carrier-category'], aggregation_method='Sum')
    # Keep refineries  sector
    energy_imported_TWh = energy_imported_TWh.loc[energy_imported_TWh['sector'].isin(['refineries'])].copy()
    # Rename variable to fossil-energy-imported[TWh]
    out_9342_1 = energy_imported_TWh.rename(columns={'energy-imported[TWh]': 'fossil-energy-imported[TWh]'})
    out_1_2 = pd.concat([out_9342_1, out_9348_1.set_index(out_9348_1.index.astype(str) + '_dup')])
    out_1_2 = pd.concat([out_1_2, out_1_3.set_index(out_1_3.index.astype(str) + '_dup')])

    # For : Pathway Explorer
    # - New / total / existing capacities

    # total-capacities [GW]
    total_capacities_GW = use_variable(input_table=out_9763_9, selected_variable='total-capacities[GW]')
    # Unit conversion GW to MW
    total_capacities_MW = total_capacities_GW.drop(columns='total-capacities[GW]').assign(**{'total-capacities[MW]': total_capacities_GW['total-capacities[GW]'] * 1000.0})
    # Rename variable to total-capacities-by-ets[MW]
    out_9262_1 = total_capacities_MW.rename(columns={'total-capacities[MW]': 'total-capacities-by-ets[MW]'})
    # Group by Country, Years, way-of-prod (sum)
    total_capacities_MW = group_by_dimensions(df=total_capacities_MW, groupby_dimensions=['Country', 'Years', 'way-of-production'], aggregation_method='Sum')
    out_9263_1 = pd.concat([out_9262_1, total_capacities_MW.set_index(total_capacities_MW.index.astype(str) + '_dup')])

    # Installed renewable energy capacity per capita

    # total-capacities [GW]
    total_capacities_GW = use_variable(input_table=total_capacities_GW, selected_variable='total-capacities[GW]')
    # Keep only energy-carrier fossil fuels
    total_capacities_GW = total_capacities_GW.loc[total_capacities_GW['way-of-production'].isin(['RES-geothermal', 'RES-hydroelectric', 'RES-marine', 'RES-solar-csp', 'RES-solar-pv', 'RES-wind-offshore', 'RES-wind-onshore'])].copy()
    # Group by  country, years
    total_capacities_GW = group_by_dimensions(df=total_capacities_GW, groupby_dimensions=['Country', 'Years'], aggregation_method='Sum')
    # Unit conversion GW to W
    total_capacities_W = total_capacities_GW.drop(columns='total-capacities[GW]').assign(**{'total-capacities[W]': total_capacities_GW['total-capacities[GW]'] * 1000000000.0})
    # RES-cap-total-per-cap[W/cap] = total-capacities[W] (RES) / population[cap]
    RES_cap_total_per_cap_W_per_cap = mcd(input_table_1=total_capacities_W, input_table_2=population_cap, operation_selection='x / y', output_name='RES-cap-total-per-cap[W/cap]')
    # RES-cap-total-per-cap[W/cap]
    RES_cap_total_per_cap_W_per_cap = export_variable(input_table=RES_cap_total_per_cap_W_per_cap, selected_variable='RES-cap-total-per-cap[W/cap]')
    # KPI
    per_cap_per_cap = pd.concat([RES_cap_total_per_cap_W_per_cap, ff_demand_per_cap_MWh_per_cap.set_index(ff_demand_per_cap_MWh_per_cap.index.astype(str) + '_dup')])
    # new-cum-capacities [GW]
    cum_new_capacities_GW = use_variable(input_table=out_9763_10, selected_variable='cum-new-capacities[GW]')
    # Unit conversion GW to MW
    cum_new_capacities_MW = cum_new_capacities_GW.drop(columns='cum-new-capacities[GW]').assign(**{'cum-new-capacities[MW]': cum_new_capacities_GW['cum-new-capacities[GW]'] * 1000.0})
    # Lag  cum-new-capacities[num]
    out_7610_1, _ = lag_variable(df=cum_new_capacities_GW, in_var='cum-new-capacities[GW]')
    # cum-new-capacities -lagged [GW]
    cum_new_capacities_lagged_GW = use_variable(input_table=out_7610_1, selected_variable='cum-new-capacities_lagged[GW]')
    # new-capacities[GW] = cum-new-capacities [GW] - cum-new-capacities-lagged [GW]
    new_capacities_GW = mcd(input_table_1=cum_new_capacities_GW, input_table_2=cum_new_capacities_lagged_GW, operation_selection='x - y', output_name='new-capacities[GW]')

    # Pivot

    out_7260_1, _, _ = pivoting(df=new_capacities_GW, agg_dict={'new-capacities[GW]': 'sum'}, column_name_option='Pivot name+Aggregation name', column_name_policy='Keep original name(s)', list_group_columns=['Country', 'Years'], list_pivots=['way-of-production'])
    out_7261_1 = missing_value_column_filter(df=out_7260_1, missing_threshold=0.9, type_of_pattern='Manual')
    # Same as in Pathway Explorer
    out_7261_1 = column_rename_regex(df=out_7261_1, search_string='(.*)\\+(.*)\\[.*', replace_string='$2_$1[GW]')

    def helper_6983(input_table_1, input_table_2) -> pd.DataFrame:
        # Input tables
        corr_table = input_table_1.copy()
        output_table = input_table_2.copy()
        
        # Create dict with old_new_name
        dict_names = {}
        for row in corr_table.iterrows():
            old_name = row[1]["old_name"]
            new_name = row[1]["new_name"]
            dict_names[new_name] = old_name
        
        # For col in columns > if col correspond to new name => get in dict for rename
        dict_rename = {}
        for col in output_table.head():
            for i in dict_names.keys():
                if i == col:
                    dict_rename[i] = dict_names[i]
        
        output_table = output_table.rename(columns = dict_rename)
        return output_table
    # rename based on table creator
    out_6983_1 = helper_6983(input_table_1=out_6982_1, input_table_2=out_7261_1)
    # Remove solar-pv
    out_6983_1 = column_filter(df=out_6983_1, columns_to_drop=['new-capacities_RES-solar-pv[GW]'])
    out_7054_1 = joiner(df_left=out_7062_1, df_right=out_6983_1, joiner='inner', left_input=['Country', 'Years'], right_input=['Country', 'Years'])
    # Module = Minerals
    out_7054_1 = column_filter(df=out_7054_1, pattern='^.*$')
    # existing-capacities [GW]
    existing_capacities_GW = use_variable(input_table=out_9763_14, selected_variable='existing-capacities[GW]')
    # Unit conversion GW to MW
    existing_capacities_MW = existing_capacities_GW.drop(columns='existing-capacities[GW]').assign(**{'existing-capacities[MW]': existing_capacities_GW['existing-capacities[GW]'] * 1000.0})
    out_9264_1 = pd.concat([out_9263_1, existing_capacities_MW.set_index(existing_capacities_MW.index.astype(str) + '_dup')])
    out_9265_1 = pd.concat([out_9264_1, cum_new_capacities_MW.set_index(cum_new_capacities_MW.index.astype(str) + '_dup')])
    out_9669_1 = pd.concat([out_9265_1, new_capacities_GW.set_index(new_capacities_GW.index.astype(str) + '_dup')])
    out_9225_1 = pd.concat([capacity_factor_percent, out_9669_1.set_index(out_9669_1.index.astype(str) + '_dup')])
    out_9266_1 = pd.concat([out_9225_1, backup_capacity_GW.set_index(backup_capacity_GW.index.astype(str) + '_dup')])
    out_1 = pd.concat([out_9266_1, out_1.set_index(out_1.index.astype(str) + '_dup')])
    out_1 = pd.concat([out_1_2, out_1.set_index(out_1.index.astype(str) + '_dup')])
    # Calibration (primary energy demand & emissions)
    out_9763_1 = column_filter(df=out_9763_1, columns_to_drop=[])
    # Calibration (primary energy demand & emissions)
    out_9763_1 = column_filter(df=out_9763_1, columns_to_drop=[])
    # capex-opex[MEUR]
    out_9763_5 = column_filter(df=out_9763_5, pattern='^.*$')
    # capex-opex [MEUR]
    out_9763_5 = column_filter(df=out_9763_5, pattern='^.*$')
    # Node 5876
    out = pd.concat([out_3, out_9763_5.set_index(out_9763_5.index.astype(str) + '_dup')])
    # Set to 0
    out = missing_value(df=out, DTS_DT_O=[['org.knime.core.data.def.IntCell', 'org.knime.base.node.preproc.pmml.missingval.handlers.DoNothingMissingCellHandlerFactory'], ['org.knime.core.data.def.StringCell', 'org.knime.base.node.preproc.pmml.missingval.handlers.DoNothingMissingCellHandlerFactory'], ['org.knime.core.data.def.DoubleCell', 'org.knime.base.node.preproc.pmml.missingval.handlers.FixedDoubleValueMissingCellHandlerFactory']], FixedValue='0.0')
    # capex-opex [MEUR]
    out = column_filter(df=out, pattern='^.*$')
    # Node 5876
    out_2 = pd.concat([out, out_6432_5.set_index(out_6432_5.index.astype(str) + '_dup')])

    # Concatenate of a same variable
    # coming from differents sources
    # 
    # Here : capex-opex

    # Group by  Coutry, years,  sectors, way-of-prod (sum)
    out = group_by_dimensions(df=out_2, groupby_dimensions=['Country', 'Years', 'sector', 'way-of-production'], aggregation_method='Sum')
    # Set to 0
    out = missing_value(df=out, DTS_DT_O=[['org.knime.core.data.def.IntCell', 'org.knime.base.node.preproc.pmml.missingval.handlers.DoNothingMissingCellHandlerFactory'], ['org.knime.core.data.def.StringCell', 'org.knime.base.node.preproc.pmml.missingval.handlers.DoNothingMissingCellHandlerFactory'], ['org.knime.core.data.def.DoubleCell', 'org.knime.base.node.preproc.pmml.missingval.handlers.FixedDoubleValueMissingCellHandlerFactory']], FixedValue='0.0')
    # capex-opex[MEUR]
    out_3 = column_filter(df=out, pattern='^.*$')
    # Keep sector = electricity
    out = out_3.loc[out_3['sector'].isin(['electricity'])].copy()

    # Fuel Costs (Hydrogen)

    # capex-opex[MEUR]
    out_4 = column_filter(df=out_3, pattern='^.*$')
    # Keep sector = hydrogen
    out_3 = out_4.loc[out_4['sector'].isin(['hydrogen'])].copy()

    # Fuel Costs (Efuels)

    # capex-opex[MEUR]
    out_5 = column_filter(df=out_4, pattern='^.*$')
    # Keep sector = efuels
    out_4 = out_5.loc[out_5['sector'].isin(['efuels'])].copy()
    # capex-opex[MEUR]
    out_5 = column_filter(df=out_5, pattern='^.*$')

    # For : Costs
    # Capex / Opex

    # capex-opex[MEUR]
    out_6 = column_filter(df=out_5, pattern='^.*$')
    # sector = electricity
    out_7 = out_6.loc[out_6['sector'].isin(['electricity'])].copy()
    # Group by  Country, Years, way-of-production (sum)
    out_7 = group_by_dimensions(df=out_7, groupby_dimensions=['Country', 'Years', 'way-of-production'], aggregation_method='Sum')
    # opex/capex -by-way-of-prod
    out_7 = column_rename_regex(df=out_7, search_string='(.*)(\\[.*)', replace_string='$1-by-way-of-prod$2')
    # Group by  Country, Years, sector (sum)
    out_6 = group_by_dimensions(df=out_6, groupby_dimensions=['Country', 'Years', 'sector'], aggregation_method='Sum')
    out_6 = pd.concat([out_6, out_7.set_index(out_7.index.astype(str) + '_dup')])
    # Keep sector = heat
    out_5 = out_5.loc[out_5['sector'].isin(['heat'])].copy()
    # capex-opex[MEUR]
    out_2 = column_filter(df=out_2, pattern='^.*$')
    # Group by  Country, Years, cost-user (sum)
    out_2 = group_by_dimensions(df=out_2, groupby_dimensions=['Country', 'Years', 'cost-user'], aggregation_method='Sum')
    out_2 = pd.concat([out_2, out_6.set_index(out_6.index.astype(str) + '_dup')])
    # energy-production[TWh]
    energy_production_TWh_3 = export_variable(input_table=out_9763_4, selected_variable='energy-production[TWh]')

    # Energy Production
    # 
    # = Concatenate energy production from all power sectors (heat / efuels / hydrogen / eletricity / fossil fuels)

    # energy-production [TWh]
    energy_production_TWh_3 = use_variable(input_table=energy_production_TWh_3, selected_variable='energy-production[TWh]')
    # energy-production [TWh]
    energy_production_TWh_2 = use_variable(input_table=energy_production_TWh_2, selected_variable='energy-production[TWh]')
    # Node 5876
    energy_production_TWh_2 = pd.concat([energy_production_TWh_2, energy_production_TWh_3.set_index(energy_production_TWh_3.index.astype(str) + '_dup')])
    # energy-production [TWh]
    energy_production_TWh_2 = use_variable(input_table=energy_production_TWh_2, selected_variable='energy-production[TWh]')
    # Node 5876
    energy_production_TWh = pd.concat([energy_production_TWh_2, energy_production_TWh.set_index(energy_production_TWh.index.astype(str) + '_dup')])

    # Share of renewable energy in final energy consumption
    # 
    # We consider that renewable goes first to Hydrogen production, then to efuels production and finally to other demands

    # energy-production [TWh]
    energy_production_TWh_2 = use_variable(input_table=energy_production_TWh, selected_variable='energy-production[TWh]')

    # Coal in electricity supply
    # 
    # % of coal used for electricity production

    # energy-production [TWh]
    energy_production_TWh = use_variable(input_table=energy_production_TWh_2, selected_variable='energy-production[TWh]')
    # Keep sector = electricity
    energy_production_TWh_3 = energy_production_TWh.loc[energy_production_TWh['sector'].isin(['electricity'])].copy()
    # Group by way-of-prod, energy-carrier, energy-carrier-cat
    energy_production_TWh_4 = group_by_dimensions(df=energy_production_TWh_3, groupby_dimensions=['Country', 'Years', 'primary-energy-carrier', 'sector'], aggregation_method='Sum')
    # Group by  energy-carrier
    energy_production_TWh_3 = group_by_dimensions(df=energy_production_TWh_4, groupby_dimensions=['Country', 'Years', 'sector'], aggregation_method='Sum')
    # Top: solid-ff-coal (prim-energy-carrier)
    energy_production_TWh_4 = energy_production_TWh_4.loc[energy_production_TWh_4['primary-energy-carrier'].isin(['solid-ff-coal'])].copy()
    # Group by prim-energy-carrier
    energy_production_TWh_4 = group_by_dimensions(df=energy_production_TWh_4, groupby_dimensions=['Country', 'Years', 'sector'], aggregation_method='Sum')
    # coal_in_elc_production[%] = energy-demand[TWh] (coal only) / energy-demand[TWh] (total)
    share_of_coal_in_production_percent = mcd(input_table_1=energy_production_TWh_4, input_table_2=energy_production_TWh_3, operation_selection='x / y', output_name='share-of-coal-in-production[%]')
    # share-of-coal-in-production[%]
    share_of_coal_in_production_percent = export_variable(input_table=share_of_coal_in_production_percent, selected_variable='share-of-coal-in-production[%]')
    out_9702_1 = metanode_9702(port_01=energy_demand_TWh, port_02=energy_production_TWh_2)
    # KPI
    out_9862_1 = pd.concat([per_cap_per_cap, out_9702_1.set_index(out_9702_1.index.astype(str) + '_dup')])
    # KPI
    out_9729_1 = pd.concat([out_9862_1, electrification_percent.set_index(electrification_percent.index.astype(str) + '_dup')])
    # KPI
    out_9730_1 = pd.concat([out_9729_1, share_of_coal_in_production_percent.set_index(share_of_coal_in_production_percent.index.astype(str) + '_dup')])
    # Keep energy-carrier = electricity
    energy_production_TWh_3 = energy_production_TWh.loc[energy_production_TWh['energy-carrier'].isin(['electricity'])].copy()
    energy_production_TWh_excluded_2 = energy_production_TWh.loc[~energy_production_TWh['energy-carrier'].isin(['electricity'])].copy()
    # Keep way-of-prod = CHP
    energy_production_TWh_4 = energy_production_TWh_3.loc[energy_production_TWh_3['way-of-production'].isin(['CHP'])].copy()
    energy_production_TWh_excluded = energy_production_TWh_3.loc[~energy_production_TWh_3['way-of-production'].isin(['CHP'])].copy()
    # sector -> electricity
    energy_production_TWh_3 = energy_production_TWh_4.assign(**{'sector': "electricity"})
    energy_production_TWh_3 = pd.concat([energy_production_TWh_3, energy_production_TWh_excluded.set_index(energy_production_TWh_excluded.index.astype(str) + '_dup')])
    energy_production_TWh_3 = pd.concat([energy_production_TWh_3, energy_production_TWh_excluded_2.set_index(energy_production_TWh_excluded_2.index.astype(str) + '_dup')])
    # Group by Country, Years, sector (sum)
    energy_production_TWh_4 = group_by_dimensions(df=energy_production_TWh_3, groupby_dimensions=['Country', 'Years', 'sector'], aggregation_method='Sum')
    # carbon-intensity[MtCO2/TWh] = emissions[MtCO2eq] / energy-production[TWh]
    carbon_intensity_MtCO2_per_TWh = mcd(input_table_1=emissions_MtCO2eq, input_table_2=energy_production_TWh_4, operation_selection='x / y', output_name='carbon-intensity[MtCO2/TWh]')
    # Convert Unit MtCO2eq / TWh to gCO2eq / kWh (x1000)
    carbon_intensity_gCO2eq_per_kWh = carbon_intensity_MtCO2_per_TWh.drop(columns='carbon-intensity[MtCO2/TWh]').assign(**{'carbon-intensity[gCO2eq/kWh]': carbon_intensity_MtCO2_per_TWh['carbon-intensity[MtCO2/TWh]'] * 1000.0})
    # carbon-intensity [gCO2/kWh]
    carbon_intensity_gCO2eq_per_kWh = export_variable(input_table=carbon_intensity_gCO2eq_per_kWh, selected_variable='carbon-intensity[gCO2eq/kWh]')
    # KPI
    out_9758_1 = pd.concat([out_9730_1, carbon_intensity_gCO2eq_per_kWh.set_index(carbon_intensity_gCO2eq_per_kWh.index.astype(str) + '_dup')])
    # KPI
    out_9721_1 = pd.concat([out_9758_1, total_fec_share_percent.set_index(total_fec_share_percent.index.astype(str) + '_dup')])

    # Create energy-carrier/sector table mapping

    # Group by Country, Years, sector, energy-carrier (sum)
    energy_production_TWh_3 = group_by_dimensions(df=energy_production_TWh_3, groupby_dimensions=['energy-carrier', 'sector'], aggregation_method='Sum')
    energy_production_TWh_3 = missing_value(df=energy_production_TWh_3, dimension_rx='^.*\\[.*•\\]$', DTS_DT_O=[['org.knime.core.data.def.StringCell', 'org.knime.base.node.preproc.pmml.missingval.handlers.FixedStringValueMissingCellHandlerFactory'], ['org.knime.core.data.def.DoubleCell', 'org.knime.base.node.preproc.pmml.missingval.handlers.DoNothingMissingCellHandlerFactory']], FixedValue='undefined')
    # 1
    energy_production_TWh_3['energy-production[TWh]'] = 1.0
    # energy-production[TWh] -> ratio[-]
    out_9779_1 = energy_production_TWh_3.rename(columns={'energy-production[TWh]': 'ratio[-]'})
    # [gCO2/kWh] = [gCO2/kWh] / ratio[-]
    carbon_intensity_gCO2eq_per_kWh = mcd(input_table_1=carbon_intensity_gCO2eq_per_kWh, input_table_2=out_9779_1, operation_selection='x * y', output_name='carbon-intensity[gCO2eq/kWh]')
    # Group by country, years, energy-carrier (sum)
    carbon_intensity_gCO2eq_per_kWh = group_by_dimensions(df=carbon_intensity_gCO2eq_per_kWh, groupby_dimensions=['Country', 'Years', 'energy-carrier'], aggregation_method='Sum')
    # For climate
    out_9773_1 = pd.concat([carbon_intensity_gCO2eq_per_kWh, emissions_Mt.set_index(emissions_Mt.index.astype(str) + '_dup')])
    # Module = Climate
    out_9773_1 = column_filter(df=out_9773_1, pattern='^.*$')

    # For : Water
    # - Energy-production by way of production (only electricity sector)

    # energy-production [TWh]
    energy_production_TWh_3 = use_variable(input_table=energy_production_TWh, selected_variable='energy-production[TWh]')
    # Keep energy-carrier = electricity
    energy_production_TWh_4 = energy_production_TWh_3.loc[energy_production_TWh_3['energy-carrier'].isin(['electricity'])].copy()
    # Group by  Country, Years, way-of-production (sum)
    energy_production_TWh_4 = group_by_dimensions(df=energy_production_TWh_4, groupby_dimensions=['Country', 'Years', 'way-of-production'], aggregation_method='Sum')
    # Module = Water
    energy_production_TWh_4 = column_filter(df=energy_production_TWh_4, pattern='^.*$')
    # Exclude energy-carrier = liquid-syn-.*
    energy_production_TWh_excluded = energy_production_TWh.loc[energy_production_TWh['energy-carrier'].isin(['liquid-syn', 'liquid-syn-diesel', 'liquid-syn-gasoline', 'liquid-syn-kerosene', 'liquid-syn-marinefueloil'])].copy()
    energy_production_TWh_5 = energy_production_TWh.loc[~energy_production_TWh['energy-carrier'].isin(['liquid-syn', 'liquid-syn-diesel', 'liquid-syn-gasoline', 'liquid-syn-kerosene', 'liquid-syn-marinefueloil'])].copy()
    # force energy-carrier to be equaled to liquid-syn
    energy_production_TWh_excluded['energy-carrier'] = "liquid-syn"
    energy_production_TWh_6 = pd.concat([energy_production_TWh_5, energy_production_TWh_excluded.set_index(energy_production_TWh_excluded.index.astype(str) + '_dup')])
    # Keep energy-carrier = gaseous-syn, liquid-syn, H2, heat (excluding electricity)
    energy_production_TWh_5 = energy_production_TWh_6.loc[energy_production_TWh_6['energy-carrier'].isin(['heat', 'gaseous-syn', 'hydrogen', 'liquid-syn'])].copy()
    energy_production_TWh_excluded = energy_production_TWh_6.loc[~energy_production_TWh_6['energy-carrier'].isin(['heat', 'gaseous-syn', 'hydrogen', 'liquid-syn'])].copy()
    # Keep way-of-production = CHP (electricity produced by CHP)
    energy_production_TWh_excluded = energy_production_TWh_excluded.loc[energy_production_TWh_excluded['way-of-production'].isin(['CHP'])].copy()
    # Group by  country, years, way-of-prod, energy-carrier, primary-energy-carrier
    energy_production_TWh_excluded = group_by_dimensions(df=energy_production_TWh_excluded, groupby_dimensions=['Country', 'Years', 'energy-carrier', 'way-of-production', 'primary-energy-carrier'], aggregation_method='Sum')
    # net-energy-production-by- carrier-primary- carrier-way-of-prod[TWh]
    out_9548_1 = energy_production_TWh_excluded.rename(columns={'energy-production[TWh]': 'net-energy-production-by-carrier-primary-carrier-way-of-prod[TWh]'})
    # graphes 89 et 90 5e (Top and bottom)  + graphe 88 = aggrégation des catégories ici  + graphe 14 (idem)
    out_1_2 = pd.concat([out_9548_1, out_9543_1.set_index(out_9543_1.index.astype(str) + '_dup')])
    out_1_2 = pd.concat([out_1_2, out_9546_1.set_index(out_9546_1.index.astype(str) + '_dup')])
    # Keep energy-carrier = heat (we want more granulo forthis sector)
    energy_production_TWh_6 = energy_production_TWh_5.loc[energy_production_TWh_5['energy-carrier'].isin(['heat'])].copy()
    # Group by  country, years, energyc-arrier way-of-prod, primary-energy-carrier
    energy_production_TWh_6 = group_by_dimensions(df=energy_production_TWh_6, groupby_dimensions=['Country', 'Years', 'energy-carrier', 'way-of-production', 'primary-energy-carrier'], aggregation_method='Sum')
    # gross-energy-production-by- carrier-way-of-prod  graphs 95 and 96 (5h Top and Bottom)
    out_9553_1 = energy_production_TWh_6.rename(columns={'energy-production[TWh]': 'gross-energy-production-by-carrier-way-of-prod[TWh]'})
    # Group by  country, years, sector
    energy_production_TWh_5 = group_by_dimensions(df=energy_production_TWh_5, groupby_dimensions=['Country', 'Years', 'sector'], aggregation_method='Sum')
    # energy-production-by- sector [TWh]  graph 92 (5f Bottom)
    out_9550_1 = energy_production_TWh_5.rename(columns={'energy-production[TWh]': 'energy-production-by-sector[TWh]'})
    out_1_2 = pd.concat([out_9550_1, out_1_2.set_index(out_1_2.index.astype(str) + '_dup')])
    out_1_2 = pd.concat([out_9553_1, out_1_2.set_index(out_1_2.index.astype(str) + '_dup')])
    out_1 = pd.concat([out_1_2, out_1.set_index(out_1.index.astype(str) + '_dup')])
    out_2 = pd.concat([out_1, out_2.set_index(out_2.index.astype(str) + '_dup')])
    # Keep only energy-carrier electricity
    energy_production_TWh_2 = energy_production_TWh_2.loc[energy_production_TWh_2['energy-carrier'].isin(['electricity'])].copy()
    out_6469_1, out_6469_2, out_6469_3 = metanode_6469(port_04=primary_costs_MEUR_2, port_01=energy_demand_TWh_6, port_02=energy_production_TWh_2, port_03=out)
    # fuel-price [MEUR/TWh]
    fuel_price_MEUR_per_TWh_2 = export_variable(input_table=out_6469_2, selected_variable='fuel-price[MEUR/TWh]')
    # fuel-price [MEUR/TWh]
    fuel_price_MEUR_per_TWh_2 = use_variable(input_table=fuel_price_MEUR_per_TWh_2, selected_variable='fuel-price[MEUR/TWh]')
    # Node 5876
    fuel_price_MEUR_per_TWh = pd.concat([fuel_price_MEUR_per_TWh_2, fuel_price_MEUR_per_TWh.set_index(fuel_price_MEUR_per_TWh.index.astype(str) + '_dup')])

    # Fuel Price
    # 
    # = Concatenate fuel price

    # fuel-price [MEUR/TWh]
    fuel_price_MEUR_per_TWh = use_variable(input_table=fuel_price_MEUR_per_TWh, selected_variable='fuel-price[MEUR/TWh]')
    # electricity-COE [EUR/MEUR]
    electricity_COE_EUR_per_MWh = export_variable(input_table=out_6469_3, selected_variable='electricity-COE[EUR/MWh]')

    # For : Costs
    # COE

    # electricity-COE [EUR/MWh]
    electricity_COE_EUR_per_MWh = use_variable(input_table=electricity_COE_EUR_per_MWh, selected_variable='electricity-COE[EUR/MWh]')
    # final-energy-costs [MEUR]
    final_energy_costs_MEUR = export_variable(input_table=out_6469_1, selected_variable='final-energy-costs[MEUR]')
    # final-energy-costs [MEUR]
    final_energy_costs_MEUR_2 = use_variable(input_table=final_energy_costs_MEUR, selected_variable='final-energy-costs[MEUR]')
    # Node 5876
    final_energy_costs_MEUR_2 = pd.concat([final_energy_costs_MEUR_2, final_energy_costs_MEUR_3.set_index(final_energy_costs_MEUR_3.index.astype(str) + '_dup')])

    # Final Energy Costs
    # 
    # = Concatenate final energy costs

    # final-energy-costs [MEUR]
    final_energy_costs_MEUR_2 = use_variable(input_table=final_energy_costs_MEUR_2, selected_variable='final-energy-costs[MEUR]')
    # Group by  country, years, sector
    final_energy_costs_MEUR = group_by_dimensions(df=final_energy_costs_MEUR, groupby_dimensions=['Country', 'Years', 'sector'], aggregation_method='Sum')
    # final-energy-costs to primary-costs
    out_6506_1 = final_energy_costs_MEUR.rename(columns={'final-energy-costs[MEUR]': 'primary-costs[MEUR]'})
    out_6505_1 = pd.concat([primary_costs_MEUR, out_6506_1.set_index(out_6506_1.index.astype(str) + '_dup')])
    # primary-costs [MEUR]
    primary_costs_MEUR = use_variable(input_table=out_6505_1, selected_variable='primary-costs[MEUR]')
    # Keep only sector hydrogen
    primary_costs_MEUR_2 = primary_costs_MEUR.loc[primary_costs_MEUR['sector'].isin(['hydrogen'])].copy()
    # Group by  country, years, sector
    primary_costs_MEUR_2 = group_by_dimensions(df=primary_costs_MEUR_2, groupby_dimensions=['Country', 'Years', 'sector'], aggregation_method='Sum')
    # Keep only energy-carrier hydrogen
    energy_production_TWh = energy_production_TWh.loc[energy_production_TWh['energy-carrier'].isin(['hydrogen'])].copy()
    out_6480_1, out_6480_2 = metanode_6480(port_04=primary_costs_MEUR_2, port_01=energy_demand_TWh_7, port_02=energy_production_TWh, port_03=out_3)
    # fuel-price [MEUR/TWh]
    fuel_price_MEUR_per_TWh_2 = export_variable(input_table=out_6480_2, selected_variable='fuel-price[MEUR/TWh]')
    # fuel-price [MEUR/TWh]
    fuel_price_MEUR_per_TWh_2 = use_variable(input_table=fuel_price_MEUR_per_TWh_2, selected_variable='fuel-price[MEUR/TWh]')
    # Node 5876
    fuel_price_MEUR_per_TWh = pd.concat([fuel_price_MEUR_per_TWh_2, fuel_price_MEUR_per_TWh.set_index(fuel_price_MEUR_per_TWh.index.astype(str) + '_dup')])

    # Fuel Price
    # 
    # = Concatenate fuel price

    # fuel-price [MEUR/TWh]
    fuel_price_MEUR_per_TWh = use_variable(input_table=fuel_price_MEUR_per_TWh, selected_variable='fuel-price[MEUR/TWh]')
    # final-energy-costs [MEUR]
    final_energy_costs_MEUR = export_variable(input_table=out_6480_1, selected_variable='final-energy-costs[MEUR]')
    # final-energy-costs [MEUR]
    final_energy_costs_MEUR_3 = use_variable(input_table=final_energy_costs_MEUR, selected_variable='final-energy-costs[MEUR]')
    # Node 5876
    final_energy_costs_MEUR_2 = pd.concat([final_energy_costs_MEUR_3, final_energy_costs_MEUR_2.set_index(final_energy_costs_MEUR_2.index.astype(str) + '_dup')])

    # Final Energy Costs
    # 
    # = Concatenate final energy costs

    # final-energy-costs [MEUR]
    final_energy_costs_MEUR_2 = use_variable(input_table=final_energy_costs_MEUR_2, selected_variable='final-energy-costs[MEUR]')
    # Group by  country, years, sector
    final_energy_costs_MEUR = group_by_dimensions(df=final_energy_costs_MEUR, groupby_dimensions=['Country', 'Years', 'sector'], aggregation_method='Sum')
    # final-energy-costs to primary-costs
    out_6507_1 = final_energy_costs_MEUR.rename(columns={'final-energy-costs[MEUR]': 'primary-costs[MEUR]'})
    out_6508_1 = pd.concat([out_6507_1, primary_costs_MEUR.set_index(primary_costs_MEUR.index.astype(str) + '_dup')])
    # primary-costs [MEUR]
    primary_costs_MEUR = use_variable(input_table=out_6508_1, selected_variable='primary-costs[MEUR]')
    # Keep only energy-carrier syn fuels
    energy_production_TWh = energy_production_TWh_3.loc[energy_production_TWh_3['energy-carrier'].isin(['gaseous-syn', 'liquid-syn', 'liquid-syn-diesel', 'liquid-syn-gasoline', 'liquid-syn-kerosene', 'liquid-syn-marinefueloil'])].copy()
    out_6501_1, out_6501_2 = metanode_6501(port_04=primary_costs_MEUR, port_01=energy_demand_TWh_3, port_02=energy_production_TWh, port_03=out_4)
    # fuel-price [MEUR/TWh]
    fuel_price_MEUR_per_TWh_2 = export_variable(input_table=out_6501_2, selected_variable='fuel-price[MEUR/TWh]')
    # fuel-price [MEUR/TWh]
    fuel_price_MEUR_per_TWh_2 = use_variable(input_table=fuel_price_MEUR_per_TWh_2, selected_variable='fuel-price[MEUR/TWh]')
    # Node 5876
    fuel_price_MEUR_per_TWh = pd.concat([fuel_price_MEUR_per_TWh_2, fuel_price_MEUR_per_TWh.set_index(fuel_price_MEUR_per_TWh.index.astype(str) + '_dup')])

    # Fuel Price
    # 
    # = Concatenate fuel price

    # fuel-price [MEUR/TWh]
    fuel_price_MEUR_per_TWh = use_variable(input_table=fuel_price_MEUR_per_TWh, selected_variable='fuel-price[MEUR/TWh]')
    # final-energy-costs [MEUR]
    final_energy_costs_MEUR = export_variable(input_table=out_6501_1, selected_variable='final-energy-costs[MEUR]')
    # final-energy-costs [MEUR]
    final_energy_costs_MEUR_3 = use_variable(input_table=final_energy_costs_MEUR, selected_variable='final-energy-costs[MEUR]')
    # Node 5876
    final_energy_costs_MEUR_2 = pd.concat([final_energy_costs_MEUR_3, final_energy_costs_MEUR_2.set_index(final_energy_costs_MEUR_2.index.astype(str) + '_dup')])

    # Final Energy Costs
    # 
    # = Concatenate final energy costs

    # final-energy-costs [MEUR]
    final_energy_costs_MEUR_2 = use_variable(input_table=final_energy_costs_MEUR_2, selected_variable='final-energy-costs[MEUR]')

    # Add Final Energy Costs to Primary-Costs

    # Group by  country, years, sector
    final_energy_costs_MEUR = group_by_dimensions(df=final_energy_costs_MEUR, groupby_dimensions=['Country', 'Years', 'sector'], aggregation_method='Sum')
    # final-energy-costs to primary-costs
    out_6509_1 = final_energy_costs_MEUR.rename(columns={'final-energy-costs[MEUR]': 'primary-costs[MEUR]'})
    out_6510_1 = pd.concat([out_6509_1, primary_costs_MEUR.set_index(primary_costs_MEUR.index.astype(str) + '_dup')])
    # primary-costs [MEUR]
    primary_costs_MEUR = use_variable(input_table=out_6510_1, selected_variable='primary-costs[MEUR]')
    # energy-production [TWh]
    energy_production_TWh = use_variable(input_table=energy_production_TWh_3, selected_variable='energy-production[TWh]')
    # Keep only energy-carrier heat
    energy_production_TWh = energy_production_TWh.loc[energy_production_TWh['energy-carrier'].isin(['heat'])].copy()
    out_6514_1, out_6514_2 = metanode_6514(port_04=primary_costs_MEUR, port_01=energy_demand_TWh_8, port_02=energy_production_TWh, port_03=out_5)
    # fuel-price [MEUR/TWh]
    fuel_price_MEUR_per_TWh_2 = export_variable(input_table=out_6514_2, selected_variable='fuel-price[MEUR/TWh]')
    # fuel-price [MEUR/TWh]
    fuel_price_MEUR_per_TWh_2 = use_variable(input_table=fuel_price_MEUR_per_TWh_2, selected_variable='fuel-price[MEUR/TWh]')
    # Node 5876
    fuel_price_MEUR_per_TWh = pd.concat([fuel_price_MEUR_per_TWh_2, fuel_price_MEUR_per_TWh.set_index(fuel_price_MEUR_per_TWh.index.astype(str) + '_dup')])
    # fuel-price [MEUR/TWh]
    fuel_price_MEUR_per_TWh = export_variable(input_table=fuel_price_MEUR_per_TWh, selected_variable='fuel-price[MEUR/TWh]')

    # For : Costs
    # Fuel price [MEUR /TWh]

    # fuel-price [MEUR/TWh]
    fuel_price_MEUR_per_TWh = use_variable(input_table=fuel_price_MEUR_per_TWh, selected_variable='fuel-price[MEUR/TWh]')
    # Keep elec, efuels, hydrogen and heat
    fuel_price_MEUR_per_TWh = fuel_price_MEUR_per_TWh.loc[fuel_price_MEUR_per_TWh['energy-carrier'].isin(['heat', 'synfuels', 'hydrogen', 'electricity'])].copy()
    per = pd.concat([electricity_COE_EUR_per_MWh, fuel_price_MEUR_per_TWh.set_index(fuel_price_MEUR_per_TWh.index.astype(str) + '_dup')])
    # final-energy-costs [MEUR]
    final_energy_costs_MEUR = export_variable(input_table=out_6514_1, selected_variable='final-energy-costs[MEUR]')
    # final-energy-costs [MEUR]
    final_energy_costs_MEUR = use_variable(input_table=final_energy_costs_MEUR, selected_variable='final-energy-costs[MEUR]')
    # Node 5876
    final_energy_costs_MEUR = pd.concat([final_energy_costs_MEUR, final_energy_costs_MEUR_2.set_index(final_energy_costs_MEUR_2.index.astype(str) + '_dup')])
    # final-energy-costs [MEUR]
    final_energy_costs_MEUR = export_variable(input_table=final_energy_costs_MEUR, selected_variable='final-energy-costs[MEUR]')
    # If missing txt set to "" (efuels => way-of-prod and energy-carrier-cat if non efuels)
    final_energy_costs_MEUR = missing_value(df=final_energy_costs_MEUR, dimension_rx='^.*\\[.*•\\]$', DTS_DT_O=[['org.knime.core.data.def.IntCell', 'org.knime.base.node.preproc.pmml.missingval.handlers.DoNothingMissingCellHandlerFactory'], ['org.knime.core.data.def.StringCell', 'org.knime.base.node.preproc.pmml.missingval.handlers.FixedStringValueMissingCellHandlerFactory'], ['org.knime.core.data.def.DoubleCell', 'org.knime.base.node.preproc.pmml.missingval.handlers.DoNothingMissingCellHandlerFactory']], FixedValue='')

    # For : Costs
    # Final energy costs [MEUR] => Correspond to Fuel price [MEUR / TWh] x energy demand [TWh]

    # final-energy-costs [MEUR]
    final_energy_costs_MEUR = use_variable(input_table=final_energy_costs_MEUR, selected_variable='final-energy-costs[MEUR]')

    def helper_7313(input_table) -> pd.DataFrame:
        # Copy input to output
        output_table = input_table.copy()
        
        mask = (output_table['sector'] == "amm")
        output_table.loc[mask, 'sector'] = "ind"
        return output_table
    # set amm to ind
    out_7313_1 = helper_7313(input_table=final_energy_costs_MEUR)
    # Exclude energy-carrier = heat-.*
    out_7313_1_excluded = out_7313_1.loc[out_7313_1['energy-carrier'].isin(['heat', 'heat-geothermal', 'heat-solar'])].copy()
    out_7313_1 = out_7313_1.loc[~out_7313_1['energy-carrier'].isin(['heat', 'heat-geothermal', 'heat-solar'])].copy()
    # force energy-carrier to be equaled to heat
    out_7313_1_excluded['energy-carrier'] = "heat"
    # Exclude energy-carrier = liquid-bio-.*
    out_7313_1_excluded_2 = out_7313_1.loc[out_7313_1['energy-carrier'].isin(['liquid-bio', 'liquid-bio-diesel', 'liquid-bio-gasoline', 'liquid-bio-kerosene', 'liquid-bio-marinefueloil'])].copy()
    out_7313_1 = out_7313_1.loc[~out_7313_1['energy-carrier'].isin(['liquid-bio', 'liquid-bio-diesel', 'liquid-bio-gasoline', 'liquid-bio-kerosene', 'liquid-bio-marinefueloil'])].copy()
    # force energy-carrier to be equaled to liquid-bio
    out_7313_1_excluded_2['energy-carrier'] = "liquid-bio"
    out_7313_1_excluded = pd.concat([out_7313_1_excluded_2, out_7313_1_excluded.set_index(out_7313_1_excluded.index.astype(str) + '_dup')])
    # Exclude energy-carrier = gaseous-ff-.*
    out_7313_1_excluded_2 = out_7313_1.loc[out_7313_1['energy-carrier'].isin(['gaseous-ff', 'gaseous-ff-natural'])].copy()
    out_7313_1 = out_7313_1.loc[~out_7313_1['energy-carrier'].isin(['gaseous-ff', 'gaseous-ff-natural'])].copy()
    # force energy-carrier to be equaled to gaseous-ff
    out_7313_1_excluded_2['energy-carrier'] = "gaseous-ff"
    out_7313_1_excluded = pd.concat([out_7313_1_excluded_2, out_7313_1_excluded.set_index(out_7313_1_excluded.index.astype(str) + '_dup')])
    # Exclude energy-carrier = all liquid-ff excluding diesel and gasoline
    out_7313_1_excluded_2 = out_7313_1.loc[out_7313_1['energy-carrier'].isin(['liquid-ff-crudeoil', 'liquid-ff-marinefueloil', 'liquid-ff-oil', 'liquid-ff', 'liquid-ff-kerosene'])].copy()
    out_7313_1 = out_7313_1.loc[~out_7313_1['energy-carrier'].isin(['liquid-ff-crudeoil', 'liquid-ff-marinefueloil', 'liquid-ff-oil', 'liquid-ff', 'liquid-ff-kerosene'])].copy()
    # force energy-carrier to be equaled to liquid-ff-oil
    out_7313_1_excluded_2['energy-carrier'] = "liquid-ff-oil"
    out_7313_1_excluded = pd.concat([out_7313_1_excluded_2, out_7313_1_excluded.set_index(out_7313_1_excluded.index.astype(str) + '_dup')])
    out_7313_1 = pd.concat([out_7313_1, out_7313_1_excluded.set_index(out_7313_1_excluded.index.astype(str) + '_dup')])
    # Group by energy-carrier, sector (sum) graphs 2g, 3j, 4t 5q, 6g
    out_7313_1 = group_by_dimensions(df=out_7313_1, groupby_dimensions=['Country', 'Years', 'energy-carrier', 'sector'], aggregation_method='Sum')
    # fuel-costs-by- energy-carrier-sector[MEUR]
    out_9581_1 = out_7313_1.rename(columns={'final-energy-costs[MEUR]': 'fuel-costs-by-energy-carrier-sector[MEUR]'})
    # Group by energy-carrier (sum) graph 1l Bottom
    out_7313_1_2 = group_by_dimensions(df=out_7313_1, groupby_dimensions=['Country', 'Years', 'energy-carrier'], aggregation_method='Sum')
    # fuel-costs-by- energy-carrier[MEUR]
    out_9583_1 = out_7313_1_2.rename(columns={'final-energy-costs[MEUR]': 'fuel-costs-by-energy-carrier[MEUR]'})
    # Group by sector (sum) graphs 1l Top / 5n Bottom
    out_7313_1 = group_by_dimensions(df=out_7313_1, groupby_dimensions=['Country', 'Years', 'sector'], aggregation_method='Sum')
    # fuel-costs-by- sector[MEUR]
    out_9582_1 = out_7313_1.rename(columns={'final-energy-costs[MEUR]': 'fuel-costs-by-sector[MEUR]'})
    out_1 = pd.concat([out_9582_1, out_9583_1.set_index(out_9583_1.index.astype(str) + '_dup')])
    out_1 = pd.concat([out_9581_1, out_1.set_index(out_1.index.astype(str) + '_dup')])
    out_9427_1 = pd.concat([out_1, per.set_index(per.index.astype(str) + '_dup')])
    out = pd.concat([out_2, out_9427_1.set_index(out_9427_1.index.astype(str) + '_dup')])
    # Add KPI's
    out = pd.concat([out_9721_1, out.set_index(out.index.astype(str) + '_dup')])
    out_9382_1 = add_trigram(module_name=module_name, df=out)
    # Module = Pathway Explorer + Costs  + KPIs
    out_9382_1 = column_filter(df=out_9382_1, pattern='^.*$')
    # Calibration (enery-demand)
    cal_rate_energy_demand_TWh_2 = export_variable(input_table=out_7248_2, selected_variable='cal_rate_energy-demand[TWh]')
    out_9590_1 = pd.concat([out_6428_6, cal_rate_energy_demand_TWh_2.set_index(cal_rate_energy_demand_TWh_2.index.astype(str) + '_dup')])
    out_9223_1 = pd.concat([out_9590_1, cal_rate_energy_demand_TWh.set_index(cal_rate_energy_demand_TWh.index.astype(str) + '_dup')])
    out_1 = pd.concat([out_9223_1, out_9763_1.set_index(out_9763_1.index.astype(str) + '_dup')])
    out_1 = pd.concat([out_1, out_7429_1.set_index(out_7429_1.index.astype(str) + '_dup')])
    # Module = CALIBRATION
    out_1 = column_filter(df=out_1, pattern='^.*$')

    return out_9382_1, out_1, out_9773_1, energy_demand_TWh_4, energy_demand_TWh_2, out_7054_1, energy_imported_TWh_2, energy_production_TWh_4


