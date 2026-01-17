"""
German Grid-Scale Battery Storage Dashboard
Tracks operational (in Betrieb) and planned (in Planung) battery projects from MaStR data.
"""

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from dash import Dash, html, dcc, callback, Output, Input, dash_table
import dash_bootstrap_components as dbc
from datetime import datetime
import os
import io

# =============================================================================
# OUTLIER FILTERING RULES
# =============================================================================
# These rules are applied automatically when data is loaded to ensure data quality.
# Adjust thresholds as needed for your use case.

OUTLIER_RULES = {
    # Minimum power capacity (MW) - filters out small installations
    'min_power_mw': 1.0,
    # Maximum power capacity (MW) - filters out likely data entry errors
    'max_power_mw': 1000.0,
    # Minimum duration (hours) - filters out implausible short durations
    'min_duration_hours': 0.25,  # 15 minutes
    # Maximum duration (hours) - filters out likely data entry errors
    'max_duration_hours': 10.0,
    # Minimum storage capacity relative to power (MWh must be > 0)
    'min_capacity_mwh': 0.1,
    # Earliest valid commissioning year
    'min_year': 2010,
    # Latest valid planned year
    'max_year': 2035,
}


def apply_outlier_filters(df, rules=OUTLIER_RULES):
    """
    Apply outlier filtering rules to the dataframe.
    Returns filtered dataframe and count of removed records.
    """
    initial_count = len(df)
    removed_reasons = []

    # Filter by power capacity
    if 'Leistung_MW' in df.columns:
        mask = (df['Leistung_MW'] >= rules['min_power_mw']) & (df['Leistung_MW'] <= rules['max_power_mw'])
        removed = (~mask).sum()
        if removed > 0:
            removed_reasons.append(f"Power out of range ({rules['min_power_mw']}-{rules['max_power_mw']} MW): {removed}")
        df = df[mask]

    # Filter by storage capacity
    if 'Kapazitaet_MWh' in df.columns:
        mask = df['Kapazitaet_MWh'] >= rules['min_capacity_mwh']
        removed = (~mask).sum()
        if removed > 0:
            removed_reasons.append(f"Capacity below {rules['min_capacity_mwh']} MWh: {removed}")
        df = df[mask]

    # Filter by duration
    if 'Dauer_Stunden' in df.columns:
        mask = (df['Dauer_Stunden'] >= rules['min_duration_hours']) & (df['Dauer_Stunden'] <= rules['max_duration_hours'])
        removed = (~mask).sum()
        if removed > 0:
            removed_reasons.append(f"Duration out of range ({rules['min_duration_hours']}-{rules['max_duration_hours']}h): {removed}")
        df = df[mask]

    # Filter by year
    if 'Jahr' in df.columns:
        mask = (df['Jahr'] >= rules['min_year']) & (df['Jahr'] <= rules['max_year']) | df['Jahr'].isna()
        removed = (~mask).sum()
        if removed > 0:
            removed_reasons.append(f"Year out of range ({rules['min_year']}-{rules['max_year']}): {removed}")
        df = df[mask]

    final_count = len(df)
    total_removed = initial_count - final_count

    if total_removed > 0:
        print(f"Outlier filtering removed {total_removed} records:")
        for reason in removed_reasons:
            print(f"  - {reason}")

    return df


# =============================================================================
# OWNER CONSOLIDATION
# =============================================================================
# Maps multiple entity names to their parent company for cleaner analysis.
# Original owner names are preserved in 'Betreiber_Original' column.

OWNER_GROUPS = {
    'ECO STOR': [
        'ECO POWER FOUR GmbH',
        'ECO POWER SIX GmbH',
        'ECO POWER THREE GmbH',
        'ECO POWER ONE GmbH',
        'ECO POWER TWO GmbH',
    ],
    'East Energy': [
        'East Energy PV Wiendorf GmbH & Co.KG',
        'East Energy PV Rostock GmbH & Co.KG',
        'East Energy PV Wentow GmbH & Co.KG',
        'East Energy PV Detershagen GmbH & Co. KG',
        'East Energy PV Klevenow GmbH & Co. KG',
        'East Energy PV Werle GmbH & Co. KG',
    ],
    'RWE': [
        'RWE Generation SE',
        'RWE Supply & Trading GmbH',
        'RWE Battery Solutions GmbH',
        'RWE Wind Onshore & PV Deutschland GmbH',
        'RWE Neuland Erneuerbare Energien GmbH & Co. KG',
    ],
    'Kyon Energy': [
        'Kyon-51 Battery Storage GmbH',
        'Kyon Storage 232 GmbH',
        'Kyon-42 Battery Storage GmbH',
        'Kyon-100 Battery Storage GmbH',
        'Kyon-41 Battery Storage GmbH',
        'Kyon-50 Battery Storage GmbH',
        'Kyon-29 Battery Storage GmbH',
    ],
    'Anumar': [
        'Anumar Solarpark Inchenhofen GmbH & Co. KG',
        'Anumar Solarpark Martinsheim GmbH & Co. KG',
        'Anumar Solarpark Vohburg-Oberdolling GmbH & Co. KG',
        'Anumar Solarpark Schrobenhausen-Öd GmbH & Co. KG',
        'Anumar Solarpark Edelshausen II GmbH & Co. KG',
        'Anumar Solarpark Dielkirchen GmbH & Co. KG',
        'Anumar Solarpark Dettelbach GmbH & Co. KG',
        'Anumar Solarpark Altdorf II GmbH & Co. KG',
        'Anumar Solarpark Feuchtwangen GmbH & Co. KG',
        'Anumar Solarpark Seubersdorf GmbH & Co. KG',
        'Anumar Solarpark Weil III GmbH & Co. KG',
        'Anumar Solarpark Dasing GmbH & Co. KG',
        'Anumar Solarpark Barbing II GmbH & Co. KG',
        'Anumar Solarpark Großmehring II GmbH & Co. KG',
        'Anumar Solarpark Velburg VI GmbH & Co. KG',
        'Anumar Solarpark Prosselsheim GmbH & Co. KG',
        'Anumar Solarpark Colbitz II GmbH & Co. KG',
        'Anumar Solarpark Oberschleißheim GmbH & Co. KG',
        'Anumar Solarpark Hilgertshausen-Tandern GmbH & Co. KG',
        'Anumar Solarpark Sparneck GmbH & Co. KG',
        'Anumar Solarpark Seetz GmbH & Co. KG',
        'Anumar Solarpark Schweitenkirchen GmbH & Co. KG',
        'Anumar Solarpark Winterbach RLP GmbH & Co. KG',
        'Anumar Solarpark Wolnzach-Gebrontshausen GmbH & Co. KG',
        'Anumar Solarpark Mögglingen GmbH & Co. KG',
        'Anumar Solarpark Neustadt a. d. Donau GmbH & Co. KG',
        'Anumar Solarpark Siglohe GmbH & Co. KG',
        'Anumar Solarpark Wernberg-Köblitz GmbH & Co. KG',
        'Anumar Solarpark Kühbach V GmbH & Co. KG',
        'Anumar Solarpark Gachenbach-Peutenhausen GmbH & Co. KG',
        'Anumar Solarpark Beilngries-Kirchbuch GmbH & Co. KG',
        'Anumar Solarpark Karlskron-Pobenhausen GmbH & Co. KG',
        'Anumar Solarpark Wölfersheim GmbH & Co. KG',
        'Anumar Solarpark Rohrbach XI GmbH & Co. KG',
        'Anumar Solarpark Wolnzach VI GmbH & Co. KG',
        'Anumar Solarpark Mallersdorf-Pfaffenberg GmbH & Co. KG',
        'Anumar Solarpark Freystadt-Höfen GmbH & Co. KG',
        'Anumar Solarpark Ingolstadt VI GmbH & Co. KG',
        'Anumar Solarpark Petersberg GmbH & Co. KG',
        'Anumar Solarpark Menning GmbH & Co. KG',
        'Anumar Solarpark Wolnzach III GmbH & Co. KG',
        'Anumar Solarpark Bad Camberg GmbH & Co. KG',
        'Anumar Solarpark Jetzendorf GmbH & Co. KG',
        'Anumar Solarpark Burkau GmbH & Co. KG',
        'Anumar Solarpark Meeder IV GmbH & Co. KG',
        'Anumar Solarpark Kirchhaslach GmbH & Co. KG',
        'Anumar Solarpark Mosbach GmbH & Co. KG',
        'Anumar Solarpark Seckach III GmbH & Co. KG',
        'Anumar Solarpark Beilngries-Arnbuch GmbH & Co. KG',
        'Anumar Solarpark Parsberg V GmbH & Co. KG',
        'Anumar Solarpark Denkendorf GmbH & Co. KG',
        'Anumar Solarpark Laaber GmbH & Co. KG',
        'Anumar Solarpark Stamsried GmbH & Co. KG',
        'Anumar Solarpark Winterbach II GmbH & Co. KG',
        'Anumar Solarpark Weinheim GmbH & Co. KG',
        'Anumar Solarpark Schernfeld GmbH & Co. KG',
        'Anumar Solarpark Trugenhofen-Rohrbach-Rennertshofen GmbH & Co. KG',
        'Anumar Solarpark Schnelldorf GmbH & Co. KG',
        'Anumar Solarpark Beratzhausen GmbH & Co. KG',
        'Anumar Solarpark Werne GmbH & Co. KG',
        'Anumar Solarpark Großrinderfeld GmbH & Co. KG',
        'Anumar Solarpark Vierkirchen GmbH & Co. KG',
        'Anumar Solarpark Ingolstadt-Etting GmbH & Co. KG',
        'Anumar Solarpark Ingolstadt VII GmbH & Co. KG',
        'Anumar Solarpark Steinach II GmbH & Co. KG',
        'Anumar Solarpark Allershausen GmbH & Co. KG',
        'Anumar Solarpark Uettingen GmbH & Co. KG',
        'Anumar Solarpark Reichertshofen-Winden GmbH & Co. KG',
        'Anumar Solarpark Eching GmbH & Co. KG',
        'Anumar Solarpark Wolfratshausen GmbH & Co. KG',
        'Anumar Solarpark Seubersdorf PR GmbH & Co. KG',
        'Anumar Solarpark Altmannstein GmbH & Co. KG',
        'Anumar Solarpark Adelsheim GmbH & Co. KG',
    ],
    'Ju:niz': [
        'SMAREG4 GmbH & Co. KG',
        'SMAREG 6 GmbH & Co. KG',
        'SMAREG8 GmbH & Co. KG',
        'SMAREG9 GmbH & Co. KG',
        'SMAREG12 GmbH & Co.KG',
        'SMAREG3 GmbH & Co. KG',
        'SMAREG5 GmbH & Co. KG',
        'SMAREG7 GmbH & Co. KG',
        'SMAREG 1',
        'SMAREG14 GmbH & Co.KG',
    ],
    'Obton': [
        'Obton Alfeld GmbH & Co. KG',
        'Obton Storage GmbH & Co. KG',
        'Obton Karstädt GmbH & Co. KG',
        'Obton Tangermünde GmbH & Co. KG',
    ],
    'EnBW': [
        'EnBW Energie Baden-Württemberg AG',
        'EnBW Solar GmbH',
        'EnBW Windkraftprojekte GmbH',
        'EnBW Solarpark Gottesgabe GmbH',
        'EnBW SunInvest GmbH & Co. KG',
    ],
    'ENERPARC': [
        'ENERPARC Solar Invest 243 EU 1.7 GmbH',
        'ENERPARC Solar Invest 222 GmbH',
        'ENERPARC Solar Invest 211 TU 2 GmbH',
        'ENERPARC Solar Invest 182 GmbH',
        'ENERPARC Solar Invest 230 TU 32 GmbH & Co. KG',
        'ENERPARC Solar Invest 230 TU 28 GmbH & Co. KG',
        'ENERPARC Solar Invest 273 GmbH',
        'ENERPARC Solar Invest 230 TU 7 GmbH & Co. KG',
        'ENERPARC Solar Invest 230 TU 33 GmbH & Co. KG',
        'ENERPARC Solar Invest 230 TU 31 GmbH & Co. KG',
        'ENERPARC Solar Invest 274 GmbH',
        'ENERPARC Solar Invest 211 TU 3 GmbH',
        'ENERPARC Solar Invest 230 TU 38 GmbH & Co. KG',
        'ENERPARC Solar Invest 216 GmbH',
        'ENERPARC Solar Invest 230 TU 36 GmbH & Co. KG',
        'ENERPARC Solar Invest 230 TU 25 GmbH & Co. KG',
        'ENERPARC Solar Invest 243 EU 1.9 GmbH & Co. KG',
        'ENERPARC Solar Invest 230 TU 35 GmbH & Co. KG',
        'ENERPARC Solar Invest 230 TU 1 GmbH & Co. KG',
        'ENERPARC Solar Invest 192 GmbH',
        'ENERPARC Solar Invest 169 GmbH',
        'ENERPARC Solar Invest 202 GmbH',
        'ENERPARC Solar Invest 257 GmbH',
        'ENERPARC Solar Invest 230 TU 59 GmbH & Co. KG',
        'ENERPARC Solar Invest 230 TU 24 GmbH & Co. KG',
        'ENERPARC Solar Invest 272 GmbH',
        'ENERPARC Solar Invest 230 TU 37 GmbH & Co. KG',
        'ENERPARC Solar Invest 230 TU 26 GmbH & Co. KG',
        'ENERPARC Solar Invest 123 GmbH',
        'ENERPARC Solar Invest 253 GmbH',
    ],
    'Iqony': [
        'Iqony Battery System',
        'Iqony 2. Battery System GmbH',
    ],
    'trlyr': [
        'trlyr 8 GmbH & Co. KG',
        'trlyr 9 GmbH & Co. KG',
        'trlyr 4 GmbH & Co. KG',
        'trlyr 5 GmbH & Co. KG',
        'trlyr 3 GmbH & Co. KG',
        'trlyr2 GmbH',
        'trlyr1 GmbH',
    ],
    'BESS': [
        'BESS Neumünster GmbH & Co. KG',
        'BESS Oberröblingen GmbH & Co. KG',
        'BESS Quedlinburg GmbH & Co. KG',
        'BESS Brietlingen',
        'BESS Hettstedt Fünfte Energie GmbH',
        'BESS Königsee GmbH',
        'BESS Kolitzheim 1 GmbH & Co. KG',
    ],
    'TEP': [
        'TEP Juno GmbH & Co. KG',
        'TEP Melpomene GmbH & Co. KG',
        'TEP Thalia GmbH & Co. KG',
        'TEP Eunomia GmbH & Co. KG',
        'TEP Irene GmbH & Co. KG',
        'TEP Flora',
        'TEP Vesta Gmbh & Co. KG',
        'TEP Lipperhey GmbH & Co. KG',
        'TEP Fortuna GmbH & Co. KG',
        'TEP Lutetia GmbH & Co. KG',
    ],
    'BES': [
        'BES Bennewitz GmbH & Co. KG',
        'BES Groitzsch GmbH& Co. KG',
        'BES Langenreichenbach GmbH & Co. KG',
        'BES Dresden Süd GmbH & Co. KG',
    ],
    'Sunnic Lighthouse': [
        'Sunnic Lighthouse Solar Invest 11 TU 2 GmbH',
        'Sunnic Lighthouse Solar Invest 17 GmbH',
        'Sunnic Lighthouse Solar Invest 20 GmbH',
        'Sunnic Lighthouse Solar Invest 18 TU 11 GmbH & Co. KG',
        'Sunnic Lighthouse Solar Invest 18 TU 6 GmbH & Co. KG',
        'Sunnic Lighthouse Solar Invest 18 TU 4 GmbH & Co. KG',
        'Sunnic Lighthouse Solar Invest 18 TU 13 GmbH & Co. KG',
        'Sunnic Lighthouse Solar Invest 18 TU 3 GmbH & Co. KG',
    ],
    'Bürgerwindpark': [
        'Bürgerwindpark Reußenköge GmbH & Co. KG',
        'Fünfundzwanzigste Bürgerwindpark GmbH & Co. KG',
        'Dreißigste Bürgerwindpark GmbH & Co. KG',
    ],
    'UGE': [
        'UGE Lübbenau GmbH & Co. KG Umweltgereche Energie',
        'UGE Laubst GmbH & Co. KG Umweltgereche Energie',
        'UGE Neuwiese GmbH & Co. KG Umweltgereche Energie',
    ],
    'MJ Solarbetriebsgesellschaft': [
        'MJ 6. Solarbetriebsgesellschaft',
        'MJ 13. Solarbetriebsgesellschaft',
        'MJ 12. Solarbetriebsgesellschaft',
    ],
    'Vattenfall': [
        'Vattenfall Solar Neubrandenburg GmbH & Co. KG',
        'Vattenfall Solar InnoA GmbH',
    ],
    'ABO Energy': [
        'ABO Energy Solarpark 8 GmbH & Co. KG',
        'ABO Wind Solarpark 2 GmbH & Co. KG',
        'ABO Wind Solarpark 11 GmbH & Co. KG',
        'ABO Energy Solarpark 11 GmbH & Co. KG',
        'ABO Energy Solarpark 3 GmbH & Co. KG',
        'ABO Energy Solarpark 7 GmbH & Co. KG',
        'ABO Wind Biogas Barby GmbH & Co. KG',
    ],
    'ESG': [
        'ESG Kraftwerke I GmbH',
        'ESG Speicherwerke I GmbH & Co. KG',
    ],
    'Volkswagen': [
        'Volkswagen Group Charging',
        'VW Kraftwerk GmbH',
    ],
    'Energiegesellschaft Balder': [
        'Energiegesellschaft Balder MV III mbH & Co. KG',
        'Energiegesellschaft Balder MV II GmbH & Co. KG',
    ],
    'Sonnenwerk': [
        'Sonnenwerk Issigau Reitzenstein GmbH & Co. KG',
        'Sonnenwerk Kirchenlamitz GmbH & Co. KG',
        'Sonnenwerk Zell im Fichtelgebirge GmbH & Co. KG',
    ],
    'LHI': [
        'LHI SolarWind PV Gammertingen 2695 GmbH & Co. KG',
        'LHI SolarWind PV Letschin 2683 GmbH & Co. KG',
        'LHI EE Invest PV Lübars 2699 GmbH & Co. KG',
        'LHI GII3 PV Badem Gindorf 2697 GmbH & Co.KG',
    ],
    'Sonnenkraft': [
        'Sonnenkraft Marnitz 3 GmbH',
        'Sonnenkraft Emkendorf GmbH & Co. KG',
    ],
    'VPS BATTERY PARK': [
        'VPS BATTERY PARK 1 GmbH & Co. KG',
        'VPS BATTERY PARK 2 GmbH & Co. KG',
    ],
    'RP Deutschland': [
        'RP Deutschland 1 UG',
        'RP Deutschland 11 UG',
    ],
    'ECO BATTERY PARK': [
        'ECO BATTERY PARK 4 GmbH & Co. KG',
        'ECO BATTERY PARK 3 GmbH & Co. KG',
        'ECO BATTERY PARK 5 GmbH & Co. KG',
    ],
    'Mando Multiwerke': [
        'Mando Multiwerke Nr. 6 GmbH & Co. KG',
        'Mando Multiwerke Nr.8 GmbH & Co.KG',
        'Mando Multiwerke Nr. 1 GmbH & Co. KG',
    ],
    'EnValue': [
        'EnValue MSE SP Großmehring I GmbH & Co. KG',
        'EnValue Solarpark 29 GmbH & Co. KG',
        'EnValue Solarpark 43 GmbH & Co. KG',
        'EnValue Solarpark 36 GmbH & Co. KG',
        'EnValue Solarpark 35 GmbH & Co. KG',
    ],
    'GPJ Energiepark': [
        'GPJ Energiepark Zeller-Land 3 GmbH & Co. KG',
        'GPJ Energiepark Zeller-Land 2 GmbH & Co. KG',
    ],
    'PEE': [
        'PEE2 GmbH & Co. KG',
        'PEE3 GmbH & Co. KG',
        'PEE4 GmbH & Co. KG',
    ],
    'Lintas Energiepark': [
        '27. Lintas Energiepark GmbH & Co. KG',
        '29. Lintas Energiepark GmbH & Co. KG',
    ],
    'Iniku': [
        'EnspireME',
    ],
}

# Build reverse lookup: entity name -> parent group
_OWNER_LOOKUP = {}
for parent, entities in OWNER_GROUPS.items():
    for entity in entities:
        _OWNER_LOOKUP[entity] = parent


# =============================================================================
# NETZBETREIBER (NETWORK OPERATOR) CONSOLIDATION
# =============================================================================
# Cleans up Netzbetreiber names by removing the MaStR ID suffix and grouping variants.

NETZBETREIBER_GROUPS = {
    # Transmission System Operators (TSOs) - Übertragungsnetzbetreiber
    '50Hertz': ['50Hertz Transmission GmbH'],
    'Amprion': ['Amprion GmbH'],
    'TenneT': ['TenneT TSO GmbH'],
    'TransnetBW': ['TransnetBW GmbH'],

    # Major Distribution System Operators (DSOs)
    'Westnetz': ['Westnetz GmbH'],
    'E.DIS Netz': ['E.DIS Netz GmbH'],
    'Bayernwerk Netz': ['Bayernwerk Netz GmbH', 'Bayernwerk Netz GmbH; Elektrizitätswerk Goldbach-Hösbach GmbH & Co. KG'],
    'Avacon Netz': ['Avacon Netz GmbH'],
    'Mitteldeutsche Netzgesellschaft': ['Mitteldeutsche Netzgesellschaft Strom mbH'],
    'LEW Verteilnetz': ['LEW Verteilnetz GmbH'],
    'Schleswig-Holstein Netz': ['Schleswig-Holstein Netz GmbH'],
    'TEN Thüringer Energienetze': ['TEN Thüringer Energienetze GmbH & Co. KG'],
    'WEMAG Netz': ['WEMAG Netz GmbH'],
    'N-ERGIE Netz': ['N-ERGIE Netz GmbH'],
    'EWE NETZ': ['EWE NETZ GmbH'],
    'Netze BW': ['Netze BW GmbH'],
}


def clean_netzbetreiber_name(name):
    """
    Clean Netzbetreiber name by removing the MaStR ID suffix (SNBxxxxxxxxxx).
    """
    if pd.isna(name):
        return name
    # Remove the (SNBxxxxxxxxxx) suffix
    import re
    cleaned = re.sub(r'\s*\(SNB\d+\)\s*$', '', str(name))
    return cleaned.strip()


def consolidate_netzbetreiber_names(df):
    """
    Consolidate Netzbetreiber names to cleaner parent groups.
    First cleans the MaStR ID suffix, then applies grouping.
    """
    if 'Netzbetreiber' not in df.columns:
        return df

    # First, clean the names by removing MaStR ID suffix
    df['Netzbetreiber_Original'] = df['Netzbetreiber']
    df['Netzbetreiber'] = df['Netzbetreiber_Original'].apply(clean_netzbetreiber_name)

    # Build reverse lookup for Netzbetreiber
    nb_lookup = {}
    for parent, entities in NETZBETREIBER_GROUPS.items():
        for entity in entities:
            nb_lookup[entity] = parent

    # Apply grouping
    df['Netzbetreiber'] = df['Netzbetreiber'].apply(lambda x: nb_lookup.get(x, x))

    # Count consolidations
    consolidated_count = (df['Netzbetreiber'] != df['Netzbetreiber_Original'].apply(clean_netzbetreiber_name)).sum()
    if consolidated_count > 0:
        print(f"Netzbetreiber consolidation: {consolidated_count} projects mapped to parent groups")

    return df


def consolidate_owner_names(df):
    """
    Consolidate owner names to parent company groups.
    Preserves original names in 'Betreiber_Original' column.
    """
    if 'Betreiber' not in df.columns:
        return df

    # Preserve original owner name
    df['Betreiber_Original'] = df['Betreiber']

    # Map to consolidated parent group (or keep original if no match)
    df['Betreiber'] = df['Betreiber_Original'].apply(
        lambda x: _OWNER_LOOKUP.get(x, x)
    )

    # Count how many were consolidated
    consolidated_count = (df['Betreiber'] != df['Betreiber_Original']).sum()
    unique_before = df['Betreiber_Original'].nunique()
    unique_after = df['Betreiber'].nunique()

    if consolidated_count > 0:
        print(f"Owner consolidation: {consolidated_count} projects mapped to parent groups")
        print(f"  Unique owners: {unique_before} -> {unique_after}")

    return df


# =============================================================================
# DATA PROCESSING
# =============================================================================

def load_and_process_data(filepath):
    """Load CSV data and process it for the dashboard."""

    # Read CSV with German formatting (semicolon separator, comma decimal)
    df = pd.read_csv(filepath, sep=';', decimal=',', encoding='utf-8')

    # Standardize column names (remove extra spaces, handle variations)
    df.columns = df.columns.str.strip()

    # Map common column name variations to standardized names
    column_mapping = {
        'Nettonennleistung der Einheit': 'Nettonennleistung_kW',
        'Netto-Nennleistung der Einheit': 'Nettonennleistung_kW',
        'Nutzbare Speicherkapazität in kWh': 'Speicherkapazitaet_kWh',
        'Nutzbare Speicherkapazität der Einheit': 'Speicherkapazitaet_kWh',
        'Nutzbare Speicherkapazität': 'Speicherkapazitaet_kWh',
        'Betriebs-Status': 'Betriebsstatus',
        'Betriebsstatus': 'Betriebsstatus',
        'Inbetriebnahmedatum der Einheit': 'Inbetriebnahmedatum',
        'Inbetriebnahmedatum': 'Inbetriebnahmedatum',
        'Datum der geplanten Inbetriebnahme': 'Geplantes_Inbetriebnahmedatum',
        'Geplantes Inbetriebnahmedatum': 'Geplantes_Inbetriebnahmedatum',
        'Name des Anlagenbetreibers (nur Org.)': 'Betreiber',
        'Anlagenbetreiber': 'Betreiber',
        'Bundesland': 'Bundesland',
        'Standort: Bundesland': 'Bundesland',
        'MaStR-Nr. der Einheit': 'MaStR_Nr',
        'Anzeige-Name der Einheit': 'Anlagename',
        'Anzeigename der Einheit': 'Anlagename',
        'Name des Anschluss-Netzbetreibers': 'Netzbetreiber',
    }

    for old_name, new_name in column_mapping.items():
        if old_name in df.columns:
            df = df.rename(columns={old_name: new_name})

    # Convert power from kW to MW
    if 'Nettonennleistung_kW' in df.columns:
        df['Leistung_MW'] = pd.to_numeric(df['Nettonennleistung_kW'], errors='coerce') / 1000

    # Convert storage capacity from kWh to MWh
    if 'Speicherkapazitaet_kWh' in df.columns:
        df['Kapazitaet_MWh'] = pd.to_numeric(df['Speicherkapazitaet_kWh'], errors='coerce') / 1000

    # Calculate duration in hours (MWh / MW)
    if 'Leistung_MW' in df.columns and 'Kapazitaet_MWh' in df.columns:
        df['Dauer_Stunden'] = df['Kapazitaet_MWh'] / df['Leistung_MW']
        df['Dauer_Stunden'] = df['Dauer_Stunden'].replace([float('inf'), -float('inf')], None)

    # Parse dates
    for date_col in ['Inbetriebnahmedatum', 'Geplantes_Inbetriebnahmedatum']:
        if date_col in df.columns:
            df[date_col] = pd.to_datetime(df[date_col], dayfirst=True, errors='coerce')

    # Create unified date column for timeline analysis
    df['Datum'] = df.get('Inbetriebnahmedatum', pd.NaT)
    if 'Geplantes_Inbetriebnahmedatum' in df.columns:
        df['Datum'] = df['Datum'].fillna(df['Geplantes_Inbetriebnahmedatum'])

    # Extract year for trend analysis
    df['Jahr'] = df['Datum'].dt.year

    # Standardize status values
    if 'Betriebsstatus' in df.columns:
        df['Status'] = df['Betriebsstatus'].apply(lambda x:
            'In Betrieb' if 'betrieb' in str(x).lower() and 'planung' not in str(x).lower()
            else ('In Planung' if 'planung' in str(x).lower() else str(x)))

    # Apply outlier filters
    df = apply_outlier_filters(df)

    # Consolidate owner names to parent groups
    df = consolidate_owner_names(df)

    # Consolidate Netzbetreiber names
    df = consolidate_netzbetreiber_names(df)

    return df


def get_data():
    """Load data from CSV file. Searches multiple locations for compatibility."""
    # List of possible CSV locations (in order of priority)
    possible_paths = [
        # Local development path
        '/Users/cosimasagmeister/Documents/Buildout Dashboard/202601_MaStR data.csv',
        # Same directory as app (for deployment)
        os.path.join(os.path.dirname(__file__), '202601_MaStR data.csv'),
        # Data subdirectory
        os.path.join(os.path.dirname(__file__), 'data', '202601_MaStR data.csv'),
    ]

    for csv_path in possible_paths:
        if os.path.exists(csv_path):
            try:
                test_df = pd.read_csv(csv_path, sep=';', nrows=5)
                if len(test_df) > 0:
                    print(f"Loading data from: {csv_path}")
                    return load_and_process_data(csv_path)
            except Exception as e:
                print(f"Error reading {csv_path}: {e}")
                continue

    print("No valid CSV found.")
    return pd.DataFrame()


# =============================================================================
# CHART STYLING - Compact and clean
# =============================================================================

CHART_CONFIG = {'displayModeBar': False}

CHART_LAYOUT = {
    'margin': dict(l=40, r=20, t=40, b=40),
    'font': dict(size=11),
    'title_font_size': 13,
    'legend': dict(orientation='h', yanchor='bottom', y=1.02, xanchor='right', x=1, font=dict(size=10)),
    'paper_bgcolor': 'rgba(0,0,0,0)',
    'plot_bgcolor': 'rgba(0,0,0,0)',
}


def apply_chart_style(fig, height=280):
    """Apply consistent styling to charts."""
    fig.update_layout(
        height=height,
        **CHART_LAYOUT
    )
    fig.update_xaxes(gridcolor='#eee', gridwidth=1)
    fig.update_yaxes(gridcolor='#eee', gridwidth=1)
    return fig


# =============================================================================
# DASHBOARD
# =============================================================================

app = Dash(__name__, external_stylesheets=[dbc.themes.FLATLY])
app.title = "Germany Battery Storage Dashboard"
server = app.server  # Required for gunicorn/Render deployment

# Load data
df = get_data()

# Calculate summary statistics
total_operational_mw = df[df['Status'] == 'In Betrieb']['Leistung_MW'].sum()
total_planned_mw = df[df['Status'] == 'In Planung']['Leistung_MW'].sum()
total_operational_mwh = df[df['Status'] == 'In Betrieb']['Kapazitaet_MWh'].sum()
total_planned_mwh = df[df['Status'] == 'In Planung']['Kapazitaet_MWh'].sum()
avg_duration_operational = df[df['Status'] == 'In Betrieb']['Dauer_Stunden'].mean()
avg_duration_planned = df[df['Status'] == 'In Planung']['Dauer_Stunden'].mean()
count_operational = len(df[df['Status'] == 'In Betrieb'])
count_planned = len(df[df['Status'] == 'In Planung'])


def create_summary_cards():
    """Create compact summary statistic cards."""
    card_style = "mb-2 shadow-sm"
    return dbc.Row([
        dbc.Col([
            dbc.Card([
                dbc.CardBody([
                    html.H6("Operational", className="text-success mb-1", style={'fontSize': '0.85rem'}),
                    html.H4(f"{total_operational_mw:,.0f} MW", className="text-success mb-0"),
                    html.Small(f"{total_operational_mwh:,.0f} MWh | {count_operational} projects | {avg_duration_operational:.1f}h avg", className="text-muted")
                ], className="py-2")
            ], className=card_style)
        ], md=3, sm=6),
        dbc.Col([
            dbc.Card([
                dbc.CardBody([
                    html.H6("In Planning", className="text-warning mb-1", style={'fontSize': '0.85rem'}),
                    html.H4(f"{total_planned_mw:,.0f} MW", className="text-warning mb-0"),
                    html.Small(f"{total_planned_mwh:,.0f} MWh | {count_planned} projects | {avg_duration_planned:.1f}h avg", className="text-muted")
                ], className="py-2")
            ], className=card_style)
        ], md=3, sm=6),
        dbc.Col([
            dbc.Card([
                dbc.CardBody([
                    html.H6("Total Pipeline", className="text-primary mb-1", style={'fontSize': '0.85rem'}),
                    html.H4(f"{total_operational_mw + total_planned_mw:,.0f} MW", className="text-primary mb-0"),
                    html.Small(f"{total_operational_mwh + total_planned_mwh:,.0f} MWh | {count_operational + count_planned} projects", className="text-muted")
                ], className="py-2")
            ], className=card_style)
        ], md=3, sm=6),
        dbc.Col([
            dbc.Card([
                dbc.CardBody([
                    html.H6("Avg Project Size", className="text-info mb-1", style={'fontSize': '0.85rem'}),
                    html.H4(f"{df['Leistung_MW'].mean():,.1f} MW", className="text-info mb-0"),
                    html.Small(f"Op: {df[df['Status']=='In Betrieb']['Leistung_MW'].mean():,.1f} | Plan: {df[df['Status']=='In Planung']['Leistung_MW'].mean():,.1f} MW", className="text-muted")
                ], className="py-2")
            ], className=card_style)
        ], md=3, sm=6),
    ], className="g-2")


def create_capacity_trend_chart():
    """Create annual capacity additions chart."""
    df_trend = df[df['Jahr'] >= 2020].dropna(subset=['Jahr']).copy()

    yearly = df_trend.groupby(['Jahr', 'Status']).agg({
        'Leistung_MW': 'sum'
    }).reset_index()
    yearly.columns = ['Year', 'Status', 'MW']

    fig = go.Figure()

    for status in ['In Betrieb', 'In Planung']:
        d = yearly[yearly['Status'] == status]
        color = '#28a745' if status == 'In Betrieb' else '#ffc107'
        label = 'Operational' if status == 'In Betrieb' else 'Planned'
        fig.add_trace(go.Bar(x=d['Year'], y=d['MW'], name=label, marker_color=color, opacity=0.8))

    fig.update_layout(
        title='Annual Capacity Additions (2020+)',
        xaxis_title='', yaxis_title='MW Added',
        barmode='group'
    )
    return apply_chart_style(fig, height=280)


def create_cumulative_capacity_chart():
    """Create cumulative capacity chart showing total installed base over time."""
    # Get ALL operational projects (including pre-2020) for true cumulative
    df_operational = df[df['Status'] == 'In Betrieb'].dropna(subset=['Jahr']).copy()

    # Group by year
    yearly_op = df_operational.groupby('Jahr')['Leistung_MW'].sum().reset_index()
    yearly_op.columns = ['Year', 'MW']
    yearly_op = yearly_op.sort_values('Year')

    # Calculate true cumulative (sum of all capacity up to and including each year)
    yearly_op['Cumulative'] = yearly_op['MW'].cumsum()

    # Filter to show from 2020 onwards, but cumulative includes everything before
    # First, calculate the base (everything before 2020)
    base_capacity = yearly_op[yearly_op['Year'] < 2020]['MW'].sum()

    # Now filter to 2020+
    yearly_op_display = yearly_op[yearly_op['Year'] >= 2020].copy()

    # For planned projects, show what the cumulative WOULD be if they come online
    df_planned = df[df['Status'] == 'In Planung'].dropna(subset=['Jahr']).copy()
    yearly_planned = df_planned.groupby('Jahr')['Leistung_MW'].sum().reset_index()
    yearly_planned.columns = ['Year', 'MW']
    yearly_planned = yearly_planned.sort_values('Year')

    # Get the last operational cumulative value
    if len(yearly_op_display) > 0:
        last_operational_year = yearly_op_display['Year'].max()
        last_operational_cumulative = yearly_op_display['Cumulative'].max()
    else:
        last_operational_year = 2020
        last_operational_cumulative = base_capacity

    # Calculate planned cumulative (starting from last operational)
    yearly_planned = yearly_planned[yearly_planned['Year'] > last_operational_year].copy()
    if len(yearly_planned) > 0:
        yearly_planned['Cumulative'] = last_operational_cumulative + yearly_planned['MW'].cumsum()

    fig = go.Figure()

    # Operational cumulative line
    fig.add_trace(go.Scatter(
        x=yearly_op_display['Year'],
        y=yearly_op_display['Cumulative'],
        name='Operational (cumulative)',
        mode='lines+markers',
        line=dict(color='#28a745', width=3),
        marker=dict(size=8),
        fill='tozeroy',
        fillcolor='rgba(40, 167, 69, 0.2)'
    ))

    # Planned cumulative line (projected)
    if len(yearly_planned) > 0:
        # Connect from last operational point
        x_planned = [last_operational_year] + list(yearly_planned['Year'])
        y_planned = [last_operational_cumulative] + list(yearly_planned['Cumulative'])

        fig.add_trace(go.Scatter(
            x=x_planned,
            y=y_planned,
            name='+ Planned (projected)',
            mode='lines+markers',
            line=dict(color='#ffc107', width=3, dash='dash'),
            marker=dict(size=8),
            fill='tonexty',
            fillcolor='rgba(255, 193, 7, 0.2)'
        ))

    fig.update_layout(
        title='Cumulative Installed Capacity',
        xaxis_title='',
        yaxis_title='Total MW Installed',
    )
    return apply_chart_style(fig, height=280)


def create_duration_trend_chart():
    """Create cumulative average duration trend chart.
    Shows the capacity-weighted average duration of all projects up to each year.
    """
    fig = go.Figure()

    for status in ['In Betrieb', 'In Planung']:
        df_status = df[(df['Status'] == status)].dropna(subset=['Jahr', 'Dauer_Stunden']).copy()
        df_status = df_status.sort_values('Jahr')

        years = sorted(df_status['Jahr'].unique())
        years = [y for y in years if y >= 2020]

        cumulative_durations = []
        for year in years:
            # Get all projects up to and including this year
            projects_up_to_year = df_status[df_status['Jahr'] <= year]
            # Calculate capacity-weighted average duration
            total_mw = projects_up_to_year['Leistung_MW'].sum()
            if total_mw > 0:
                weighted_duration = (projects_up_to_year['Leistung_MW'] * projects_up_to_year['Dauer_Stunden']).sum() / total_mw
            else:
                weighted_duration = 0
            cumulative_durations.append({'Year': year, 'Duration': weighted_duration})

        if cumulative_durations:
            d = pd.DataFrame(cumulative_durations)
            color = '#28a745' if status == 'In Betrieb' else '#ffc107'
            label = 'Operational' if status == 'In Betrieb' else 'Planned'
            fig.add_trace(go.Scatter(x=d['Year'], y=d['Duration'], name=label,
                                     mode='lines+markers', line=dict(color=color, width=2), marker=dict(size=6)))

    fig.update_layout(title='Cumulative Avg Duration', xaxis_title='', yaxis_title='Hours (MW-weighted)')
    return apply_chart_style(fig, height=250)


def create_size_trend_chart():
    """Create cumulative average project size trend chart.
    Shows the average size of all projects up to each year.
    """
    fig = go.Figure()

    for status in ['In Betrieb', 'In Planung']:
        df_status = df[(df['Status'] == status)].dropna(subset=['Jahr']).copy()
        df_status = df_status.sort_values('Jahr')

        years = sorted(df_status['Jahr'].unique())
        years = [y for y in years if y >= 2020]

        cumulative_sizes = []
        for year in years:
            # Get all projects up to and including this year
            projects_up_to_year = df_status[df_status['Jahr'] <= year]
            avg_size = projects_up_to_year['Leistung_MW'].mean()
            cumulative_sizes.append({'Year': year, 'AvgMW': avg_size})

        if cumulative_sizes:
            d = pd.DataFrame(cumulative_sizes)
            color = '#28a745' if status == 'In Betrieb' else '#ffc107'
            label = 'Operational' if status == 'In Betrieb' else 'Planned'
            fig.add_trace(go.Scatter(x=d['Year'], y=d['AvgMW'], name=label,
                                     mode='lines+markers', line=dict(color=color, width=2), marker=dict(size=6)))

    fig.update_layout(title='Cumulative Avg Project Size', xaxis_title='', yaxis_title='Avg MW')
    return apply_chart_style(fig, height=250)


def create_owner_chart(status, color, title):
    """Create owner analysis chart for given status."""
    owner_data = df[df['Status'] == status].groupby('Betreiber').agg({
        'Leistung_MW': 'sum', 'MaStR_Nr': 'count'
    }).reset_index()
    owner_data.columns = ['Owner', 'MW', 'Count']
    owner_data = owner_data.sort_values('MW', ascending=True).tail(15)

    # Truncate long names
    owner_data['Owner_Short'] = owner_data['Owner'].apply(lambda x: x[:35] + '...' if len(x) > 35 else x)

    fig = go.Figure(go.Bar(
        x=owner_data['MW'], y=owner_data['Owner_Short'], orientation='h',
        marker_color=color, text=owner_data['MW'].round(0).astype(int),
        textposition='outside', textfont=dict(size=9),
        hovertemplate='<b>%{customdata}</b><br>%{x:.1f} MW<extra></extra>',
        customdata=owner_data['Owner']
    ))

    fig.update_layout(title=title, xaxis_title='MW', yaxis_title='',
                      margin=dict(l=150, r=40, t=40, b=30))
    return apply_chart_style(fig, height=400)


def create_largest_projects_chart():
    """Create largest projects chart."""
    largest = df.nlargest(10, 'Leistung_MW')[['Anlagename', 'Betreiber', 'Leistung_MW', 'Kapazitaet_MWh', 'Dauer_Stunden', 'Status']].copy()
    largest['Name_Short'] = largest['Anlagename'].apply(lambda x: x[:30] + '...' if len(x) > 30 else x)
    colors = ['#28a745' if s == 'In Betrieb' else '#ffc107' for s in largest['Status']]

    fig = go.Figure(go.Bar(
        x=largest['Leistung_MW'], y=largest['Name_Short'], orientation='h',
        marker_color=colors,
        text=[f"{mw:.0f} MW | {h:.1f}h" for mw, h in zip(largest['Leistung_MW'], largest['Dauer_Stunden'])],
        textposition='outside', textfont=dict(size=9),
        hovertemplate='<b>%{customdata[0]}</b><br>%{x:.0f} MW | %{customdata[1]:.0f} MWh<br>Owner: %{customdata[2]}<extra></extra>',
        customdata=list(zip(largest['Anlagename'], largest['Kapazitaet_MWh'], largest['Betreiber']))
    ))

    fig.update_layout(title='Top 10 Largest Projects', xaxis_title='MW', yaxis_title='',
                      margin=dict(l=180, r=60, t=40, b=30))
    return apply_chart_style(fig, height=320)


def create_bundesland_chart():
    """Create Bundesland comparison chart."""
    bl_data = df.groupby(['Bundesland', 'Status'])['Leistung_MW'].sum().reset_index()
    bl_pivot = bl_data.pivot(index='Bundesland', columns='Status', values='Leistung_MW').fillna(0)
    bl_pivot['Total'] = bl_pivot.sum(axis=1)
    bl_pivot = bl_pivot.sort_values('Total', ascending=True)

    fig = go.Figure()
    if 'In Betrieb' in bl_pivot.columns:
        fig.add_trace(go.Bar(y=bl_pivot.index, x=bl_pivot['In Betrieb'], name='Operational',
                             orientation='h', marker_color='#28a745'))
    if 'In Planung' in bl_pivot.columns:
        fig.add_trace(go.Bar(y=bl_pivot.index, x=bl_pivot['In Planung'], name='Planned',
                             orientation='h', marker_color='#ffc107'))

    fig.update_layout(title='Capacity by Federal State', xaxis_title='MW', yaxis_title='',
                      barmode='stack', margin=dict(l=140, r=20, t=40, b=30))
    return apply_chart_style(fig, height=400)


def create_bundesland_table():
    """Create Bundesland summary table."""
    bl = df.groupby('Bundesland').agg({
        'Leistung_MW': ['sum', 'mean', 'count'],
        'Dauer_Stunden': 'mean'
    }).reset_index()
    bl.columns = ['Bundesland', 'Total MW', 'Avg MW', 'Count', 'Avg Duration']
    bl = bl.sort_values('Total MW', ascending=False)
    for col in ['Total MW', 'Avg MW', 'Avg Duration']:
        bl[col] = bl[col].round(1)
    return bl


def create_netzbetreiber_chart():
    """Create Netzbetreiber (Grid Operator) comparison chart."""
    nb_data = df.groupby(['Netzbetreiber', 'Status'])['Leistung_MW'].sum().reset_index()
    nb_pivot = nb_data.pivot(index='Netzbetreiber', columns='Status', values='Leistung_MW').fillna(0)
    nb_pivot['Total'] = nb_pivot.sum(axis=1)
    nb_pivot = nb_pivot.sort_values('Total', ascending=True).tail(15)

    fig = go.Figure()
    if 'In Betrieb' in nb_pivot.columns:
        fig.add_trace(go.Bar(y=nb_pivot.index, x=nb_pivot['In Betrieb'], name='Operational',
                             orientation='h', marker_color='#28a745'))
    if 'In Planung' in nb_pivot.columns:
        fig.add_trace(go.Bar(y=nb_pivot.index, x=nb_pivot['In Planung'], name='Planned',
                             orientation='h', marker_color='#ffc107'))

    fig.update_layout(title='Top 15 Grid Operators by Capacity', xaxis_title='MW', yaxis_title='',
                      barmode='stack', margin=dict(l=180, r=20, t=40, b=30))
    return apply_chart_style(fig, height=400)


def create_excel_export():
    """Create an Excel file with one sheet per visualization data."""
    output = io.BytesIO()

    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        # Sheet 1: Summary Statistics
        summary_data = {
            'Metric': [
                'Operational Capacity (MW)', 'Operational Capacity (MWh)', 'Operational Projects',
                'Operational Avg Duration (h)', 'Planned Capacity (MW)', 'Planned Capacity (MWh)',
                'Planned Projects', 'Planned Avg Duration (h)', 'Total Capacity (MW)',
                'Total Capacity (MWh)', 'Total Projects', 'Average Project Size (MW)'
            ],
            'Value': [
                total_operational_mw, total_operational_mwh, count_operational,
                avg_duration_operational, total_planned_mw, total_planned_mwh,
                count_planned, avg_duration_planned, total_operational_mw + total_planned_mw,
                total_operational_mwh + total_planned_mwh, count_operational + count_planned,
                df['Leistung_MW'].mean()
            ]
        }
        pd.DataFrame(summary_data).to_excel(writer, sheet_name='Summary', index=False)

        # Sheet 2: Annual Capacity Additions
        df_trend = df[df['Jahr'] >= 2020].dropna(subset=['Jahr']).copy()
        yearly = df_trend.groupby(['Jahr', 'Status']).agg({
            'Leistung_MW': 'sum', 'Kapazitaet_MWh': 'sum', 'MaStR_Nr': 'count'
        }).reset_index()
        yearly.columns = ['Year', 'Status', 'MW', 'MWh', 'Project Count']
        yearly.to_excel(writer, sheet_name='Annual Additions', index=False)

        # Sheet 3: Cumulative Capacity
        df_operational = df[df['Status'] == 'In Betrieb'].dropna(subset=['Jahr']).copy()
        yearly_op = df_operational.groupby('Jahr')['Leistung_MW'].sum().reset_index()
        yearly_op.columns = ['Year', 'MW Added']
        yearly_op = yearly_op.sort_values('Year')
        yearly_op['Cumulative MW'] = yearly_op['MW Added'].cumsum()
        yearly_op.to_excel(writer, sheet_name='Cumulative Capacity', index=False)

        # Sheet 4: Duration Trend
        duration_data = []
        for status in ['In Betrieb', 'In Planung']:
            df_status = df[(df['Status'] == status)].dropna(subset=['Jahr', 'Dauer_Stunden']).copy()
            df_status = df_status.sort_values('Jahr')
            years = sorted(df_status['Jahr'].unique())
            years = [y for y in years if y >= 2020]
            for year in years:
                projects_up_to_year = df_status[df_status['Jahr'] <= year]
                total_mw = projects_up_to_year['Leistung_MW'].sum()
                if total_mw > 0:
                    weighted_duration = (projects_up_to_year['Leistung_MW'] * projects_up_to_year['Dauer_Stunden']).sum() / total_mw
                else:
                    weighted_duration = 0
                duration_data.append({'Year': year, 'Status': status, 'Cumulative Avg Duration (h)': weighted_duration})
        pd.DataFrame(duration_data).to_excel(writer, sheet_name='Duration Trend', index=False)

        # Sheet 5: Size Trend
        size_data = []
        for status in ['In Betrieb', 'In Planung']:
            df_status = df[(df['Status'] == status)].dropna(subset=['Jahr']).copy()
            df_status = df_status.sort_values('Jahr')
            years = sorted(df_status['Jahr'].unique())
            years = [y for y in years if y >= 2020]
            for year in years:
                projects_up_to_year = df_status[df_status['Jahr'] <= year]
                avg_size = projects_up_to_year['Leistung_MW'].mean()
                size_data.append({'Year': year, 'Status': status, 'Cumulative Avg Size (MW)': avg_size})
        pd.DataFrame(size_data).to_excel(writer, sheet_name='Size Trend', index=False)

        # Sheet 6: Top Owners - Operational
        owner_op = df[df['Status'] == 'In Betrieb'].groupby('Betreiber').agg({
            'Leistung_MW': 'sum', 'Kapazitaet_MWh': 'sum', 'MaStR_Nr': 'count'
        }).reset_index()
        owner_op.columns = ['Owner', 'MW', 'MWh', 'Project Count']
        owner_op = owner_op.sort_values('MW', ascending=False)
        owner_op.to_excel(writer, sheet_name='Owners - Operational', index=False)

        # Sheet 7: Top Owners - Planned
        owner_pl = df[df['Status'] == 'In Planung'].groupby('Betreiber').agg({
            'Leistung_MW': 'sum', 'Kapazitaet_MWh': 'sum', 'MaStR_Nr': 'count'
        }).reset_index()
        owner_pl.columns = ['Owner', 'MW', 'MWh', 'Project Count']
        owner_pl = owner_pl.sort_values('MW', ascending=False)
        owner_pl.to_excel(writer, sheet_name='Owners - Planned', index=False)

        # Sheet 8: Largest Projects
        largest = df.nlargest(50, 'Leistung_MW')[['Anlagename', 'Betreiber', 'Netzbetreiber', 'Leistung_MW', 'Kapazitaet_MWh', 'Dauer_Stunden', 'Status', 'Bundesland', 'Jahr']].copy()
        largest.columns = ['Project Name', 'Owner', 'Grid Operator', 'MW', 'MWh', 'Duration (h)', 'Status', 'State', 'Year']
        largest.to_excel(writer, sheet_name='Largest Projects', index=False)

        # Sheet 9: Bundesland Summary
        bl = df.groupby('Bundesland').agg({
            'Leistung_MW': ['sum', 'mean', 'count'],
            'Kapazitaet_MWh': 'sum',
            'Dauer_Stunden': 'mean'
        }).reset_index()
        bl.columns = ['State', 'Total MW', 'Avg MW', 'Project Count', 'Total MWh', 'Avg Duration (h)']
        bl = bl.sort_values('Total MW', ascending=False)
        bl.to_excel(writer, sheet_name='By State', index=False)

        # Sheet 10: Bundesland by Status
        bl_status = df.groupby(['Bundesland', 'Status'])['Leistung_MW'].sum().reset_index()
        bl_status.columns = ['State', 'Status', 'MW']
        bl_status_pivot = bl_status.pivot(index='State', columns='Status', values='MW').fillna(0).reset_index()
        bl_status_pivot.to_excel(writer, sheet_name='By State & Status', index=False)

        # Sheet 11: Netzbetreiber Summary
        nb = df.groupby('Netzbetreiber').agg({
            'Leistung_MW': ['sum', 'count'],
            'Kapazitaet_MWh': 'sum'
        }).reset_index()
        nb.columns = ['Grid Operator', 'Total MW', 'Project Count', 'Total MWh']
        nb = nb.sort_values('Total MW', ascending=False)
        nb.to_excel(writer, sheet_name='Grid Operators', index=False)

        # Sheet 12: Netzbetreiber by Status
        nb_status = df.groupby(['Netzbetreiber', 'Status'])['Leistung_MW'].sum().reset_index()
        nb_status.columns = ['Grid Operator', 'Status', 'MW']
        nb_status_pivot = nb_status.pivot(index='Grid Operator', columns='Status', values='MW').fillna(0).reset_index()
        nb_status_pivot = nb_status_pivot.sort_values(nb_status_pivot.columns[1], ascending=False) if len(nb_status_pivot.columns) > 1 else nb_status_pivot
        nb_status_pivot.to_excel(writer, sheet_name='Grid Operators by Status', index=False)

        # Sheet 13: Full Project List
        full_list = df[['Anlagename', 'Betreiber', 'Netzbetreiber', 'Status', 'Leistung_MW', 'Kapazitaet_MWh', 'Dauer_Stunden', 'Bundesland', 'Jahr']].copy()
        full_list.columns = ['Project Name', 'Owner', 'Grid Operator', 'Status', 'MW', 'MWh', 'Duration (h)', 'State', 'Year']
        full_list = full_list.sort_values('MW', ascending=False)
        full_list.to_excel(writer, sheet_name='All Projects', index=False)

    output.seek(0)
    return output.getvalue()


# Data source URL
MASTR_DATA_URL = "https://www.marktstammdatenregister.de/MaStR/Einheit/Einheiten/ErweiterteOeffentlicheEinheitenuebersicht?filter=Name%20des%20Anlagenbetreibers%20(nur%20Org.)~nct~%27nat%C3%BCrliche%20Person%27~and~Nettonennleistung%20der%20Einheit~gt~%27999%27~and~Speichertechnologie~eq~%27524%27~and~Betriebs-Status~neq~%2738%27~and~Betriebs-Status~neq~%2737%27"

# App Layout
app.layout = dbc.Container([
    # Header
    dbc.Row([
        dbc.Col([
            html.H4("Germany Grid-Scale Battery Storage", className="text-primary mb-1"),
            html.Small("Dashboard tracking large-scale battery projects from MaStR", className="text-muted"),
        ])
    ], className="mb-2 mt-2"),

    # Data source link and Export button
    dbc.Row([
        dbc.Col([
            dbc.Alert([
                html.Strong("Data Source: "),
                html.A("Marktstammdatenregister (MaStR)", href=MASTR_DATA_URL, target="_blank", className="alert-link")
            ], color="light", className="py-2 mb-2", style={'fontSize': '0.85rem'})
        ], md=10),
        dbc.Col([
            dbc.Button(
                ["Export Excel"],
                id="export-excel-btn",
                color="primary",
                size="sm",
                className="mt-1"
            ),
            dcc.Download(id="download-excel")
        ], md=2, className="text-end")
    ]),

    # Summary Cards
    create_summary_cards(),

    # Capacity Trends - Annual and Cumulative side by side
    dbc.Row([
        dbc.Col([
            dcc.Graph(figure=create_capacity_trend_chart(), config=CHART_CONFIG)
        ], md=6),
        dbc.Col([
            dcc.Graph(figure=create_cumulative_capacity_chart(), config=CHART_CONFIG)
        ], md=6)
    ], className="mt-3"),

    # Duration and Size Trends - side by side
    dbc.Row([
        dbc.Col([
            dcc.Graph(figure=create_duration_trend_chart(), config=CHART_CONFIG)
        ], md=6),
        dbc.Col([
            dcc.Graph(figure=create_size_trend_chart(), config=CHART_CONFIG)
        ], md=6)
    ]),

    # Owner Analysis - side by side
    dbc.Row([
        dbc.Col([
            dcc.Graph(figure=create_owner_chart('In Betrieb', '#28a745', 'Top 15 Owners - Operational'), config=CHART_CONFIG)
        ], md=6),
        dbc.Col([
            dcc.Graph(figure=create_owner_chart('In Planung', '#ffc107', 'Top 15 Owners - Planned'), config=CHART_CONFIG)
        ], md=6)
    ]),

    # Largest Projects
    dbc.Row([
        dbc.Col([
            dcc.Graph(figure=create_largest_projects_chart(), config=CHART_CONFIG)
        ])
    ]),

    # Bundesland Analysis
    dbc.Row([
        dbc.Col([
            dcc.Graph(figure=create_bundesland_chart(), config=CHART_CONFIG)
        ], md=7),
        dbc.Col([
            html.H6("Summary by State", className="mb-2"),
            dash_table.DataTable(
                data=create_bundesland_table().to_dict('records'),
                columns=[
                    {'name': 'State', 'id': 'Bundesland'},
                    {'name': 'MW', 'id': 'Total MW', 'type': 'numeric'},
                    {'name': '#', 'id': 'Count', 'type': 'numeric'},
                    {'name': 'Avg MW', 'id': 'Avg MW', 'type': 'numeric'},
                    {'name': 'Dur (h)', 'id': 'Avg Duration', 'type': 'numeric'}
                ],
                style_table={'overflowX': 'auto', 'fontSize': '12px'},
                style_cell={'textAlign': 'left', 'padding': '4px 8px', 'fontSize': '11px'},
                style_header={'backgroundColor': '#f8f9fa', 'fontWeight': 'bold', 'fontSize': '11px'},
                style_data_conditional=[{'if': {'row_index': 'odd'}, 'backgroundColor': '#f8f9fa'}],
                page_size=16
            )
        ], md=5)
    ], className="mt-2"),

    # Netzbetreiber (Grid Operator) Analysis
    dbc.Row([
        dbc.Col([
            dcc.Graph(figure=create_netzbetreiber_chart(), config=CHART_CONFIG)
        ])
    ], className="mt-2"),

    # Project Table
    dbc.Row([
        dbc.Col([
            html.H6("Project Overview", className="mt-3 mb-2"),
            dbc.Row([
                dbc.Col([
                    dcc.Dropdown(id='status-filter',
                                 options=[{'label': 'All', 'value': 'All'},
                                          {'label': 'Operational', 'value': 'In Betrieb'},
                                          {'label': 'Planned', 'value': 'In Planung'}],
                                 value='All', placeholder='Status', clearable=False, style={'fontSize': '12px'})
                ], md=2),
                dbc.Col([
                    dcc.Dropdown(id='bundesland-filter',
                                 options=[{'label': 'All States', 'value': 'All'}] +
                                         [{'label': bl, 'value': bl} for bl in sorted(df['Bundesland'].unique())],
                                 value='All', placeholder='State', clearable=False, style={'fontSize': '12px'})
                ], md=2),
            ], className="mb-2 g-2"),
            dash_table.DataTable(
                id='project-table',
                columns=[
                    {'name': 'Project', 'id': 'Anlagename'},
                    {'name': 'Owner', 'id': 'Betreiber'},
                    {'name': 'Grid Operator', 'id': 'Netzbetreiber'},
                    {'name': 'Status', 'id': 'Status'},
                    {'name': 'MW', 'id': 'Leistung_MW', 'type': 'numeric', 'format': {'specifier': ',.1f'}},
                    {'name': 'MWh', 'id': 'Kapazitaet_MWh', 'type': 'numeric', 'format': {'specifier': ',.1f'}},
                    {'name': 'Dur (h)', 'id': 'Dauer_Stunden', 'type': 'numeric', 'format': {'specifier': '.1f'}},
                    {'name': 'State', 'id': 'Bundesland'},
                    {'name': 'Year', 'id': 'Jahr', 'type': 'numeric'}
                ],
                data=df.sort_values('Leistung_MW', ascending=False).to_dict('records'),
                style_table={'overflowX': 'auto'},
                style_cell={'textAlign': 'left', 'padding': '4px 8px', 'fontSize': '11px', 'maxWidth': '200px', 'overflow': 'hidden', 'textOverflow': 'ellipsis'},
                style_header={'backgroundColor': '#f8f9fa', 'fontWeight': 'bold', 'fontSize': '11px'},
                style_data_conditional=[
                    {'if': {'row_index': 'odd'}, 'backgroundColor': '#f8f9fa'},
                    {'if': {'filter_query': '{Status} = "In Betrieb"', 'column_id': 'Status'}, 'backgroundColor': '#d4edda', 'color': '#155724'},
                    {'if': {'filter_query': '{Status} = "In Planung"', 'column_id': 'Status'}, 'backgroundColor': '#fff3cd', 'color': '#856404'}
                ],
                page_size=15, sort_action='native', filter_action='native'
            )
        ])
    ]),

    # Footer
    dbc.Row([
        dbc.Col([
            html.Hr(className="my-2"),
            html.Small([
                f"Last updated: {datetime.now().strftime('%Y-%m-%d')} | {len(df)} projects after filtering"
            ], className="text-muted")
        ])
    ], className="mb-3")

], fluid=True, style={'maxWidth': '1400px'})


@callback(
    Output('project-table', 'data'),
    Input('status-filter', 'value'),
    Input('bundesland-filter', 'value')
)
def filter_table(status, bundesland):
    filtered = df.copy()
    if status != 'All':
        filtered = filtered[filtered['Status'] == status]
    if bundesland != 'All':
        filtered = filtered[filtered['Bundesland'] == bundesland]
    return filtered.sort_values('Leistung_MW', ascending=False).to_dict('records')


@callback(
    Output('download-excel', 'data'),
    Input('export-excel-btn', 'n_clicks'),
    prevent_initial_call=True
)
def export_excel(n_clicks):
    """Export dashboard data to Excel file."""
    if n_clicks:
        excel_data = create_excel_export()
        filename = f"Germany_Battery_Storage_Data_{datetime.now().strftime('%Y%m%d')}.xlsx"
        return dcc.send_bytes(excel_data, filename)
    return None


if __name__ == '__main__':
    print("\n" + "="*60)
    print("Germany Grid-Scale Battery Storage Dashboard")
    print("="*60)
    print(f"\nLoaded {len(df)} projects after outlier filtering")
    print(f"Outlier rules applied:")
    for rule, value in OUTLIER_RULES.items():
        print(f"  - {rule}: {value}")
    print("\nOpen: http://127.0.0.1:8050")
    print("="*60 + "\n")
    app.run(debug=True)
