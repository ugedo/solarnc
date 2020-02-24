import json
import os
import csv
import pandas as pd
import numpy as np
import jsonschema as jsch
#import pvlib
from pvlib.location import Location
from pvlib import clearsky
import pvlib
import multiprocessing as mp

def load_config(fname):
    with open(fname, 'r') as f:
        config = json.load(f)
    current_file = os.path.realpath(__file__)
    schema_path = os.path.dirname(current_file)
    schema_fname = "{}/../jsons/solarnc_schema.json".format(schema_path)
    with open(schema_fname, 'r') as f:
        schema = json.load(f)
    jsch.validate(config, schema)
    return config

def read_csv(infile, tzone):
    df = pd.read_csv(infile, index_col = "datetime", parse_dates=True)
    df.index = df.index.tz_localize('UTC').tz_convert(tzone)
    return df

def save_csv(df, outfile):
    df.to_csv(outfile, header = True, index = True)

def save_list(l, fname):
    with open(fname, 'w') as f:
        wr = csv.writer(f, quoting=csv.QUOTE_ALL)
        wr.writerow(l)

def read_list(fname):
    with open(fname, newline='') as f:
        reader = csv.reader(f)
        l = list(reader)
    return l[0]

# pvlib provides: ‘ineichen’, ‘haurwitz’, ‘simplified_solis'
def csm_pvlib(df, skip_existing, stations, models, position):
    for sta in stations:
        staname = sta['name']
        latitude = sta["latitude"]
        longitude = sta["longitude"]
        altitude = sta["MASL"] if "MASL" in sta else 0
        ecol = 'elevation {}'.format(staname)
        acol = 'azimuth {}'.format(staname)

        solpos = pvlib.solarposition.get_solarposition(df.index, latitude,\
                longitude)
        apparent_elevation = solpos['apparent_elevation']
        apparent_zenith = solpos['apparent_zenith']

        if position and ecol not in df.columns:
            df[ecol] = apparent_elevation
            df[acol] = solpos['azimuth']

        if 'ineichen' in models or 'solis' in models \
                or 'simplified_solis' in models:
            pressure = pvlib.atmosphere.alt2pres(altitude)
            dni_extra = pvlib.irradiance.get_extra_radiation(df.index)

        for model in models:
            ghicol = "GHI {}".format(staname)
            csmcol = "{}_ghi {}".format(model, staname)
            kcol = "K_{} {}".format(model, staname)
            if skip_existing and csmcol in df.columns:
                continue
            if model == 'ineichen':
                airmass = pvlib.atmosphere.get_relative_airmass(apparent_zenith)
                airmass = pvlib.atmosphere.get_absolute_airmass(airmass, \
                        pressure)
                linke_turbidity = pvlib.clearsky.lookup_linke_turbidity(\
                        df.index, latitude, longitude)
                df[csmcol] = clearsky.ineichen(apparent_zenith, airmass,\
                        linke_turbidity, altitude, dni_extra)['ghi']
            elif model == 'solis' or model == 'simplified_solis':
                aod700 = 0.1
                precipitable_water = 1
                df[csmcol] = clearsky.simplified_solis(apparent_elevation,\
                        aod700, precipitable_water, pressure, dni_extra)['ghi']
            elif model == 'haurwitz':
                sin_elevation = np.sin(np.radians(apparent_elevation))
                df[csmcol] = 1098 * sin_elevation * np.exp(-0.057/sin_elevation)
            else:
                print("Error: {} model not supportd")
                raise ValueError
            df[kcol] = df[ghicol]/df[csmcol]

def solar_position(df, skip_existing, stations):
    for sta in stations:
        ecol = 'elevation {}'.format(sta['name'])
        acol = 'azimuth {}'.format(sta['name'])
        if skip_existing and ecol in df.columns and acol in df.columns:
            continue
        latitude = sta["latitude"]
        longitude = sta["longitude"]
        altitude = sta["MASL"] if "MASL" in sta else 0
        loc = Location(latitude, longitude, df.index.tz.zone, altitude)
        #TODO: should use temperature if available
        solpos = loc.get_solarposition(df.index)
        df[ecol] = solpos['elevation']
        df[acol] = solpos['azimuth']

def run_cbk_unpack(tup):
    cbk, argtup = tup
    cbk(*argtup)

def runjobs(cbk, arglist, npjobs):
    nj = len(arglist)
    l = list(zip([cbk] * nj, arglist))
    j = 0;
    print("\rDone: {}/{}".format(j,nj), end='')
    if npjobs > 1:
        p = mp.Pool(npjobs)
        for x in p.imap(run_cbk_unpack, l):
            j += 1
            print("\rDone: {}/{}".format(j,nj), end='')
    else:
        for argtup in arglist:
            cbk(*argtup)
            j += 1
            print("\rDone: {}/{}".format(j,nj), end='')
    print("")



