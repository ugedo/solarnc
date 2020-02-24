import json
import os
import csv
import pandas as pd
import jsonschema as jsch
#import pvlib
from pvlib.location import Location
import multiprocessing as mp
#import numpy as np

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
def csm_pvlib(df, skip_existing, stations, model, itype, decimals):
    for sta in stations:
        staname = sta['name']
        ghicol = "GHI {}".format(staname)
        csmcol = "{}_ghi {}".format(model, staname)
        kcol = "K_{} {}".format(model, staname)
        if skip_existing and csmcol in df.columns:
            continue
        latitude = sta["latitude"]
        longitude = sta["longitude"]
        altitude = sta["MASL"] if "MASL" in sta else 0
        loc = Location(latitude, longitude, df.index.tz.zone, altitude)
        df[csmcol] = round(loc.get_clearsky(df.index, model = model)[itype]\
                    , decimals)
        df[kcol] = round(df[ghicol]/df[csmcol], decimals)

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



