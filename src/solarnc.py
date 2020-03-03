import json
import os
import csv
import pandas as pd
import numpy as np
import jsonschema as jsch
import pvlib
import glob
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

def get_ttlist_names(config):
    if "extend" in config:
        extend = config['extend']
        outpath = "{}/lists".format(extend['outpath'])
    else:
        fconfig = config['format']
        outpath = "{}/lists".format(fconfig['outpath'])
    trainset_fname = "{}/trainset.csv".format(outpath)
    testset_fname = "{}/testset.csv".format(outpath)
    return (trainset_fname, testset_fname)

def get_split_infiles(config):
    if "extend" in config:
        extend = config['extend']
        infiles  = glob.glob("{}/*.csv".format(extend['outpath']))
    else:
        fconfig = config['format']
        infiles  = glob.glob("{}/*.csv".format(fconfig['outpath']))
    return infiles

# pvlib provides: ‘ineichen’, ‘haurwitz’, ‘simplified_solis'
def csm_pvlib(df, skip_existing, stations, models, position):
    def elevation_azimuth(df, solpos, staname, skip_existing):
        ecol = 'elevation {}'.format(staname)
        acol = 'azimuth {}'.format(staname)
        if position and (ecol not in df.columns or not skip_existing):
            df[ecol] = solpos['apparent_elevation']
            df[acol] = solpos['azimuth']

    def csm(df, model, sta, solpos, pressure, dni_extra):
        latitude = sta["latitude"]
        longitude = sta["longitude"]
        altitude = sta["MASL"] if "MASL" in sta else 0
        apparent_elevation = solpos['apparent_elevation']
        apparent_zenith = solpos['apparent_zenith']

        if model == 'ineichen':
            airmass = pvlib.atmosphere.get_relative_airmass(apparent_zenith)
            airmass = pvlib.atmosphere.get_absolute_airmass(airmass, \
                    pressure)
            linke_turbidity = pvlib.clearsky.lookup_linke_turbidity(\
                    df.index, latitude, longitude)
            ghi_csm = pvlib.clearsky.ineichen(apparent_zenith, airmass,\
                    linke_turbidity, altitude, dni_extra)['ghi']
        elif model == 'simplified_solis':
            aod700 = 0.1
            precipitable_water = 1
            ghi_csm = pvlib.clearsky.simplified_solis(apparent_elevation,\
                    aod700, precipitable_water, pressure, dni_extra)['ghi']
        elif model == 'haurwitz':
            ghi_csm = pvlib.clearsky.haurwitz(apparent_zenith)
        else:
            print("Error: {} model not supportd".format(model))
            raise ValueError
        ghi_csm[ghi_csm < np.finfo(float).eps] = 0
        return ghi_csm

    if 'ineichen' in models or 'simplified_solis' in models:
        dni_extra = pvlib.irradiance.get_extra_radiation(df.index)

    for sta in stations:
        staname = sta['name']
        altitude = sta["MASL"] if "MASL" in sta else 0
        pressure = pvlib.atmosphere.alt2pres(altitude)
        solpos = pvlib.solarposition.get_solarposition(df.index,\
                sta["latitude"], sta["longitude"], altitude, pressure)
        elevation_azimuth(df, solpos, staname, skip_existing)

        for model in models:
            ghicol = "GHI {}".format(staname)
            csmcol = "{}_ghi {}".format(model, staname)
            kcol = "K_{} {}".format(model, staname)
            if skip_existing and csmcol in df.columns:
                continue
            df[csmcol] = csm(df, model, sta, solpos, pressure, dni_extra)
            df[kcol] = df[ghicol]/df[csmcol].replace({0: np.finfo(float).eps})

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



