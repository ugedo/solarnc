import json
import os
import csv
import pandas as pd
import jsonschema as jsch
from pvlib.location import Location
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
def csm_pvlib(df, skip_existing, name, stations, model, itype):
    for sta in stations:
        colname = "{} {}".format(name, sta["name"])
        if skip_existing and colname in df.columns:
            continue
        loc = Location(sta["latitude"], sta["longitude"])
        df[colname] = loc.get_clearsky(df.index, model = model)[itype]

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



