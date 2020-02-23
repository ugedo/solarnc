import json
import os
import pandas as pd
import jsonschema as jsch
from pvlib.location import Location

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

# pvlib provides: ‘ineichen’, ‘haurwitz’, ‘simplified_solis'
def csm_pvlib(df, skip_existing, name, stations, model, itype):
    for sta in stations:
        colname = "{} {}".format(name, sta["name"])
        if skip_existing and colname in df.columns:
            continue
        loc = Location(sta["latitude"], sta["longitude"])
        df[colname] = loc.get_clearsky(df.index, model = model)[itype]


def runjobs(cbk, arglist, npjobs):
    nj = len(arglist)
    j = 0;
    print("\rDone: {}/{}".format(j,nj), end='')
    if npjobs > 1:
        p = mp.Pool(npjobs)
        for x in p.imap(lambda argtup: cbk(*argtup), arglist):
            j += 1
            print("\rDone: {}/{}".format(j,nj), end='')
    else:
        for argtup in arglist:
            cbk(*argtup)
            j += 1
            print("\rDone: {}/{}".format(j,nj), end='')
    print("")


