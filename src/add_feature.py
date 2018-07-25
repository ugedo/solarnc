#!/usr/bin/env python

import pandas as pd
import json
import optparse
import os
import iso8601
import multiprocessing as mp
from tqdm import tqdm
import sncfeatures as feat
import psutil

config_keys = ['path', 'function', 'stations']

def parse_options():
    usage_str = "usage: %prog -c json_config_file"
    parser = optparse.OptionParser(usage_str)
    parser.add_option("-c", "--config", dest="config", type="string",
            help="path to the json file with the configuration parameters")

    options, args  = parser.parse_args()

    if not options.config:
        parser.error("missing json config file")

    return (options, args)

def get_input_files(path):
    files = []
    for f in os.listdir(path):
        if f.endswith(".csv"):
            files.append("{}/{}".format(path, f))
    return files

def parse_config(conf_file):
    with open(conf_file, 'r') as cf:
        config = json.load(cf)

    for k in config_keys:
        if k not in config:
            raise ValueError('{} not found in the json config file {}'
                    .format(k, conf_file))

    if 'outpath' not in config:
        print('Warning: No outpath found in config json file.'
              ' Using the same as the input path.\n')
        config['outpath'] = config['path']

    return config

def add_new_feature(infile, stations, featfun):
    df = pd.read_csv(infile, index_col = None)
    # FIXME: using pd.to_datetime is faster (20%) but timezone info is lost
    # I do not now any better solution than using the iso8601 parse_date parser
    #df['datetime'] = pd.to_datetime(df['datetime'])
    df['datetime'] = df['datetime'].apply(iso8601.parse_date)

    featfun(df, stations)

    base = os.path.basename(infile)
    day = os.path.splitext(base)[0]
    outfile = "{}/{}.csv".format(config['outpath'], day)
    df.to_csv(outfile, header = True, index = False)

def add_new_feature_unpack(arg):
    add_new_feature(*arg)

def kk(i):
    time.sleep(1)

if __name__ == "__main__":
    options, args = parse_options()

    config = parse_config(options.config)
    stations = config['stations']
    fname = config['function']
    try:
        featfun = getattr(feat, fname)
    except AttributeError:
        print("Error, {} is not a valid feature function".format(fname))
    else:
        print("Generatinf features with function {}".format(fname))

    infiles = get_input_files(config['path'])

    if not os.path.exists(config['outpath']):
        os.makedirs(config['outpath'])

    num_cores = mp.cpu_count()
    #num_cores = psutil.cpu_count(logical = False)
    with mp.Pool(2) as p:
        args = [(infile, stations, featfun) for infile in infiles]
        for _ in tqdm(p.imap_unordered(add_new_feature_unpack, args),
                total = len(infiles)):
            pass

