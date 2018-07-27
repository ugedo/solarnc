#!/usr/bin/env python

import pandas as pd
import optparse
import os
import multiprocessing as mp
from tqdm import tqdm
import psutil
import jsonschema as jsch
from solarnc import features as sncf
from solarnc import utils as utils

config_keys = ['path', 'outpath', 'skip existing', 'function', 'stations']

def parse_options():
    usage_str = "usage: %prog -c json_config_file"
    parser = optparse.OptionParser(usage_str)
    parser.add_option("-c", "--config", dest="config", type="string",
            help="path to the json file with the configuration parameters")
    parser.add_option("-l", "--logical", dest="logical", action="store_true",
            default = False,
            help="parallelize also using logical cpus (hyperthreading)")

    options, args  = parser.parse_args()

    if not options.config:
        parser.error("missing json config file")

    return (options, args)

def get_input_files(path, outpath, skip_existing):
    files = utils.get_csv_files_list(path)
    if skip_existing:
        for f in utils.get_csv_files_list(outpath):
            if f in files:
                files.remove(f)
    return files

def load_config(conf_file):
    config = utils.load_json(conf_file)
    current_file = os.path.realpath(__file__)
    schema_path = os.path.dirname(current_file)
    schema_file = "{}/../jsons/add_feature_config_schema.json".format(schema_path)
    schema = utils.load_json(schema_file)
    jsch.validate(config, schema)
    return config

def add_new_feature(infile, stations, fetures):
    df = utils.read_solarnc_csv(infile)
    for f in features:
        try:
            ffun = getattr(sncf, f['function'])
        except AttributeError:
            print("Error: {} is not a valid feature feature".format(feature))
        else:
            if 'args' in f:
                args = tuple(f['args'])
                ffun(df, stations, *args)
            else:
                ffun(df, stations)

    base = os.path.basename(infile)
    day = os.path.splitext(base)[0]
    outfile = "{}/{}.csv".format(config['outpath'], day)
    utils.save_solarnc_csv(df, outfile)

def print_stations(stations):
    print("Considering the following stations: ")
    sta_list = list(stations.keys())
    for sta in sta_list:
        print("\t{}".format(sta))

def print_features(features):
    print("Computing the following features:")
    for f in features:
        if 'args' in f:
            msg = '\t{}, with args: {}'.format(f['function'], f['args'])
        else:
            msg = '\t{}'.format(f['function'])
        print(msg)

def add_new_feature_unpack(arg):
    add_new_feature(*arg)

if __name__ == "__main__":
    options, args = parse_options()

    config = load_config(options.config)
    print("Correct config file {}".format(options.config))
    stations = config['stations']
    print_stations(stations)
    features = config['features']
    print_features(features)

    if not os.path.exists(config['outpath']):
        os.makedirs(config['outpath'])

    infiles = get_input_files(config['path'], config['outpath'],
            config['skip existing'])

    if options.logical:
        num_cores = mp.cpu_count() # with hyperthreading as cpus
    else:
        num_cores = psutil.cpu_count(logical = False)

    with mp.Pool(num_cores) as p:
        args = [(infile, stations, features) for infile in infiles]
        for _ in tqdm(p.imap_unordered(add_new_feature_unpack, args),
                total = len(infiles)):
            pass

