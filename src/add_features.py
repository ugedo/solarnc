#!/usr/bin/env python3

import pandas as pd
import optparse
import os
import glob
import multiprocessing as mp
import jsonschema as jsch
from solarnc import features as sncf
from solarnc import utils as utils

def parse_options():
    usage_str = "usage: %prog -c json_config_file [-j num_jobs]"
    parser = optparse.OptionParser(usage_str)
    parser.add_option("-c", dest="config", type="string",
            help="path to the json file with the configuration parameters")
    parser.add_option("-j", dest="npjobs", type="int",
            help="number of parallel jobs to use", default = mp.cpu_count())

    options, args  = parser.parse_args()

    if not options.config:
        parser.error("missing json config file")

    return (options, args)

def skip_existing_files(l,path,ext = "csv"):
    f = glob.glob("{}/*.{}".format(path, ext))
    skipl = list(map(os.path.basename, f))
    newl = [x for x in l if os.path.basename(x) not in skipl]
    return (newl, skipl)

def load_config(conf_file):
    config = utils.load_json(conf_file)
    current_file = os.path.realpath(__file__)
    schema_path = os.path.dirname(current_file)
    schema_file = "{}/../jsons/add_feature_config_schema.json".format(schema_path)
    schema = utils.load_json(schema_file)
    jsch.validate(config, schema)
    return config

def add_new_features(infile, stations, timezone, features, outpath):
    df = utils.read_solarnc_csv(infile, timezone)
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
    outfile = "{}/{}.csv".format(outpath, day)
    utils.save_solarnc_csv(df, outfile)

def print_stations(stations):
    print("Considering the following stations: ")
    for sta in stations:
        print(sta)

def print_features(features):
    print("Computing the following features:")
    for f in features:
        if 'args' in f:
            msg = '\t{}, with args: {}'.format(f['function'], f['args'])
        else:
            msg = '\t{}'.format(f['function'])
        print(msg)

def add_new_features_unpack(arg):
    add_new_features(*arg)

def main():
    options, args = parse_options()

    config = load_config(options.config)
    print("Correct config file {}".format(options.config))
    stations = config['stations']
    print_stations(stations)
    features = config['features']
    print_features(features)
    if not os.path.exists(config['outpath']):
        os.makedirs(config['outpath'])

    infiles  = glob.glob("{}/*.csv".format(config['path']))
    if config['skip existing']:
        infiles, skipped = skip_existing_files(infiles, config['outpath'])
        print("Skipped files:")
        print(skipped)

    print("Input files from: {}".format(config['path']))
    print(infiles)
    timezone = config['timezone']
    print("Timezone is: {}".format(timezone))
    outpath = config['outpath']
    print("Output files to: {}".format(outpath))

    njobs = len(infiles)
    args = [(infile, stations, timezone, features, outpath) for infile in infiles]
    j = 0;
    print("\r{}/{}".format(j,njobs), end='')
    if options.npjobs > 1:
        p = mp.Pool(options.npjobs)
        for x in p.imap_unordered(add_new_features_unpack, args):
            j += 1
            print("\r{}/{}".format(j,njobs), end='')
    else:
        for argtup in args:
            add_new_features_unpack(argtup)
            j += 1
            print("\r{}/{}".format(j,njobs), end='')
    print("")


if __name__ == "__main__":
    main()
