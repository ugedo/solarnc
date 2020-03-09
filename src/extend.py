#!/usr/bin/env python3

import pandas as pd
import optparse
import os
import glob
import multiprocessing as mp
import solarnc as snc


def parse_options():
    usage_str = "%prog -c json_config_file [-j num_jobs]"
    parser = optparse.OptionParser(usage_str)
    parser.add_option("-c", dest="config", type="string",
            help="path to the json file with the configuration parameters")
    parser.add_option("-j", dest="npjobs", type="int",
            help="number of parallel jobs to use", default = mp.cpu_count())

    options, args  = parser.parse_args()
    if not options.config: parser.error("missing json config file")
    return (options, args)

def add_new_variables(infile, stations, timezone, functions):
    df = snc.read_csv(infile, timezone)
    for f in functions:
        try:
            module = __import__(f['module'])
            fun = getattr(module, f['fname'])
            if not callable(fun):
                print("fname should be a callable python object")
                raise ValueError
        except AttributeError:
            print("Error: could not find function {} in module {}"\
                    .format(f['fname'],f['module']))
        else:
            if 'args' in f:
                args = tuple(f['args'])
                df = fun(df, f['skip existing'], stations, *args)
            else:
                df = fun(df, f['skip existing'], stations)
    snc.save_csv(df, infile)

def print_stations(stations):
    print("Considering the following stations: ")
    for sta in stations:
        print(sta)

def print_functions(functions):
    print("Applying the following functions on the files:")
    for f in functions:
        if 'args' in f:
            msg = '\t{}, with args: {}'.format(f['fname'], f['args'])
        else:
            msg = '\t{}'.format(f['fname'])
        print(msg)

def main(options, args):
    config = snc.load_config(options.config)
    if 'extend' not in config:
        print("Error: no extend property in {}".format(options.config))
        os._exit(-1)
    else:
        extend = config['extend']
        print("Correct config format")

    dtset = config['dataset']
    print("Dataset: {}".format(dtset['name']))
    timezone = dtset['timezone']
    print("Timezone is: {}".format(timezone))

    fconfig = config['format']
    stations = [sta for sta in dtset['stations'] if sta['name'] in fconfig['stations']]
    print_stations(stations)

    path = fconfig['outpath']
    print("Extending files from: {}".format(path))
    infiles  = glob.glob("{}/*.csv".format(path))
    print(infiles)

    functions = extend['functions']
    print_functions(functions)

    args = [(f, stations, timezone, functions) for f in infiles]
    snc.runjobs(add_new_variables, args, options.npjobs)

if __name__ == "__main__":
    main(*parse_options())
