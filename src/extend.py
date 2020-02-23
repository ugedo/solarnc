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

def skip_existing_files(l,path,ext = "csv"):
    f = glob.glob("{}/*.{}".format(path, ext))
    skipl = list(map(os.path.basename, f))
    newl = [x for x in l if os.path.basename(x) not in skipl]
    return (newl, skipl)

def add_new_variables(infile, stations, timezone, functions, outpath):
    df = snc.read_csv(infile, timezone)
    for f in functions:
        try:
            module = __import__(f['module'])
            fun = getattr(module, f['fname'])
        except AttributeError:
            print("Error: could not find function {} in module {}"\
                    .format(f['fname'],f['module']))
        else:
            if 'args' in f:
                args = tuple(f['args'])
                fun(df, f['skip existing'], f['name'], stations, *args)
            else:
                fun(df, f['skip existing'], f['name'], stations)

    base = os.path.basename(infile)
    day = os.path.splitext(base)[0]
    outfile = "{}/{}.csv".format(outpath, day)
    snc.save_csv(df, outfile)

def print_stations(stations):
    print("Considering the following stations: ")
    for sta in stations:
        print(sta)

def print_functions(functions):
    print("Applying the following functions on the files:")
    for f in functions:
        if 'args' in f:
            msg = '\t{}, with args: {} -> {}'.format(f['fname'], f['args'],\
                    f['name'])
        else:
            msg = '\t{} -> {}'.format(f['fname'],f['name'])
        print(msg)

def main():
    options, args = parse_options()

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
    print("Input files from: {}".format(path))
    infiles  = glob.glob("{}/*.csv".format(path))
    print(infiles)

    outpath = extend['outpath']
    print("Output files to: {}".format(outpath))
    if not os.path.exists(outpath):
        os.makedirs(outpath)

    functions = extend['functions']
    print_functions(functions)

    if extend['skip existing files']:
        infiles, skipped = skip_existing_files(infiles, outpath)
        print("Skipped files:")
        print(skipped)

    args = [(infile, stations, timezone, functions, outpath) for infile in infiles]
    snc.runjobs(add_new_variables, args, options.npjobs)

if __name__ == "__main__":
    main()
