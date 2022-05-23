#!/usr/bin/env python3
import optparse
import os
import solarnc as snc
import glob
import multiprocessing as mp

def parse_options():
    usage_str = "%prog -c json_config_file [-j num_jobs]"
    parser = optparse.OptionParser(usage_str)
    parser.add_option("-c", dest="config", type="string",
            help="path to the json file with the configuration parameters")
    parser.add_option("-j", dest="npjobs", type="int",
            help="number of parallel jobs to use", default = mp.cpu_count())

    options, args  = parser.parse_args()
    if not options.config:
        parser.error("missing json config file")
    return (options, args)

def main(options, args):
    config = snc.load_config(options.config)
    print("Correct config format")

    dtset = config['dataset']
    print("Dataset: {}".format(dtset['name']))
    path = dtset['path']
    print("Input files from {}".format(path))

    fconfig = config['format']
    outpath = fconfig['outpath']
    print("Output files to {}".format(outpath))
    outpath = fconfig['outpath']
    if not os.path.exists(outpath):
        os.makedirs(outpath)

    stations = fconfig['stations']
    print("Selected stations:")
    print(stations)

    try:
        module = __import__(fconfig['module'])
        fun = getattr(module, fconfig['fname'])
    except AttributeError:
        print("Error: could not find function {} in module {}"\
                    .format(fconfig['fname'],fconfig['module']))
    else:
        fun(dtset, fconfig, options.npjobs)

if __name__=="__main__":
    main(*parse_options())
