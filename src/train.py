#!/usr/bin/env python3
import optparse
import os
import solarnc as snc
import glob
import multiprocessing as mp
import random

def parse_options():
    usage_str = "%prog -c json_config_file"
    parser = optparse.OptionParser(usage_str)
    parser.add_option("-c", dest="config", type="string",
            help="path to the json file with the configuration parameters")

    options, args  = parser.parse_args()
    if not options.config:
        parser.error("missing json config file")
    return (options, args)

def print_model(m):
    mtype = m['model type']
    if mtype == 'MLP':
        print(mtype)
        hp = m['hyperparameters']
        alpha("hidden layers {}".format(hp['hidden layers']))
        print("alpha {}".format(hp['alpha']))
    elif mtype == 'Linear':
        print(mtype)

def main(options, args):
    config = snc.load_config(options.config)
    print("Correct config format")

    fsconfig = config['fselect']
    ffiles  = glob.glob("{}/*_features.csv".format(fsconfig['outpath']))
    print("Input features files:")
    print(ffiles)

    tfiles  = glob.glob("{}/*_targets.csv".format(fsconfig['outpath']))
    print("Input targets files:")
    print(tfiles)

    spconfig = config['split']
    trainset_fname = "{}/trainset.csv".format(spconfig['outpath'])
    trainset = snc.read_list(trainset_fname)
    print("Trainset from {} is:".format(trainset_fname))
    print(trainset)

    tconfig = config['train']
    models = tconfig['models']
    for m in models:
        print_model(m)

if __name__=="__main__":
    main(*parse_options())
