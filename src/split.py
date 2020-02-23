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

def split_random(infiles, outpath, args):
    if "train" not in args:
        print("Error: random methods requires train arg")
        os._exit(-1)

    percent = args['train']
    if percent < 1 or percent > 99:
        print("Error: train shoyld be in the range 1-99")
        os._exit(-1)

    nfiles = len(infiles)
    ntrain = round(nfiles * percent / 100)
    ntest = nfiles - ntrain
    print("Random split: train {} days, test {} days".format(ntrain, ntest))
    trainset = random.sample(infiles, ntrain)
    testset = [f for f in infiles if f not in trainset]
    #trainset = [os.path.basename(f) for f in trainset]
    #testset = [os.path.basename(f) for f in testset]
    return trainset, testset

def main(options, args):
    config = snc.load_config(options.config)
    print("Correct config format")

    if "extend" in config:
        extend = config['extend']
        path = extend['outpath']
    else:
        fconfig = config['format']
        path = fconfig['outpath']

    print("Input files from {}".format(path))
    infiles  = glob.glob("{}/*.csv".format(path))
    print(infiles)

    outpath = "{}/lists".format(path)
    if not os.path.exists(outpath):
        os.makedirs(outpath)
    print("Output files to {}".format(outpath))

    spconfig = config['split']
    method = spconfig['method']
    mname = "split_{}".format(method['name'])
    if "args" in method:
        args = method['args']
    else:
        args = {}

    if mname not in globals():
        print("Error: unknown method {}".method['name'])
        os._exit(-1)

    trainset_fname = "{}/trainset.csv".format(outpath)
    testset_fname = "{}/testset.csv".format(outpath)

    if spconfig['skip existing']:
        existing = glob.glob("{}/*.csv".format(outpath))
        if trainset_fname in existing and testset_fname in existing:
            print("Skipping split fase, lists already exist")
            os._exit(0)

    #TODO: check that it is a funtion/callable
    fun = globals()[mname]
    trainset, testset = fun(infiles, outpath, args)

    print("Train set:")
    print(trainset)
    snc.save_list(trainset, trainset_fname)

    print("test set:")
    print(testset)
    snc.save_list(testset, testset_fname)

if __name__=="__main__":
    main(*parse_options())
