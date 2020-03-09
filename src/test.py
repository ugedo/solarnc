#!/usr/bin/env python3
import optparse
import os
import solarnc as snc
import pandas as pd
import multiprocessing as mp
import random
from sklearn.neural_network import MLPRegressor
from sklearn.model_selection import GridSearchCV
from sklearn import metrics
import joblib
import itertools
import solarnc as snc
import numpy as np

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

def print_model(m):
    mtype = m['model type']
    if mtype == 'MLPRegressor':
        print(mtype)
        hp = m['hyperparameters']
        print("hidden layers {}".format(hp['hidden layers']))
        print("alpha {}".format(hp['alpha']))
    elif mtype == 'Linear':
        print(mtype)

def main(options, args):
    config = snc.load_config(options.config)
    print("Correct config format")

    spconfig = config['split']
    testset_fname = "{}/testset.csv".format(spconfig['outpath'])
    testset = snc.read_list(testset_fname)
    print("Testset from {} is:".format(testset_fname))
    print(testset)

    fsconfig = config['fselect']
    ffiles  = ["{}/{}_features.csv".format(fsconfig['outpath'], f)\
            .replace("//","/") for f in testset]
    print("Features files:")
    print(ffiles)

    tfiles  = ["{}/{}_targets.csv".format(fsconfig['outpath'], f)\
            .replace("//","/") for f in testset]
    print("Input targets files:")
    print(tfiles)

    tzone = config['dataset']['timezone']
    X, Y = snc.get_feature_target_data(ffiles, tfiles, tzone)

    tconfig = config['train']
    mpath = tconfig['outpath']
    #if not os.path.exists(outpath):
    #    os.makedirs(outpath)

    for m in tconfig['models']:
        mfile = "{}/{}.joblib".format(mpath,m['filename'])
        sklm = joblib.load(mfile)
        Yp = sklm.predict(np.array(X))
        snc.save_csv(pd.DataFrame(Yp, index=Y.index, columns = Y.columns), "{}/{}_Yp.csv"\
                .format(mpath,m['filename']))
        snc.save_csv(Y, "{}/{}_Y.csv"\
                .format(mpath,m['filename']))
        rmse = np.sqrt(((np.array(Y)-Yp)**2).mean().mean())
        print("RMSE for model {} is: {}".format(mfile, rmse))

if __name__=="__main__":
    main(*parse_options())
