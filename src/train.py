#!/usr/bin/env python3
import optparse
import os
import solarnc as snc
import pandas as pd
import multiprocessing as mp
import random
from sklearn.neural_network import MLPRegressor
from sklearn.model_selection import GridSearchCV
from sklearn import linear_model
from sklearn import metrics
import joblib
import itertools
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

def log_gridsearch(gs, logf):
    logf.write("GridSearchCV results:\n")
    results = pd.DataFrame(gs.cv_results_)
    logf.write(str(results))
    logf.write("\n")
    logf.write("Best score: {}\n".format(gs.best_score_))
    logf.write("Best params: {}\n".format(str(gs.best_params_)))
    logf.write("Best index: {}\n".format(gs.best_index_))
    logf.write("Scorer: {}\n".format(str(gs.scorer_)))
    logf.write("number of splits: {}\n".format(gs.n_splits_))
    logf.write("refit time: {}\n".format(gs.refit_time_))

def train_MLPRegressor(m, X, Y, logfile, npjobs):
    alphas = m['alpha']
    hlnum = m['hidden layers count']
    hlsizes = tuple(m['hidden layers sizes'])
    hlayers = []
    for n in hlnum:
        hlayers += [x for x in itertools.product(hlsizes,repeat=n)]
    pgrid={'alpha': alphas, 'hidden_layer_sizes': hlayers}
    mlr = MLPRegressor(solver='lbfgs')

    gs = GridSearchCV(mlr, param_grid = pgrid, \
            scoring='neg_mean_squared_error', n_jobs=npjobs, cv = m['cv'])
    gs.fit(X,Y)

    with open(logfile, "w") as logf:
        logf.write("Searched for hidden layers: {}\n".format(hlayers))
        logf.write("Searched for alpha values: {}\n".format(alphas))
        log_gridsearch(gs, logf)

    return gs.best_estimator_

def train_Linear(m, X, Y, logfile, npjobs):
    reg = linear_model.LinearRegression()
    reg.fit(X,Y)
    with open(logfile, "w") as logf:
        logf.write("Results for Linear Model:\n")
        logf.write("Coefficients: {}\n".format(reg.coef_))
    return reg

def train_Ridge(m, X, Y, logfile, npjobs):
    alphas = m['alpha']
    pgrid={'alpha': alphas}
    reg = linear_model.Ridge()
    gs = GridSearchCV(reg, param_grid = pgrid, \
            scoring='neg_mean_squared_error', n_jobs=npjobs, cv = m['cv'])
    gs.fit(X,Y)

    with open(logfile, "w") as logf:
        logf.write("Searched for alpha values: {}\n".format(alphas))
        log_gridsearch(gs, logf)

    return gs.best_estimator_

def train_model(m, X, Y, outpath, skip, npjobs):
    ofile = "{}/{}.joblib".format(outpath, m['filename'])
    if skip and os.path.exists(ofile):
        return

    logfile = "{}/{}.log".format(outpath, m['filename'])
    mtype = m['model type']
    tfun = globals()["train_{}".format(mtype)]
    if not callable(tfun):
        print("Model {} is not supported".format(mtype))
        return
    mod = tfun(m, X, Y, logfile, npjobs)
    joblib.dump(mod, ofile)

def main(options, args):
    config = snc.load_config(options.config)
    print("Correct config format")

    spconfig = config['split']
    trainset_fname = "{}/trainset.csv".format(spconfig['outpath'])
    trainset = snc.read_list(trainset_fname)
    print("Trainset from {} is:".format(trainset_fname))
    print(trainset)

    fsconfig = config['fselect']
    ffiles  = ["{}/{}_features.csv".format(fsconfig['outpath'], f)\
            .replace("//","/") for f in trainset]
    print("Features files:")
    print(ffiles)

    tfiles  = ["{}/{}_targets.csv".format(fsconfig['outpath'], f)\
            .replace("//","/") for f in trainset]
    print("Input targets files:")
    print(tfiles)

    tconfig = config['train']
    outpath = tconfig['outpath']
    if not os.path.exists(outpath):
        os.makedirs(outpath)
    print("Models stored in {}".format(outpath))

    models = tconfig['models']
    for m in models:
        print(m)
    skip = tconfig['skip existing']
    if skip:
        print("Skipping existing models")

    tzone = config['dataset']['timezone']
    X, Y = snc.get_feature_target_data(ffiles, tfiles, tzone)

    for m in models:
        train_model(m, X, Y, outpath, skip, options.npjobs)

if __name__=="__main__":
    main(*parse_options())
