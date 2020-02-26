#!/usr/bin/env python3

import pandas as pd
import optparse
import os
import glob
from pytimeparse.timeparse import timeparse
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

def print_stations(stations):
    print("Considering the following stations: ")
    for sta in stations:
        print(sta)

def get_ttsets(config):
    trainset_fname, testset_fname = snc.get_ttlist_names(config)
    trainset = snc.read_list(trainset_fname)
    testset = snc.read_list(testset_fname)
    return (trainset, testset)

def check_fsparams(fsconfig):
    fsparams = fsconfig.copy()
    outpath = fsconfig['outpath']
    period = fsconfig['period']
    window = fsconfig['window']
    upint = fsconfig['updating interval']
    leadtime = fsconfig['lead time']
    features = fsconfig['features']

    if timeparse(upint) % timeparse(period) != 0:
        print("Error: update interval should be a multiple of period")
        raise ValueError

    if timeparse(window) % timeparse(upint) != 0:
        print("Error: window should be a multiple of the update interval")
        raise ValueError

    if timeparse(leadtime) % timeparse(upint) != 0:
        print("Error: the lead time should be a multiple of the update interval")
        raise ValueError

    lags = int(timeparse(window) / timeparse(period))
    fsparams['lags'] = lags
    fsparams['lead time'] = int(timeparse(leadtime)/timeparse(upint))
    fsparams['updating interval'] = int(timeparse(upint)/timeparse(period))

    print("Output goes to {}".format(outpath))
    print("Sampling period is {}".format(period))
    print("Window in the past of: {}".format(window))
    print("Lags (window/period): {}".format(lags))
    print("Station dependent lagged features: {}".\
            format(features['station lagged']))
    print("Station dependent unlagged features: {}".\
            format(features['station unlagged']))
    print("Station independent lagged features: {}".\
            format(features['lagged']))
    print("Station independent unlagged features: {}".\
            format(features['unlagged']))

    print("Updating interval is {}".format(upint))
    print("Lead time is {}. Number of predict intervals is {}".\
            format(leadtime, fsparams['lead time']))
    print("Prediction target is: {}".format(fsparams['target']))

    return fsparams

def get_lagged_vars(stations, fsparams):
    features = fsparams["features"]
    stav = features["station lagged"]
    columns = []
    for sta in stations:
        columns.extend(["{} {}".format(v,sta['name']) for v in stav])
    columns.extend(features["lagged"])
    return columns

def get_unlagged_vars(stations, fsparams):
    features = fsparams["features"]
    stav = features["station unlagged"]
    columns = []
    for sta in stations:
        columns.extend(["{} {}".format(v,sta['name']) for v in stav])
    columns.extend(features["unlagged"])
    return columns

def get_lagged_data(df, columns, lags):
    newd = {}
    for c in columns:
        newnames = ["{} lag{}".format(c,l) for l in range(lags)]
        newcols = [df[c].shift(l) for l in range(lags)]
        newd.update(list(zip(newnames, newcols)))
    return newd

def get_target_data(df, stations, fsparams):
    lt = fsparams['lead time'] # units are updating interval
    ui = fsparams['updating interval'] # number of periods
    tname = fsparams['target']
    newd = {}
    for sta in stations:
        staname = sta['name']
        target = df["{} {}".format(tname,staname)]
        newnames = ["{} {} t{}".format(tname,staname,u) for u in range(1,lt+1)]
        newcols = [target.shift(-u*ui) for u in range(1,lt+1)]
        newd.update(list(zip(newnames, newcols)))
    return newd

def fselect(infile, stations, timezone, fsparams):
    outpath = fsparams['outpath']
    base = os.path.basename(infile)
    day = os.path.splitext(base)[0]

    # Select feature and target columns and resample to period
    period = fsparams['period']
    lags = fsparams['lags']
    lagged = get_lagged_vars(stations, fsparams)
    unlagged = get_unlagged_vars(stations, fsparams)
    fcolumns = lagged.copy()
    fcolumns.extend(unlagged)
    tcolumns = ["{} {}".format(fsparams['target'],sta['name'])\
            for sta in stations]
    columns = list(set().union(fcolumns,tcolumns))
    df = snc.read_csv(infile, timezone)[columns]
    df = df.resample(period, closed='right', label='right').mean()

    # build feature matrix (1 row is a feature vector)
    ldata = get_lagged_data(df, lagged, lags)
    fdf = pd.DataFrame(ldata, index = df.index).dropna()
    df = df.reindex(fdf.index)
    fdf = pd.concat([fdf, df[unlagged]], axis=1)

    # build target matrix
    tdata = get_target_data(df, stations, fsparams)
    tdf = pd.DataFrame(tdata, index = df.index).dropna()
    fdf = fdf.reindex(tdf.index)

    # save files
    snc.save_csv(fdf, "{}/{}_features.csv".format(outpath, day))
    snc.save_csv(tdf, "{}/{}_targets.csv".format(outpath, day))

def main(options, margs):
    config = snc.load_config(options.config)
    print("Correct config format")

    dtset = config['dataset']
    print("Dataset: {}".format(dtset['name']))
    timezone = dtset['timezone']
    print("Timezone is: {}".format(timezone))

    fconfig = config['format']
    stations = [sta for sta in dtset['stations'] \
            if sta['name'] in fconfig['stations']]
    print_stations(stations)

    infiles = snc.get_split_infiles(config)
    print("Input files:")
    print(infiles)

    fsconfig = config['fselect']
    outpath = fsconfig['outpath']
    if not os.path.exists(outpath):
        os.makedirs(outpath)

    fsparams = check_fsparams(fsconfig)

    args = [(f, stations, timezone, fsparams) for f in infiles]
    snc.runjobs(fselect, args, options.npjobs)

if __name__ == "__main__":
    main(*parse_options())
