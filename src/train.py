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

def get_interval_string(horizon, interval):
    if interval[0] == '-':
        istr = '[t+{}{},t+{}]'.format(horizon,interval,horizon)
    elif interval[0] == '+':
        istr = '[t+{},t+{}{}]'.format(horizon,horizon,interval)
    else:
        istr = '[t+{},t+{}+{}]'.format(horizon,horizon,interval)
    return istr

def print_model(m):
    print(m['model type'])

def check_config(config):
    outpath = config['outpath']
    period = config['period']
    window = config['window']
    features = config['features']

    if timeparse(window) % timeparse(period) != 0:
        print("Error: window should be a multiple of the sampling period")
        raise ValueError
    lags = int(timeparse(window) / timeparse(period))

    print("Output goes to {}".format(outpath))
    fpath = "{}/features".format(outpath)
    print("Features go to {}".format(fpath))
    if not os.path.exists(fpath):
        os.makedirs(fpath)
    tpath = "{}/targets".format(outpath)
    print("Targets go to {}".format(tpath))
    if not os.path.exists(tpath):
        os.makedirs(tpath)

    print("Sampling period is {}".format(period))
    print("Window in the past of: {}".format(window))
    print("Lags (window/period): {}".format(lags))

    print("Lagged features for all stations: {}".\
            format(features['station lagged']))
    print("Unlagged features for all stations: {}".\
            format(features['station unlagged']))
    print("Single lagged features: {}".\
            format(features['lagged']))
    print("Single unlagged features: {}".\
            format(features['unlagged']))

    fvar = config['forecasting target']['variable']
    horizon = config['forecasting target']['horizon']
    interval = config['forecasting target']['interval']
    fsta = config['forecasting target']['stations']

    fvarlist = ["{} {}".format(sta, fvar) for sta in fsta]
    print("Forecasting variables: {}".format(fvarlist))
    print("Forecasting horizon: t+{}".format(horizon))
    istr = get_interval_string(horizon, interval)
    print("Forecasting interval {}".format(istr))

    print("Train model:")
    print_model(config['model'])

def get_lagged_vars(stations, fsconfig):
    features = fsconfig["features"]
    stav = features["station lagged"]
    columns = []
    for sta in stations:
        columns.extend(["{} {}".format(v,sta['name']) for v in stav])
    columns.extend(features["lagged"])
    return columns

def get_unlagged_vars(stations, fsconfig):
    features = fsconfig["features"]
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
        newcols = [df[c].shift(s) for s in range(lags)]
        newd.update(list(zip(newnames, newcols)))
    return newd

def get_target_data(df, tstations, fvar, horizon, interval):
    def get_values(tdata, horizon, interval):
        off1 = pd.Timedelta(horizon)
        off2 = off1
        if interval[0] == '-':
             off1 += pd.Timedelta(interval)
        else:
             off2 += pd.Timedelta(interval)
        th = tdata.index.map(lambda s: tdata.loc[s + off1:s + off2].mean())
        return pd.Series(th, index=tdata.index)

    newd = {}
    for sta in tstations:
        fdata = df['{} {}'.format(fvar,sta)]
        tname = "{} {} {}".format(fvar,sta,horizon)
        tdata = get_values(fdata, horizon, interval)
        newd[tname] = tdata
    return newd

def get_offset(df, period):
    T = df.index.inferred_freq
    if T in ['Y', 'M', 'W', 'D', 'T', 'S', 'L', 'U', 'N']:
        T = '1' + T
    return (pd.Timedelta(period) - pd.Timedelta(T))

def fselect(infile, stations, timezone, tconfig):
    outpath = tconfig['outpath']
    base = os.path.basename(infile)
    day = os.path.splitext(base)[0]

    period = tconfig['period']
    window = tconfig['window']
    lags = int(timeparse(window) / timeparse(period))
    fvar = tconfig['forecasting target']['variable']
    horizon = tconfig['forecasting target']['horizon']
    interval = tconfig['forecasting target']['interval']
    tstations = tconfig['forecasting target']['stations']

    # build features matrix
    lagged = get_lagged_vars(stations, tconfig)
    unlagged = get_unlagged_vars(stations, tconfig)
    targets = ["{} {}".format(fvar,sta) for sta in tstations]
    columns = list(set().union(lagged, unlagged, targets))

    # resample data, start clustering from the first sample to the right
    # label with the last sample of the cluster
    df = snc.read_csv(infile, timezone)[columns]
    offset = get_offset(df, period)
    df = df.resample(period, loffset=offset).mean()

    # build feature matrix (1 row is a feature vector)
    ldata = get_lagged_data(df, lagged, lags)
    fdf = pd.DataFrame(ldata, index=df.index).dropna()
    df = df.reindex(fdf.index)
    fdf = pd.concat([fdf, df[unlagged]], axis=1)

    # build target matrix
    tdata = get_target_data(df, tstations, fvar, horizon, interval)
    tdf = pd.DataFrame(tdata, index=df.index).dropna()
    fdf.reindex(tdf.index)

    # save files
    snc.save_csv(fdf, "{}/features/{}.csv".format(outpath, day))
    snc.save_csv(tdf, "{}/targets/{}.csv".format(outpath, day))

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

    infiles  = glob.glob("{}/*.csv".format(fconfig['outpath']))
    print("Input files:")
    print(infiles)

    tconfig = config['train']
    outpath = tconfig['outpath']
    if not os.path.exists(outpath):
        os.makedirs(outpath)

    check_config(tconfig)

    args = [(f, stations, timezone, tconfig) for f in infiles]
    snc.runjobs(fselect, args, options.npjobs)

if __name__ == "__main__":
    main(*parse_options())
