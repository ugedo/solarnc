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

def print_horizon(h):
    ltime = h['lead time']
    interval = h['interval']
    if interval[0] == '-':
        istr = '(t+{}{},t+{})'.format(ltime,interval,ltime)
    elif interval[0] == '+':
        istr = '(t+{},t+{}{})'.format(ltime,ltime,interval)
    else:
        istr = '(t+{},t+{}+{})'.format(ltime,ltime,interval)
    print("\t- horizon t + {}, interval {}".format(ltime, istr))

def check_config(fsconfig):
    outpath = fsconfig['outpath']
    period = fsconfig['period']
    window = fsconfig['window']
    features = fsconfig['features']

    if timeparse(window) % timeparse(period) != 0:
        print("Error: window should be a multiple of the sampling period")
        raise ValueError
    lags = int(timeparse(window) / timeparse(period))

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

    print("Forecasted variable: {}".format(fsconfig['forecasted variable']))
    print("Forecasting horizons for instant t:")
    for h in fsconfig['horizons']:
        print_horizon(h)

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

def get_target_data(df, stations, fsconfig):
    def get_values(tdata, h):
        ltime = h['lead time']
        fint = h['interval']
        off1 = pd.Timedelta(ltime)
        off2 = off1
        if fint[0] == '-':
             off1 += pd.Timedelta(fint)
        else:
             off2 += pd.Timedelta(fint)
        th = tdata.index.map(lambda s: tdata.loc[s + off1:s + off2].mean())
        return pd.Series(th, index=tdata.index)

    fvar = fsconfig['forecasted variable']
    horizons = fsconfig['horizons']
    newd = {}
    for sta in stations:
        staname = sta['name']
        tdata = df['{} {}'.format(fvar,staname)]
        newnames = ["{} {} {}".format(fvar,staname,h['lead time']) \
                for h in horizons]
        newcols = [get_values(tdata, h) for h in horizons]
        newd.update(list(zip(newnames, newcols)))
    return newd

def get_offset(df, period):
    T = df.index.inferred_freq
    if T in ['Y', 'M', 'W', 'D', 'T', 'S', 'L', 'U', 'N']:
        T = '1' + T
    return (pd.Timedelta(period) - pd.Timedelta(T))

def fselect(infile, stations, timezone, fsconfig):
    outpath = fsconfig['outpath']
    base = os.path.basename(infile)
    day = os.path.splitext(base)[0]

    fvar = fsconfig['forecasted variable']
    period = fsconfig['period']
    window = fsconfig['window']
    lags = int(timeparse(window) / timeparse(period))

    # build features matrix
    lagged = get_lagged_vars(stations, fsconfig)
    unlagged = get_unlagged_vars(stations, fsconfig)
    targets = ["{} {}".format(fvar,sta['name']) for sta in stations]
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
    tdata = get_target_data(df, stations, fsconfig)
    tdf = pd.DataFrame(tdata, index=df.index).dropna()
    fdf.reindex(tdf.index)

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

    check_config(fsconfig)

    args = [(f, stations, timezone, fsconfig) for f in infiles]
    snc.runjobs(fselect, args, options.npjobs)

if __name__ == "__main__":
    main(*parse_options())
