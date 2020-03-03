#!/usr/bin/env python3
import csv
import datetime as dt
import os
import multiprocessing as mp
import solarnc as snc
import glob
import pandas as pd

# original columns in the file
data_columns = ["Seconds", "Year", "DOY", "HST",
        "GHI DH3",
        "GHI DH4",
        "GHI DH5",
        "GHI DH10",
        "GHI DH11",
        "GHI DH9",
        "GHI DH2",
        "GHI DH1",
        "Global Tilt DH1",
        "GHI AP6",
        "Global Tilt AP6",
        "GHI AP1",
        "GHI AP3",
        "GHI AP5",
        "GHI AP4",
        "GHI AP7",
        "GHI DH6",
        "GHI DH7",
        "GHI DH8"]

def format_data(infile,outpath, ghi_columns, rejectpath):
    base = os.path.basename(infile)
    day = os.path.splitext(base)[0]
    df = pd.read_csv(infile, header=None, names=data_columns)

    dt = pd.to_datetime(df.Seconds.apply(str) + ' ' + df.Year.apply(str) + ' '\
            + df.DOY.apply(str) + ' ' + df.HST.apply(str), \
            format='%S %Y %j %H%M')
    dt = dt.dt.tz_localize('HST')
    df = pd.DataFrame(dict([('datetime', dt)] + \
            [(g, df[g]) for g in ghi_columns]))
    df.set_index('datetime', inplace=True)
    # small negative values are truncated to 0
    df[df.lt(0) & df.gt(-1)] = 0
    # large negative values are errors
    rem_neg_cols = df.lt(0).sum().gt(0).sum()
    if rem_neg_cols > 0:
        snc.save_csv(df, "{}/{}.csv".format(rejectpath, day))
    else:
        snc.save_csv(df, "{}/{}.csv".format(outpath, day))

def nrelformat(dtset, fconfig, npjobs):
    path = dtset['path']
    print("Input files:")
    infiles  = glob.glob("{}/*.txt".format(path))
    print(infiles)

    outpath = fconfig['outpath']

    if 'rejectpath' not in fconfig:
        print("Error: rejectpath property missing")
        os._exit(-1)

    rejectpath = fconfig['rejectpath']
    print("Rejected files to {}".format(rejectpath))
    if not os.path.exists(rejectpath):
        os.makedirs(rejectpath)

    stations = fconfig['stations']
    ghi_columns = ["GHI {}".format(sta) for sta in stations]

    arglist = [(f, outpath, ghi_columns, rejectpath) for f in infiles]
    snc.runjobs(format_data, arglist, npjobs)
