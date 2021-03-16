#!/usr/bin/env python3
import csv
import datetime as dt
import os
import multiprocessing as mp
import solarnc as snc
import glob
import pandas as pd

# original columns in the file
data_columns = ["TIMESTAMP",
                "CESA NORTE",
                "CESA SUR",
                "PROMETEO",
                "CRS",
                "DESALACION",
                "NEW METEO",
                "HORNO",
                "METAS"]


def format_data(infile, outpath, ghi_columns, rejectpath):
    base = os.path.basename(infile)
    day = os.path.splitext(base)[0]
    df = pd.read_csv(infile, header=None, names=data_columns)

    dt = pd.to_datetime(df.TIMESTAMP.apply(str),
                        format='%Y-%m-%d %H:%M:%S')
    dt = dt.dt.tz_localize('UTC')
    df = pd.DataFrame(dict([('datetime', dt)] +
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


def ciematformat(dtset, fconfig, npjobs):
    path = dtset['path']
    print("Input files:")
    infiles = glob.glob("{}/*.txt".format(path))
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
