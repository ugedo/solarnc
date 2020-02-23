#!/usr/bin/env python3
import csv
import datetime as dt
import os
import multiprocessing as mp
import solarnc as snc
import glob

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

def format_data(infile,outpath, ghi_index, header, start, end, rejectpath):
    base = os.path.basename(infile)
    day = os.path.splitext(base)[0]
    numlines = 0
    nneg = [0] * 16
    ifile = open(infile, 'r')
    ofilename = "{}/{}.csv".format(outpath, day)
    ofile = open(ofilename, 'w')
    ofile.write(header)

    reader = csv.reader(ifile)
    for f in reader:
        numlines += 1
        if int(f[3]) < start or int(f[3]) > end:
            continue
        hh = int(f[3][:-2])
        mm = int(f[3][-2:])
        dtime = dt.datetime(int(f[1]), 1, 1,hh,mm,int(f[0])) + dt.timedelta(int(f[2]) - 1)
        outline = "{}-10:00,".format(dtime)
        ghis = [float(f[g]) for g in ghi_index]
        nneg = [nneg[i] + 1 if ghis[i] < 0.0 else nneg[i] for i in range(16)]
        outline += ','.join(["{}".format(g) for g in ghis])
        ofile.write(outline + '\n')

    ifile.close()
    ofile.close()
    if any(x >= 0.1*numlines for x in nneg):
        os.rename(ofilename, "{}/{}.csv".format(rejectpath, day))

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
    ghi_index = [data_columns.index(c) for c in ghi_columns]

    if 'start' in fconfig:
        start = fconfig['start']
    else:
        start = 730

    if 'end' in fconfig:
        end = fconfig['end']
    else:
        end = 1730

    header = ','.join(["datetime"] + ghi_columns)
    header = header + '\n'
    arglist = [(f, outpath, ghi_index, header, start, end, rejectpath) \
            for f in infiles]
    snc.runjobs(format_data, arglist, npjobs)
