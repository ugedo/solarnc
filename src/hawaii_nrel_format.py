#!/usr/bin/env python3
import pytz
import csv
import datetime as dt
import optparse
import os
import multiprocessing as mp

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

## Dict with (latitude,longitud) tuples
#sensor_data = {
#	"DH3": (21.31236, -158.08463),
#	"DH4": (21.31303, -158.08505),
#	"DH5": (21.31357, -158.08424),
#	"DH10": (21.31183, -158.08554),
#	"DH11": (21.31042, -158.0853),
#	"DH9": (21.31268, -158.08688),
#	"DH2": (21.31451, -158.08534),
#	"DH1": (21.31533, -158.087),
#	"AP6": (21.30812, -158.07935),
#	"AP1": (21.31276, -158.08389),
#	"AP3": (21.31281, -158.08163),
#	"AP5": (21.30983, -158.08249),
#	"AP4": (21.31141, -158.07947),
#	"AP7": (21.31478, -158.07785),
#	"DH6": (21.31179, -158.08678),
#	"DH7": (21.31418, -158.08685),
#	"DH8": (21.31034, -158.08675)}

def parse_options():
    usage_str = \
''' %prog -p data_directory_path -o output path [-j num_jobs] [-s start_time]
                        [-e end_time]

    It reads al the txt files from the data_directory_path, were one file per
    day is expected, with the samples of each NREL station for that day. It
    writes one output file per day in the output path with the formated data.

    The outputs are csv files, where the first column is in datetime localized
    format (sample instant), and the rest of the columns contain the GHI values,
    each column representing one of the NREL stations.
    '''

    parser = optparse.OptionParser(usage_str)
    parser.add_option("-p", dest="path", type="string",
            help="path to the raw data directory")
    parser.add_option("-o", dest="outpath", type="string",
            help="output directory")
    parser.add_option("-j", dest="npjobs", type="int",
            help="number of parallel jobs to use", default = mp.cpu_count())
    parser.add_option("-s", dest="start", type="int",
            help="start time %HH%MM", default = 730)
    parser.add_option("-e", dest="end", type="int",
            help="end time %HH%MM", default = 1730)

    options, args  = parser.parse_args()

    if not options.path:
        parser.error("missing path")

    if not options.outpath:
        parser.error("missing outpath")

    options.path = options.path.rstrip("/")
    options.outpath = options.outpath.rstrip("/")

    return (options, args)


def get_input_files(path):
    files = []
    for f in os.listdir(path):
        if f.endswith(".txt"):
            files.append("{}/{}".format(path, f))
    return files


def format_data(infile, common):
    outpath, ghi_index, header, start, end, outpath_reject  = common

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
        outline = "{},".format(dtime)
        ghis = [float(f[g]) for g in ghi_index]
        nneg = [nneg[i] + 1 if ghis[i] < 0.0 else nneg[i] for i in range(16)]
        outline += ','.join(["{}".format(g) for g in ghis])
        ofile.write(outline + '\n')

    ifile.close()
    ofile.close()
    if any(x >= 0.1*numlines for x in nneg):
        if not os.path.exists(outpath_reject):
            os.makedirs(outpath_reject)
        os.rename(ofilename, "{}/{}.csv".format(outpath_reject, day))

def format_data_unpack(arg):
    format_data(*arg)

def get_ghi_index():
    return list(range(4,12)) + [13,15] + list(range(17,23))

def get_header(l):
    header = "datetime"
    for g in [data_columns[i] for i in l]:
        header += ",{}".format(g)
    return header + '\n'

def main():
    options, args = parse_options()
    infiles = get_input_files(options.path)
    rejected_files = {}

    if not os.path.exists(options.outpath):
        os.makedirs(options.outpath)

    outpath_reject = '{}/reject'.format(options.outpath)
    ghi_index = get_ghi_index()
    header = get_header(ghi_index)
    p = mp.Pool(options.npjobs)
    common = (options.outpath, ghi_index, header, options.start, options.end, \
            outpath_reject)
    arglist = [(inf, common) for inf in infiles]
    njobs = len(infiles)
    j = 0;
    for x in p.imap(format_data_unpack, arglist):
        j += 1
        print("\r{}/{}".format(j,njobs), end='')
    print("")

if __name__=="__main__":
    main()
