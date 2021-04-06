# -*- coding: utf-8 -*-
from datetime import timedelta
from datetime import datetime
import multiprocessing as mp
import solarnc as snc
import pandas as pd
import numpy as np
import optparse
import glob
import sys
import csv
import os


def import_metas_data(infiles, flog, columns_selected):
    horasluz = {}
    for d in infiles:
        df = snc.read_csv(d, [1, 2, 3])
        # df.droplevel([1, 2], axis='columns')
        columns_selected_metas = ['Elevacion'] + columns_selected
        df = df[columns_selected_metas]
        df = df[df[('Elevacion', 'deg', 'Avg')] > 7]
        amanecer = min(df.index)
        ocaso = max(df.index)
        dia = amanecer.date()
        if dia not in horasluz:
            horasluz[dia] = (amanecer, ocaso)
        else:
            horasluz[dia] = (min(amanecer, horasluz[dia][0]), max(ocaso, horasluz[dia][1]))
        if df.loc[amanecer][('Elevacion', 'deg', 'Avg')] > 7.2 or \
                df.loc[ocaso][('Elevacion', 'deg', 'Avg')] > 7.2:
            print(5, amanecer, ocaso, file=flog, sep=',')
            print('Error en amanecer/ocaso', amanecer, ocaso, file=sys.stderr)
        df.drop(columns=["Elevacion"], level=0, inplace=True)

        if 'df_station' not in locals():
            df_station = pd.DataFrame(columns=df.columns)
        df_station = pd.concat([df_station, df])

    return horasluz, df_station


def import_station_data(infiles, s, horasluz, flog, columns_selected):
    for d in infiles:
        df = snc.read_csv(d, [1, 2, 3])
        df = df[columns_selected]
        dia = df.index[1].date()

        if 'df_station' not in locals():
            df_station = pd.DataFrame(columns=df.columns)

        if dia not in horasluz:
            print(6, dia, s, file=flog, sep=',')
            print('Geometría solar no disponible para el día', dia, file=sys.stderr)
        else:
            df = df[(df.index >= horasluz[dia][0]) & (df.index <= horasluz[dia][1])]
            df_station = pd.concat([df_station, df])

    return df_station


def format_data(df, outpath, rejectpath, horasluz, flog, columns_selected):
    if horasluz is not None:
        dfd = df[(df.index >= horasluz[0]) & (df.index <= horasluz[1])]
        day = dfd.index[0].date()
        # small negative values are truncated to 0
        # dfd[dfd.lt(0) & dfd.gt(-1)] = 0 # TODO. Uso deprecado. usar loc[].
        # TODO Error, Prometeo y Desalacion column type is object not float. Comprobar...
        # large negative values are errors
        rem_neg_cols = dfd.lt(0).sum().gt(0).sum()
        if rem_neg_cols > 0:
            snc.save_csv(dfd, "{}/{}.csv".format(rejectpath, day))
        else:
            snc.save_csv(dfd, "{}/{}.csv".format(outpath, day))
    else:
        print(6, file=flog, sep=',')
        print('Geometría solar no disponible para el día desconocido', file=sys.stderr)


def ciematformat(dtset, fconfig, npjobs):
    path = dtset['path']
    outpath = fconfig['outpath']

    if 'rejectpath' not in fconfig:
        print("Error: rejectpath property missing")
        os._exit(-1)

    rejectpath = fconfig['rejectpath']
    print("Rejected files to {}".format(rejectpath))
    if not os.path.exists(rejectpath):
        os.makedirs(rejectpath)

    stations = fconfig['stations']
    print("Selected stations:")
    print(stations)

    columns_selected = fconfig['columns']
    print("Selected columns:")
    print(columns_selected)

    # Se crea y abre fichero de log
    flog = datetime.now()
    flog = os.path.dirname(os.path.realpath(__file__)) + "/../logs/" + flog.strftime('%Y-%m-%d_%H%M') + '.txt'
    if not os.path.exists(os.path.dirname(flog)):
        os.makedirs(flog)
    flog = open(flog, 'w')
    print('Loging...')
    print('0', datetime.now().strftime('%H:%M:%S.%f'), file=flog, sep=',')

    print('Reading METAS data')
    infiles = glob.glob("{}/METAS/Minutos/*.dat".format(path))
    horasluz, df_metas = import_metas_data(infiles, flog, columns_selected)

    print('Reading stantions data')
    for s in stations:
        if s != 'METAS':
            print('   ' + s)
            infiles = glob.glob("{}/{}/Minutos/*.dat".format(path, s))
            df_station = import_station_data(infiles, s, horasluz, flog, columns_selected)
            df_metas = df_metas.join(df_station, rsuffix=str('_' + s))
            # Una vez concatenado se elimina para ahorrar espacio en memoria
            df_station = None

    day = min(horasluz.keys())
    last_day = max(horasluz.keys())
    while day <= last_day:
        format_data(df_metas, outpath, rejectpath, horasluz[day], flog, columns_selected)
        day += timedelta(days=1)


    # Se cierra el fichero de log
    print('0', datetime.now().strftime('%H:%M:%S.%f'), file=flog, sep=',')
    flog.close()
