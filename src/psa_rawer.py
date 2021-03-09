# -*- coding: utf-8 -*-
from datetime import datetime
import solarnc as snc
import pandas as pd
import numpy as np
import optparse
import glob
import os


def parse_options():
    usage_str = "%prog -c json_config_file "#[-j num_jobs]"
    parser = optparse.OptionParser(usage_str)
    parser.add_option("-c", dest="config", type="string",
            help="path to the json file with the configuration parameters")
#    parser.add_option("-j", dest="npjobs", type="int",
#            help="number of parallel jobs to use", default = mp.cpu_count())

    options, args = parser.parse_args()
    if not options.config:
        parser.error("missing json config file")
    return options, args


def getFromFile(f):
    # Apertura y carga del fichero en DataFrame
    df = pd.read_csv(f, header=[1, 2, 3])
    # Elimina columnas que no vamos a usar
    # df.drop(columns=["RN"], level=1, inplace=True)
    # Remplaza los 0 por NaN para omitirlos en el cálculo
    # df.replace(0, np.nan, inplace=True)
    df[df == 0] = np.nan
    # Remplaza las cadenas "NAN" por el valor np.nan (NaN de numpy)
    # df.replace("NAN", np.nan, inplace=True)
    # Covienrte la columna "TIMESTAMP" a tipo datatime
    df[["TIMESTAMP"]] = df[["TIMESTAMP"]].astype(np.datetime64)
    return df


# TODO filtrar datos con valores físicamente imposible, según las indicaciones de la BSRN (mirar en el libro)
# TODO generar log con los datos que se han filtrado y un código de error que indique el motivo
# TODO incluir en el log los momentos para los que alguna estación no tiene datos registrados
# TODO obtener datos de elevación solar y eliminar los registros con una elevación inferior a 7º


def main(options, args):
    config = snc.load_config(options.config)
    print("Correct config format")

    dtset = config['dataset']
    print("Dataset: {}".format(dtset['name']))

    rconfig = config['rawer']
    path = rconfig['inpath']
    print("Input files from {}".format(path))
    outpath = rconfig['outpath']
    print("Output files to {}".format(outpath))
    if not os.path.exists(outpath):
        os.makedirs(outpath)

    stations = rconfig['stations']
    print("Selected stations:")
    print(stations)

    # TODO obtener fecha comienzo de la muestra de datos, crear variable a las 0:00 de ese día
    flog = datetime.now()
    flog = os.path.dirname(os.path.realpath(__file__)) + "/../logs/" + flog.strftime('%Y-%m-%d_%H%M') + '.txt'
    if not os.path.exists(os.path.dirname(flog)):
        os.makedirs(flog)
    flog = open(flog, 'w')
    current_file = os.path.realpath(__file__)
    schema_path = os.path.dirname(current_file)
    schema_fname = "{}/../jsons/solarnc_schema.json".format(schema_path)
    fechas = {}
    df_stations = {}
    for s in stations:
        infiles = glob.glob("{}/{}/Minutos/*.dat".format(path, s))
        fechas[s] = {}
        for i, f in enumerate(infiles):
            name = os.path.basename(f).split('_')
            a = len(s.split('_')) + 1
            try:
                name[a+2] = name[a+2].split('.')[0]
                name[a+2] = '0' + name[a+2] if len(name[a+2]) < 2 else name[a+2]
                name[a+1] = '0' + name[a+1] if len(name[a+1]) < 2 else name[a+1]
                fecha = np.datetime64(name[a] + '-' + name[a+1] + '-' + name[a+2])
                # fecha = name[2] + '-' + name[3] + '-' + name[4]
            except:
                print('Nombre de fichero incorrecto en la posición ', i, ':\n    -x ' + f)
                # Codifico el error para el log. error 1, posición de ocurrencia, nombre
                print(1, i, f, file=flog, sep=',')
            else:
                if fecha not in fechas[s]:
                    fechas[s][fecha] = f
                else:
                    print('Se han encontrado ficheros duplicadaos en la posición ', i,
                          ':\n    -x ' + f + '\n    -> ' + fechas[s][fecha])
                    print(2, i, f, fechas[s][fecha], file=flog, sep=',')
        minima = min(fechas[s]) if 'minima' not in locals() else min(min(fechas[s]), minima)
        maxima = max(fechas[s]) if 'maxima' not in locals() else max(max(fechas[s]), maxima)

        ordenadas = sorted(fechas[s]) # en verdad no tendría por qué hacer falta que estén ordenadas.
        # se tiene que añadir la opción de que para un mismo día exista más de un fichero
        for d in ordenadas:
            df = getFromFile(fechas[s][d])
            if s not in df_stations:
                columnas = df.columns
                df_stations[s] = pd.DataFrame(columns=columnas)
            df_stations[s] = pd.concat([df_stations[s], df], ignore_index=True)

    fecha_i = minima + np.timedelta64(0, 'm')
    fecha_fin = np.datetime64(maxima + 1) - np.timedelta64(1, 'm')
    ffechas = os.path.dirname(os.path.realpath(__file__)) + "/../logs/" + 'fechas.txt'
    ffechas = open(ffechas, 'w')
    df = df_stations['METAS']
    df.set_index(columnas[0], inplace=True)
    while fecha_i <= fecha_fin:
        try:
            esta = df.loc[fecha_i]
        except:
            print(3, fecha_i, file=flog, sep=',')
        #        print(fecha_i, file=ffechas)
        fecha_i += np.timedelta64(1, 'm')
        # TODO obtener los datos de esa marca temporal para cada una de las estaciones, incrementar en minutos

    flog.close()
    ffechas.close()


if __name__ == "__main__":
    main(*parse_options())
