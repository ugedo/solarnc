# -*- coding: utf-8 -*-
from datetime import datetime
import solarnc as snc
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
    for s in stations:
        infiles = glob.glob("{}/{}/Minutos/*.dat".format(path, s))
        fechas = {}
        for i, f in enumerate(infiles):
            name = os.path.basename(f).split('_')
            try:
                name[-3] = '0' + name[-3] if int(name[-3]) < 10 else name[-3]
                name[-4] = '0' + name[-4] if int(name[-4]) < 10 else name[-4]
                fecha = np.datetime64(name[-5] + '-' + name[-4] + '-' + name[-3])
                # fecha = name[2] + '-' + name[3] + '-' + name[4]
            except:
                print('Nombre de fichero incorrecto en la posición ', i, ':\n    -x ' + f)
                # Codifico el error para el log. error 1, posición de ocurrencia, nombre
                print(1, i, f, file=flog, sep=',')
            else:
                if fecha not in fechas:
                    fechas[fecha] = f
                else:
                    print('Se han encontrado ficheros duplicadaos en la posición ', i,
                          ':\n    -x ' + f + '\n    -> ' + fechas[fecha])
                    print(2, i, f, file=flog, sep=',')
        minima = min(fechas) if 'minima' not in locals() else min(min(fechas), minima)
        maxima = max(fechas) if 'maxima' not in locals() else max(max(fechas), maxima)
        # ordenadas = sorted(fechas)
    fecha = minima + np.timedelta64(0, 'm')
    fecha_fin = np.datetime64(maxima + 1) - np.timedelta64(1, 'm')
    ffechas = os.path.dirname(os.path.realpath(__file__)) + "/../logs/" + 'fechas.txt'
    ffechas = open(ffechas, 'w')
    while fecha <= fecha_fin:
        print(fecha, file=ffechas)
        fecha += np.timedelta64(1, 'm')
        # TODO obtener los datos de esa marca temporal para cada una de las estaciones, incrementar en minutos

    flog.close()
    ffechas.close()


if __name__ == "__main__":
    main(*parse_options())
