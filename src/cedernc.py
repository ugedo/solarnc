import pandas as pd
import numpy as np
import sys
import os


if __name__ == '__main__':
    # Crea un mapa que asocia una fecha AAAA-MM-DD a una ruta del sistema
    # la ruta es el fichero/s pasado como argumento, que contiene las rutas de los .dat de ese día
    # una línea para cada uno de los sensores, de ese mismo día y misma periodicidad
    fechas = {}
    for i, f in enumerate(sys.argv[1:]):
        base = os.path.basename(f)
        name = base.split('_')
        name[4] = '0' + name[4] if int(name[4]) < 10 else name[4]
        name[3] = '0' + name[3] if int(name[3]) < 10 else name[3]
        # fecha = np.datetime64(name[2] + '-' + name[3] + '-' + name[4])
        fecha = name[2] + '-' + name[3] + '-' + name[4]
        if fecha not in fechas:
            fechas[fecha] = f
        else:
            print('Se han encontrado ficheros duplicadaos en la posición ', i,
                  ':\n    -x ' + f + '\n    -> ' + fechas[fecha])
    ordenadas = sorted(fechas)
    iter = 0
    df_diario = []
    for d in ordenadas:
        with open(fechas[d], 'r') as f:
            sensors = []
            for linea in f:
                sensors.append(str.rstrip(linea))
        for i, s in enumerate(sensors):
            df = pd.read_csv(s, header=[1, 2, 3])
            df = df.iloc[:, [0, 3]]
            df[["TIMESTAMP"]] = df[["TIMESTAMP"]].astype(np.datetime64)
            df[('Sec', 'Sec', 'Sec')] = df.apply(lambda x: x[df.columns[0]].minute, axis=1)
            df[('Year', 'Year', 'Year')] = df.apply(lambda x: x[df.columns[0]].year, axis=1)
            df[('DJ', 'DJ', 'DJ')] = df.apply(lambda x: x[df.columns[0]].dayofyear, axis=1)
            df.drop(columns=["TIMESTAMP"], level=0, inplace=True)
            col = df.columns
            df = df.reindex(columns=[col[1], col[2], col[3], col[0]], copy=False)
            # TODO preguntar qué es el cuarto parámetro de los .txt de raw e incluir. (HST hhmm)
            if 'df_out' not in locals():
                df_out = df
            else:
                sensor = 'Sen' + str(i)
                df_out[(sensor, sensor, sensor)] = df[df.columns[-1]]
        o = os.path.dirname(fechas[d]) + '/raw/' + d + '.txt'
        df_out.to_csv(o, header=False, index=False)
