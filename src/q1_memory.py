from typing import List, Tuple
import datetime
import pandas as pd
import os
from memory_profiler import profile, memory_usage


def q1_memory(file_path: str) -> List[Tuple[datetime.date, str]]:
    lines = pd.read_json(file_path, lines=True, chunksize=1000)

    dates = {}
    users = {}

    for df in lines:
        # El stream envia los index originales del DataFrame y esto
        # estorba el procesamiento aislado de cada DataFrame

        df.reset_index(inplace=True)
        # 1. Contar fechas

        # Mantener solo año, mes y dia - dateime.date(AAAA, MM, DD)
        df['date'] = df.loc[:, 'date'].dt.date

        for date, val in df['date'].value_counts().items():
            dates[date] = dates.get(date, 0) + val

        # 2. Contar usuarios
        # Elegir solo las filas de interes - date y user,
        # y una tercera para contar
        df_users = df[['date', 'user', 'id']]

        # Normalizar usuario
        df_users.loc[:, 'user'] = pd.json_normalize(df_users['user'])['username']

        # Agrupar fecha y usuario
        df_users = df_users.groupby(['date', 'user']).count()

        # Ordenar por fecha y numero de tweets por usuario
        df_users = df_users.sort_values(['date', 'id'], ascending=False)

        # Crear un diccionario donde se va sumando el conteo de cada usuario por fecha
        # y asi sacar al usuario con mas tweets dentro las top 10 fechas
        for index, value in df_users.iterrows():
            date = index[0]
            user = index[1]
            val = value.iloc[0]

            if date in users.keys():
                if user in users[date].keys():
                    # Usuario y fecha existen en el diccionario, asi que se suma
                    users[date][user] += val

                else:
                    # Crear entrada de usuario
                    users[date][user] = val
            else:
                # Crear entrada de fecha
                users[date] = {user: val}

    # Crear DataFrame para ordenar las fechas en orden descendente
    dates = pd.DataFrame.from_dict(
        {'date': dates.keys(), 'count': dates.values()})
    dates = dates.sort_values(['count'], ascending=False).iloc[:10, :]

    # Lista con las top 10 fechas con mas tweets
    top_dates = list(dates['date'])

    # Elegir los usuarios con mas tweets dentro de las fechas de interes
    top_users = [max(users[date], key=users[date].get) for date in top_dates]

    # Juntar en una lista de 'Tuples' los top 10 dias
    # con su respectivo usuario con mas tweets

    result = [(top_dates[i], top_users[i]) for i in range(10)]

    return result


if __name__ == '__main__':
    basePath = os.path.abspath('')
    fp = os.path.join(basePath, "farmers-protest-tweets-2021-2-4.json")

    mem = memory_usage((q1_memory, (), {'file_path': fp}), max_usage=True)
    print(mem)
