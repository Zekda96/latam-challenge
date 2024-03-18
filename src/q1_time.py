from typing import List, Tuple
import datetime
import pandas as pd


def q1_time(file_path: str) -> List[Tuple[datetime.date, str]]:
    # Leer archivo JSON
    df = pd.read_json(path_or_buf=file_path, lines=True)

    # Normalizar atributo 'username'
    df['user'] = pd.json_normalize(df.user)['username']

    # Mantener solo año, mes y dia - dateime.date(AAAA, MM, DD)
    df['date'] = df['date'].dt.date

    # - Paso 1. Obtener top 10 fechas con mas tweets

    # Crear nuevo DataFrame solo con las columnas 'date' y 'user'
    df_dates = df[['date', 'user']]

    # Agrupar por fecha y contar filas
    df_dates = df_dates.groupby('date').count()

    # Ordenar por conteo de filas de manera descendente
    df_dates = df_dates.sort_values('user', ascending=False)

    # Elegir solo los primeros 10 resultados
    df_dates = df_dates.iloc[:10, :]

    top_dates = df_dates.index.to_list()

    # - Paso 2. Obtener top usuario con mas tweets por dia

    top_users = []
    # Iterar por cada dia
    for date in top_dates:
        # Crear nuevo DataFrame son con las columnas `data` y `user`
        df_users = df[['date', 'user']]

        # Seleccionar filas solo con la fecha a considerar
        mask = df_users['date'] == date
        df_users = df_users[mask]

        # Agrupar por usuario y contar filas
        df_users = df_users.groupby('user').count()

        # Ordenar por conteo de filas de manera descendente
        df_users = df_users.sort_values('date', ascending=False)

        # Guardar usuario con mas tweets
        top_users.append(df_users.index[0])

    # Juntar en una lista de 'Tuples' los top 10 dias
    # con su respectivo usuario con mas tweets
    result = [(top_dates[i], top_users[i]) for i in range(10)]

    return result