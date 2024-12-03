"""
Escriba el codigo que ejecute la accion solicitada.
"""


# pylint: disable=import-outside-toplevel

import pandas as pd
import os

def lectura_comprimido(direccion: str) -> pd.DataFrame:
    """
    Esta función lee un archivo CSV comprimido y retorna un marco
    de datos de pandas. Esta función también bota la primea columna
    del marco de datos, ya que se trata de una columna de índices
    """

    df = pd.read_csv(
            direccion,
            header=0,
            compression='zip'
    )

    df.pop(df.columns[0])

    return df

# ================================================================
# ================================================================
# ================================================================

def base_dividida(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    columnas_clientes = ['client_id', 'age', 'job', 'marital', 'education',
                         'credit_default', 'mortgage']
    columnas_campana = [
        'client_id', 'number_contacts', 'contact_duration',
        'previous_campaign_contacts', 'previous_outcome',
        'campaign_outcome', 'day', 'month'
    ]
    columnas_economics = ['client_id', 'cons_price_idx', 'euribor_three_months']

    # Crear copias explícitas
    df_clientes = df[columnas_clientes].copy()
    df_campana = df[columnas_campana].copy()
    df_economics = df[columnas_economics].copy()

    # Transformaciones en df_clientes
    df_clientes.loc[:, 'job'] = df_clientes['job'].str.replace('.', '', regex=False)
    df_clientes.loc[:, 'job'] = df_clientes['job'].str.replace('-', '_', regex=False)

    df_clientes.loc[:, 'education'] = df_clientes['education'].str.replace('.', '_', regex=False)
    df_clientes.loc[:, 'education'] = df_clientes['education'].replace('unknown', pd.NA)

    df_clientes.loc[:, 'credit_default'] = df_clientes['credit_default'].apply(
        lambda x: 1 if x == 'yes' else 0
    )

    df_clientes.loc[:, 'mortgage'] = df_clientes['mortgage'].apply(
        lambda x: 1 if x == 'yes' else 0
    )

    # Transformaciones en df_campana
    mes_equiv = {
        'jan': '01', 'feb': '02', 'mar': '03', 'apr': '04',
        'may': '05', 'jun': '06', 'jul': '07', 'aug': '08',
        'sep': '09', 'oct': '10', 'nov': '11', 'dec': '12',
    }

    df_campana.loc[:, 'previous_outcome'] = df_campana['previous_outcome'].apply(
        lambda x: 1 if x == 'success' else 0
    )
    df_campana.loc[:, 'campaign_outcome'] = df_campana['campaign_outcome'].apply(
        lambda x: 1 if x == 'yes' else 0
    )

    df_campana.loc[:, 'mes_equiv'] = df_campana['month'].str.lower().map(mes_equiv)
    df_campana.loc[:, 'dia_aux'] = df_campana['day'].astype(str).str.zfill(2)
    df_campana.loc[:, 'last_contact_date'] = (
        '2022' + '-' + df_campana['mes_equiv'] + '-' + df_campana['dia_aux']
    )

    columnas_campana_upd = [
        'client_id', 'number_contacts', 'contact_duration',
        'previous_campaign_contacts', 'previous_outcome',
        'campaign_outcome', 'last_contact_date'
    ]

    return df_clientes, df_campana[columnas_campana_upd].copy(), df_economics


# ================================================================
# ================================================================
# ================================================================

def lista_archivos(ruta_carpeta: str) -> list:
    """
    Esta función retorna una lista con el nombre de todos los archivos
    que se ubican dentro de una carpeta
    """

    lista_archivos = os.listdir(ruta_carpeta)

    lista_archivos = [ruta_carpeta + '/' + x for x in lista_archivos]

    return sorted(lista_archivos)

# ================================================================
# ================================================================
# ================================================================

def unificacion(lista: str):
    """
    Esta función recibe una lista de varios archivos y los unifica
    en tres marcos de datos
    """

    df = lectura_comprimido(lista[0])

    for archivo in lista[1:]:
        df_nuevo = lectura_comprimido(archivo)
        df = pd.concat([df, df_nuevo], axis = 0)

    return df

# ================================================================
# ================================================================
# ================================================================

def guardado(df: pd.DataFrame, carpeta: str, archivo: str):
    """
    Esta carpeta guarda un elemento entregado dentro de un carpeta
    con cierto nombre de archivo.
    """

    if not os.path.exists(carpeta):
        os.makedirs(carpeta)

    ruta_archivo = os.path.join(carpeta, archivo)

    df.to_csv(ruta_archivo, index = False)

    print('Se ha guardado en ' + ruta_archivo)



# ================================================================
# ================================================================
# ================================================================

def clean_campaign_data():
    """
    En esta tarea se le pide que limpie los datos de una campaña de
    marketing realizada por un banco, la cual tiene como fin la
    recolección de datos de clientes para ofrecerls un préstamo.

    La información recolectada se encuentra en la carpeta
    files/input/ en varios archivos csv.zip comprimidos para ahorrar
    espacio en disco.

    Usted debe procesar directamente los archivos comprimidos (sin
    descomprimirlos). Se desea partir la data en tres archivos csv
    (sin comprimir): client.csv, campaign.csv y economics.csv.
    Cada archivo debe tener las columnas indicadas.

    Los tres archivos generados se almacenarán en la carpeta files/output/.

    client.csv:
    - client_id
    - age
    - job: se debe cambiar el "." por "" y el "-" por "_"
    - marital
    - education: se debe cambiar "." por "_" y "unknown" por pd.NA
    - credit_default: convertir a "yes" a 1 y cualquier otro valor a 0
    - mortage: convertir a "yes" a 1 y cualquier otro valor a 0

    campaign.csv:
    - client_id
    - number_contacts
    - contact_duration
    - previous_campaing_contacts
    - previous_outcome: cmabiar "success" por 1, y cualquier otro valor a 0
    - campaign_outcome: cambiar "yes" por 1 y cualquier otro valor a 0
    - last_contact_day: crear un valor con el formato "YYYY-MM-DD",
        combinando los campos "day" y "month" con el año 2022.

    economics.csv:
    - client_id
    - const_price_idx
    - eurobor_three_months
    """
    # Carpeta donde se alojan todos los datos
    carpeta = lista_archivos('./files/input')

    # Lectura de todos los datos y unificación en un solo
    # marco de datos
    df = unificacion(carpeta)

    # División en tres marcos de datos
    df0, df1, df2 = base_dividida(df)

    # Carpeta donde se van a alojar los datos
    carpeta_out = './files/output'

    # Guardado de cada uno de los datos
    df0 = guardado(df0, carpeta_out, 'client.csv')
    df1 = guardado(df1, carpeta_out, 'campaign.csv')
    df2 = guardado(df2, carpeta_out, 'economics.csv')


if __name__ == "__main__":
    clean_campaign_data()
