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

def base_dividida(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Esta base recibe un marco de datos y selecciona solo algunas
    columnas de interés, retornando un marco de datos solo con
    aquellas columnas. Para algunas de esas columnas de interés
    se realiza un preprocesamiento.
    """

    ## División uno =============
    columnas_clientes = ['client_id', 'age', 'job', 'marital', 'education',
                'credit_default', 'mortgage']
    
    df_clientes = df[columnas_clientes]

    # Transformaciones
    df_clientes['job'] = df_clientes['job'].str.replace('.', '')
    df_clientes['job'] = df_clientes['job'].str.replace('-', '_')
    
    df_clientes['education'] = df_clientes['education'].str.replace('.', '_')
    df_clientes['education'] = df_clientes['education'].replace('unknown', pd.NA)

    df_clientes['credit_default'] = df_clientes['credit_default'].apply(
        lambda x: 1 if x == 'yes' else 0
    )

    df_clientes['mortgage'] = df_clientes['mortgage'].apply(
        lambda x: 1 if x == 'yes' else 0
    )

    ## División dos =============
    columnas_campana = [
        'client_id', 'number_contacts', 'contact_duration',
        'previous_campaign_contacts', 'previous_outcome',
        'campaign_outcome', 'day', 'month'
    ]

    df_campana = df[columnas_campana]

    # Transformaciones
    df_campana['previous_outcome'] = df_campana['previous_outcome'].apply(
        lambda x: 1 if x == 'success' else 0
    )
    df_campana['campaign_outcome'] = df_campana['campaign_outcome'].apply(
        lambda x: 1 if x == 'yes' else 0
    )

    mes_equiv = {
        'jan': '01', 'feb': '02', 'mar': '03', 'apr': '04',
        'may': '05', 'jun': '06', 'jul': '07', 'aug': '08',
        'sep': '09', 'oct': '10', 'nov': '11', 'dec': '12',
    }
    df_campana['mes_equiv'] = df_campana['month'].str.lower().map(mes_equiv)
    df_campana['dia_aux'] = df_campana['day'].astype(str).str.zfill(2)
    df_campana['last_contact_day'] = '2022' + '-' + df_campana['mes_equiv'] + '-' + df_campana['dia_aux']
    columnas_campana_upd = [
        'client_id', 'number_contacts', 'contact_duration',
        'previous_campaign_contacts', 'previous_outcome',
        'campaign_outcome', 'last_contact_day'
    ]


    return df_clientes, df_campana[columnas_campana_upd]


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

    direccion = './files/input/bank-marketing-campaing-7.csv.zip'
    df0 = lectura_comprimido(direccion)
    df0, df1 = base_dividida(df0)

    return df0, df1


if __name__ == "__main__":
    df0, df1 = clean_campaign_data()
    print(df1.head())
