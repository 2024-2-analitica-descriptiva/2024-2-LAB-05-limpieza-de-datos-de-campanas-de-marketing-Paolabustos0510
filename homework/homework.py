"""
Escriba el codigo que ejecute la accion solicitada.
"""

# pylint: disable=import-outside-toplevel


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
    import pandas as pd
    import os
    import shutil

    def open_clean(path):
        # Leer todos los archivos en la carpeta de entrada
        files = os.listdir(path)
        data = []
        for file in files:
            full_path = os.path.join(path, file)
            data.append(full_path)
        final_df = pd.DataFrame()

        
        for i in data:
            df = pd.read_csv(i, index_col=False, compression='zip')    
            df['job'] = df['job'].str.replace('.', '').str.replace('-', '_')
            df['education'] = df['education'].str.replace('.', '_', regex=False).replace('unknown', pd.NA)
            df['credit_default'] = df['credit_default'].apply(lambda x: 1 if x == 'yes' else 0)
            df['mortgage'] = df['mortgage'].apply(lambda x: 1 if x == 'yes' else 0)
            df['previous_outcome'] = df['previous_outcome'].apply(lambda x: 1 if x == 'success' else 0)
            df['campaign_outcome'] = df['campaign_outcome'].apply(lambda x: 1 if x == 'yes' else 0)
            
            month_dict = {'jan': 1, 'feb': 2, 'mar': 3, 'apr': 4, 'may': 5, 'jun': 6,'jul': 7, 'aug': 8, 'sep': 9, 'oct': 10, 'nov': 11, 'dec': 12}
            df['month'] = df['month'].str.lower().map(month_dict).fillna(df['month'])
            df['last_contact_date'] = "2022-" + df['month'].astype(str).str.zfill(2) + "-" + df['day'].astype(str).str.zfill(2)
            
            final_df = pd.concat([final_df, df], ignore_index=True)
        
        return final_df

    def divide_save(df):

        client = df[['client_id', 'age', 'job', 'marital', 'education', 'credit_default', 'mortgage']]
        campaign = df[['client_id', 'number_contacts', 'contact_duration', 'previous_campaign_contacts', 
                    'previous_outcome', 'campaign_outcome', 'last_contact_date']]
        economics = df[['client_id', 'cons_price_idx', 'euribor_three_months']]
        
        files = {
            'client': client,
            'campaign': campaign,
            'economics': economics
        }
        output_folder = 'files/output/'
        if not os.path.exists(output_folder):
            os.makedirs(output_folder)
            print(f"La carpeta '{output_folder}' fue creada.")
        for name, data in files.items():
            path = f'files/output/{name}.csv'
            data.to_csv(path, index=False)




    # Cargar los datos y dividirlos
    df = open_clean('files/input/')
    # Guardar los datos
    divide_save(df)

if __name__ == "__main__":
    clean_campaign_data()



