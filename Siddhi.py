import dask.dataframe as dd
from sqlalchemy import create_engine
import glob

#mapping dictionary Base Name
base_name_mapping = {
    'B02512': 'Unter',
    'B02598': 'Hinter',
    'B02617': 'Weiter',
    'B02682': 'Schmecken',
    'B02764': 'Danach-NY',
    'B02765': 'Grun',
    'B02835': 'Dreist',
    'B02836': 'Drinnen'
}

folder_path = 'E:/New/'

#glob to get a list of CSV files
csv_files = glob.glob(folder_path + '*.csv')

engine = create_engine('mysql+mysqlconnector://sqluser:gupta123@localhost:3307/db2')

#initializing merged_df_dask
merged_df_dask = dd.read_csv(csv_files[0])

if 'Base' in merged_df_dask.columns:
    merged_df_dask['Base'] = merged_df_dask['Base'].map(base_name_mapping)

for file_path in csv_files[1:]:
    # Read CSV file using Dask
    df_dask = dd.read_csv(file_path)

    if 'Base' in df_dask.columns:
        df_dask['Base'] = df_dask['Base'].map(base_name_mapping)

    # Append the current Dask DataFrame to the merged Dask DataFrame
    merged_df_dask = dd.concat([merged_df_dask, df_dask], axis=0)

# Persist the merged Dask DataFrame to trigger computation
merged_df_dask = merged_df_dask.persist()

table_name = "merged_data_table"

# Saving to MySQL
merged_df_dask.to_sql(name=table_name, uri=f'mysql+mysqlconnector://sqluser:gupta123@localhost:3307/db2', if_exists='replace', index=False, method='multi')
