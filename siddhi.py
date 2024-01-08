import dask.dataframe as dd
from sqlalchemy import create_engine
import glob
import re

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

# Use glob to get a list of CSV files in the folder
csv_files = glob.glob(folder_path + '*.csv')
engine = create_engine('mysql+mysqlconnector://sqluser:gupta123@localhost:3307/db2')

#Reading the file using dask
for file_path in csv_files:
    df_dask = dd.read_csv(file_path)
    if 'Base' in df_dask.columns:
        df_dask['Base'] = df_dask['Base'].map(base_name_mapping)

    # Persist the Dask DataFrame to trigger computation
    df_dask = df_dask.persist()

    # Regex for table names
    table_name = re.sub('[^0-9a-zA-Z]+', '_', file_path.split('/')[-1].split('.')[0])
   
    # Saving to mysql
    df_dask.to_sql(name=table_name, uri=f'mysql+mysqlconnector://sqluser:gupta123@localhost:3307/db2', if_exists='replace', index=False, method='multi')