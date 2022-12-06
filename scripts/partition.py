from sqlalchemy import create_engine
import pandas as pd
from scripts.get_metadata import GetMetadata
import os
from pathlib import Path
import sqlite3


engine = create_engine('sqlite://', echo=False)
structure = {'parent':[], 'child':[]}
table = {'filename': [], 'partition_location': []}
partition = {'data_id':[], 'location':[]}


def to_data_db(name, k):  # to data.db
    Path('db/data.db').touch()
    data_conn = sqlite3.connect('db/data.db')
    for i in range(k):
        globals()['df_%s' % str(i+1)] = pd.read_csv(f'data/{name}_{i + 1}.csv')
        globals()['df_%s' % str(i+1)].to_sql(f'{name}_{i + 1}', data_conn, if_exists='replace', index=False)


def partition_table(path, name, k):
    parent = path.split('/')[-1]
    for i in range(k):
        table['filename'].append(f'{name}.csv')
        table['partition_location'].append(path + '/' + f'{name}_{i+1}.csv')
        structure['parent'].append(parent)
        structure['child'].append(f'{name}_{i+1}.csv')
    df_table = pd.DataFrame(table)

    # update to structure.db
    conn_structure = sqlite3.connect('db/structure.db')
    df_table.to_sql('file_structure', conn_structure, if_exists='append', index=False)
    pd.DataFrame(structure).to_sql('parent_child', conn_structure, if_exists='append', index=False)


def make_partition(file, input_path, k):  # make partitions and update to sql db
    filepath = 'data/' + file
    data = pd.read_csv(filepath)
    name = file.split('.')[0]
    size = int(len(data) / k)
    for i in range(k):
        df = data[size * i:size * (i + 1)]
        df.to_csv(f'data/{name}_{i + 1}.csv', index=False)
    for i in range(k):
        globals()['df_%s' % str(i+1)] = pd.read_csv(f'data/{name}_{i+1}.csv')

    to_data_db(name, k)  # update actual data to data.db

    partition_table(input_path, name, k)  # update partition parent child relationship to node.db


# if __name__ == "__main__":
#     path = '/user/john'
#     file = 'sales.csv'
#     name = 'reviews'
#     k = 4
#     make_partition(file, k)
#     df_table_out, structure_out = partition_table(path, name, k)
#     print(df_table_out)
#     print(list(df_table_out.loc[df_table_out['filename'] == 'reviews.csv']['partition_location']))