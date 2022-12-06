import pandas as pd
from scripts.partition import make_partition
from scripts.search_analytics import search_analytic
import sqlite3

help_info = 'available commands: help, mkdir, ls, cat, rm, put, getPartitionLocations, readPartition'
structure = {'parent': [], 'child': []}
table = {'filename': [], 'partition_location': []}
conn_structure = sqlite3.connect('db/structure.db')
pd.DataFrame(structure).to_sql('parent_child', conn_structure, if_exists='append', index=False)


def commands():
    cmd_in = input('Enter your command:')
    print('>>>', cmd_in, '\n')
    params = cmd_in.strip().split()
    commands_list = ['help', 'mkdir', 'ls', 'cat', 'rm', 'put', 'getPartitionLocations', 'readPartition']

    while params[0] in commands_list:
        if len(params) >= 1:
            if params[0] == 'help':
                print(help_info)
                break

            if params[0] == 'mkdir':
                if len(params) != 2:
                    raise Exception('mkdir takes exactly 1 argument')
                else:
                    directory = params[1].split('/')
                    for n, i in enumerate(directory):
                        if n < len(directory) - 1:
                            structure['parent'] = i
                            structure['child'] = directory[n + 1]
                    structure_df = pd.DataFrame(structure, index=[0])
                    conn_structure = sqlite3.connect('db/structure.db')
                    structure_df.to_sql('parent_child', conn_structure, if_exists='append', index=False)
                    print('Directory Created!')
                    print('\nFile system structure now:\n', pd.read_sql_query("select * from parent_child", conn_structure))
                    break

            if params[0] == 'ls':
                if len(params) != 2:
                    print('ls takes exactly 1 argument')
                else:
                    my_dir = params[1].split('/')[-1]
                    conn_structure = sqlite3.connect('db/structure.db')
                    structure_df = pd.read_sql_query('select * from parent_child', conn_structure)
                    pruned_df = structure_df.loc[structure_df['parent'] == my_dir]
                    child_ls = pruned_df['child'].tolist()
                    print('Files/Directories list:\n')
                    print(child_ls)
                    break

            if params[0] == 'cat':
                if len(params) != 2:
                    print('cat takes exactly 1 argument')
                else:
                    filename = params[1].split('/')[-1]
                    filetype = filename.split('.')[-1]
                    if filetype == 'csv':
                        data = pd.read_csv('data/' + filename)
                        print(data)
                        break
                    else:
                        print("Only csv files are allowed")

            if params[0] == 'rm':  # rm /user/aaa/reviews_1.csv
                if len(params) != 2:
                    print('rm takes exactly 1 argument')
                else:
                    if '.' not in params[-1]:
                        print('Can only remove files')
                        break
                    else:
                        conn_structure = sqlite3.connect('db/structure.db')
                        my_dir = params[1].split('/')[-2]
                        my_file = params[1].split('/')[-1]
                        structure_df = pd.read_sql_query('select * from parent_child', conn_structure)
                        structure_df = structure_df.loc[(structure_df['child'] != my_file) | (structure_df['parent'] != my_dir)]
                        structure_df.to_sql('parent_child', conn_structure, if_exists='replace', index=False)

                        table_df = pd.read_sql_query(
                            "select * from file_structure where partition_location !='" + params[1] + "'",
                            conn_structure)
                        table_df.to_sql('file_structure', conn_structure, if_exists='replace', index=False)
                        print('File has been removed!')
                        print('\nFile system structure now:\n', pd.read_sql_query("select * from parent_child", conn_structure))
                        break

            if params[0] == 'put':  # put reviews.csv /user/ccc 4
                if len(params) != 4:
                    print('put takes exactly 3 arguments')
                else:
                    conn_structure = sqlite3.connect('db/structure.db')
                    par_child_df = pd.read_sql_query('select * from parent_child', conn_structure)
                    dir_list = list(par_child_df['parent'])
                    dir_list.extend(list(par_child_df['child']))
                    is_in = False
                    for i in params[2].split('/')[1:]:
                        if i not in dir_list:
                            print('Directory not exist!')
                            is_in = True
                            break
                    if not is_in:
                        filename, path, partition_number = params[1], params[2], int(params[3])
                        make_partition(filename, path, partition_number)
                        print('Partition successful')
                        print('\nFile system structure now:\n', pd.read_sql_query("select * from parent_child", conn_structure))
                        print('\n', pd.read_sql_query("select partition_location from file_structure", conn_structure))
                        break
                    else:
                        break

            if params[0] == 'getPartitionLocations': # getPartitionLocations reviews
                if len(params) != 2:
                    print('getPartitionLocations takes exactly 1 argument')
                else:
                    conn_structure = sqlite3.connect('db/structure.db')
                    if params[1].split('.')[0] == 'reviews':
                        filename = params[1].split('.')[0] + '.csv'
                        df = pd.read_sql_query(
                            "select partition_location from file_structure where filename ='" + filename + "'",
                            conn_structure)
                        if len(df):
                            print(df)
                        else:
                            print('No location found!')
                        break
                    if params[1].split('.')[0] == 'sales':
                        filename = params[1].split('.')[0] + '.csv'
                        df = pd.read_sql_query(
                            "select partition_location from file_structure where filename ='" + filename + "'",
                            conn_structure)
                        if len(df):
                            print(df)
                        else:
                            print('No location found!')
                        break

            if params[0] == 'readPartition': # readPartition reviews.csv 1
                if len(params) != 3:
                    raise Exception('readPartition takes exactly 2 arguments')
                else:
                    if '.' in params[1]:
                        filename = params[1].split('.')[0]
                    else:
                        filename = params[1]
                    part = str(params[2])
                    filepath = filename + '_' + part + '.csv'
                    data = pd.read_csv('data/' + filepath)
                    print('Partition', part, 'in', filename, ':\n', data)
                    break

    else:
        raise Exception('Invalid command!', help_info)


def sql_query():
    query_in = input("Enter your sql query:")
    output1, output2 = search_analytic(query_in)
    print('Map result: \n', output1)
    print('\nFinal result after Reduce: \n', output2)


if __name__ == "__main__":
    sql_query()
