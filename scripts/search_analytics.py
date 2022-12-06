import glob
import sqlite3
from re import search
import pandas as pd
from pandas.core.dtypes.common import is_numeric_dtype


def search_analytic(input):
    input = input.lower()
    table = input.split('from')[1].split()[0]
    conn = sqlite3.connect('db/data.db')
    c = conn.cursor()
    table_list = glob.glob('data/' + table + '_' + '[0-9].csv')
    data = []
    for i in table_list:
        data.append(i.split('/')[-1].split('.')[0])
    substr = ['count', 'avg', 'min', 'max', 'sum']
    input_list = input.split()
    is_analytics = False
    Type = []
    Var = []
    columns = input.split('select')[1].split('from')[0].split()
    for i in substr:
        for j in input_list:
            if search(i,j):
                is_analytics = True
                Type.append(i)
                Var.append(j)
    if not is_analytics:
        # Search
        columns = input.split('select')[1].split('from')[0].split()
        map_reduce_df = pd.DataFrame()
        map_partition_output = []
        for i in data:
            table = input.split('from')[1].split()[0]
            input = input.replace(table, i)
            if columns != ['*']:
                df = pd.read_sql_query(input, conn)
                df.columns = columns
            else:
                df = pd.read_sql_query(f'select * from {i}', conn)
            map_reduce_df = pd.concat([map_reduce_df, df])
            map_reduce_df = map_reduce_df.dropna()
            map_partition_output.append(df)
        for j in map_reduce_df.columns:
            if is_numeric_dtype(map_reduce_df[j]):
                print(map_reduce_df[j].value_counts(bins=9))
    else:  # Analytics
        count = 0
        map_reduce_df = pd.DataFrame()
        map_partition_output = []
        for i in data:
            table = input.split('from')[1].split()[0]
            input = input.replace(table, i)
            df = pd.read_sql_query(input, conn)
            map_partition_output.append(df)
            df.columns = columns
            if Type[0] in ['count', 'sum']:
                if count == 0:
                    map_reduce_df = df
                else:
                    map_reduce_df = map_reduce_df.dropna()
                    df = df.dropna()
                    map_reduce_df[Var[0]] = map_reduce_df[Var[0]] + df[Var[0]]
                count += 1

            elif Type[0] == 'avg':
                if count == 0:
                    map_reduce_df = df
                else:
                    map_reduce_df = map_reduce_df.dropna()
                    df = df.dropna()
                    map_reduce_df[Var[0]] = (map_reduce_df[Var[0]] + df[Var[0]])/2
                count += 1

            elif Type[0] == 'min':
                if count == 0:
                    map_reduce_df = df
                else:
                    ls = []
                    map_reduce_df = map_reduce_df.dropna()
                    df = df.dropna()
                    l1 = map_reduce_df[Var[0]].tolist()
                    l2 = df[Var[0]].tolist()
                    for m in l1:
                        for n in l2:
                            ls.append(min(m,n))
                            l2.remove(n)
                            break
                    map_reduce_df[Var[0]] = ls
                count += 1

            elif Type[0] == 'max':
                if count == 0:
                    map_reduce_df = df
                else:
                    ls = []
                    map_reduce_df = map_reduce_df.dropna()
                    df = df.dropna()
                    l1 = map_reduce_df[Var[0]].tolist()
                    l2 = df[Var[0]].tolist()
                    for m in l1:
                        for n in l2:
                            ls.append(max(m, n))
                            l2.remove(n)
                            break
                    map_reduce_df[Var[0]] = ls
                count += 1

    return map_partition_output, map_reduce_df


if __name__ == "__main__":
    Input = "select sales.Year_of_Release, max(Critic_Score) from sales where sales.Year_of_Release > 2012 group by sales.Year_of_Release"
    output1, output2 = search_analytic(Input)
    print(output2)
