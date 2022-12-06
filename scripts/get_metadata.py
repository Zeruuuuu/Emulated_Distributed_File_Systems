import glob
import pandas as pd
import os
from datetime import datetime, timezone
from pathlib import Path
import sqlite3
import warnings
warnings.filterwarnings("ignore")


class GetMetadata:
    def __init__(self, name):
        self.name = name

    # def read_df(self):
    #     sales_df = pd.read_csv('../data/sales.csv')
    #     reviews_df = pd.read_csv('../data/reviews.csv')
    #     return sales_df, reviews_df

    def get_file_metadata(self):
        metadata_df = pd.DataFrame()
        for filepath in glob.glob('../data' + '/*.csv'):
            filename = filepath.split('/')[-1]
            metadata_dict = {
                'filename': filename,
                'path': filepath,
                'size (bytes)': os.path.getsize(filepath),
                'ctime': datetime.fromtimestamp(os.path.getctime(filepath), tz=timezone.utc).strftime('%-m/%-d/%Y %-H:%M'),
                'mtime': datetime.fromtimestamp(os.path.getmtime(filepath), tz=timezone.utc).strftime('%-m/%-d/%Y %-H:%M'),
                'atime': datetime.fromtimestamp(os.path.getatime(filepath), tz=timezone.utc).strftime('%-m/%-d/%Y %-H:%M')
            }
            metadata_df = pd.concat([metadata_df, pd.DataFrame.from_dict(metadata_dict, orient='index').T])
        return metadata_df

    def to_sql(self, metadata):
        Path('../db/metadata.db').touch()
        conn = sqlite3.connect('../db/metadata.db')
        # c = conn.cursor()
        metadata.to_sql('metadata', conn, if_exists='replace', index=False)
        # print(c.execute('''SELECT * FROM metadata LIMIT 5''').fetchall())


if __name__ == '__main__':
    self = GetMetadata('data')
    # sales, reviews = self.read_df()
    self.to_sql(self.get_file_metadata())
