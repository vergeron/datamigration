import pandas as pd
from sqlalchemy import create_engine


def ask_mssql():
    engine = create_engine('mssql+pyodbc://@' + 'RONY' + '/' + 'testdb' + '?driver=SQL+Server')
    return engine


def ask_postgre():
    db_string = "postgres+psycopg2://{}:{}@{}/{}".format('postgres', 'cloudera',
                                                         '127.0.0.1',
                                                         'mockdb')
    engine = create_engine(db_string)
    return engine

if __name__ == '__main__':
    db_type = input("Please enter client's db: mssql, postgresql, oracle\n")
    db_type = db_type.lower()
    if db_type == 'mssql':
        engine_client = ask_mssql()

    db_type_our = input("Please enter your db: mssql, postgresql, oracle\n")
    db_type_our = db_type_our.lower()
    if db_type_our == 'postgresql':
        engine_our = ask_postgre()

    table_name = input('\nInput the client table:\n')
    query1 = 'select * from {}'.format(table_name)
    df_client = pd.read_sql(query1, engine_client)
    our_table = input('\nInput the table name in your db\n')
    query2 = 'select * from {}'.format(our_table)
    df_our = pd.read_sql(query2, engine_our)

    lc = df_client.values.tolist()
    lo = df_our.values.tolist()
    if len(lc) != len(lo):
        diff = lc[len(lo):len(lc)]
        lo.extend(diff)

    for i in range(len(lo)):
        for j in range(len(df_our.columns)):
            if lo[i][j] != lc[i][j]:
                lo[i][j] = lc[i][j]

    headers = [col for col in df_our.columns]

    df = pd.DataFrame(data=lo, columns=headers)

    df.to_sql(our_table, engine_our, if_exists='replace', index=False)

    print('\nTable Updated.')
