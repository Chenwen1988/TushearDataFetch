import pymysql
import pandas as pd


class QueryData(object):
    def __init__(self,
                 hostname='192.168.105.95',
                 username='root',
                 password='bbdmodelingcenter',
                 database='tusharedata'):
        self.conn = pymysql.connect(host=hostname, user=username, passwd=password, db=database)
        self.cur = self.conn.cursor()

    def show_tables(self):
        self.cur.execute('show tables;')
        tables = self.cur.fetchall()
        for table in tables:
            print(f'table {tables.index(table) + 1}： {table[0]}')
            print('-' * 30)

    def query_stock_basic(self):
        print('-' * 60)
        print(f'table: stock_basic')
        self.cur.execute('select * from stock_basic Limit 10;')
        stock_basic = self.cur.fetchall()
        df = pd.DataFrame(list(stock_basic))
        columns = [col[0] for col in self.cur.description]
        df.columns = columns

        return df

    def query_namechange(self):
        print('-' * 60)
        print(f'table: name_change')
        self.cur.execute('select * from name_change Limit 10;')
        name_change = self.cur.fetchall()
        df = pd.DataFrame(list(name_change))
        columns = [col[0] for col in self.cur.description]
        df.columns = columns

        return df

    def query_adjust_factor(self):
        print('-' * 60)
        print(f'table: adjust_factor')
        self.cur.execute("select * from adjust_factor where ts_code = '000001.SZ';")
        adjust_factor = self.cur.fetchall()
        df = pd.DataFrame(list(adjust_factor))
        columns = [col[0] for col in self.cur.description]
        df.columns = columns

        return df

    def query_qfq_data(self):
        print('-' * 60)
        print(f'table: pro_bar_daily_qfq')
        self.cur.execute("select * from pro_bar_daily_qfq where ts_code = '000001.SZ';")
        data_qfq = self.cur.fetchall()
        df = pd.DataFrame(list(data_qfq))
        columns = [col[0] for col in self.cur.description]
        df.columns = columns

        return df

    def query_trade_calender(self):
        print('-' * 60)
        print(f'table: trade_calender')
        self.cur.execute("select * from trade_calender order by cal_date desc Limit 10 ;")
        trade_calender = self.cur.fetchall()
        df = pd.DataFrame(list(trade_calender))
        columns = [col[0] for col in self.cur.description]
        df.columns = columns

        return df

    def close_conn(self):
        self.cur.close()
        self.conn.close()


if __name__ == '__main__':
    host = '192.168.105.95'
    user = 'root'
    pwd = 'bbdmodelingcenter'
    db = 'tusharedata'

    # 0. Create data instance
    data = QueryData(host, user, pwd, db)

    # 1. Show tables
    data.show_tables()

    # 2. Query stock basic
    df_stock_basic = data.query_stock_basic()
    print(df_stock_basic)

    # 3. Query name change
    df_namechange = data.query_namechange()
    print(df_namechange)

    # 4. Query adjust factor
    df_adj_factor = data.query_adjust_factor()
    print(df_adj_factor)

    # 5. Query back adjust data
    df_qfq = data.query_qfq_data()
    print(df_qfq)

    # 6. Query trade_calender
    df_trade_calender = data.query_trade_calender()
    print(df_trade_calender)

    data.close_conn()
