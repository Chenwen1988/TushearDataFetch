# _*_ coding: utf-8 _*_

import pymysql
import datetime
import numpy as np
import pandas as pd
import tushare as ts
import pyecharts.options as opts
from pyecharts.charts import Line, Pie
# from snapshot_selenium import snapshot
from snapshot_phantomjs import snapshot
from pyecharts.render import make_snapshot

"""
参考地址: https://gallery.echartsjs.com/editor.html?c=xEyDk1hwBx
pip install snapshot-phantomjs
"""


class QueryData95(object):
    def __init__(self,
                 hostname='192.168.105.95',
                 username='root',
                 password='bbdmodelingcenter',
                 database='tusharedata'):
        self.conn = pymysql.connect(host=hostname, user=username, password=password, database=database)
        self.cur = self.conn.cursor()

    def query_qfq_data(self, codes, trade_date):
        print('-' * 60)
        print(f'table: pro_bar_daily_qfq')
        self.cur.execute(f"""select * from pro_bar_daily_qfq where ts_code in {codes} and trade_date = {trade_date};""")
        data_qfq = self.cur.fetchall()
        _df = pd.DataFrame(list(data_qfq))
        columns = [col[0] for col in self.cur.description]
        _df.columns = columns

        return _df

    def query_daily_basic(self, codes, trade_date):
        print('-' * 60)
        print(f'table: stock_basic')
        self.cur.execute(f"""select * from daily_basic where ts_code in {codes} and trade_date = {trade_date};""")
        daily_basic = self.cur.fetchall()
        _df = pd.DataFrame(list(daily_basic))
        columns = [col[0] for col in self.cur.description]
        _df.columns = columns

        return _df

    def query_stock_basic(self):
        print('-' * 60)
        print(f'table: stock_basic')
        self.cur.execute("""select * from stock_basic where list_status = 'L';""")
        stock_basic = self.cur.fetchall()
        _df = pd.DataFrame(list(stock_basic))
        columns = [col[0] for col in self.cur.description]
        _df.columns = columns

        return _df


def group_by(_df, groupby='total_mv', parts=10):
    interval = 100 / parts
    percentiles = []
    for part in range(1, parts + 1):
        percentiles.append(part * interval / 100)

    percentiles = np.array(percentiles).round(2)

    quantiles = _df[f'{groupby}'].quantile(percentiles)
    _df[f'{groupby}_indicator'] = _df[f'{groupby}'].apply(lambda x: 0)
    for i in range(parts):
        if i == 0:
            _df_tmp = _df[_df[f'{groupby}'] <= quantiles.loc[percentiles[i]]]
        else:
            _df_tmp = _df[(_df[f'{groupby}'] > quantiles.loc[percentiles[i - 1]]) & (
                        _df[f'{groupby}'] <= quantiles.loc[percentiles[i]])]

        _df_tmp = _df_tmp[f'{groupby}_indicator'].apply(lambda x: '%s@Qtl%02d' % (groupby, parts - i))
        _df.update(_df_tmp)

    return _df


def group_by4plot(_df, groupby='total_mv', target='vol'):
    _df = _df.groupby([f'{groupby}_indicator'])[f'{target}'].sum()
    _df = _df.reset_index()
    _df.columns = ['name', f'{target}']

    return _df


def process_viz_volume_data(trade_date, target='vol'):
    df_listed = pro.stock_basic(exchange='', list_status='L', fields='ts_code, name, area, industry')
    ts_code_list = tuple(df_listed.ts_code.tolist())

    query = QueryData95()
    df_stock = query.query_stock_basic()[['ts_code', 'name', 'area', 'industry']]
    df_volume = query.query_qfq_data(codes=ts_code_list, trade_date=trade_date)[
        ['ts_code', 'trade_date', 'vol', 'amount']]
    df_cap = query.query_daily_basic(codes=ts_code_list, trade_date=trade_date)[
        ['ts_code', 'trade_date', 'pe', 'pb', 'total_mv']]

    _df = pd.merge(df_volume, df_cap, how='left', on=['ts_code', 'trade_date'])
    _df = pd.merge(_df, df_stock, how='left', on='ts_code')

    _df = _df.sort_values(by=f'{target}', ascending=False)

    return _df


def viz_volume_data(x, y, date, name, target='vol', title=''):
    pie = (
        Pie(init_opts=opts.InitOpts(width="1500px", height="900px", page_title=f'成交量占比'))
        .add(
                "成交量",
                [list(z) for z in zip(x, y)],
                radius=["0%", "50%"],
                # center=["60%", "50%"],
                # rosetype="radius",  # area
            )
        .set_global_opts(
            title_opts=opts.TitleOpts(title=f"VolumeRatio@{date}:{title}|{target}"),
            legend_opts=opts.LegendOpts(
                is_show=False,
                # orient="vertical",
                # pos_top="15%",
                # pos_left="2%"
            ),
        )
        .set_series_opts(label_opts=opts.LabelOpts(
            position='right',
            formatter="{b}: {d}%"),
        )
    )

    pie.render(f'{name}.html')
    # pie.render(path='Volume_ratio.pdf')
    # make_snapshot(snapshot, pie.render('test.html'), 'Vol_ratio.png')


if __name__ == '__main__':
    token = 'b655ccb1ca652edcc7f3e22b9996dff6c4a6a7c81996a8cc30f068cb'
    pro = ts.pro_api()

    today = datetime.datetime.today().strftime('%Y%m%d')
    pretrade_date = pro.trade_cal(start_date=f'{today}', end_date=f'{today}', fields='pretrade_date').values[0][0]

    groupby = 'total_mv'
    target = 'amount'
    qtl = 20
    df = process_viz_volume_data(trade_date=pretrade_date, target=target)
    df_gb = group_by(df, groupby=groupby, parts=qtl)
    df_gb_plot = group_by4plot(df_gb, groupby=groupby, target=target)
    viz_volume_data(df_gb_plot.name.tolist(), df_gb_plot[f'{target}'].tolist(), date=pretrade_date, name='Volume_ratio',
                    target=target)

    # plot quantile detail
    quantile = 1
    df = df_gb[df_gb[f'{groupby}_indicator'] == '%s@Qtl%02d' % (groupby, quantile)]
    viz_volume_data(df.name.tolist(), df[f'{target}'].tolist(), date=pretrade_date, name=f'Volume_ratio_Qtl{quantile}',
                    target=target, title=f'Qtl{quantile}')
