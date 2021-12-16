# _*_ coding: utf-8 _*_

import datetime
import tushare as ts
import matplotlib.pyplot as plt
import pyecharts.options as opts
from pyecharts.charts import Line

"""
参考地址: https://gallery.echartsjs.com/editor.html?c=xEyDk1hwBx
"""

tranform = lambda x: round(x / 1E8, 2)


def margin_data(_exchange):
    df = pro.margin(exchange_id=_exchange)[::-1]
    df[['rzye', 'rzmre', 'rzche', 'rqye', 'rqmcl', 'rqyl', 'rzrqye']] = \
        df[['rzye', 'rzmre', 'rzche', 'rqye', 'rqmcl', 'rqyl', 'rzrqye']].applymap(tranform)
    x_data = df.trade_date.values

    (
        Line(init_opts=opts.InitOpts(width="1200px", height="500px", page_title=f'两融@{_exchange}'))
        .add_xaxis(xaxis_data=x_data)
        .add_yaxis(
            series_name="融资融券余额",
            # stack="总量",
            y_axis=df.rzrqye.values,
            markline_opts=opts.MarkLineOpts(data=[opts.MarkLineItem(type_="max")]),
            markpoint_opts=opts.MarkPointOpts(data=[opts.MarkPointItem(type_="max", symbol='pin', symbol_size=100)]),
            label_opts=opts.LabelOpts(is_show=False),
        )
        .add_yaxis(
            series_name="融资余额",
            # stack="总量",
            y_axis=df.rzye.values,
            label_opts=opts.LabelOpts(is_show=False),
        )
        .add_yaxis(
            series_name="融资买入额",
            # stack="总量",
            y_axis=df.rzmre.values,
            label_opts=opts.LabelOpts(is_show=False),
        )
        .add_yaxis(
            series_name="融资偿还额",
            # stack="总量",
            y_axis=df.rzche.values,
            label_opts=opts.LabelOpts(is_show=False),
        )
        .add_yaxis(
            series_name="融券余额",
            # stack="总量",
            y_axis=df.rqye.values,
            label_opts=opts.LabelOpts(is_show=False),
        )
        .add_yaxis(
            series_name="融券卖出量",
            # stack="总量",
            y_axis=df.rqmcl.values,
            label_opts=opts.LabelOpts(is_show=False),
        )
        .add_yaxis(
            series_name="融券余量",
            # stack="总量",
            y_axis=df.rqyl.values,
            label_opts=opts.LabelOpts(is_show=False),
        )
        .set_global_opts(
            title_opts=opts.TitleOpts(
                title=f"两融数据@{_exchange}@{datetime.datetime.today().strftime('%Y-%m-%d %H:%M:%S')}",
                pos_left="left",
                pos_top="top",
                ),
            tooltip_opts=opts.TooltipOpts(trigger="axis"),
            yaxis_opts=opts.AxisOpts(
                type_="value",
                name="亿(元)",
                axistick_opts=opts.AxisTickOpts(is_show=True),
                splitline_opts=opts.SplitLineOpts(is_show=True),
            ),
            xaxis_opts=opts.AxisOpts(type_="category", boundary_gap=False),
            datazoom_opts=[
                # opts.DataZoomOpts(range_start=0, range_end=100),
                opts.DataZoomOpts(type_="inside", range_start=50, range_end=100),
            ],
            legend_opts=opts.LegendOpts(orient='horizontal', pos_bottom=0.2, item_height=10, align='auto', padding=5)
        )
        .render(f"margin_trading_{_exchange}.html")
    )


def fund_share_data():
    df = pro.fund_share()[::-1]
    df.sum = df.groupby('trade_date').sum()
    print(df.head())


if __name__ == '__main__':
    token = 'b655ccb1ca652edcc7f3e22b9996dff6c4a6a7c81996a8cc30f068cb'
    pro = ts.pro_api()
    exchanges = ['SSE', 'SZSE']
    for exchange in exchanges:
        margin_data(exchange)

    # fund_share_data() # no authorization
