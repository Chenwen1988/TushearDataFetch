# _*_ coding: utf-8 _*_

"""
http://www.tushare.org

Query Tushare Data / Save to MySQL, CSV File

"""

__auth__ = "Chen Chen, Modeling Center, BBD"

# show variables like '%expire_logs%';
# set global expire_logs_days=5;
# PURGE MASTER LOGS BEFORE '2003-04-02 22:46:26';// 删除2003-04-02 22:46:26之前产生的所有日志

import os
import sys
import json
import time
import socket
import pymysql
import logging
import datetime
import pandas as pd
import tushare as ts
from logging import handlers
from sqlalchemy import create_engine
from AutoEmail import *

logging.basicConfig(level=logging.INFO)  # CRITICAL > ERROR > WARNING > INFO > DEBUG
logging.debug(u"如果设置了日志级别为NOTSET，那么这里可以采取debug、info的级别的内容也可以显示在控制台上")


def func_timer(func):
    def func_wrapper(*args, **kwargs):
        time_start = time.process_time()
        result = func(*args, **kwargs)
        time_end = time.process_time()
        time_taken = time_end - time_start
        print(f'{func.__name__} running time: {round(time_taken, 2)} s')
        return result
    return func_wrapper


class Config(object):
    PATH_FILE = os.path.dirname(__file__)
    ConfigFolder = 'Config'
    ConfigFile = 'config.json'
    OutputFolder = 'Output'
    LogFolder = 'Log'
    LogFile = 'log'

    @staticmethod
    def get_config():
        log = Logger()
        path_config = os.path.join(Config.PATH_FILE, Config.ConfigFolder, Config.ConfigFile)
        log.logger.info('_' * 80)
        log.logger.info(f"Read Configuration @{datetime.datetime.today().strftime('%Y%m%d %H:%M:%S')}...")
        with open(path_config) as file:
            config_json = json.loads(file.read())
            for k, v in config_json.items():
                content = f'* {k}:  {v}'
                log.logger.info(content)
        log.logger.info('_' * 80)
        log.logger.handlers.pop()
        logging.shutdown()

        return config_json

    @staticmethod
    def create_folder(path_file, folder):
        # log = Logger()
        path_folder_output = os.path.join(path_file, folder)
        if not os.path.exists(path_folder_output):
            os.makedirs(path_folder_output)
            # log.logger.info(f'{folder} folder is created.')
        else:
            # log.logger.info(f'{folder} folder exists.')
            pass
        # log.logger.handlers.pop()

    @staticmethod
    def save2csv(df, path, file, index=False):
        df.to_csv(os.path.join(path, file), index=index, encoding='utf-8')


class Logger(Config):
    level_relations = {
        'debug': logging.DEBUG,
        'info': logging.INFO,
        'warning': logging.WARNING,
        'error': logging.ERROR,
        'crit': logging.CRITICAL
    }  # 日志级别关系映射

    def __init__(self):
        # 第一步，创建一个logger
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(level=logging.INFO)  # Log等级总开关

        # 第二步，创建一个handler，用于写入日志文件
        # timestamp = datetime.datetime.today().strftime('%Y%m%d')
        # log_file = os.path.join(Config.PATH_FILE, Config.LogFolder, timestamp + Config.LogFile)
        script_name = os.path.basename(__file__.strip('.py'))
        log_file = os.path.join(Config.PATH_FILE, Config.LogFolder, script_name + Config.LogFile)
        # if os.path.exists(log_file):
        #     os.remove(log_file)
        Config.create_folder(Config.PATH_FILE, Config.LogFolder)
        # fh = logging.FileHandler(log_file, mode='a')
        fh = handlers.TimedRotatingFileHandler(filename=log_file, backupCount=10, when='H', encoding='utf-8')

        fh.setLevel(logging.DEBUG)  # 输出到file的log等级的开关

        # 第三步，定义handler的输出格式
        fmt = logging.Formatter("%(asctime)s - %(filename)s@%(funcName)s[line:%(lineno)d] - %(levelname)s: %(message)s")
        fh.setFormatter(fmt)
        # th = handlers.TimedRotatingFileHandler(filename=log_file, backupCount=2, when='H',
        #                                        encoding='utf-8')  # 往文件里写入#指定间隔时间自动生成文件的处理器
        # th.setFormatter(fmt)  # 设置写入格式
        # Calculate the real rollover interval, which is just the number of
        # seconds between rollovers.  Also set the filename suffix used when
        # a rollover occurs.  Current 'when' events supported:
        # S - Seconds
        # M - Minutes
        # H - Hours
        # D - Days
        # midnight - roll over at midnight
        # W{0-6} - roll over on a certain day; 0 - Monday
        #
        # Case of the 'when' specifier is not important; lower or upper case
        # will work.

        # 第四步，将logger中添加handler
        self.logger.addHandler(fh)
        # self.logger.addHandler(th)  # 把对象加到logger里


class TsBase(object):
    def __init__(self, config):
        self.config = config
        self.token = self.config.get('token')
        ts.set_token(self.token)
        self.pro = ts.pro_api(timeout=float(self.config.get('timeout')))
        self.host = self.config.get('host')
        self.user = self.config.get('user')
        self.password = self.config.get('password')
        self._conn = None
        self._cursor = None


class Dump2MySQL(TsBase):
    """
    Dump data to MySQL

    pymysql.constants.FIELD_TYPE
        DECIMAL = 0
        TINY = 1
        SHORT = 2
        LONG = 3
        FLOAT = 4
        DOUBLE = 5
        NULL = 6
        TIMESTAMP = 7
        LONGLONG = 8
        INT24 = 9
        DATE = 10
        TIME = 11
        DATETIME = 12
        YEAR = 13
        NEWDATE = 14
        VARCHAR = 15
        BIT = 16
        JSON = 245
        NEWDECIMAL = 246
        ENUM = 247
        SET = 248
        TINY_BLOB = 249
        MEDIUM_BLOB = 250
        LONG_BLOB = 251
        BLOB = 252
        VAR_STRING = 253
        STRING = 254
        GEOMETRY = 255
        CHAR = TINY
        INTERVAL = ENUM
    """

    def __init__(self, logger, db):
        super().__init__(QueryTsData.config)
        self.log = logger
        self.database = db

    def _create_connection(self):
        """
        No need to specify a database in case intend to create one
        :return: MySQL connection
        """
        self._conn = pymysql.connect(host=self.host, user=self.user, password=self.password)

    def create_database(self, database):
        """
        Create a database
        """
        self._create_connection()
        self._cursor = self._conn.cursor()
        self._cursor.execute(
            f'CREATE DATABASE IF NOT EXISTS {database} DEFAULT CHARSET utf8 COLLATE utf8_general_ci;')
        self._cursor.close()
        self._conn.close()

    def create_table_stock_basic(self, drop_if_exist=False):
        """
        Create a table: stock_basic
        :param drop_if_exist: Default False
        :return:
        """
        self._conn = pymysql.connect(host=self.host, user=self.user, password=self.password, database=self.database)
        self._cursor = self._conn.cursor()
        if drop_if_exist:
            self._cursor.execute('DROP TABLE IF EXISTS stock_basic')
        sql = """CREATE TABLE IF NOT EXISTS `stock_basic` (
                    `ts_code` varchar (12) NOT NULL,
                    `symbol` varchar(12) NOT NULL,
                    `name` varchar (100) NOT NULL,
                    `area` varchar (6) NULL,
                    `industry` varchar (10) NULL,
                    `fullname` varchar (100) NULL,
                    `enname` varchar (100) NULL,
                    `market` varchar (10) NULL,
                    `exchange` varchar (10) NULL,
                    `curr_type` varchar (10) NULL,
                    `list_status` varchar (2) NOT NULL,
                    `list_date` varchar (10) NOT NULL,
                    `delist_date` varchar (10) NULL,
                    `is_hs` varchar (2) NULL,
                    PRIMARY KEY (`ts_code`)
                  ) ENGINE=InnoDB  DEFAULT CHARSET=utf8 AUTO_INCREMENT=0"""
        self._cursor.execute(sql)
        self._cursor.close()
        self._conn.commit()
        self._conn.close()

    def create_table_namechange(self, drop_if_exist=False):
        """
        Create a table: Name_Change
        :param drop_if_exist: Default False
        :return:
        """
        self._conn = pymysql.connect(host=self.host, user=self.user, password=self.password, database=self.database)
        self._cursor = self._conn.cursor()
        if drop_if_exist:
            self._cursor.execute('DROP TABLE IF EXISTS name_change')
        sql = """CREATE TABLE IF NOT EXISTS `name_change` (
                    `ts_code` varchar (12) NOT NULL,
                    `name` varchar (100) NOT NULL,
                    `start_date` varchar (10) NULL,
                    `end_date` varchar (10) NULL,
                    `ann_date` varchar (10) NULL,
                    `change_reason` varchar (100) NULL
                  ) ENGINE=InnoDB  DEFAULT CHARSET=utf8 AUTO_INCREMENT=0"""
        self._cursor.execute(sql)
        self._cursor.close()
        self._conn.commit()
        self._conn.close()

    def create_table_pro_bar(self, adj, drop_if_exist=False):
        """
        Create a table: Pro Bar
        :param adj: How to adjust data
        :param drop_if_exist:  Default False
        :return:
        """
        self._conn = pymysql.connect(host=self.host, user=self.user, password=self.password, database=self.database)
        self._cursor = self._conn.cursor()
        if drop_if_exist:
            self._cursor.execute(f'DROP TABLE IF EXISTS pro_bar_daily_{adj}')
        sql = f"""CREATE TABLE IF NOT EXISTS `pro_bar_daily_{adj}` (
                        `ts_code` varchar (12) NOT NULL,
                        `trade_date` varchar (10) NOT NULL,
                        `open` float,
                        `high` float,
                        `low` float,
                        `close` float,
                        `pre_close` float,
                        `change` float,
                        `pct_chg` float,
                        `vol` float,
                        `amount` float,
                        PRIMARY KEY (`ts_code`, `trade_date`)
                    ) ENGINE=Myisam  DEFAULT CHARSET=utf8 AUTO_INCREMENT=0"""
        self._cursor.execute(sql)
        self._cursor.close()
        self._conn.commit()
        self._conn.close()

    def create_table_adjust_factor(self, drop_if_exist=False):
        """
        Create a table: Adjust Factor
        :param drop_if_exist:
        :return:
        """
        self._conn = pymysql.connect(host=self.host, user=self.user, password=self.password, database=self.database)
        self._cursor = self._conn.cursor()
        if drop_if_exist:
            self._cursor.execute('DROP TABLE IF EXISTS adjust_factor')
        sql = """CREATE TABLE IF NOT EXISTS `adjust_factor` (
                    `ts_code` varchar (12) NOT NULL,
                    `trade_date` varchar (10) NOT NULL,
                    `adj_factor` float,
                    PRIMARY KEY (`ts_code`, `trade_date`)
                ) ENGINE=InnoDB  DEFAULT CHARSET=utf8 AUTO_INCREMENT=0"""
        self._cursor.execute(sql)
        self._cursor.close()
        self._conn.commit()
        self._conn.close()

    def create_table_daily_basic(self, drop_if_exist=False):
        """
        Create a table: Daily Basic
        :param drop_if_exist:
        :return:
        """
        self._conn = pymysql.connect(host=self.host, user=self.user, password=self.password, database=self.database)
        self._cursor = self._conn.cursor()
        if drop_if_exist:
            self._cursor.execute('DROP TABLE IF EXISTS daily_basic')
        sql = """CREATE TABLE IF NOT EXISTS `daily_basic` (
                              `ts_code` varchar (12) NOT NULL,
                              `trade_date` varchar (10) NOT NULL,
                              `close` float,
                              `turnover_rate` float,
                              `turnover_rate_f` float,
                              `volume_ratio` float,
                              `pe` float,
                              `pe_ttm` float,
                              `pb` float,
                              `ps` float,
                              `ps_ttm` float,
                              `dv_ratio` float,
                              `dv_ttm` float,
                              `total_share` float,
                              `float_share` float,
                              `free_share` float,
                              `total_mv` float,
                              `circ_mv` float,
                              PRIMARY KEY (`ts_code`, `trade_date`)
                        ) ENGINE=InnoDB  DEFAULT CHARSET=utf8 AUTO_INCREMENT=0"""
        self._cursor.execute(sql)
        self._cursor.close()
        self._conn.commit()
        self._conn.close()

    def create_table_trade_calender(self, drop_if_exist=False):
        """
        Create a table: Trade Calender
        :param drop_if_exist:
        :return:
        """
        self._conn = pymysql.connect(host=self.host, user=self.user, password=self.password, database=self.database)
        self._cursor = self._conn.cursor()
        if drop_if_exist:
            self._cursor.execute('DROP TABLE IF EXISTS trade_calender')
        sql = """CREATE TABLE IF NOT EXISTS `trade_calender` (
                    `exchange` varchar (8) NOT NULL,
                    `cal_date` varchar (10) NOT NULL,
                    `is_open` varchar (1) NOT NULL,
                    `pretrade_date` varchar (10),
                    PRIMARY KEY (`exchange`, `cal_date`)
                ) ENGINE=InnoDB  DEFAULT CHARSET=utf8 AUTO_INCREMENT=0"""
        self._cursor.execute(sql)
        self._cursor.close()
        self._conn.commit()
        self._conn.close()

    def insert_values(self, df, sql):
        self._conn = pymysql.connect(host=self.host, user=self.user, password=self.password, database=self.database)
        self._cursor = self._conn.cursor()
        values = [tuple(x) for x in df.values]
        try:
            # execute sql
            self._cursor.executemany(sql, values)
            # commit
            self._conn.commit()
            self.log.logger.info('Insert all records successfully.')
        except Exception as e:
            self.log.logger.error(e)
            self._conn.rollback()
            self.log.logger.warning('Fail to records all values simultaneously.')
            for value in values:
                try:
                    self._cursor.execute(sql, value)
                    self._conn.commit()
                    self.log.logger.info(f'Insert {value}...')
                except Exception as err:
                    self.log.logger.error(err)
                    self._conn.rollback()
                    # self.log.logger.warning('Fail to insert record, record already exists...')
        finally:
            self._cursor.close()
            self._conn.close()
            # self.log.logger.handlers.pop()


class QueryTsData(TsBase):
    config = Config.get_config()
    __BULK_SIZE = 20

    def __init__(self, _database):
        self.database = _database
        self.log = Logger()
        super().__init__(QueryTsData.config)
        self.start, self.end = self.get_start_end()

    def get_stock_codes(self):
        _conn = pymysql.connect(host=QueryTsData.config['host'], user=QueryTsData.config['user'],
                                password=QueryTsData.config['password'], database=self.database)
        _cursor = _conn.cursor()
        _cursor.execute('SELECT * FROM stock_basic;')
        _data_stockbasics = _cursor.fetchall()
        _stock_basic_columns = [col[0] for col in _cursor.description]
        _df = pd.DataFrame(list(_data_stockbasics))
        _df.columns = _stock_basic_columns
        _codes = _df.ts_code.tolist()
        if 'T00018.SH' in _codes:
            _codes.remove('T00018.SH')
        _cursor.close()
        _conn.close()

        return _codes

    def get_bar(self, _):
        _conn = pymysql.connect(host=QueryTsData.config['host'], user=QueryTsData.config['user'],
                                password=QueryTsData.config['password'], database=self.database)
        _cursor = _conn.cursor()
        sql = f"SELECT * FROM pro_bar_daily_ WHERE ts_code = '{_}';"
        _cursor.execute(sql)
        _data_pro_bar = _cursor.fetchall()
        _pro_bar_columns = [col[0] for col in _cursor.description]
        _df = pd.DataFrame(list(_data_pro_bar))
        if _df.empty:
            return _df
        _df.columns = _pro_bar_columns
        _cursor.close()
        _conn.close()

        return _df

    def get_adjust_factor(self, _):
        _conn = pymysql.connect(host=QueryTsData.config['host'], user=QueryTsData.config['user'],
                                password=QueryTsData.config['password'], database=self.database)
        _cursor = _conn.cursor()
        sql = f"SELECT * FROM adjust_factor WHERE ts_code='{_}';"
        _cursor.execute(sql)
        _data_adjust_factor = _cursor.fetchall()
        _adjust_factor_columns = [col[0] for col in _cursor.description]
        _df = pd.DataFrame(list(_data_adjust_factor))
        _df.columns = _adjust_factor_columns
        _cursor.close()
        _conn.close()

        return _df

    def get_columns_from_sql(self, table):
        _conn = pymysql.connect(host=QueryTsData.config['host'], user=QueryTsData.config['user'],
                                password=QueryTsData.config['password'], database=self.database)
        _cursor = _conn.cursor()
        _cursor.execute(f'SELECT * FROM {table} Limit 1;')
        description = _cursor.description
        _columns = [f'`{col[0]}`' for col in description]

        return _columns

    def get_data_from_sql(self, sql):
        _conn = pymysql.connect(host=QueryTsData.config['host'], user=QueryTsData.config['user'],
                                password=QueryTsData.config['password'], database=self.database)
        _cursor = _conn.cursor()
        _cursor.execute(sql)
        _data = _cursor.fetchall()
        _columns = [col[0] for col in _cursor.description]
        _df = pd.DataFrame(list(_data))
        _df.columns = _columns
        _cursor.close()
        _conn.close()

        return _df

    @staticmethod
    def gen_insert_sql(table, columns):
        _fields = ','.join(columns)
        values_format = ','.join(['%s' for _ in columns])
        sql = f"""INSERT INTO {table}({_fields}) VALUES({values_format})"""

        return sql

    @staticmethod
    def gen_select_sql(table):
        return f"SELECT * FROM {table};"

    @classmethod
    def get_start_end(cls):
        if int(cls.config.get("if_rolling")) != 0:
            today = datetime.datetime.today()
            end_date = today.strftime('%Y%m%d')
            start_date = (today - datetime.timedelta(cls.config.get("rolling_back"))).strftime('%Y%m%d')
        else:
            end_date = cls.config.get("end_date")
            start_date = cls.config.get("start_date")

        return start_date, end_date

    @staticmethod
    def query_full_data(func, **kwargs):
        df = pd.DataFrame()
        df_tmp = func(**kwargs)
        df = df.append(df_tmp)
        trade_date_min = df.trade_date.min()
        pre_trade_date_min = (
                datetime.datetime.strptime(trade_date_min, '%Y%m%d') - datetime.timedelta(days=1)).strftime(
            '%Y%m%d')
        if trade_date_min > '19990101':
            kwargs['end_date'] = pre_trade_date_min
            kwargs['start_date'] = '19900101'
            df_tmp = func(**kwargs)
            df = df.append(df_tmp)

        return df

    def save_mysql(self, sql, df):
        """
        :param sql: sql
        :param df: df
        :return:
        """
        _dump = Dump2MySQL(self.log, self.database)
        _dump.insert_values(df, sql)

    def save_csv(self, table, sql):
        df = self.get_data_from_sql(sql)
        Config.create_folder(Config.PATH_FILE, Config.OutputFolder)
        Config.save2csv(df, os.path.join(Config.PATH_FILE, Config.OutputFolder), f'{table}.csv')
        self.log.logger.info(f"{table}.csv updates on {datetime.datetime.today().strftime('%Y%m%d')}.")

    @func_timer
    def query_stock_basic(self, list_status='', fields='', csv=True, mysql=True):
        """
            描述：获取基础信息数据，包括股票代码、名称、上市日期、退市日期等

            输入参数:
            名称	        类型	        必选	        描述
            is_hs	    str	        N	        是否沪深港通标的：N否 H沪股通 S深股通
            list_status	str	        N	        上市状态：L上市 D退市 P暂停上市，默认L
            exchange	str	        N	        交易所: SSE上交所 SZSE深交所 HKEX港交所(未上线)

            输出参数:
            名称	        类型	        描述
            ts_code	    str	        TS代码
            symbol	    str	        股票代码
            name	    str	        股票名称
            area	    str	        所在地域
            industry	str	        所属行业
            fullname	str	        股票全称
            enname	    str	        英文全称
            market	    str	        市场类型 （主板/中小板/创业板/科创板）
            exchange	str	        交易所代码
            curr_type	str	        交易货币
            list_status	str	        上市状态： L上市 D退市 P暂停上市
            list_date	str	        上市日期
            delist_date	str	        退市日期
            is_hs	    str	        是否沪深港通标的，N否 H沪股通 S深股通

            e.g.
                  ts_code       symbol      name        area        industry        list_date
            0     000001.SZ     000001      平安银行    深圳         银行            19910403
            1     000002.SZ     000002      万科A       深圳         全国地产        19910129
            2     000004.SZ     000004      国农科技    深圳         生物制药        19910114
            6     000008.SZ     000008      神州高铁    北京         运输设备        19920507
            """
        table = 'stock_basic'
        _df_stock_basic = self.pro.stock_basic(list_status=list_status, fields=fields)
        _df_stock_basic = _df_stock_basic.where(_df_stock_basic.notnull(), None)
        self.log.logger.info(f'{sys._getframe().f_code.co_name} query {list_status} data successfully!')

        if mysql:
            columns = _df_stock_basic.columns
            sql = QueryTsData.gen_insert_sql(table, columns)
            self.save_mysql(sql, _df_stock_basic)
            self.log.logger.info(f'Stock Basic dumps to MySQL.')

        if csv:
            sql = QueryTsData.gen_select_sql(table)
            self.save_csv(table, sql)

        return _df_stock_basic

    @func_timer
    def query_namechange(self, csv=True, mysql=True):
        """
            描述：历史名称变更记录

            输入参数：
            名称	        类型	    必选	    描述
            ts_code	    str	    N	    TS代码
            start_date	str	    N	    公告开始日期
            end_date	str	    N	    公告结束日期

            输出参数：
            名称	            类型	    默认输出	    描述
            ts_code	        str	    Y	        TS代码
            name	        str	    Y	        证券名称
            start_date	    str	    Y	        开始日期
            end_date	    str	    Y	        结束日期
            ann_date	    str	    Y	        公告日期
            change_reason	str	    Y	        变更原因

                ts_code         name       start_date    end_date         change_reason
            0   600848.SH       上海临港    20151118      None             改名
            1   600848.SH       自仪股份    20070514      20151117         撤销ST
            2   600848.SH       ST自仪      20061026      20070513        完成股改
            3   600848.SH       SST自仪     20061009      20061025        未股改加S
            4   600848.SH       ST自仪      20010508      20061008        ST
            5   600848.SH       自仪股份    19940324      20010507         其他
            """
        table = 'name_change'
        codes = self.get_stock_codes()

        name_change_columns = self.get_columns_from_sql(table)
        columns = [column.strip('`') for column in name_change_columns]
        _df_namechange = pd.DataFrame(columns=columns)

        def get_namechange(_):
            df_tmp = self.pro.namechange(ts_code=_)
            self.log.logger.info(f'{sys._getframe().f_code.co_name}: {_}, {codes.index(code)}/{len(codes)}')
            return df_tmp

        count = 0
        while True:
            code = codes[count]
            try:
                _df = get_namechange(code)
                _df = _df.where(_df.notnull(), None)
                _df_namechange = _df_namechange.append(_df)
                count += 1
                if count == len(codes):
                    break
            except Exception as e:
                self.log.logger.info(f'{e}')
                time.sleep(1)
                continue
        # print(_df_namechange)

        if mysql:
            columns = self.get_columns_from_sql(table)
            sql = QueryTsData.gen_insert_sql(table, columns)
            self.save_mysql(sql, _df_namechange)
            self.log.logger.info(f'Name Change dumps to MySQL.')

        if csv:
            sql = QueryTsData.gen_select_sql(table)
            self.save_csv(table, sql)

        return _df_namechange

    @func_timer
    def query_adjust_factor(self, trade_date='', csv=True, mysql=True):
        """
        输入参数：
        名称	        类型	    必选	    描述
        ts_code	    str	    Y	    股票代码
        trade_date	str	    N	    交易日期(YYYYMMDD，下同)
        start_date	str	    N	    开始日期
        end_date	str	    N	    结束日期

        输出参数：
        名称	        类型	    描述
        ts_code	    str	    股票代码
        trade_date	str	    交易日期
        adj_factor	float	复权因子
        """

        table = 'adjust_factor'
        codes = self.get_stock_codes()
        count = 0
        _df_adjust_factor = pd.DataFrame()
        while True:
            code = codes[count]
            try:
                if self.config.get('is_rolling') == 0:
                    _df_adjust_factor_tmp = self.query_full_data(self.pro.adj_factor, ts_code=code,
                                                                 trade_date=trade_date,
                                                                 start_date=self.start, end_date=self.end)
                else:
                    _df_adjust_factor_tmp = self.pro.adj_factor(ts_code=code, trade_date=trade_date,
                                                                start_date=self.start, end_date=self.end)
                if _df_adjust_factor_tmp.empty:
                    count += 1
                    continue
                _df_adjust_factor_tmp = _df_adjust_factor_tmp.where(_df_adjust_factor_tmp.notnull(), None)
                _df_adjust_factor = _df_adjust_factor.append(_df_adjust_factor_tmp)
                count += 1
                self.log.logger.info(f'adjust factor: {code}, {codes.index(code)}/{len(codes)}')
                if count == len(codes):
                    break
            except Exception as e:
                self.log.logger.info(f'{e}')
                time.sleep(1)
                continue

        if mysql:
            columns = self.get_columns_from_sql(table)
            sql = QueryTsData.gen_insert_sql(table, columns)
            self.save_mysql(sql, _df_adjust_factor)
            self.log.logger.info(f'Adjust Factor dumps to MySQL.')

        if csv:
            sql = QueryTsData.gen_select_sql(table)
            self.save_csv(table, sql)

    @func_timer
    def query_pro_bar_daily(self, adj='adj', csv=True, mysql=True):
        """
            描述：目前整合了股票（未复权、前复权、后复权）、指数、数字货币、ETF基金、期货、期权的行情数据，
            未来还将整合包括外汇在内的所有交易行情数据，同时提供分钟数据。不同数据对应不同的积分要求，具体请参阅每类数据的文档说明。

            输入参数：
            名称	    类型	    必选	    描述
            ts_code	    str	        Y	        证券代码
            api	        str         N	        pro版api对象，如果初始化了set_token，此参数可以不需要
            start_date	str	        N	        开始日期 (格式：YYYYMMDD，提取分钟数据请用2019-09-01 09:00:00这种格式)
            end_date	str	        N	        结束日期 (格式：YYYYMMDD)
            asset	    str	        Y	        资产类别：E股票 I沪深指数 C数字货币 FT期货 FD基金 O期权 CB可转债（v1.2.39），默认E
            adj	        str	        N	        复权类型(只针对股票)：None未复权 qfq前复权 hfq后复权 , 默认None
            freq	    str	        Y	        数据频度 ：支持分钟(min)/日(D)/周(W)/月(M)K线，其中1min表示1分钟（类推1/5/15/30/60分钟） ，默认D。对于分钟数据有600积分用户可以试用（请求2次），正式权限请在QQ群私信群主或积分管理员。
            ma	        list	    N	        均线，支持任意合理int数值。注：均线是动态计算，要设置一定时间范围才能获得相应的均线，比如5日均线，开始和结束日期参数跨度必须要超过5日。目前只支持单一个股票提取均线，即需要输入ts_code参数。
            factors	    list	    N	        股票因子（asset='E'有效）支持 tor换手率 vr量比
            adjfactor	str	        N	        复权因子，在复权数据时，如果此参数为True，返回的数据中则带复权因子，默认为False。 该功能从1.2.33版本开始生效

            输出指标：
            具体输出的数据指标可参考各行情具体指标：
            股票Daily：https://tushare.pro/document/2?doc_id=27
            基金Daily：https://tushare.pro/document/2?doc_id=127
            期货Daily：https://tushare.pro/document/2?doc_id=138
            期权Daily：https://tushare.pro/document/2?doc_id=159
            指数Daily：https://tushare.pro/document/2?doc_id=95
            数字货币：https://tushare.pro/document/41?doc_id=4

            #取000001的前复权行情
            df = ts.pro_bar(ts_code='000001.SZ', adj='qfq', start_date='20180101', end_date='20181011')
                        ts_code     trade_date      open        high        low      close  \
            trade_date
            20181011    000001.SZ   20181011        1085.71     1097.59     1047.90  1065.19
            20181010    000001.SZ   20181010        1138.65     1151.61     1121.36  1128.92
            20181009    000001.SZ   20181009        1130.00     1155.93     1122.44  1140.81
            20181008    000001.SZ   20181008        1155.93     1165.65     1128.92  1128.92
            20180928    000001.SZ   20180928        1164.57     1217.51     1164.57  1193.74
            """
        table = f'pro_bar_daily_{adj}'
        _df_all_bar = pd.DataFrame()

        # save to mysql
        if mysql:
            codes = self.get_stock_codes()
            count = 0
            columns = self.get_columns_from_sql(table)
            while True:
                code = codes[count]
                try:
                    if self.config.get('is_rolling') == 0:
                        bar_data = self.query_full_data(ts.pro_bar, adj=adj, ts_code=code,
                                                        start_date=self.start, end_date=self.end)
                    else:
                        bar_data = ts.pro_bar(adj=adj, ts_code=code, start_date=self.start, end_date=self.end)
                    if bar_data is None:
                        count += 1
                        continue
                    bar_data = bar_data.where(bar_data.notnull(), None)
                    _df_all_bar = pd.concat([_df_all_bar, bar_data], ignore_index=True)
                    count += 1
                    if count % QueryTsData.__BULK_SIZE == 0:
                        sql = QueryTsData.gen_insert_sql(table, columns)
                        self.save_mysql(sql, _df_all_bar)
                        _df_all_bar = pd.DataFrame()
                    self.log.logger.info(f"query bar {code} {codes.index(code)}/{len(codes)}")
                    if count == len(codes):
                        if count % QueryTsData.__BULK_SIZE != 0:
                            sql = QueryTsData.gen_insert_sql(table, columns)
                            self.save_mysql(sql, _df_all_bar)
                        break
                except Exception as e:
                    self.log.logger.info(f'{e}')
                    # time.sleep(1)
                    if str(e) == '\'NoneType\' object has no attribute \'values\'':
                        count += 1
                    continue
            self.log.logger.info(f'Pro Bar {adj} dumps to MySQL.')

        # save to csv
        if csv:
            sql = QueryTsData.gen_select_sql(table)
            self.save_csv(table, sql)

        # return _df_all_bar

    @func_timer
    def gen_adjust_bar(self, mysql=True, csv=False):
        codes = self.get_stock_codes()
        for code in codes:
            df_raw = self.get_bar(code)
            if df_raw.empty:
                continue
            df_adjust_factor = self.get_adjust_factor(code)
            adj_columns = ['open', 'high', 'low', 'close', 'pre_close']
            df_merge = pd.merge(df_raw, df_adjust_factor, how='left', on=['ts_code', 'trade_date'])
            _ = df_merge.trade_date.max()  # max_trade_date
            for column in adj_columns:
                df_merge[f'{column}_hfq'] = df_merge[column] * df_merge['adj_factor']
                df_merge[f'{column}_qfq'] = df_merge[f'{column}_hfq'] / df_merge.query('trade_date==@_')[
                    'adj_factor'].values

            columns = df_raw.columns
            decimals = pd.Series([2 for _ in adj_columns], index=adj_columns)

            # forward adjust
            df_hfq = df_merge[
                ['ts_code', 'trade_date', 'open_hfq', 'high_hfq', 'low_hfq', 'close_hfq', 'pre_close_hfq', 'change',
                 'pct_chg', 'vol', 'amount']]
            df_hfq.columns = columns
            df_hfq = df_hfq.round(decimals)

            # back adjust
            df_qfq = df_merge[
                ['ts_code', 'trade_date', 'open_qfq', 'high_qfq', 'low_qfq', 'close_qfq', 'pre_close_qfq', 'change',
                 'pct_chg', 'vol', 'amount']]
            df_qfq.columns = columns
            df_qfq = df_qfq.round(decimals)

            if mysql:
                self.insert_dataframe(table='pro_bar_daily_hfq', df=df_hfq)
                self.log.logger.info(f'{code} hfq data dumps to MySQL. {codes.index(code)}/{len(codes)}')
                self.insert_dataframe(table='pro_bar_daily_qfq', df=df_qfq)
                self.log.logger.info(f'{code} qfq data dumps to MySQL. {codes.index(code)}/{len(codes)}')

            if csv:
                Config.create_folder(Config.PATH_FILE, Config.OutputFolder)
                Config.create_folder(os.path.join(Config.PATH_FILE, Config.OutputFolder), 'hfq')
                Config.save2csv(df_hfq, os.path.join(Config.PATH_FILE, Config.OutputFolder, 'hfq'), f'{code}.csv')
                self.log.logger.info(f'{code} hfq data save to csv. {codes.index(code)}/{len(codes)}')
                Config.create_folder(os.path.join(Config.PATH_FILE, Config.OutputFolder), 'qfq')
                Config.save2csv(df_qfq, os.path.join(Config.PATH_FILE, Config.OutputFolder, 'qfq'), f'{code}.csv')
                self.log.logger.info(f'{code} qfq data save to csv. {codes.index(code)}/{len(codes)}')

    @func_timer
    def query_daily_basic(self, trade_date='', fields='', csv=True, mysql=True):
        """
        输入参数：
        名称	        类型	    必选	    描述
        ts_code	    str	    Y	    股票代码（二选一）
        trade_date	str	    N	    交易日期 （二选一）
        start_date	str	    N	    开始日期(YYYYMMDD)
        end_date	str	    N	    结束日期(YYYYMMDD)

        输出参数：
        名称	            类型	    描述
        ts_code	        str	    TS股票代码
        trade_date	    str	    交易日期
        close	        float	当日收盘价
        turnover_rate	float	换手率（%）
        turnover_rate_f	float	换手率（自由流通股）
        volume_ratio	float	量比
        pe	            float	市盈率（总市值/净利润， 亏损的PE为空）
        pe_ttm	        float	市盈率（TTM，亏损的PE为空）
        pb	            float	市净率（总市值/净资产）
        ps	            float	市销率
        ps_ttm	        float	市销率（TTM）
        dv_ratio	    float	股息率 （%）
        dv_ttm	        float	股息率（TTM）（%）
        total_share	    float	总股本 （万股）
        float_share	    float	流通股本 （万股）
        free_share	    float	自由流通股本 （万）
        total_mv	    float	总市值 （万元）
        circ_mv	        float	流通市值（万元）
        """
        table = 'daily_basic'
        codes = self.get_stock_codes()
        count = 0
        # _df_daily_basic = pd.DataFrame()
        columns = self.get_columns_from_sql(table)

        while True:
            code = codes[count]
            try:
                if self.config.get('if_rolling') == 0:
                    _df_daily_basic_tmp = self.query_full_data(self.pro.daily_basic, ts_code=code,
                                                               trade_date=trade_date, fields=fields)
                else:
                    _df_daily_basic_tmp = self.pro.daily_basic(ts_code=code, trade_date=trade_date, fields=fields,
                                                               start_date=self.start, end_date=self.end)
                if _df_daily_basic_tmp.empty:
                    count += 1
                    continue
                _df_daily_basic_tmp = _df_daily_basic_tmp.where(_df_daily_basic_tmp.notnull(), None)
                # _df_daily_basic = _df_daily_basic.append(_df_daily_basic_tmp)
                count += 1

                # if count % QueryTsData.__BULK_SIZE == 0:
                if mysql:
                    sql = QueryTsData.gen_insert_sql(table, columns)
                    self.save_mysql(sql, _df_daily_basic_tmp)
                    # _df_daily_basic = pd.DataFrame()
                self.log.logger.info(f'daily basic: {code}, {codes.index(code)}/{len(codes)}')

                if csv:
                    Config.create_folder(Config.PATH_FILE, Config.OutputFolder)
                    Config.create_folder(os.path.join(Config.PATH_FILE, Config.OutputFolder), 'daily_basic')
                    sql = f"SELECT * FROM {table} WHERE ts_code = '{code}';"
                    df2csv = self.get_data_from_sql(sql)
                    Config.save2csv(df2csv, os.path.join(Config.PATH_FILE, Config.OutputFolder, 'daily_basic'),
                                    f'{code}.csv')
                    self.log.logger.info(f'{code} daily basic save to csv. {codes.index(code)}/{len(codes)}')

                if count == len(codes):
                    # if count % QueryTsData.__BULK_SIZE != 0:
                    #     if mysql:
                    #         sql = QueryTsData.gen_insert_sql(table, columns)
                    #         self.save_mysql(sql, _df_daily_basic)
                    break
            except Exception as e:
                self.log.logger.info(f'{e}')
                # time.sleep(1)
                continue
        self.log.logger.info(f'Daily Basic dumps to MySQL.')

    @func_timer
    def query_trade_calender(self, trade_date='', fields='', csv=True, mysql=True):
        """
        输入参数：
        名称	        类型	    必选	    描述
        exchange	str	    N	    交易所 SSE上交所,SZSE深交所,CFFEX 中金所,SHFE 上期所,CZCE 郑商所,DCE 大商所,INE 上能源,IB 银行间,XHKG 港交所
        start_date	str	    N	    开始日期 （格式：YYYYMMDD 下同）
        end_date	str	    N	    结束日期
        is_open	    str	    N	    是否交易 '0'休市 '1'交易

        输出参数：
        名称	            类型	    默认显示	       描述
        exchange	    str	Y	交易所         SSE上交所 SZSE深交所
        cal_date	    str	Y	日历日期
        is_open	        str	Y	是否交易        0休市 1交易
        pretrade_date	str	N	上一个交易日
        """
        table = 'trade_calender'

        _df_trade_cal = self.pro.trade_cal(trade_date=trade_date, fields=fields,
                                           start_date=self.start, end_date=self.end)
        _df_trade_cal = _df_trade_cal.where(_df_trade_cal.notnull(), None)

        if mysql:
            columns = self.get_columns_from_sql(table)
            sql = QueryTsData.gen_insert_sql(table, columns)
            self.save_mysql(sql, _df_trade_cal)
            self.log.logger.info(f'Trade Calender dumps to MySQL.')

        if csv:
            sql = QueryTsData.gen_select_sql(table)
            self.save_csv(table, sql)

    def insert_dataframe(self, table, df):
        """
        insert data into a table
        """
        __port = 3306
        engine = create_engine(
            f'mysql+pymysql://{self.user}:{self.password}@{self.host}:{__port}/{self.database}?charset=utf8')

        pd.io.sql.to_sql(frame=df, name=table, con=engine, if_exists='append', index=False)

        engine.dispose()

    def close_log(self):
        self.log.logger.handlers.pop()


def main():
    # 0. Point to database
    database = 'TushareData'
    dump = Dump2MySQL(TsBase, database)
    dump.create_database(database)
    data = QueryTsData(database)

    def schedule(job_id):
        # 1. stock basics
        if str(job_id) == '1':
            dump.create_table_stock_basic(drop_if_exist=True)
            list_status_list = ['L', 'D', 'P']
            f = 'ts_code, symbol, name, area, industry, list_date, fullname, enname, market, exchange, curr_type, ' \
                'list_status, delist_date, is_hs'
            for list_status in list_status_list:
                data.query_stock_basic(list_status=list_status, fields=f, mysql=True, csv=True)

        # 2. name change
        elif str(job_id) == '2':
            dump.create_table_namechange(drop_if_exist=True)
            data.query_namechange(mysql=True, csv=True)

        # 3. adjust factor
        elif str(job_id) == '3':
            dump.create_table_adjust_factor(drop_if_exist=False)
            data.query_adjust_factor(mysql=True, csv=True)

        # 4. raw data
        elif str(job_id) == '4':
            adj_type = ''
            dump.create_table_pro_bar(adj=adj_type, drop_if_exist=False)
            data.query_pro_bar_daily(adj=adj_type, mysql=True, csv=False)

        # 5. adjust data
        elif str(job_id) == '5':
            adj_types = ['qfq', 'hfq']
            for adj_type in adj_types:
                dump.create_table_pro_bar(adj=adj_type,
                                          drop_if_exist=True)  # adjust data need to update once factor changes
            data.gen_adjust_bar(mysql=True, csv=True)

        # 6. daily basic
        elif str(job_id) == '6':
            dump.create_table_daily_basic(drop_if_exist=False)
            data.query_daily_basic(mysql=True, csv=True)

        # 7. trade calender
        elif str(job_id) == '7':
            dump.create_table_trade_calender(drop_if_exist=False)
            f = 'exchange, cal_date, is_open, pretrade_date'
            data.query_trade_calender(fields=f, mysql=True, csv=True)

    hostname = socket.gethostname()
    address = socket.gethostbyname(hostname)
    credential = os.path.join('Config', 'credential.json')

    try:
        job_list = list(range(8))
        # job_list = list('0')
        for job in job_list:
            schedule(job)

        subject = f'自动邮件：更新成功。TushareData@{address}'  # 邮件标题
        receiver_list = ['chen8684@126.com']
        send_email(subject, credential, receiver_list, 'Tushare 数据更新成功。')

        data.log.logger.info('Data Update Successfully & Send Email!')
        data.close_log()

    except Exception as err:
        data.close_log()
        log = Logger()
        log.logger.error(str(err))

        subject = f'自动邮件：崩!TushareData@{address}'   # 邮件标题
        # receiver_list = ['chenchen@bbdservice.com', 'dengyaxin@bbdservice.com', 'qihang@bbdservice.com',
        #                  'chenwen@bbdservice.com', 'shixiaochun@bbdservice.com']  # 收件人列表

        receiver_list = ['chenchen@bbdservice.com']
        send_email(subject, credential, receiver_list, str(err))

        log.logger.warning('Auto Update Failed!')
        log.logger.handlers.pop()
        logging.shutdown()


if __name__ == '__main__':
    main()
