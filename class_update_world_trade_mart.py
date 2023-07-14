# import logging
import pandas as pd
import warnings
import psycopg2
from sqlalchemy import create_engine
from datetime import datetime
import clickhouse_connect
warnings.simplefilter('ignore')


class Update_world_trade_mart:
    """
    Класс для обновления витрины данных по мировой торговли
    """

    def __init__(self, dct_cfg_pg: dict, dct_cfg_ch: dict):
        """
        :param dct_cfg_pg: словарь с параметрами подключения к базе данных Postgre
        :param dct_cfg_ch: словарь с параметрами подключения к базе данных Clickhouse
        """
        # Коннект для выгрузки данных из Postgre
        self.psycopg_connect = psycopg2.connect(user=dct_cfg_pg['USER'],
                                                password=dct_cfg_pg['PASSWORD'],
                                                host=dct_cfg_pg['HOST'],
                                                port=dct_cfg_pg['PORT'],
                                                database=dct_cfg_pg['DATABASE'])
        # Движок для загрузки данных в Postgre
        self.sqlalchemy_engine = create_engine('postgresql://{}:{}@{}:{}/{}'
                                               .format(dct_cfg_pg['USER'], dct_cfg_pg['PASSWORD'],
                                                       dct_cfg_pg['HOST'],
                                                       dct_cfg_pg['PORT'],
                                                       dct_cfg_pg['DATABASE']))
        # Клиент для инициализации работы с БД Clickhouse
        self.click_house_client = clickhouse_connect.get_client(host=dct_cfg_ch['HOST'],
                                                                port=dct_cfg_ch['PORT'],
                                                                username=dct_cfg_ch['USER'],
                                                                password=dct_cfg_ch['PASSWORD'],
                                                                database=dct_cfg_ch['DATABASE'])
        # Словарь содержащий коды, значения которых пропущены в group_prod2
        self.dict_nan_tnved_code = {'code_nan': []}

    @staticmethod
    def insert_dict(dct: dict, lst_code: list):
        """
        Функция валидации кодов, значения которых в колонке group_prod2 пропущены
        :param dct: словарь для записи кодов
        :param lst_code: список кодов
        """
        for i in lst_code:
            if i not in dct['code_nan']:
                dct['code_nan'].append(i)

    def update_blank_datamart(self, sql_script: str, need_table: str, flag_truncate: bool = False) -> str:
        """
        Функция обновления промежуточной таблицы
        :param sql_script: sql скрипт, чтобы забрать данных из основной БД
        :param need_table: название промежуточной таблицы, которая будет очищена и в которую будет
                           вестись запись данных
        :param flag_truncate: флаг для очисткитки/дозаписи в таблицу
        :return: строку с длительностью выполнения операции
        """
        # Очищаем резевруню таблицу, если передан флаг True
        if flag_truncate:
            self.click_house_client.command(f'TRUNCATE TABLE IF EXISTS {need_table}')

        # Начало работы скрипта
        start_full = datetime.now()
        # Получаем датафрейм нарезанный на бакеты
        generator_df = pd.read_sql(sql_script, con=self.psycopg_connect, chunksize=100000)

        # Пробегаемся по датафрейму и обрабатываем каждый срез
        for chunk_df in generator_df:
            flag_iter = True

            # Записываем коды, названия котрых пропущены
            lst_code = chunk_df.loc[chunk_df.group_prod2.isna()].commodity_code.unique()
            self.insert_dict(self.dict_nan_tnved_code, lst_code)

            chunk_df['group_prod2'] = chunk_df['group_prod2'].fillna('Прочая продукция')
            chunk_df['switch_mpt'] = chunk_df.commodity_code.apply(
                lambda x: 'продукции АПК c кодами ТН ВЭД 01–24' if int(
                    str(x)[:2]) < 25 else 'продукции АПК с кодами ТН ВЭД  выше 24-го')
            chunk_df_for_load = chunk_df.groupby(
                ['year', 'trade_flow', 'reporter_code', 'reporter', 'partner_code', 'partner', 'group_prod1',
                 'group_prod2', 'source', 'mirror_columns', 'update_date', 'switch_mpt'], as_index=False)\
                .agg({'trade_value': 'sum', 'netweight': 'sum'})
            chunk_df_for_load['update_mart'] = datetime.now().now().strftime('%Y-%m-%d')
            chunk_df_for_load['group_prod2'] = chunk_df_for_load.group_prod2.apply(
                lambda x: x if x is None else x[:x.rfind('(') - 1])
            chunk_df_for_load = chunk_df_for_load.astype({'update_mart': 'datetime64[ns]',
                                                          'update_date': 'datetime64[ns]'})

            # Загрузка данных в промежуточную таблицу
            while flag_iter:
                try:
                    self.click_house_client.insert_df(table=need_table, df=chunk_df_for_load)
                    flag_iter = False
                except:
                    print('Данные не были загружены')

        pd.DataFrame(self.dict_nan_tnved_code).to_excel('Отсутствующие коды.xlsx', index=False)
        end_full = datetime.now()
        return str(end_full - start_full)

    def update_main_datamart(self, name_bd: str, table_source: str, table_update: str) -> str:
        """
        Функция обновления основной таблицы для дашбордов
        :param table_source: название таблицы источника (наша промежуточная таблица)
        :param table_update: название основной таблицы (витрины)
        :return: строку с длительностью выполнения операции
        """
        start = datetime.now()
        self.click_house_client.command(f'TRUNCATE TABLE IF EXISTS {table_update}')
        self.click_house_client.command(f"""INSERT INTO {name_bd}.{table_update}
                        SELECT * FROM {name_bd}.{table_source}""")
        end = datetime.now()
        return str(end - start)
