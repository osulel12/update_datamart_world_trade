from class_update_world_trade_mart import Update_world_trade_mart
from sql_scripts import update_world_trade_mart_script, sql_sqript_fish_8
from config_parse import parse_config
from dotenv import load_dotenv
import os
from py_cron_schedule import CronSchedule


def all_update_datamart():
    """
    Процесс обновления данных с таблицах CLICKHOUSE
    """
    # Подгружаем наши переменные окружения
    dotenv_path = os.path.join(os.path.dirname(__file__), '.env')
    if os.path.exists(dotenv_path):
        load_dotenv(dotenv_path)

    # Подгружаем словарь с названиями таблиц и БД
    dict_name_table = parse_config('config_js.json')

    # Формируем конфиг словари для подключения к базам данных
    dct_pg = {'USER': os.getenv('USER_NAME_PG'), 'PASSWORD': os.getenv('PASSWORD_PG'),
              'HOST': os.getenv('HOST_PG'), 'PORT': os.getenv('PORT_PG'), 'DATABASE': os.getenv('DATABASE_PG')}
    dct_ch = {'USER': os.getenv('USER_NAME_CLICKHOUSE'), 'PASSWORD': os.getenv('PASSWORD_CLICKHOUSE'),
              'HOST': os.getenv('HOST_CLICKHOUSE'), 'PORT': os.getenv('PORT_CLICKHOUSE'),
              'DATABASE': os.getenv('DATABASE_CLICKHOUSE')}

    # Создаем экземпляр класса
    update_table_mart = Update_world_trade_mart(dct_pg, dct_ch)

    # Обновляем большую часть данных и делаем truncate резервной таблицы
    update_first_script = update_table_mart.update_blank_datamart(update_world_trade_mart_script,
                                                                  dict_name_table["table_source"],
                                                                  'auto_sql_script',
                                                                  flag_truncate=True)
    print(f'Обновление авто заняло {update_first_script}')
    # Обновляем меньшую часть данных
    update_fish8_script = update_table_mart.update_blank_datamart(sql_sqript_fish_8,
                                                                  dict_name_table["table_source"],
                                                                  'fish8_sql_sqript',
                                                                  flag_truncate=False)
    print(f'Обновление fish8 заняло {update_fish8_script}')
    # Обновляем витрину данных
    time_update = update_table_mart.update_main_datamart(dict_name_table['name_bd'],
                                                         dict_name_table["table_source"],
                                                         dict_name_table["table_update"])
    print(f'Витрина данных обновлена за {time_update}')


def main():
    """
    Крон функция, которая отрабатывает в 1 час ночи с понедельника по субботу
    """
    cs = CronSchedule()
    # Запускаем основной скрипт
    cs.add_task('task_update', "0 1 * * 1,2,3,4,5,6", all_update_datamart)
    cs.start()


# Если скрипт запускается самостоятельно
if __name__ == '__main__':
    print('Обновление Витрины данных мировой торговли')
    main()
