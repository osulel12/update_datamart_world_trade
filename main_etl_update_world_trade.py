from class_update_world_trade_mart import Update_world_trade_mart
from sql_scripts import update_world_trade_mart_script, sql_sqript_fish_8
from config_parse import parse_config
from dotenv import load_dotenv
import os
from py_cron_schedule import CronSchedule
# from dag_file import task_message


def all_update_datamart():
    task_message()

    # Подгружаем наши переменные окружения
    dotenv_path = os.path.join(os.path.dirname(__file__), '.env')
    if os.path.exists(dotenv_path):
        load_dotenv(dotenv_path)

    dict_name_table = parse_config('config_js.json')

    # Формируем конфиг словари для подключения к базам данных
    dct_pg = {'USER': os.getenv('USER_NAME_PG'), 'PASSWORD': os.getenv('PASSWORD_PG'),
              'HOST': os.getenv('HOST_PG'), 'PORT': os.getenv('PORT_PG'), 'DATABASE': os.getenv('DATABASE_PG')}
    dct_ch = {'USER': os.getenv('USER_NAME_CLICKHOUSE'), 'PASSWORD': os.getenv('PASSWORD_CLICKHOUSE'),
              'HOST': os.getenv('HOST_CLICKHOUSE'), 'PORT': os.getenv('PORT_CLICKHOUSE'),
              'DATABASE': os.getenv('DATABASE_CLICKHOUSE')}

    update_table_mart = Update_world_trade_mart(dct_pg, dct_ch)

    update_table_mart.update_blank_datamart(update_world_trade_mart_script,
                                            dict_name_table["table_source"], flag_truncate=True)
    update_table_mart.update_blank_datamart(sql_sqript_fish_8,
                                            dict_name_table["table_source"], flag_truncate=False)

    update_table_mart.update_main_datamart(dict_name_table['name_bd'],
                                           dict_name_table["table_source"], dict_name_table["table_update"])


def main():
    cs = CronSchedule()
    # Запускаем основной скрипт
    cs.add_task('task_2', "0 1 * * *", all_update_datamart)
    cs.start()


# Если скрипт запускается самостоятельно
if __name__ == '__main__':
    print('Обновление FAO')
    main()
