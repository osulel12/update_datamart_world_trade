import json
import os.path


def parse_config(file: str) -> dict:
    """
    Считываем .json файл
    :param file: название считываемого файла
    :return: словарь полученный из файла
    """
    with open(file, 'r', encoding='utf-8') as fl:
        return json.load(fl)


def get_file(file_list: list):
    return [fl for fl in file_list if os.path.isfile(fl)]


