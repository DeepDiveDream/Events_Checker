#!/usr/bin/python3
from xmldiff import main, formatting
import xml.etree.ElementTree as ET
import sys
import psycopg2
from psycopg2 import Error
import json
from argparse import ArgumentParser
import configparser
from datetime import datetime, timedelta
from pathlib import Path
import os
import shutil


def fields(cursor):
    results = {}
    column = 0

    for d in cursor.description:
        results[d[0]] = column
        column += 1

    return results


def connect_to_postgre(dbase, login, password, host, port, event_source):
    try:
        conn = psycopg2.connect(
            database=dbase,
            user=login,
            password=password,
            port=port,
            host=host)

        if event_source != event_source_main:
            msg = f"{datetime.now()} | {currentScript} | source: {event_source} |" \
                f" Соединение с БД {dbase}, сервер {host} успешно!"
            write_connection_log(event_source, currentScript, host, dbase, True, msg)

        return conn

    except (Exception, Error) as connect_error:
        local_error = f'{datetime.now()} | {currentScript} | source: {event_source} | ' \
            f'Ошибка соединения с БД {dbase}, сервер {host}:{port}, {connect_error}'
        if console_messages:
            print(local_error)
        write_log(log_file_name, local_error)
        if event_source != event_source_main:
            write_connection_log(event_source, currentScript, host, dbase, False, local_error)
        return None


def write_connection_log(source_id, script_name, host, db_name, result, message):
    message = message.replace("'", " ")
    query = f"INSERT INTO  connection_log (source_id, script_name,host,db_name, date_time, result, message)" \
        f" VALUES ({source_id}, '{script_name}' ,'{host}','{db_name}','{datetime.now()}', {result},'{message}')"
    postgre_cursor.execute(query)
    postgre_conn.commit()


def delete_old_connection_log():
    old_date = datetime.now() - timedelta(days=oldest_log_delta)

    query = f"DELETE FROM  connection_log WHERE date_time < timestamp '{old_date}'"
    postgre_cursor.execute(query)
    postgre_conn.commit()


def write_log(file_name, str_log):
    with open(file_name, 'a') as logfile:
        logfile.write(str_log + '\n')
        logfile.close()


def get_name_of_deleted_tag(input_string):
    lines = input_string.split(',')
    attr_name = lines[1].lstrip()
    if attr_name[-1] == "]":
        attr_name = attr_name[:-1]

    return attr_name


def get_name_of_original_attr(input_string):
    lines = input_string.split(',')
    attr_name = lines[2].lstrip()
    if attr_name[-1] == "]":
        attr_name = attr_name[:-1]

    return attr_name


def get_path_to_original_attr(input_string):
    path_to_attribute = "."
    lines = input_string.split(',')
    tmp = lines[1].lstrip()

    if tmp.count('/') == 1:
        return path_to_attribute

    pos = tmp.find('/', tmp.find('/') + 1)
    path_to_attribute += tmp[pos:]

    if path_to_attribute[-2] == "]":
        path_to_attribute = path_to_attribute[:-1]

    return path_to_attribute


def get_caption_path_to_attr(input_string, is_original, root_origin, root_new):
    tmp_lines = input_string.split('/')
    tmp_path = ""
    caption_path = ""

    for tmp_line in tmp_lines:
        tmp_path += tmp_line

        if is_original:
            elm1 = root_origin.findall(tmp_path)
        else:
            elm1 = root_new.findall(tmp_path)

        if len(elm1) > 0:
            # caption_path += elm1[0].tag

            if elm1[0].tag == "configuration":
                continue

            if elm1[0].tag == "direction":
                caption_path += "Направление -> "

            if 'caption' in elm1[0].attrib:
                caption_path += "" + elm1[0].attrib["caption"] + "/"
            else:
                caption_path += "/"
            tmp_path += "/"

    return caption_path


def compare_xmlns(observed, expected, xml_format_mode=0):
    if xml_format_mode == 0:
        formatter = formatting.DiffFormatter(normalize=formatting.WS_BOTH)
    else:
        formatter = formatting.XMLFormatter(normalize=formatting.WS_BOTH)

    diff = main.diff_files(observed, expected, formatter=formatter)
    return diff


def write_event(last_event, field_map_event, event_source, changes_result):

    if last_event is None:
        json_data = {
            "data": [changes_result],
            f"display": {
                "Изменения": changes_result
            },
            "event_description": "Обнаружено изменение XML сервера ввода-вывода!",
            "event_date": f'{datetime.now()}',
            "event_code": event_type,
            "etalon_updated": False
        }
        data = json.dumps(json_data)
        postgre_cursor.callproc('event_new', [event_type, event_source, True, data])
        postgre_conn.commit()
    else:
        id_last = last_event[field_map_event['id']]
        last_data = last_event[field_map_event['data']]

        json_data = {
            "data": [changes_result],
            f"display": {
                "Изменения": changes_result
            },
            "event_description": last_data['event_description'],
            "event_date": last_data['event_date'],
            "event_code": event_type,
            "etalon_updated": False
        }

        query = f'delete from event where id = {id_last}'
        postgre_cursor.execute(query)
        postgre_conn.commit()

        data = json.dumps(json_data)
        query = f"insert into event (type , source, data, created, enduser) \
                  values ({event_type}, {event_source}, '{data}', '{datetime.now()}', True )"
        postgre_cursor.execute(query)
        postgre_conn.commit()


def check_event_source(event_source, param_dict):

    original_file = xml_dir_etalon + param_dict["original_file"]
    path = Path(original_file)
    if not path.is_file():
        msg = f'{currentScript} {datetime.now()} Эталонный XML файл {original_file} сервера ввода-вывода не найден!'
        if console_messages:
            print(msg)
        write_log(log_file_name, msg)
        return

    input_file = xml_dir_input + param_dict["input_file"]

    path = Path(input_file)
    if not path.is_file():
        msg = f'{currentScript} {datetime.now()} Проверяемый XML файл {input_file} сервера ввода-вывода не найден!'
        if console_messages:
            print(msg)
        write_log(log_file_name, msg)
        return

    query = f"SELECT *  FROM event where type = {event_type} and source = {event_source} " \
        f"order by id desc limit 1"
    postgre_cursor.execute(query)
    field_map_event = fields(postgre_cursor)
    last_event = postgre_cursor.fetchone()

    if last_event is not None:
        is_valid = last_event[field_map_event['legitimated']]

        # если событие несоответствия эталону есть НО помечено пользователем как валидное
        if is_valid is not None:

            last_data = last_event[field_map_event['data']]
            etalon_updated = False
            if "etalon_updated" in last_data:
                etalon_updated = last_data["etalon_updated"]

            # если файл эталона не обновлен
            if not etalon_updated:
                # обновляем эталон и помечаем в событии, что файл обновлен
                shutil.copyfile(input_file, original_file)

                msg = f'{currentScript} {datetime.now()} Эталон XML файла сервера ввода-вывода обновлён!'
                if console_messages:
                    print(msg)
                write_log(log_file_name, msg)

                json_data = {
                    "data": last_data['data'],
                    "display": last_data['display'],
                    "event_description": last_data['event_description'],
                    "event_date": last_data['event_date'],
                    "event_code": event_type,
                    "etalon_updated": True
                }
                data = json.dumps(json_data)
                query = f"update event set data = '{data}' where id = {last_event[field_map_event['id']]} "
                postgre_cursor.execute(query)
                postgre_conn.commit()
                return

            # сбрасываем текущее событие, что бы создавать новые
            last_event = None

    original_size = os.path.getsize(original_file)
    input_size = os.path.getsize(input_file)

    size_diff = abs(original_size-input_size)

    if size_diff > 1024:
        changes_result = "<br>Обнаружено значительное несоответствие XML файлов!" + \
                          "<br> &nbsp &nbsp &nbsp Размер проверяемого XML файла сервера ввода-вывода " + \
                          f"отличается от эталона на {size_diff} символов!" + \
                          f"<br> &nbsp &nbsp &nbsp({round(size_diff/1024,1)}Кбайт)"

        write_event(last_event, field_map_event, event_source, changes_result)
        return

    out = compare_xmlns(original_file, input_file)

    if out == '':
        msg = f'{currentScript} {datetime.now()} Изменения в XML файле {input_file} сервера ввода-вывода не найдены!'
        if console_messages:
            print(msg)
        write_log(log_file_name, msg)
        return

    root_origin = ET.parse(original_file)
    root_new = ET.parse(input_file)
    changes_result = ""
    changes_path = ""
    for line in out.splitlines():
        path = get_path_to_original_attr(line)
        caption_path = get_caption_path_to_attr(path, True, root_origin, root_new)
        split_result = line.split(',')
        action_value = split_result[0][1:]

        changes_path += line + "\n"

        if "update-attribute" in line or "delete-attribute" in line:

            elm = root_origin.findall(path)

            name = get_name_of_original_attr(line)
            new_value = ""
            if len(split_result) > 3:
                new_value = line.split(',')[3][:-1]

            if action_value == "update-attribute":
                action_value = "<br>Обнаружено изменение значения свойства!"
            else:
                action_value = "<br>Обнаружено удаление свойства!"

            if name in elm[0].attrib:
                changes_result += action_value + "<br> &nbsp &nbsp &nbsp Место изменения: &nbsp " + caption_path + \
                                  "<br> &nbsp &nbsp &nbsp Свойство: &nbsp " + name + ', ' \
                                                                                     '&nbsp Новое значение: &nbsp' + new_value + \
                                  ',&nbsp Старое значение: "' + elm[0].attrib[name] + '"<br>'
            else:
                changes_result += action_value + "<br>&nbsp &nbsp &nbsp Место изменения: " + caption_path + "<br>"

        elif "insert-attribute" in line:

            caption_path = get_caption_path_to_attr(path, False, root_origin, root_new)
            name = get_name_of_original_attr(line)
            new_value = ""
            if len(split_result) > 3:
                new_value = line.split(',')[3][:-1]

            action_value = "<br>Обнаружено добавление нового свойства!"

            changes_result += action_value + \
                              "<br> &nbsp &nbsp &nbsp Место изменения:&nbsp " + caption_path + \
                              "<br>&nbsp &nbsp &nbsp Свойство: " + name + ",&nbsp Новое значение:" \
                              + new_value + "<br>"

        elif "insert" in line and "attribute" not in line:

            action_value = "<br>Обнаружено изменение структуры XML!"

            changes_result += action_value + "<br> &nbsp &nbsp &nbsp Путь в XML:&nbsp " + caption_path + \
                              ",&nbsp Добавлен тэг:&nbsp " \
                              + split_result[2].strip() + "<br>"

        elif "rename" in line and "attribute" not in line:
            action_value = "<br>Обнаружено изменение структуры XML!"
            changes_result += action_value + "<br>&nbsp &nbsp &nbspПуть в XML:&nbsp " + \
                              caption_path + ",&nbsp Новое имя тега:&nbsp [" + split_result[2].strip() + "<br>"

        elif "delete" in line and "attribute" not in line:

            action_value = "<br>Обнаружено изменение структуры XML!"

            deleted_tag_path = split_result[1].strip()
            deleted_tag_pos = deleted_tag_path.rfind('/')
            deleted_tag_name = deleted_tag_path[deleted_tag_pos + 1:]

            deleted_tag_pos = deleted_tag_name.find('[')
            deleted_tag_name = deleted_tag_name[:deleted_tag_pos]

            changes_result += action_value + "<br> &nbsp &nbsp &nbsp Путь в XML: " + caption_path + "," \
                                                                                                    "&nbsp Удаленный тег:&nbsp " + deleted_tag_name + "<br>"

        else:
            action_value = "<br>Обнаружено изменение структуры XML!"
            changes_result += action_value + "<br> &nbsp &nbsp &nbsp Путь в XML:&nbsp " + caption_path + "<br>"

    if console_messages:
        print(changes_result)
        print(changes_path)

    write_event(last_event,field_map_event,event_source,changes_result)


def check_all_event_sources():
    for param in event_source_params:
        event_source = 0
        try:
            event_source = param[fields_map_params['id']]
            param_dict = param[fields_map_params['params']]

            if len(param_dict) == 0:
                local_error = f'{datetime.now()} | {currentScript} | source: {event_source} ' \
                    f'| Не заполнены параметры для источника "{event_source_name}"'
                if console_messages:
                    print(local_error)
                write_log(log_file_name, local_error)
                continue

            check_event_source(event_source, param_dict)

        except (Exception, Error) as error1:
            local_error = f'{datetime.now()} | {currentScript} | source: {event_source} | Ошибка: {error1}'
            if console_messages:
                print(local_error)
            write_log(log_file_name, local_error)
            continue


if __name__ == "__main__":

    currentScript = '[IO_Server_XML_Comparer]'
    event_type_name = 'configChanged.ioServerXML'
    event_source_name = "ioServerXML"
    event_source_main = 0
    log_file_name = None
    postgre_cursor = None
    postgre_conn = None
    postgre_host = None
    postgre_database = None
    console_messages = False

    oldest_log_delta = 30

    try:
        parser = ArgumentParser()
        parser.add_argument('configPath', type=str, help='Path to config file', default='config.json', nargs='?')
        args = parser.parse_args()
        config_path = args.configPath

        with open(config_path, 'r') as f:
            config_data = json.load(f)
            ini_file_path = config_data['ini_file_path']

        config_ini = configparser.ConfigParser()
        config_ini.read(ini_file_path)

        postgre_user = config_ini.get('common', 'pguser')
        postgre_pass = config_ini.get('common', 'pgpassword')
        postgre_host = config_ini.get('common', 'pghost')
        postgre_port = config_ini.get('common', 'pgport')
        postgre_database = config_ini.get('common', 'pgdb')
        log_dir = config_ini.get('common', 'logdir')

        xml_dir_etalon = config_ini.get('io_server_xml_comparer', 'xml_dir_etalon')
        xml_dir_input = config_ini.get('io_server_xml_comparer', 'xml_dir_input')

        log_file_name = config_ini.get('io_server_xml_comparer', 'logfile')
        if log_dir != "":
            log_file_name = log_dir + "/" + log_file_name

        postgre_conn = connect_to_postgre(postgre_database, postgre_user, postgre_pass,
                                          postgre_host, postgre_port, event_source_main)

        if not postgre_conn:
            sys.exit(1)

        postgre_cursor = postgre_conn.cursor()

        result_msg = f"{datetime.now()} | {currentScript} | source: {event_source_main} |" \
            f" Соединение с БД {postgre_database}, сервер {postgre_host} успешно!"
        write_connection_log(event_source_main, currentScript, postgre_host, postgre_database, True, result_msg)

        postgre_cursor.execute(f"SELECT * FROM event_type where name = \'{event_type_name}\'")
        event_type = postgre_cursor.fetchone()[0]

        postgre_cursor.execute(f"SELECT * FROM event_source_params('{event_source_name}')")
        fields_map_params = fields(postgre_cursor)
        event_source_params = postgre_cursor.fetchall()

        if event_source_params is None or len(event_source_params) == 0:
            str_error = f'{datetime.now()} | {currentScript} | source: {event_source_main} ' \
                f'| Не найдет источник событий "{event_source_name}", ' \
                f'БД "{postgre_database}", сервер "{postgre_host}"'
            if console_messages:
                print(str_error)
            write_log(log_file_name, str_error)
            sys.exit(1)

        delete_old_connection_log()

        check_all_event_sources()

    except(Exception, Error) as main_error:
        str_error = f"{datetime.now()} | {currentScript} | source: {event_source_main} | Ошибка: {main_error}"
        if console_messages:
            print(str_error)
        if log_file_name:
            write_log(log_file_name, str_error)
    finally:
        if postgre_cursor:
            postgre_cursor.close()

        if postgre_conn:
            postgre_conn.close()
