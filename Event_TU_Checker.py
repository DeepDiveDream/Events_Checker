#!/usr/bin/python3
import psycopg2
import pyodbc
import json
from argparse import ArgumentParser
from datetime import datetime
from psycopg2 import Error
import sys
import pandas as pd
from configparser import ConfigParser
from pathlib import Path

def connect_to_mssql(dbase, login, password, server):
    try:
        # conn_str = 'DRIVER={SQL Server}; SERVER=' + server + ';DATABASE=' + dbase + ';UID=' + login + ';PWD=' + password
        conn_str = 'Driver={ODBC Driver 18 for SQL Server}; SERVER=' + server + ';DATABASE=' + dbase + ';UID=' \
                   + login + '; PWD=' + password + ';TrustServerCertificate=yes;'
        conn = pyodbc.connect(conn_str)
        print(f"{currentScript} {datetime.now()} Соединение с БД {dbase}, сервер {server} успешно!")
        return conn
    except (Exception, Error) as connect_error:
        print(f"{currentScript} {datetime.now()} Ошибка соеденения с БД {dbase} сервер {server}: {connect_error}")


def connect_to_postgre(dbase, login, password, host, port):
    try:
        conn = psycopg2.connect(
            database=dbase,
            user=login,
            password=password,
            port=port,
            host=host)
        print(f"{currentScript} {datetime.now()} Соединение с БД {dbase}, сервер {host} успешно!")
        return conn
    except (Exception, Error) as connect_error:
        print(f"{currentScript} {datetime.now()} Ошибка соеденения с БД {dbase} сервер {host}: {connect_error}")


def fields(cursor):
    results = {}
    column = 0

    for d in cursor.description:
        results[d[0]] = column
        column += 1

    return results


if __name__ == "__main__":

    currentScript = '[Event_TU_Checker]'

    parser = ArgumentParser()
    parser.add_argument('configPath', type=str, help='Path to config file', default='config.json', nargs='?')
    args = parser.parse_args()
    config_path = args.configPath

    with open(config_path, 'r') as f:
        config_data = json.load(f)
        ini_file_path = config_data['ini_file_path']

    config_ini = ConfigParser()
    config_ini.read(ini_file_path)

    postgre_user = config_ini.get('common', 'pguser')
    postgre_pass = config_ini.get('common', 'pgpassword')
    postgre_host = config_ini.get('common', 'pghost')
    postgre_port = config_ini.get('common', 'pgport')
    postgre_database = config_ini.get('common', 'pgdb')

    sap_dir = config_ini.get('events_checker', 'sap_dir')

    postgre_elsec_conn = connect_to_postgre(postgre_database, postgre_user, postgre_pass, postgre_host, postgre_port)

    if not postgre_elsec_conn:
        sys.exit(1)

    postgre_elsec_cursor = postgre_elsec_conn.cursor()

    # event_code = 901   это для ТУ
    # event_code = 703   это для HM
    # event_table = "EvTu"
    # event_table = "EvHm"

    event_codes = [901, 902, 903, 904]
    event_table = 'EvTu'
    event_type_name = 'unplannedTU'

    postgre_elsec_cursor.execute(f"SELECT * FROM event_type where name = \'{event_type_name}\'")
    event_type = postgre_elsec_cursor.fetchone()[0]
    postgre_elsec_cursor.execute(f"SELECT * FROM event_source_params('telescada')")
    fields_map_params = fields(postgre_elsec_cursor)
    params = postgre_elsec_cursor.fetchone()

    if params is None:
        print(f'{currentScript} {datetime.now()} Не заполнены параметры для источника telescada')
        sys.exit(2)

    event_source = params[fields_map_params['id']]
    param_dict = params[fields_map_params['params']]

    if len(param_dict) == 0:
        print(f'{currentScript} {datetime.now()} Не заполнены параметры для источника telescada')
        sys.exit(2)

    ip_address = param_dict["ip_scada_events"]
    login = param_dict["login_scada_events"]
    password = param_dict["password_scada_events"]
    dbase_scada_events = param_dict["db_scada_events"]

    scada_events_conn = connect_to_mssql(dbase_scada_events, login, password, ip_address)

    if not scada_events_conn:
        sys.exit(1)

    # получаем курсор и структуру таблиц типа events в скаде в БД событий
    scada_events_cursor = scada_events_conn.cursor()
    scada_events_cursor.execute(f"Select code, dt, obj, user_ FROM [{dbase_scada_events}].[dbo].[{event_table}] where code = 0")
    field_map_event = fields(scada_events_cursor)
    scada_events_cursor.fetchone()

    ip_address = param_dict["ip_scada_model"]
    login = param_dict["login_scada_model"]
    password = param_dict["password_scada_model"]
    dbase_scada_model = param_dict["db_scada_model"]

    scada_model_conn = connect_to_mssql(dbase_scada_model, login, password, ip_address)

    if not scada_model_conn:
        sys.exit(1)

    # получаем курсор и в скаде в БД Модели
    scada_model_cursor = scada_events_conn.cursor()

    ip_address = param_dict["ip_asu_reo"]
    login = param_dict["login_asu_reo"]
    password = param_dict["password_asu_reo"]
    dbase_asu_reo = param_dict["db_asu_reo"]

    mssql_asu_reo_conn = connect_to_mssql(dbase_asu_reo, login, password, ip_address)

    if not mssql_asu_reo_conn:
        sys.exit(1)

    # получаем курсор и структуру таблиц заявок ZVKBody в АСУ РЭО
    mssql_asu_reo_cursor = mssql_asu_reo_conn.cursor()
    mssql_asu_reo_cursor.execute(f"SELECT top 1 *  FROM [{dbase_asu_reo}].[dbo].[ZVKBody]")
    mssql_asu_reo_cursor.fetchone()
    field_map_zvk_body = fields(mssql_asu_reo_cursor)

    #  загружаем данные из экселя от сапа
    excel_sap_file = sap_dir + param_dict["sap_file"]
    # excel_sap_file =  param_dict["sap_file_path"]
    # Load spreadsheet

    path = Path(excel_sap_file)
    if not path.is_file():
        print(f'{currentScript} {datetime.now()} Файл {excel_sap_file} с данными SUP-R3 не найден!')
        sys.exit(2)

    try:
        tech_places = pd.read_excel(excel_sap_file, usecols='F,H,I,J,K')
        print(f"{currentScript} {datetime.now()} Файл {excel_sap_file} с данными SUP-R3 успешно прочитан!")
    except (Exception, Error) as connect_error:
        print(f"{currentScript} {datetime.now()} Ошибка открытия файла {excel_sap_file} с данными SUP-R3: {connect_error}")
        sys.exit(2)

    for event_code in event_codes:

        # Считываем последнее запомненое и обработанное событие с таким кодом и от этого источника из нашей БД
        postgre_elsec_cursor.execute(
            f"Select  date, unix_date "
            f"FROM scada_last_request where type_code = {event_code} and source_code = {event_source}")
        field_map_last_request = fields(postgre_elsec_cursor)
        last_processed_request = postgre_elsec_cursor.fetchone()

        query = ""
        last_request_unix_time = 0

        # Считываем  все события с таким кодом из БД Скады которыен произошли позже последнего обработанного нами
        if last_processed_request is None:
            query = f"Select code, dt, obj, user_ FROM [{dbase_scada_events}].[dbo].[{event_table}] " \
                f"where code = {event_code} order by dt"
        else:
            last_request_unix_time = last_processed_request[field_map_last_request['unix_date']]
            last_request_date = last_processed_request[field_map_last_request['date']]
            query = f"Select code, dt, obj, user_ FROM [{dbase_scada_events}].[dbo].[{event_table}] " \
                f"where code = {event_code} and dt > {last_request_unix_time} order by dt"

        scada_events_cursor.execute(query)
        all_events = scada_events_cursor.fetchall()

        for event_row in all_events:
            scada_event_obj = event_row[field_map_event['obj']]
            unix_ts = event_row[field_map_event['dt']]
            scada_event_datetime = (datetime.fromtimestamp(unix_ts))

            user_id = event_row[field_map_event['user_']]

            # user_id = '526CFAC151354DC69A687993D6685820'

            user_json_data = {
                "user_KeyLink": -1,
                "user_short": "не найден"
            }
            user_values_string = "<b>Пользователь:</b> не найден<br>"

            if user_id is not None:
                # Получаем Юзера создавшего события
                query = f"Select login_, firstName, midlName, lastName, department, post FROM [{dbase_scada_model}].[dbo].[Users] where KeyLink = '{user_id}'"
                scada_model_cursor.execute(query)
                res_user = scada_model_cursor.fetchone()

                if res_user:
                    user_json_data = {
                        "user_KeyLink": user_id,
                        "user_short": res_user[0]
                    }
                    user_values_string = f"<b>Пользователь:</b> {res_user[1]} {res_user[2]} {res_user[3]}, " \
                        f"{res_user[4]}, {res_user[5]}<br>"

            # Получаем Оборудование по которому создано событие
            query = f"Select name_ FROM [{dbase_scada_model}].[dbo].[IdentifiedObject] where KeyLink = '{scada_event_obj}'"
            scada_model_cursor.execute(query)
            query_result = scada_model_cursor.fetchone()

            dev_type_name = "не определено"

            if query_result:
                dev_type_name = query_result[0]

            # получаем ИД оборудования по этой заявке в АСУ РЭО
            scada_model_cursor.execute(
                f"SELECT top 1 AsureoDevId FROM [{dbase_scada_model}].[dbo].[LinkAsureo] where KeyLink = '{scada_event_obj}'")

            asureo_dev_id = scada_model_cursor.fetchone()

            # Получаем IP откуда создано событие
            event_source_host = ''

            if user_id is not None:
                query = f"Select top 1 caption FROM [{dbase_scada_events}].[dbo].[EvSys] where user_ = '{user_id}' and code = 1103 order by dt desc"
                scada_events_cursor.execute(query)
                caption = scada_events_cursor.fetchone()[0]

                if caption:
                    if caption is not None and caption != 'None':
                        start = caption.find(',')
                        end = caption.rfind(',')
                        if start > 0 and end > 0:
                            event_source_host = caption[start + 1:end]

            zvk_res = []

            # asureo_dev_id = 19475

            if asureo_dev_id is not None:
                mssql_asu_reo_cursor.execute(f"SELECT *  FROM [{dbase_asu_reo}].[dbo].[ZVKBody]  "
                                             f"where DeviceId = {asureo_dev_id} "
                                             f"and ZVKCategoryId = 48 order by ZVKBodyId")

                zvk_res = mssql_asu_reo_cursor.fetchall()

            unplanned_event = True

            json_data = {
                "data": "",
                "display": {"": ""}
            }

            for last_zvk in zvk_res:

                begin_date = last_zvk[field_map_zvk_body['PlanDateBegin']]
                end_date = last_zvk[field_map_zvk_body['PlanDateEnd']]

                if begin_date <= scada_event_datetime <= end_date:
                    unplanned_event = False
                    break

            if unplanned_event:

                scada_model_cursor.execute(
                    f"SELECT top 1 sapTM FROM [{dbase_scada_model}].[dbo].[LinkSAP] where KeyLink = '{scada_event_obj}'")

                sap_tech_place = scada_model_cursor.fetchone()

                # sap_tech_place = 'PS035-000284'

                if sap_tech_place:
                    for row in tech_places.itertuples():
                        if row[1] == sap_tech_place:
                            begin_date = row[3]
                            end_date = row[4]
                            sap_device = row[2]
                            if begin_date <= scada_event_datetime <= end_date:
                                unplanned_event = False
                                break

            if unplanned_event:
                result_string = f"<b>Внимание!:</b> событие не запланировано!<br>" \
                                    f"<b>Фактическое исполнение:</b> {scada_event_datetime}<br>" \
                                    f"<b>Оборудование:</b> {dev_type_name}<br>" \
                                + user_values_string + \
                                f"<i>Информация получена по данным АСУ РЭО и SAP R3</i>"

                json_data = {
                    "data": user_json_data,
                    "display": {
                        "Внеплановое событие!": result_string
                    }
                }

                data = json.dumps(json_data)
                postgre_elsec_cursor.callproc('event_new', [event_type, event_source, True, data])
                postgre_elsec_conn.commit()

                if event_source_host != "":
                    postgre_elsec_cursor.execute(f"SELECT * FROM event_type where name = \'blockip\'")
                    event_type = postgre_elsec_cursor.fetchone()[0]
                    json_data = {
                        "clienthost": event_source_host
                    }

                    data = json.dumps(json_data)
                    postgre_elsec_cursor.callproc('event_new', [event_type, event_source, False, data])
                    postgre_elsec_conn.commit()

            if last_request_unix_time <= 0:
                query = f"INSERT INTO  scada_last_request (date, unix_date, " \
                    f"type_code, source_code)" \
                    f" VALUES ('{scada_event_datetime}', {unix_ts} ,{event_code},{event_source})"
            else:
                query = f"UPDATE scada_last_request SET " \
                    f" date = '{scada_event_datetime}'," \
                    f" unix_date = {unix_ts} " \
                    f"WHERE type_code = {event_code} and source_code = {event_source}"

            postgre_elsec_cursor.execute(query)
            postgre_elsec_conn.commit()
            last_request_unix_time = unix_ts

    scada_events_cursor.close()
    scada_model_cursor.close()
    postgre_elsec_cursor.close()
    mssql_asu_reo_cursor.close()

    postgre_elsec_conn.close()
    scada_events_conn.close()
    scada_model_conn.close()
    mssql_asu_reo_conn.close()
