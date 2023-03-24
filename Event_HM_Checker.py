#!/usr/bin/python3
import psycopg2
import pyodbc
import json
import os
from argparse import ArgumentParser
from datetime import datetime, timedelta
from psycopg2 import Error
import sys
import pandas as pd
from configparser import ConfigParser
from pathlib import Path
import sap_log_checker


class CheckResult:
    unplannedEvent = True
    processed = False
    message = ""


def connect_to_mssql(dbase, login, password, server, event_source):
    try:
        # conn_str = 'DRIVER={SQL Server}; SERVER=' + server + ';DATABASE=' + dbase + ';UID=' + login + ';PWD=' + password
        conn_str = 'Driver={ODBC Driver 18 for SQL Server}; SERVER=' + server + ';DATABASE=' + dbase + ';UID=' \
                   + login + '; PWD=' + password + ';TrustServerCertificate=yes;'
        conn = pyodbc.connect(conn_str)

        msg = f"{datetime.now()} | {currentScript} | source: {event_source} |" \
            f" Соединение с БД {dbase}, сервер {server} успешно!"
        write_connection_log(event_source, currentScript, server, dbase, True, msg)

        return conn

    except (Exception, Error) as connect_error:
        local_error = f"{datetime.now()} | {currentScript} | source: {event_source} | " \
            f"Ошибка соединения с БД {dbase}, сервер {server}, {connect_error}"
        if console_messages:
            print(local_error)
        write_log(log_file_name, local_error)
        write_connection_log(event_source, currentScript, server, dbase, False, local_error)
        return None


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


def fields(cursor):
    results = {}
    column = 0

    for d in cursor.description:
        results[d[0]] = column
        column += 1

    return results


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


def load_sap_file(sap_file_path, event_source, is_asap_mode):
    # Load spreadsheet
    path = Path(sap_file_path)
    if not path.is_file():
        msg = f'{datetime.now()} | {currentScript} | source: {event_source} | ' \
            f'Файл {sap_file_path} с данными SAP не найден!'
        if console_messages:
            print(msg)
        write_log(log_file_name, msg)
        write_connection_log(event_source, currentScript, "SAP", sap_file_path, False, msg)
    try:
        df = pd.read_csv(sap_file_path, sep="\t")

        df['combo_start'] = pd.to_numeric(df['combo_start'], errors="coerce")
        df['combo_end'] = pd.to_numeric(df['combo_end'], errors="coerce")

        if is_asap_mode:
            df[sap_end_date_column_asap] = pd.to_numeric(df[sap_end_date_column_asap], errors="coerce")
        else:
            df[sap_end_date_column] = pd.to_numeric(df[sap_end_date_column], errors="coerce")

        msg = f'{datetime.now()} | {currentScript} | source: {event_source} | ' \
            f'Файл {sap_file_path} с данными SAP успешно прочитан!'
        if console_messages:
            print(msg)
        write_log(log_file_name, msg)
        write_connection_log(event_source, currentScript, "SAP", sap_file_path, True, msg)

        return df

    except (Exception, Error) as open_xls_error:
        msg = f'{datetime.now()} | {currentScript} | source: {event_source} | ' \
            f'Ошибка открытия файла {sap_file_path} с данными SAP: {open_xls_error}'
        if console_messages:
            print(msg)
        write_log(log_file_name, msg)
        write_connection_log(event_source, currentScript, "SAP", sap_file_path, False, msg)
        return None


def check_lines_asu_reo(dbase_scada_model, event_source, scada_event_datetime,
                        scada_model_cursor, asu_reo_cursor, substation_key):
    result = CheckResult()

    if asu_reo_cursor is None:
        return result

    # Список всех Line по коду подстанции

    query = f"SELECT distinct [{dbase_scada_model}].[dbo].[Equipment].EquipmentContainerKey, " \
        f"[{dbase_scada_model}].[dbo].[IdentifiedObject].KeyLink as LineKey, " \
        f"[{dbase_scada_model}].[dbo].[IdentifiedObject].name_, " \
        f"[{dbase_scada_model}].[dbo].[IdentifiedObject].ClassName " \
        f"FROM  [{dbase_scada_model}].[dbo].[Equipment] " \
        f"INNER JOIN  [{dbase_scada_model}].[dbo].[IdentifiedObject]  " \
        f"ON  [{dbase_scada_model}].[dbo].[Equipment].EquipmentContainerKey = " \
        f"[{dbase_scada_model}].[dbo].[IdentifiedObject].KeyLink" \
        f" WHERE " \
        f"EXISTS(" \
        f"SELECT  1 FROM  [{dbase_scada_model}].[dbo].[IdentifiedObject] " \
        f"INNER JOIN [{dbase_scada_model}].[dbo].[Terminal] " \
        f"ON [{dbase_scada_model}].[dbo].[Terminal].[ConductingEquipmentKey] = " \
        f"[{dbase_scada_model}].[dbo].[IdentifiedObject].KeyLink " \
        f"INNER JOIN [{dbase_scada_model}].[dbo].[ConnectivityNode] " \
        f"ON [{dbase_scada_model}].[dbo].[ConnectivityNode].KeyLink = " \
        f"[{dbase_scada_model}].[dbo].[Terminal].ConnectivityNodeKey " \
        f"INNER JOIN [{dbase_scada_model}].[dbo].[ACLineSegment] " \
        f"ON [{dbase_scada_model}].[dbo].[ACLineSegment].KeyLink = " \
        f"[{dbase_scada_model}].[dbo].[IdentifiedObject].KeyLink" \
        f" WHERE [{dbase_scada_model}].[dbo].[ConnectivityNode].ContainerKey = '{substation_key}' " \
        f"AND [{dbase_scada_model}].[dbo].IdentifiedObject.KeyLink = [{dbase_scada_model}].[dbo].[Equipment].KeyLink)"

    scada_model_cursor.execute(query)
    lines = scada_model_cursor.fetchall()
    field_map_lines = fields(scada_model_cursor)

    oldest_zvk_date = (scada_event_datetime - timedelta(days=oldest_zvk_date_delta)).date()

    find_any_line = False

    # Пробегаем по всем линиям которые подключены к подстанции на которой произошло событие
    for line in lines:
        line_id = line[field_map_lines['LineKey']]
        asureo_line_id = None

        # получаем ИД линии в АСУ РЭО
        scada_model_cursor.execute(
            f"SELECT top 1 AsureoDevId FROM [{dbase_scada_model}].[dbo].[LinkAsureo] "
            f"where scdPSRKey = '{line_id}'")

        res = scada_model_cursor.fetchone()
        if res:
            asureo_line_id = res[0]

        if asureo_line_id is None:
            continue

        # помечаем, что хоть 1 линию нашли в АСУ РЭО
        find_any_line = True

        query = f"SELECT ZVKBodyId  FROM zvkbody  " \
            f"where zvkobjectid = {asureo_line_id} " \
            f"and " \
            f"((zvkcategoryid = 48 and plandatebegin >= '{oldest_zvk_date}' " \
            f"and plandateend <= '{scada_event_datetime}')" \
            f"or " \
            f"(zvkcategoryid = 49  and permitrepairdatebegin >= '{oldest_zvk_date}' " \
            f"and permitrepairdateend <= '{scada_event_datetime}'))" \
            f"order by zvkbodyid"

        asu_reo_cursor.execute(query)
        res = asu_reo_cursor.fetchall()
        # Если хоть что-то нашли - значит запланировано
        if len(res) > 0:
            result.unplannedEvent = False
            result.processed = True
            return result

    if not find_any_line:
        msg = f'{datetime.now()} | {currentScript} | source: {event_source} | ' \
            f'Не удалось определить линии АСУРЭО для подстанции с KeyLink = \'{substation_key}\''
        write_log(log_file_name, msg)

    # Если ни по одной линии ничего не нашли - значит НЕ запланировано
    result.unplannedEvent = True
    result.processed = find_any_line
    return result


def check_substation_asu_reo(dbase_scada_model, event_source, scada_event_datetime,
                             scada_model_cursor, asu_reo_cursor, substation_key):
    result = CheckResult()

    if asu_reo_cursor is None:
        return result

    asureo_power_obj_id = None

    # получаем ИД подстанции по этой заявке в АСУ РЭО
    scada_model_cursor.execute(
        f"SELECT top 1 AsureoDevId FROM [{dbase_scada_model}].[dbo].[LinkAsureo] "
        f"where scdPSRKey = '{substation_key}'")

    res = scada_model_cursor.fetchone()
    if res:
        asureo_power_obj_id = res[0]

    # asureo_power_obj_id = 199

    if asureo_power_obj_id is None:
        msg = f'{datetime.now()} | {currentScript} | source: {event_source} | ' \
            f'Не удалось определить подстанцию АСУРЭО для подстанции с KeyLink = \'{substation_key}\''
        write_log(log_file_name, msg)
        result.processed = False
        result.unplannedEvent = True
        return result

    oldest_zvk_date = (scada_event_datetime - timedelta(days=oldest_zvk_date_delta)).date()

    query = f"SELECT ZVKBodyId  FROM zvkbody  " \
        f"where zvkobjectid = {asureo_power_obj_id} " \
        f"and " \
        f"((zvkcategoryid = 48 and plandatebegin >= '{oldest_zvk_date}' " \
        f"and plandateend <= '{scada_event_datetime}')" \
        f"or " \
        f"(zvkcategoryid = 49  and permitrepairdatebegin >= '{oldest_zvk_date}' " \
        f"and permitrepairdateend <= '{scada_event_datetime}'))" \
        f"order by zvkbodyid"
    asu_reo_cursor.execute(query)

    res = asu_reo_cursor.fetchall()

    unplanned_event = True

    # Если хоть что-то нашли - значит запланировано
    if len(res) > 0:
        unplanned_event = False

    result.processed = True
    result.unplannedEvent = unplanned_event
    return result


def check_substation_sap(dbase_scada_model, scada_model_cursor, event_source, sap_requests,
                         sap_requests_asap, scada_event_datetime, substation_key):

    result = CheckResult()

    if sap_requests is None and sap_requests_asap is None:
        return result

    scada_model_cursor.execute(f"SELECT top 1 sapTM FROM [{dbase_scada_model}].[dbo].[LinkSAP] "
                               f"where scdPSRKey = '{substation_key}'")

    sap_substation = None
    res = scada_model_cursor.fetchone()
    if res:
        sap_substation = res[0]

    # sap_substation = "PS110-000618"

    if sap_substation is None:
        msg = f'{datetime.now()} | {currentScript} | source: {event_source} | ' \
            f'Не удалось определить подстанцию SAP для подстанции с KeyLink = \'{substation_key}\''
        write_log(log_file_name, msg)
        result.processed = False
        result.unplannedEvent = True
        return result

    # Get the current date and time as integers
    event_datetime = int(scada_event_datetime.strftime('%Y%m%d%H%M%S'))

    # Сначала проверяем аварийные заявки
    sap_events = sap_requests_asap

    mask = sap_events[sap_tech_place_column].str.contains(sap_substation) & \
           ((sap_events['combo_start'] <= event_datetime) & (sap_events['combo_end'] >= event_datetime)
            | (sap_events[sap_end_date_column_asap] == 0))

    sap_events = sap_events.loc[mask]

    if len(sap_events) > 0:
        result.unplanned_event = False
        result.processed = True
        return result

    # Потом проверяем обычные заявки
    sap_events = sap_requests

    mask = sap_events[sap_tech_place_column].str.contains(sap_substation) & \
           ((sap_events['combo_start'] <= event_datetime) & (sap_events['combo_end'] >= event_datetime)
            | (sap_events[sap_end_date_column] == 0))

    sap_events = sap_events.loc[mask]

    if len(sap_events) > 0:
        result.unplanned_event = False
        result.processed = True
        return result

    result.processed = True
    result.unplannedEvent = True
    return result


def check_lines_sap(dbase_scada_model, scada_model_cursor, event_source, sap_requests, sap_requests_asap,
                    scada_event_datetime, substation_key):
    result = CheckResult()

    if sap_requests is None:
        return result

    # Список всех Line по коду подстанции

    query = f"SELECT distinct [{dbase_scada_model}].[dbo].[Equipment].EquipmentContainerKey, " \
        f"[{dbase_scada_model}].[dbo].[IdentifiedObject].KeyLink as LineKey, " \
        f"[{dbase_scada_model}].[dbo].[IdentifiedObject].name_, " \
        f"[{dbase_scada_model}].[dbo].[IdentifiedObject].ClassName " \
        f"FROM  [{dbase_scada_model}].[dbo].[Equipment] " \
        f"INNER JOIN  [{dbase_scada_model}].[dbo].[IdentifiedObject]  " \
        f"ON  [{dbase_scada_model}].[dbo].[Equipment].EquipmentContainerKey = " \
        f"[{dbase_scada_model}].[dbo].[IdentifiedObject].KeyLink" \
        f" WHERE " \
        f"EXISTS(" \
        f"SELECT  1 FROM  [{dbase_scada_model}].[dbo].[IdentifiedObject] " \
        f"INNER JOIN [{dbase_scada_model}].[dbo].[Terminal] " \
        f"ON [{dbase_scada_model}].[dbo].[Terminal].[ConductingEquipmentKey] = " \
        f"[{dbase_scada_model}].[dbo].[IdentifiedObject].KeyLink " \
        f"INNER JOIN [{dbase_scada_model}].[dbo].[ConnectivityNode] " \
        f"ON [{dbase_scada_model}].[dbo].[ConnectivityNode].KeyLink = " \
        f"[{dbase_scada_model}].[dbo].[Terminal].ConnectivityNodeKey " \
        f"INNER JOIN [{dbase_scada_model}].[dbo].[ACLineSegment] " \
        f"ON [{dbase_scada_model}].[dbo].[ACLineSegment].KeyLink = " \
        f"[{dbase_scada_model}].[dbo].[IdentifiedObject].KeyLink" \
        f" WHERE [{dbase_scada_model}].[dbo].[ConnectivityNode].ContainerKey = '{substation_key}' " \
        f"AND [{dbase_scada_model}].[dbo].IdentifiedObject.KeyLink = [{dbase_scada_model}].[dbo].[Equipment].KeyLink)"

    scada_model_cursor.execute(query)
    lines = scada_model_cursor.fetchall()
    field_map_lines = fields(scada_model_cursor)

    find_any_line = False
    unplanned_event = True

    # Пробегаем по всем линиям которые подключены к подстанции на которой произошло событие
    for line in lines:
        line_id = line[field_map_lines['LineKey']]

        scada_model_cursor.execute(f"SELECT top 1 sapTM FROM [{dbase_scada_model}].[dbo].[LinkSAP] "
                                   f"where scdPSRKey = '{line_id}'")

        sap_line = None
        res = scada_model_cursor.fetchone()
        if res:
            sap_line = res[0]

        # sap_line = "VS010-0010687"
        if sap_line is None:
            continue

        # помечаем, что хоть 1 линию нашли в САП
        find_any_line = True

        # Get the current date and time as integers
        event_datetime = int(scada_event_datetime.strftime('%Y%m%d%H%M%S'))

        # Сначала проверяем аварийные заявки
        sap_events = sap_requests_asap
        mask = sap_events[sap_tech_place_column].str.contains(sap_line) & \
               ((sap_events['combo_start'] <= event_datetime) & (sap_events['combo_end'] >= event_datetime)
                | (sap_events[sap_end_date_column_asap] == 0))

        sap_events = sap_events.loc[mask]

        if len(sap_events) > 0:
            result.unplanned_event = False
            result.processed = True
            return result

        # Потом проверяем обычные заявки
        sap_events = sap_requests

        mask = sap_events[sap_tech_place_column].str.contains(sap_line) & \
               ((sap_events['combo_start'] <= event_datetime) & (sap_events['combo_end'] >= event_datetime)
                | (sap_events[sap_end_date_column] == 0))

        sap_events = sap_events.loc[mask]

        if len(sap_events) > 0:
            result.unplanned_event = False
            result.processed = True
            return result

    if not find_any_line:
        msg = f'{datetime.now()} | {currentScript} | source: {event_source} | ' \
            f'Не удалось определить линии САП для подстанции с KeyLink = \'{substation_key}\''
        write_log(log_file_name, msg)

    # Если ни по одной линии ничего не нашли - значит НЕ запланировано
    result.unplannedEvent = unplanned_event
    result.processed = find_any_line
    return result


def check_event_source(event_source, param_dict):
    ip_address = param_dict["ip_scada_events"]
    login = param_dict["login_scada_events"]
    password = param_dict["password_scada_events"]
    dbase_scada_events = param_dict["db_scada_events"]

    scada_events_conn = connect_to_mssql(dbase_scada_events, login, password, ip_address, event_source)

    if not scada_events_conn:
        return

    # получаем курсор и структуру таблиц типа events в скаде в БД событий
    scada_events_cursor = scada_events_conn.cursor()
    scada_events_cursor.execute(f"Select KeyLink, code, dt, obj, user_ "
                                f"FROM [{dbase_scada_events}].[dbo].[{scada_event_table}] where code = 0")

    field_map_event = fields(scada_events_cursor)

    ip_address = param_dict["ip_scada_model"]
    login = param_dict["login_scada_model"]
    password = param_dict["password_scada_model"]
    dbase_scada_model = param_dict["db_scada_model"]

    scada_model_conn = connect_to_mssql(dbase_scada_model, login, password, ip_address, event_source)

    if not scada_model_conn:
        return

    # получаем курсор и в скаде в БД Модели
    scada_model_cursor = scada_events_conn.cursor()

    ip_address = param_dict["ip_asu_reo"]
    login = param_dict["login_asu_reo"]
    password = param_dict["password_asu_reo"]
    dbase_asu_reo = param_dict["db_asu_reo"]
    port_asu_reo = param_dict["port_asu_reo"]

    asu_reo_conn = connect_to_postgre(dbase_asu_reo, login, password, ip_address, port_asu_reo, event_source)

    if not asu_reo_conn:
        asu_reo_cursor = None
    else:
        asu_reo_cursor = asu_reo_conn.cursor()

    sap_file_path = os.path.join(sap_dir, sap_file)
    sap_requests = load_sap_file(sap_file_path, event_source, False)

    sap_file_path = os.path.join(sap_dir, sap_file+"_asap")
    sap_requests_asap = load_sap_file(sap_file_path, event_source, True)

    query = f"SELECT scada_codes, description, scada_table " \
        f"FROM scada_event_codes where event_source = {event_source} " \
        f" and scada_table = \'{scada_event_table}\'"

    postgre_cursor.execute(query)
    res = postgre_cursor.fetchone()

    if res is None:
        msg = f'{datetime.now()} | {currentScript} | source: {event_source} | ' \
            f'Не удалось считать коды событий для таблицы  \'{scada_event_table}\''
        if console_messages:
            print(msg)
        write_log(log_file_name, msg)
        return

    event_codes = res[0]

    if not event_codes:
        msg = f'{datetime.now()} | {currentScript} | source: {event_source} | ' \
            f'Не заданы коды событий для таблицы  \'{scada_event_table}\' "{res[1]} [{res[2]}]"'
        if console_messages:
            print(msg)
        write_log(log_file_name, msg)
        return

    for event_code in event_codes:

        # Считываем последнее запомненое и обработанное событие с таким кодом и от этого источника из нашей БД
        postgre_cursor.execute(
            f"Select  date, unix_date "
            f"FROM scada_last_request where type_code = {event_code} and source_code = {event_source}")
        field_map_last_request = fields(postgre_cursor)
        last_processed_request = postgre_cursor.fetchone()

        last_request_unix_time = 0

        # Считываем  все события с таким кодом из БД Скады которые произошли позже последнего обработанного нами
        if last_processed_request is None:
            query = f"Select KeyLink, code, dt, obj, user_ FROM [{dbase_scada_events}].[dbo].[{scada_event_table}] " \
                f"where code = {event_code} order by dt"
        else:
            last_request_unix_time = last_processed_request[field_map_last_request['unix_date']]
            query = f"Select KeyLink, code, dt, obj, user_ FROM [{dbase_scada_events}].[dbo].[{scada_event_table}] " \
                f"where code = {event_code} and dt > {last_request_unix_time} order by dt"

        scada_events_cursor.execute(query)
        all_events = scada_events_cursor.fetchall()

        event_description = 'не определено'
        query = f"Select nameLoc FROM [{dbase_scada_model}].[dbo].[EventCod] where code = {event_code} "
        scada_model_cursor.execute(query)
        res = scada_model_cursor.fetchone()

        if res:
            event_description = res[0]

        for event_row in all_events:

            scada_event_obj = event_row[field_map_event['obj']]
            unix_ts = event_row[field_map_event['dt']]
            scada_event_datetime = (datetime.fromtimestamp(unix_ts))
            scada_event_keylink = event_row[field_map_event['KeyLink']]

            user_id = event_row[field_map_event['user_']]

            user_json_data = {
                "user_KeyLink": -1,
                "user_short": "не найден"
            }
            user_full_name = "не найден"
            user_values_string = f"<b>Пользователь:</b> {user_full_name}<br>"
            event_source_host = ''

            if user_id is not None:
                # Получаем Юзера создавшего события
                query = f"Select login_, firstName, midlName, lastName, department, post " \
                    f"FROM [{dbase_scada_model}].[dbo].[Users] where KeyLink = '{user_id}'"
                scada_model_cursor.execute(query)
                res_user = scada_model_cursor.fetchone()

                if res_user:
                    user_json_data = {
                        "user_KeyLink": user_id,
                        "user_short": res_user[0]
                    }
                    user_full_name = f"{res_user[1]} {res_user[2]} {res_user[3]} {res_user[4]}"
                    user_values_string = f"<b>Пользователь:</b> , {user_full_name}, " \
                        f"{res_user[4]}, {res_user[5]}<br>"

                # Получаем IP откуда создано событие
                query = f"Select top 1 caption FROM [{dbase_scada_events}].[dbo].[EvSys] " \
                    f"where user_ = '{user_id}' and code = 1103 order by dt desc"
                scada_events_cursor.execute(query)
                res = scada_events_cursor.fetchone()
                caption = None
                if res:
                    caption = res[0]

                if caption:
                    if caption is not None and caption != 'None':
                        start = caption.find(',')
                        end = caption.rfind(',')
                        if start > 0 and end > 0:
                            event_source_host = caption[start + 1:end]

            # Получаем Оборудование для которого создано событие
            dev_type_name = "не определено"
            substation_name = "не определено"
            equipment_key = None
            substation_key = None

            query = f"Select PSRKey FROM [{dbase_scada_model}].[dbo].[Measurement] " \
                f"where KeyLink = '{scada_event_obj}' and PSRKey is not NULL"

            scada_model_cursor.execute(query)
            res = scada_model_cursor.fetchone()
            if res:
                equipment_key = res[0]

            if equipment_key is None:
                msg = f'{datetime.now()} | {currentScript} | source: {event_source} | ' \
                    f'Не удалось определить оборудование для измерения с KeyLink = \'{scada_event_obj}\''
                write_log(log_file_name, msg)
                continue

            query = f" SELECT [{dbase_scada_model}].[dbo].[IdentifiedObject].name_, " \
                f"[{dbase_scada_model}].[dbo].[IdentifiedObject].ClassName, " \
                f"IdentifiedObject_1.name_ as Substation_Name," \
                f"IdentifiedObject_1.ClassName as Substation_Class, " \
                f"IdentifiedObject_1.KeyLink as Substation_KeyLink " \
                f"FROM ((([{dbase_scada_model}].[dbo].[Equipment] " \
                f"INNER JOIN " \
                f"[{dbase_scada_model}].[dbo].[IdentifiedObject] " \
                f"ON [Equipment].KeyLink = [IdentifiedObject].KeyLink ) " \
                f"INNER JOIN " \
                f"[{dbase_scada_model}].[dbo].[VoltageLevel] " \
                f"ON [Equipment].EquipmentContainerKey = [VoltageLevel].KeyLink) " \
                f"INNER JOIN [{dbase_scada_model}].[dbo].[Substation] " \
                f"ON [VoltageLevel].SubstationKey = [Substation].KeyLink) " \
                f"INNER JOIN [{dbase_scada_model}].[dbo].[IdentifiedObject] AS IdentifiedObject_1 ON " \
                f"[Substation].KeyLink = IdentifiedObject_1.KeyLink where " \
                f"[IdentifiedObject].KeyLink='{equipment_key}'"

            scada_model_cursor.execute(query)
            field_map_device_substaion = fields(scada_model_cursor)
            query_result = scada_model_cursor.fetchone()

            if query_result:
                dev_type_name = query_result[field_map_device_substaion['name_']]
                substation_name = query_result[field_map_device_substaion['Substation_Name']]
                substation_key = query_result[field_map_device_substaion['Substation_KeyLink']]

            if substation_key is None:
                msg = f'{datetime.now()} | {currentScript} | source: {event_source} | ' \
                    f'Не удалось определить подстанцию события \'{scada_event_keylink}\' ' \
                    f'для оборудования  \'{equipment_key}\''
                write_log(log_file_name, msg)
                continue

            # Проверяем событие по подстанции в АСУ РЭО
            check_result = check_substation_asu_reo(dbase_scada_model, event_source, scada_event_datetime,
                                                    scada_model_cursor, asu_reo_cursor, substation_key)

            # Проверяем событие по подстанции в САП
            if check_result.unplannedEvent:
                check_result = check_substation_sap(dbase_scada_model, scada_model_cursor, event_source,
                                                    sap_requests, sap_requests_asap,
                                                    scada_event_datetime,
                                                    substation_key)

            # Проверяем событие по линиям к которым подключена подстанция  в АСУ РЭО
            if check_result.unplannedEvent:
                check_result = check_lines_asu_reo(dbase_scada_model, event_source, scada_event_datetime,
                                                   scada_model_cursor, asu_reo_cursor, substation_key)

            # Проверяем событие по линиям к которым подключена подстанция в САП
            if check_result.unplannedEvent:
                check_result = check_lines_sap(dbase_scada_model, scada_model_cursor, event_source,
                                               sap_requests, sap_requests_asap,
                                               scada_event_datetime,
                                               substation_key)

            if check_result.unplannedEvent:
                result_string = f"<b>{event_description}</b>, код [{event_code}]<br>" \
                                    f"<b>Фактическое исполнение:</b> {scada_event_datetime}<br>" \
                                    f"<b>Подстанция:</b> {substation_name}<br>" \
                                    f"<b>Оборудование:</b> {dev_type_name}<br>" \
                                + user_values_string

                json_data = {
                    "data": user_json_data,
                    "display": {
                        "Внеплановое событие!": result_string
                    },
                    "event_description": event_description,
                    "event_host": event_source_host,
                    "event_date": f'{scada_event_datetime}',
                    "event_code": event_code,
                    "event_equipment": dev_type_name,
                    "event_substation": substation_name,
                    "user_name": user_full_name,
                    "equipment_key": equipment_key,
                    "substation_key": substation_key,
                    "event_key": scada_event_keylink
                }

                data = json.dumps(json_data)
                postgre_cursor.callproc('event_new', [event_type, event_source, True, data])
                postgre_conn.commit()

            elif log_planned_events:
                postgre_cursor.execute(f"SELECT * FROM event_type where name = \'plannedEvent\'")
                planned_event_type = postgre_cursor.fetchone()[0]

                result_string = f"{event_description}, код [{event_code}]<br>" \
                                    f"<b>Фактическое исполнение:</b> {scada_event_datetime}<br>" \
                                    f"<b>Подстанция:</b> {substation_name}<br>" \
                                    f"<b>Оборудование:</b> {dev_type_name}<br>" \
                                + user_values_string

                txt_planned = "Событие запланировано. "

                json_data = {
                    "data": user_json_data,
                    f"display": {
                        txt_planned: result_string
                    },
                    "event_description": event_description,
                    "event_host": event_source_host,
                    "event_date": f'{scada_event_datetime}',
                    "event_code": event_code,
                    "event_equipment": dev_type_name,
                    "event_substation": substation_name,
                    "user_name": user_full_name,
                    "equipment_key": equipment_key,
                    "substation_key": substation_key,
                    "event_key": scada_event_keylink
                }

                data = json.dumps(json_data)
                postgre_cursor.callproc('event_new', [planned_event_type, event_source, True, data])
                postgre_conn.commit()

            if last_request_unix_time <= 0:
                query = f"INSERT INTO  scada_last_request (date, unix_date, " \
                    f"type_code, source_code)" \
                    f" VALUES ('{scada_event_datetime}', {unix_ts} ,{event_code},{event_source})"
            else:
                query = f"UPDATE scada_last_request SET " \
                    f" date = '{scada_event_datetime}'," \
                    f" unix_date = {unix_ts} " \
                    f"WHERE type_code = {event_code} and source_code = {event_source}"

            postgre_cursor.execute(query)
            postgre_conn.commit()
            last_request_unix_time = unix_ts

    # Закрываем все соеденения
    scada_events_cursor.close()
    scada_model_cursor.close()

    if asu_reo_cursor:
        asu_reo_cursor.close()

    scada_events_conn.close()
    scada_model_conn.close()

    if asu_reo_conn:
        asu_reo_conn.close()


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

        except (Exception, Error) as error:
            local_error = f'{datetime.now()} | {currentScript} | source: {event_source} | Ошибка: {error}'
            if console_messages:
                print(local_error)
            write_log(log_file_name, local_error)
            continue


if __name__ == "__main__":

    currentScript = '[Event_HM_Checker]'
    scada_event_table = 'EvHm'
    event_type_name = 'unplannedHM'
    event_source_name = "telescada"
    event_source_main = 0
    log_file_name = None
    postgre_cursor = None
    postgre_conn = None
    postgre_host = None
    postgre_database = None
    oldest_zvk_date_delta = 100
    oldest_log_delta = 30
    log_planned_events = True
    console_messages = False

    sap_end_date_column = 'tfdate'
    sap_end_date_column_asap = 'ausbs'

    sap_tech_place_column = 'tplnr'

    try:
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
        log_dir = config_ini.get('common', 'logdir')

        sap_dir = config_ini.get('events_checker', 'sap_dir')
        sap_file = config_ini.get('events_checker', 'sap_file')

        log_file_name = config_ini.get('events_checker', 'logfile')

        if log_dir != "":
            log_file_name = log_dir + "/" + log_file_name

        postgre_conn = connect_to_postgre(postgre_database, postgre_user,
                                          postgre_pass, postgre_host, postgre_port, event_source_main)

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

        sap_log_checker.update(ini_file_path, 0)

        check_all_event_sources()

    except (Exception, Error) as main_error:
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
