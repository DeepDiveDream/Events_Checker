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


# import sap_log_checker


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
        # write_log(log_file_name, msg)
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
            f"where deviceid = {asureo_line_id} " \
            f"and needrepairdatebegin <= '{scada_event_datetime}' " \
            f"and needrepairdateend >= '{scada_event_datetime}' order by zvkbodyid"

        asu_reo_cursor.execute(query)
        res = asu_reo_cursor.fetchall()

        # Если хоть что-то нашли - значит запланировано
        if len(res) > 0:
            result.unplannedEvent = False
            result.processed = True

            query = f"SELECT zvkopener, createdate, deviceid, zvkobjectid, " \
                f"selfnumber, needrepairdatebegin as datebegin, " \
                f"needrepairdateend as dateend, z_cat.name as cat_name " \
                f"FROM public.zvkbody as z_body " \
                f"inner join public.zvkcategory as z_cat " \
                f"on z_body.zvkcategoryid = z_cat.zvkcategoryid where ZVKBodyId = {res[0][0]}"

            asu_reo_cursor.execute(query)
            fields_map_zvk_content = fields(asu_reo_cursor)
            zvk_content = asu_reo_cursor.fetchone()
            result.message = f"№ {zvk_content[fields_map_zvk_content['selfnumber']]}, " \
                f"Автор: {zvk_content[fields_map_zvk_content['zvkopener']]}, " \
                f"Дата: {zvk_content[fields_map_zvk_content['createdate']]}, " \
                f"Тип: {zvk_content[fields_map_zvk_content['cat_name']]}, " \
                f"Период: с {zvk_content[fields_map_zvk_content['datebegin']]}, " \
                f"по {zvk_content[fields_map_zvk_content['dateend']]}, " \
                f"ID оборудования АСУ РЭО: {zvk_content[fields_map_zvk_content['deviceid']]}, " \
                f"ID энергообъекта АСУ РЭО: {zvk_content[fields_map_zvk_content['zvkobjectid']]}"

            return result

    if not find_any_line:
        if not_found_messages:
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

    if asureo_power_obj_id is None:
        if not_found_messages:
            msg = f'{datetime.now()} | {currentScript} | source: {event_source} | ' \
                f'Не удалось определить подстанцию АСУРЭО для подстанции с KeyLink = \'{substation_key}\''
            write_log(log_file_name, msg)
        result.processed = False
        result.unplannedEvent = True
        return result

    query = f"SELECT ZVKBodyId  FROM zvkbody  " \
        f"where zvkobjectid = {asureo_power_obj_id} "  \
        f"and needrepairdatebegin <= '{scada_event_datetime}' " \
        f"and needrepairdateend >= '{scada_event_datetime}' order by zvkbodyid"

    asu_reo_cursor.execute(query)
    res = asu_reo_cursor.fetchall()

    # Если хоть что-то нашли - значит запланировано
    if len(res) > 0:
        result.processed = True
        result.unplannedEvent = False
        query = f"SELECT zvkopener, createdate, deviceid, zvkobjectid, " \
            f"selfnumber, needrepairdatebegin as datebegin, " \
            f"needrepairdateend as dateend, z_cat.name as cat_name " \
            f"FROM public.zvkbody as z_body " \
            f"inner join public.zvkcategory as z_cat " \
            f"on z_body.zvkcategoryid = z_cat.zvkcategoryid where ZVKBodyId = {res[0][0]}"

        asu_reo_cursor.execute(query)
        fields_map_zvk_content = fields(asu_reo_cursor)
        zvk_content = asu_reo_cursor.fetchone()
        result.message = f"№ {zvk_content[fields_map_zvk_content['selfnumber']]}, " \
            f"Автор: {zvk_content[fields_map_zvk_content['zvkopener']]}, " \
            f"Дата: {zvk_content[fields_map_zvk_content['createdate']]}, " \
            f"Тип: {zvk_content[fields_map_zvk_content['cat_name']]}, " \
            f"Период: с {zvk_content[fields_map_zvk_content['datebegin']]}, " \
            f"по {zvk_content[fields_map_zvk_content['dateend']]}, " \
            f"ID оборудования АСУ РЭО: {zvk_content[fields_map_zvk_content['deviceid']]}, " \
            f"ID энергообъекта АСУ РЭО: {zvk_content[fields_map_zvk_content['zvkobjectid']]}"

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

    if sap_substation is None:
        if not_found_messages:
            msg = f'{datetime.now()} | {currentScript} | source: {event_source} | ' \
                f'Не удалось определить подстанцию SAP для подстанции с KeyLink = \'{substation_key}\''
            write_log(log_file_name, msg)

        result.processed = False
        result.unplannedEvent = True
        return result

    event_datetime = int(scada_event_datetime.strftime('%Y%m%d%H%M%S'))

    # Сначала проверяем аварийные заявки
    sap_events = sap_requests_asap

    mask = sap_events[sap_tech_place_column].str.contains(sap_substation) & \
           (
                   ((sap_events['combo_start'] <= event_datetime) & (sap_events['combo_end'] >= event_datetime))
                   |
                   ((sap_events['combo_start'] <= event_datetime) & (sap_events[sap_end_date_column_asap] == 0))
           )

    sap_events = sap_events.loc[mask]

    if len(sap_events) > 0:
        result.unplannedEvent = False
        result.processed = True
        msg_lst = sap_events.iloc[0].tolist()
        sap_msg = ''
        for lst in msg_lst:
            lst = str(lst)
            lst = lst.replace('\t', '')
            lst = lst.strip()
            sap_msg += lst + "|"
        result.message = sap_msg
        return result

    # Потом проверяем обычные заявки
    sap_events = sap_requests

    mask = sap_events[sap_tech_place_column].str.contains(sap_substation) & \
           ((sap_events['combo_start'] <= event_datetime) & (sap_events['combo_end'] >= event_datetime))

    sap_events = sap_events.loc[mask]

    if len(sap_events) > 0:
        result.unplannedEvent = False
        result.processed = True
        msg_lst = sap_events.iloc[0].tolist()
        sap_msg = ''
        for lst in msg_lst:
            lst = str(lst)
            lst = lst.replace('\t', '')
            lst = lst.strip()
            sap_msg += lst + "|"
        result.message = sap_msg
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

        if sap_line is None:
            continue

        # помечаем, что хоть 1 линию нашли в САП
        find_any_line = True

        # Get the current date and time as integers
        event_datetime = int(scada_event_datetime.strftime('%Y%m%d%H%M%S'))

        # Сначала проверяем аварийные заявки
        sap_events = sap_requests_asap
        mask = sap_events[sap_tech_place_column].str.contains(sap_line) & \
               (
                       ((sap_events['combo_start'] <= event_datetime) & (sap_events['combo_end'] >= event_datetime))
                       |
                       ((sap_events['combo_start'] <= event_datetime) & (sap_events[sap_end_date_column_asap] == 0))
               )

        sap_events = sap_events.loc[mask]

        if len(sap_events) > 0:
            result.unplannedEvent = False
            result.processed = True
            msg_lst = sap_events.iloc[0].tolist()
            sap_msg = ''
            for lst in msg_lst:
                lst = str(lst)
                lst = lst.replace('\t', '')
                lst = lst.strip()
                sap_msg += lst + "|"
            result.message = sap_msg
            return result

        # Потом проверяем обычные заявки
        sap_events = sap_requests

        mask = sap_events[sap_tech_place_column].str.contains(sap_line) & \
               ((sap_events['combo_start'] <= event_datetime) & (sap_events['combo_end'] >= event_datetime))

        sap_events = sap_events.loc[mask]

        if len(sap_events) > 0:
            result.unplannedEvent = False
            result.processed = True
            msg_lst = sap_events.iloc[0].tolist()
            sap_msg = ''
            for lst in msg_lst:
                lst = str(lst)
                lst = lst.replace('\t', '')
                lst = lst.strip()
                sap_msg += lst + "|"
            result.message = sap_msg
            return result

    if not find_any_line:
        if not_found_messages:
            msg = f'{datetime.now()} | {currentScript} | source: {event_source} | ' \
                f'Не удалось определить линии САП для подстанции с KeyLink = \'{substation_key}\''
            write_log(log_file_name, msg)

    # Если ни по одной линии ничего не нашли - значит НЕ запланировано
    result.unplannedEvent = unplanned_event
    result.processed = find_any_line
    return result


def check_event_source(event_source, param_dict):

    detectorlabel = param_dict["detectorlabel"]
    substation_key = param_dict["keylink"]
    scada_id = param_dict["scada_id"]

    query = f"Select params from event_source where id = {scada_id}"
    postgre_cursor.execute(query)
    params_connect = postgre_cursor.fetchone()[0]

    if params_connect is None:
        msg = f'{datetime.now()} | {currentScript} | source: {event_source} | ' \
            f'Не удалось определить Scada с Id = \'{substation_key}\''
        write_log(log_file_name, msg)
        return

    ip_address = params_connect["ip_scada_model"]
    login = params_connect["login_scada_model"]
    password = params_connect["password_scada_model"]
    dbase_scada_model = params_connect["db_scada_model"]

    scada_model_conn = connect_to_mssql(dbase_scada_model, login, password, ip_address, event_source)

    if not scada_model_conn:
        return

    # получаем курсор и в скаде в БД Модели
    scada_model_cursor = scada_model_conn.cursor()

    ip_address = params_connect["ip_asu_reo"]
    login = params_connect["login_asu_reo"]
    password = params_connect["password_asu_reo"]
    dbase_asu_reo = params_connect["db_asu_reo"]
    port_asu_reo = params_connect["port_asu_reo"]

    asu_reo_conn = connect_to_postgre(dbase_asu_reo, login, password, ip_address, port_asu_reo, event_source)

    if not asu_reo_conn:
        asu_reo_cursor = None
    else:
        asu_reo_cursor = asu_reo_conn.cursor()

    sap_file_path = os.path.join(sap_dir, sap_file)
    sap_requests = load_sap_file(sap_file_path, event_source, False)

    sap_file_path = os.path.join(sap_dir, sap_file + "_asap")
    sap_requests_asap = load_sap_file(sap_file_path, event_source, True)

    event_description = 'Событие отслежено по данным XFirewall'

    if not os.path.exists(firewall_events_dir):
        msg = f'{datetime.now()} | {currentScript} | source: {event_source} | ' \
            f'Не найдена папка с файлами событий фаервола = \'{firewall_events_dir}\' '
        write_log(log_file_name, msg)
        os.makedirs(firewall_events_dir)

    # Get list of all files in a given directory sorted by name
    list_of_files = sorted(filter(lambda x: os.path.isfile(os.path.join(firewall_events_dir, x)),
                                  os.listdir(firewall_events_dir)))

    for filename in list_of_files:
        event_host = filename.split('_')[0]
        if event_host != detectorlabel:
            continue

        query = f"Select name_ FROM [{dbase_scada_model}].[dbo].[IdentifiedObject] " \
            f"where KeyLink = '{substation_key}'"
        scada_model_cursor.execute(query)
        res = scada_model_cursor.fetchone()
        substation_name = "не определено"

        if res:
            substation_name = res[0]

        filename = os.path.join(firewall_events_dir, filename)

        file = open(filename, 'r')
        lines = file.readlines()
        file.close()
        os.remove(filename)

        for line in lines:
            parts = line.split(" ")
            from_ip = str(parts[0])
            to_ip = str(parts[1])
            ts = int(parts[2])
            count = int(parts[3])
            event_datetime = (datetime.fromtimestamp(ts))

            query = f"select * from  event_source_net_caption('{to_ip}')"
            postgre_cursor.execute(query)
            eq_from_ip = postgre_cursor.fetchone()[0]

            # Проверяем событие по подстанции в САП
            check_result = check_substation_sap(dbase_scada_model, scada_model_cursor, event_source,
                                                sap_requests, sap_requests_asap,
                                                event_datetime,
                                                substation_key)

            # Проверяем событие по подстанции в АСУ РЭО
            if check_result.unplannedEvent:
                check_result = check_substation_asu_reo(dbase_scada_model, event_source, event_datetime,
                                                        scada_model_cursor, asu_reo_cursor, substation_key)

            # Проверяем событие по линиям к которым подключена подстанция  в АСУ РЭО
            if check_result.unplannedEvent:
                check_result = check_lines_asu_reo(dbase_scada_model, event_source, event_datetime,
                                                   scada_model_cursor, asu_reo_cursor, substation_key)

            # Проверяем событие по линиям к которым подключена подстанция в САП
            if check_result.unplannedEvent:
                check_result = check_lines_sap(dbase_scada_model, scada_model_cursor, event_source,
                                               sap_requests, sap_requests_asap,
                                               event_datetime,
                                               substation_key)

            if check_result.unplannedEvent:
                result_string = f"<b>{event_description}</b>, код [{event_type}]<br>" \
                    f"<b>Фактическое исполнение:</b> {event_datetime}<br>" \
                    f"<b>Подстанция:</b> {substation_name}<br>" \
                    f"<b>Оборудование:</b> {eq_from_ip}<br>" \
                    f"<b>IP АРМ с которого был доступ:</b> {from_ip}<br>" \
                    f"<b>IP и порт устройства к которому был доступ:</b> {to_ip}<br>" \
                    f"<b>количество IP-пакетов:</b> {count}<br>"

                json_data = {
                    "data": "Событие отслежено по данным XFirewall",
                    "display": {
                        "Внеплановое событие!": result_string
                    },
                    "event_description": event_description,
                    "event_host": event_host,
                    "event_date": f'{event_datetime}',
                    "event_code": event_type,
                    "event_equipment": eq_from_ip,
                    "event_substation": substation_name,
                    "user_name": "",
                    "equipment_key": "",
                    "substation_key": substation_key,
                    "event_key": ""
                }

                data = json.dumps(json_data)
                postgre_cursor.callproc('event_new', [event_type, event_source, True, data])
                postgre_conn.commit()

            elif log_planned_events:
                postgre_cursor.execute(f"SELECT * FROM event_type where name = \'plannedEvent\'")
                planned_event_type = postgre_cursor.fetchone()[0]

                result_string = f"{event_description}, код [{event_type}]<br>" \
                    f"<b>Фактическое исполнение:</b> {event_datetime}<br>" \
                    f"<b>Подстанция:</b> {substation_name}<br>" \
                    f"<b>Оборудование:</b> {eq_from_ip}<br>" \
                    f"<b>IP АРМ с которого был доступ:</b> {from_ip}<br>" \
                    f"<b>IP и порт устройства к которому был доступ:</b> {to_ip}<br>" \
                    f"<b>количество IP-пакетов:</b> {count}<br>" \
                    f"Заявка:{check_result.message}"

                txt_planned = "Событие запланировано. "

                json_data = {
                    "data": "Событие отслежено по данным XFirewall",
                    f"display": {
                        txt_planned: result_string
                    },
                    "event_description": event_description,
                    "event_host": event_host,
                    "event_date": f'{event_datetime}',
                    "event_code": event_type,
                    "event_equipment": eq_from_ip,
                    "event_substation": substation_name,
                    "user_name": "",
                    "equipment_key": "",
                    "substation_key": substation_key,
                    "event_key": ""
                }

                data = json.dumps(json_data)
                postgre_cursor.callproc('event_new', [planned_event_type, event_source, True, data])
                postgre_conn.commit()

    # Закрываем все соеденения
    scada_model_cursor.close()
    if asu_reo_cursor:
        asu_reo_cursor.close()

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

    currentScript = '[Event_FWALL_Checker]'
    event_type_name = 'unplannedFWall'
    # event_source_name = "telescada"
    event_source_name = "netviolation"
    event_source_main = 0
    log_file_name = None
    postgre_cursor = None
    postgre_conn = None
    postgre_host = None
    postgre_database = None
    oldest_log_delta = 30
    log_planned_events = True
    console_messages = False
    not_found_messages = False

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
        data_dir = config_ini.get('common', 'datadir')
        sap_dir = config_ini.get('events_checker', 'sap_dir')

        sap_file = config_ini.get('events_checker', 'sap_file')
        sap_dir = os.path.join(data_dir, sap_dir)

        firewall_events_dir = config_ini.get('netviolation', 'essyslogddir')

        log_file_name = config_ini.get('events_checker', 'firewall_checker_logfile')
        log_file_name = os.path.join(log_dir, log_file_name)

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


        # sap_log_checker.update(ini_file_path, 0)

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
