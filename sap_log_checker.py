import os
import pandas as pd
from datetime import datetime, timedelta
from ftplib import FTP
from configparser import ConfigParser
import psycopg2
import json


def __fields(cursor):
    results = {}
    column = 0
    for d in cursor.description:
        results[d[0]] = column
        column += 1
    return results


def __write_log(file_name, str_log):
    with open(file_name, 'a') as logfile:
        logfile.write(str_log + '\n')
        logfile.close()


def __connect_to_postgre(dbase, login, password, host, port):
    try:
        conn = psycopg2.connect(
            database=dbase,
            user=login,
            password=password,
            port=port,
            host=host)
        return conn

    except Exception as e:
        __write_log("Error_SapLog.txt", f"{datetime.now()}| Failed to update SAP Log. Reason: {e}")
        return None


def __check_need_update(postgre_cursor, event_source, update_interval):
    if postgre_cursor is None:
        return False

    postgre_cursor.execute(f"SELECT id, data ->> 'event_date' as event_date "
                           f"FROM event_source_data where source = {event_source} "
                           f"and data ->> 'event_type'='Update_SAP'")
    field_map_event = __fields(postgre_cursor)
    res = postgre_cursor.fetchone()

    if res is not None:
        today = datetime.now()
        need_to_check_date = int((today - timedelta(minutes=update_interval)).strftime('%Y%m%d%H%M'))
        last_date = int(res[field_map_event['event_date']])
        if last_date < need_to_check_date:
            return True
        else:
            return False
    return True


def __update_requests(result_file_path, result_file_name, start_date_column, start_time_column,  end_date_column,
                      end_time_column, file_list, sap_dir, is_asap_mode):
    today = datetime.today()
    today_date_int = int(today.strftime('%Y%m%d%H%M%S'))

    if os.path.isfile(result_file_path):

        df = pd.read_csv(result_file_path, sep="\t")
        df['combo_end'] = pd.to_numeric(df['combo_end'], errors="coerce")
        df[end_date_column] = pd.to_numeric(df[end_date_column], errors="coerce")

        # оставляем только события дата окончания которых ещё не прошла или не указана
        mask = (df['combo_end'] >= today_date_int) | (df[end_date_column] == 0)
        df = df.loc[mask]

        df.to_csv(result_file_path, sep="\t", index=False, header=True, mode="w")
    else:
        # создаем пустой файл в зависимости от режима проверки заявок
        if is_asap_mode:
            df = pd.DataFrame(columns=['bukrs', 'butxt', 'swerk', 'name1', 'stort', 'ktext', 'qmnum', 'stat_sys',
                                       'stat_cus', 'tplnr', 'pltxt', 'ausvn', 'auztv', 'ausbs', 'auztb',
                                       'txt_short', 'txt_long', 'combo_start', 'combo_end'])
        else:
            df = pd.DataFrame(columns=['bukrs', 'butxt', 'swerk', 'name1', 'stort', 'ktext', 'wapinr', 'stat_sys',
                                       'stat_cus', 'priokx', 'tplnr', 'pltxt', 'tsdate', 'tstime', 'tfdate', 'tftime',
                                       'txt_short', 'txt_long', 'combo_start', 'combo_end'])
        df.to_csv(result_file_path, sep="\t", index=False, header=True, mode="a")

    for filename in file_list:
        file_path = os.path.join(sap_dir, filename)
        if os.path.isfile(file_path) and filename != result_file_name:
            df = pd.read_csv(file_path, sep="\t", header=0)

            df['combo_start'] = df[start_date_column].astype(str) + df[start_time_column].astype(str)
            df['combo_end'] = df[end_date_column].astype(str) + df[end_time_column].astype(str)

            df['combo_end'] = pd.to_numeric(df['combo_end'], errors="coerce")
            df[end_date_column] = pd.to_numeric(df[end_date_column], errors="coerce")

            # оставляем только события дата окончания которых ещё не прошла или не указана
            mask = (df['combo_end'] >= today_date_int) | (df[end_date_column] == 0)
            df = df.loc[mask]

            df.to_csv(result_file_path, sep="\t", index=False, header=False, mode="a")


def __update_sap_file(path_to_config, source):
    event_source = source

    start_date_column = 'tsdate'
    start_time_column = 'tstime'
    end_date_column = 'tfdate'
    end_time_column = 'tftime'

    start_date_column_asap = 'ausvn'
    start_time_column_asap = 'auztv'
    end_date_column_asap = 'ausbs'
    end_time_column_asap = 'auztb'

    config_ini = ConfigParser()
    config_ini.read(path_to_config)

    postgre_user = config_ini.get('common', 'pguser')
    postgre_pass = config_ini.get('common', 'pgpassword')
    postgre_host = config_ini.get('common', 'pghost')
    postgre_port = config_ini.get('common', 'pgport')
    postgre_database = config_ini.get('common', 'pgdb')

    postgre_conn = __connect_to_postgre(postgre_database, postgre_user, postgre_pass, postgre_host, postgre_port)
    postgre_cursor = None

    if postgre_conn:
        postgre_cursor = postgre_conn.cursor()

    update_interval_minutes = int(config_ini.get('sap_log_checker', 'update_interval_minutes'))

    need_update = __check_need_update(postgre_cursor, event_source, update_interval_minutes)

    if not need_update:
        return

    shift_days = int(config_ini.get('sap_log_checker', 'shift_days'))
    log_dir = config_ini.get('common', 'logdir')
    log_file_name = config_ini.get('sap_log_checker', 'logfile')

    if log_dir != "":
        log_file_name = log_dir + "/" + log_file_name

    sap_dir = config_ini.get('sap_log_checker', 'sap_dir')
    result_filename = config_ini.get('sap_log_checker', 'sap_file')
    ftp_server = config_ini.get('sap_log_checker', 'ftp_server')
    ftp_port = int(config_ini.get('sap_log_checker', 'ftp_port'))
    ftp_username = config_ini.get('sap_log_checker', 'ftp_username')
    ftp_password = config_ini.get('sap_log_checker', 'ftp_password')
    ftp_dir = config_ini.get('sap_log_checker', 'ftp_dir')

    result_file_path = os.path.join(sap_dir, result_filename)

    result_asap_filename = result_filename + "_asap"
    result_asap_file_path = os.path.join(sap_dir, result_asap_filename)

    today = datetime.today()
    cutoff_date_int = int((today - timedelta(days=shift_days)).strftime('%Y%m%d'))

    if not os.path.exists(sap_dir):
        os.makedirs(sap_dir)

    for filename in os.listdir(sap_dir):
        if filename != result_filename and filename != result_asap_filename:
            file_path = os.path.join(sap_dir, filename)
            try:
                parts = filename.split("_")
                date_int = -1
                if len(parts) > 2:
                    date_int = int(parts[-2])
                # удаляем файлы которые старше даты отсечения
                if date_int < cutoff_date_int:
                    if os.path.isfile(file_path):
                        os.remove(file_path)
            except Exception as e:
                __write_log(log_file_name, f"{datetime.now()}| Failed to delete {file_path}. Reason: {e}")
                continue

    downloaded_request_files = []
    downloaded_asap_files = []

    ftp = FTP()
    ftp.connect(ftp_server, ftp_port, timeout=100)
    ftp.login(user=ftp_username, passwd=ftp_password)

    ftp.cwd(ftp_dir)
    # качаем  файлы заявок и аварийных запросов
    filenames = ftp.nlst("*ER2*.txt")
    for filename in filenames:
        parts = filename.split("_")
        try:
            date_int = int(parts[-2])
            # которые младше даты отсечения
            if date_int >= cutoff_date_int:
                local_file_path = os.path.join(sap_dir, filename)

                if not os.path.isfile(local_file_path):
                    with open(local_file_path, 'wb') as local_file:
                        ftp.retrbinary('RETR ' + filename, local_file.write)
                        if 'zay' in filename:
                            downloaded_request_files.append(filename)
                        if 'P4' in filename:
                            downloaded_asap_files.append(filename)
        except Exception as e:
            __write_log(log_file_name, f"{datetime.now()}| Failed to copy from FTP file {filename}. Reason: {e}")
            continue
    ftp.quit()

    __update_requests(result_asap_file_path, result_asap_filename, start_date_column_asap, start_time_column_asap,
                      end_date_column_asap, end_time_column_asap, downloaded_asap_files, sap_dir, True)

    __write_log(log_file_name, f"{datetime.now()}| Successfully update {result_file_path}")

    __update_requests(result_file_path, result_filename, start_date_column, start_time_column,
                      end_date_column, end_time_column, downloaded_request_files, sap_dir, False)
    __write_log(log_file_name, f"{datetime.now()}| Successfully update {result_asap_file_path}")

    postgre_cursor.execute(f"SELECT id, data ->> 'event_date' as event_date "
                           f"FROM event_source_data where source = {event_source} "
                           f"and data ->> 'event_type'='Update_SAP'")
    field_map_event = __fields(postgre_cursor)
    res = postgre_cursor.fetchone()
    json_data = {
        "event_type": "Update_SAP",
        "event_date": datetime.now().strftime("%Y%m%d%H%M")
    }
    data = json.dumps(json_data)

    if res is not None:
        last_id = res[field_map_event['id']]
        query = f"UPDATE event_source_data SET " \
            f"created = '{datetime.now()}', " \
            f"source = {event_source}, " \
            f"data = '{data}' " \
            f"WHERE id = {last_id}"
    else:
        query = f"INSERT INTO  event_source_data (created, source, data)" \
            f" VALUES ('{datetime.now()}',{event_source},'{data}')"

    postgre_cursor.execute(query)
    postgre_conn.commit()


def update(path_to_config, source):
    try:
        __update_sap_file(path_to_config, source)
    except Exception as e:
        __write_log("error_sap_log.txt", f"{datetime.now()}| Failed to update SAP Log. Reason: {e}")
        return


if __name__ == "__main__":
    update("elsec.conf", 0)
