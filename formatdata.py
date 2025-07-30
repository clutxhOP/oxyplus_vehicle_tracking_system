import os
import pandas as pd
from lxml import etree
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

def find_latest_excel_file(folder_path):
    files = [f for f in os.listdir(folder_path) if f.endswith(".xlsx")]
    if not files:
        return None

    files = sorted(files, key=lambda x: os.path.getmtime(os.path.join(folder_path, x)), reverse=True)
    return os.path.join(folder_path, files[0])

def convert_xlsx_to_csv(folder_name, skip_rows=0):
    try:
        excel_file = find_latest_excel_file(folder_name)
        if not excel_file:
            print(f"No new Excel (.xlsx) files found in '{folder_name}'. Skipping.")
            return

        print(f"Processing file: {excel_file}")
        df = pd.read_excel(excel_file, skiprows=skip_rows)
        df.to_csv(f"{folder_name}/temp.csv", index=False)
        print(f"Saved cleaned data to: {folder_name}/temp.csv")
    except Exception as e:
        print(f"[convert_xlsx_to_csv] Error: {e}")
        return

def extract_data_from_xml(file_path):
    ns = {
        'ss': 'urn:schemas-microsoft-com:office:spreadsheet'
    }
    try:
        with open(file_path, 'rb') as f:
            tree = etree.parse(f)

        rows = tree.xpath('//ss:Worksheet/ss:Table/ss:Row', namespaces=ns)
        extracted = []

        for row in rows:
            cells = row.xpath('./ss:Cell/ss:Data', namespaces=ns)
            extracted.append([cell.text if cell.text is not None else '' for cell in cells])

        return extracted
    except Exception as e:
        print(f"[extract_data_from_xml] Error: {e}")
        return []

def collapse_xml_to_csv(folder_name):
    all_rows = []
    header = None

    for file in os.listdir(folder_name):
        if file.lower().endswith('.xml'):
            file_path = os.path.join(folder_name, file)
            try:
                rows = extract_data_from_xml(file_path)
                if not rows:
                    continue
                if header is None:
                    header = rows[0]
                all_rows.extend(rows[1:])
            except Exception as e:
                print(f"Failed to parse {file_path}: {e}")

    if header is None:
        print("No valid XML data found.")
        return

    df = pd.DataFrame(all_rows, columns=header)
    df.drop_duplicates(inplace=True)
    df.to_csv(f'{folder_name}/temp.csv', index=False)
    print(f"Collapsed {len(df)} unique rows into 'temp.csv'.")

def get_time_range_uae(delta_minutes: int = 30):
    timezone_str = "Asia/Dubai"
    now = datetime.now(tz=ZoneInfo(timezone_str))
    delta = timedelta(minutes=delta_minutes)

    start_date = now - delta
    end_date = now

    time_format = '%d-%m-%Y %I:%M%p'
    return start_date.strftime(time_format), end_date.strftime(time_format)

def format_generic_report(folder_name, date_column, date_formats):
    temp_csv = os.path.join(folder_name, "temp.csv")
    history_csv = os.path.join(folder_name, "history.csv")
    current_csv = os.path.join(folder_name, "current.csv")

    timezone_str = "Asia/Dubai"
    now = datetime.now(tz=ZoneInfo(timezone_str))
    today_str = now.strftime('%d-%m-%Y')

    def is_today(date_str):
        for fmt in date_formats:
            try:
                dt = datetime.strptime(date_str.strip(), fmt)
                return dt.strftime('%d-%m-%Y') == today_str
            except:
                continue
        return False

    if os.path.exists(current_csv):
        try:
            df_current = pd.read_csv(current_csv)
            if not df_current.empty and date_column in df_current.columns:
                df_current['__is_today'] = df_current[date_column].apply(is_today)
                outdated_rows = df_current[~df_current['__is_today']].drop(columns=['__is_today'])
                df_current_today = df_current[df_current['__is_today']].drop(columns=['__is_today'])

                if not outdated_rows.empty:
                    if os.path.exists(history_csv):
                        df_history_existing = pd.read_csv(history_csv)
                        df_combined_history = pd.concat([df_history_existing, outdated_rows], ignore_index=True)
                        df_combined_history.to_csv(history_csv, index=False)
                    else:
                        outdated_rows.to_csv(history_csv, index=False)

                df_current_today.to_csv(current_csv, index=False)
        except Exception as e:
            print(f"Warning: Failed to process existing current.csv in '{folder_name}': {e}")

    if not os.path.exists(temp_csv):
        print(f"No temp.csv in '{folder_name}'. Skipping new data.")
        return

    try:
        df = pd.read_csv(temp_csv)
    except Exception as e:
        print(f"Failed to read temp.csv in '{folder_name}': {e}")
        os.remove(temp_csv)
        return

    if df.empty:
        print(f"temp.csv in '{folder_name}' is empty. Skipping.")
        os.remove(temp_csv)
        return

    if date_column not in df.columns:
        print(f"Date column '{date_column}' missing in temp.csv for '{folder_name}'. Skipping.")
        os.remove(temp_csv)
        return

    df.drop_duplicates(inplace=True)

    df['__is_today'] = df[date_column].apply(is_today)
    df_today = df[df['__is_today']].drop(columns=['__is_today'])
    df_history = df[~df['__is_today']].drop(columns=['__is_today'])

    if not df_history.empty:
        if os.path.exists(history_csv):
            df_existing = pd.read_csv(history_csv)
            df_combined = pd.concat([df_existing, df_history], ignore_index=True).drop_duplicates()
            df_combined.to_csv(history_csv, index=False)
        else:
            df_history.to_csv(history_csv, index=False)
    if not df_today.empty:
        if os.path.exists(current_csv):
            df_existing = pd.read_csv(current_csv)
            df_combined = pd.concat([df_existing, df_today], ignore_index=True).drop_duplicates()
            df_combined.to_csv(current_csv, index=False)
        else:
            df_today.to_csv(current_csv, index=False)

    os.remove(temp_csv)
    print(f"{folder_name} -> current.csv (deduplicated today), history.csv (deduplicated past) updated.")
    return

def format_travel_report(folder_name):
    collapse_xml_to_csv(folder_name)
    temp_csv_path = os.path.join(folder_name, 'temp.csv')

    if os.path.exists(temp_csv_path):
        travel_temp = pd.read_csv(temp_csv_path)
        columns = ['DateTime']

        for column in columns:
            travel_temp[column] = travel_temp[column].str.replace(r'\s+(AM|PM)', '', regex=True)

            travel_temp[column] = pd.to_datetime(
                travel_temp[column],
                format="%d-%m-%Y %H:%M:%S",
                errors='coerce'
            )

        travel_temp.dropna(subset=columns, inplace=True)

        travel_temp.to_csv(temp_csv_path, index=False)

    format_generic_report(
        folder_name=folder_name,
        date_column="DateTime",
        date_formats=["%Y-%m-%d %H:%M:%S"]
    )

    return

def format_geofence_report(folder_name):
    convert_xlsx_to_csv(folder_name,skip_rows = 8)
    format_generic_report(folder_name, date_column="In Time", date_formats=["%Y-%m-%d %H:%M:%S"])
    return
def format_idle_report(folder_name):
    convert_xlsx_to_csv(folder_name,skip_rows = 2)
    format_generic_report(folder_name, date_column="Idle From", date_formats=["%Y-%m-%d %H:%M:%S"])
    return
def format_exidle_report(folder_name):
    convert_xlsx_to_csv(folder_name,skip_rows = 8)
    format_generic_report(folder_name, date_column="Idle From", date_formats=["%Y-%m-%d %H:%M:%S"])
    return
def format_driver_performance(folder_name):
    collapse_xml_to_csv(folder_name)
    temp_csv_path = os.path.join(folder_name, 'temp.csv')

    if os.path.exists(temp_csv_path):
        performance_temp = pd.read_csv(temp_csv_path)
        columns = ['Login Time', 'Logout Time']
        for column in columns:
            performance_temp[column] = performance_temp[column].astype(str).str.replace(r'\s+(AM|PM)', '', regex=True).str.strip()
            performance_temp[column] = pd.to_datetime(
                performance_temp[column],
                format="%d-%m-%Y %H:%M:%S",
                errors='coerce'
            )
        performance_temp.dropna(subset=columns, inplace=True)
        performance_temp.to_csv(temp_csv_path, index=False)
    format_generic_report(
        folder_name=folder_name,
        date_column="Login Time",
        date_formats=["%Y-%m-%d %H:%M:%S"]
    )
    return

def clean_folder(folder_name):
    keep_files = {'current.csv', 'history.csv'}

    for filename in os.listdir(folder_name):
        file_path = os.path.join(folder_name, filename)

        if os.path.isdir(file_path) or filename in keep_files:
            continue

        try:
            os.remove(file_path)
            print(f"Deleted: {file_path}")
        except Exception as e:
            print(f"Failed to delete {file_path}: {e}")

def format_everything():
    folders = [
        "data/travelreport",
        "data/geofence",
        "data/idlereport",
        "data/exidlereport",
        "data/driverperformance"
    ]

    format_travel_report(folder_name=folders[0])
    format_geofence_report(folder_name=folders[1])
    format_idle_report(folder_name=folders[2])
    format_exidle_report(folder_name=folders[3])
    format_driver_performance(folder_name=folders[4])

    for folder in folders:
        clean_folder(folder)