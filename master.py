from utils import load_settings, load_vehicle_aliases, load_phone_numbers, generate_route_comparison
import schedule, subprocess, contextlib, psutil, signal, atexit
from datetime import datetime, timedelta, timezone
from datetime import time as dtime
from flask import Flask, render_template, request, jsonify, send_file
import os
import sys
import pandas as pd
import requests
import time
import json
import hashlib
from collections import defaultdict
import threading

flask_process = None
whatsapp_process = None
monitoring_active = True

IDLE_REPORT_PATH = "data/idlereport/current.csv"
EXIDLE_REPORT_PATH = "data/exidlereport/current.csv" 
GEOFENCE_REPORT_PATH = "data/geofence/current.csv"
DRIVER_PERFORMANCE_PATH = "data/driverperformance/current.csv"
TRAVEL_REPORT_PATH = "data/travelreport/current.csv"
ALERT_LOGS_PATH = "alerts/alert_logs.json"
DRIVER_VIOLATION_LOGS_PATH = "alerts/driver_violations.json"
ROUTE_DEVIATION_LOGS_PATH = "alerts/route_deviation_logs.json"
SENT_ALERTS_PATH = "alerts/sent_alerts.json"
ALERT_CACHE_PATH = "alerts/alert_cache.json"

IDLE_THRESHOLD_MINUTES = 20
VIOLATION_THRESHOLD = 12
ROUTE_DEVIATION_THRESHOLD = 4000
VISIT_PERCENT_THRESHOLD = 50

def start_whatsapp_job():
    global whatsapp_process
    print("DEBUG: Starting WhatsApp script")

    try:
        whatsapp_process = subprocess.Popen(
            [sys.executable, "whatsappbot/script.py"],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL
        )
        print(f"DEBUG: WhatsApp script started with PID {whatsapp_process.pid}")
    except Exception as e:
        print(f"ERROR: Failed to start WhatsApp script: {e}")
        whatsapp_process = None

def stop_whatsapp_job():
    global whatsapp_process
    if whatsapp_process and whatsapp_process.poll() is None:
        try:
            print("DEBUG: Terminating WhatsApp script...")
            whatsapp_process.terminate()
            whatsapp_process.wait(timeout=10)
            print("DEBUG: WhatsApp script terminated gracefully")
        except subprocess.TimeoutExpired:
            print("DEBUG: WhatsApp script did not terminate in time. Killing...")
            whatsapp_process.kill()
            print("DEBUG: WhatsApp script killed forcefully")
        except Exception as e:
            print(f"ERROR: Error stopping WhatsApp script: {e}")
    else:
        print("DEBUG: No running WhatsApp script to stop")
    whatsapp_process = None

def whatsapp_restart_job():
    current_time = datetime.now().time()
    start_time = dtime(8, 0)
    end_time = dtime(22, 0)

    if not (start_time <= current_time <= end_time):
        print("DEBUG: Skipping WhatsApp restart due to quiet hours")
        return

    print("DEBUG: Restarting WhatsApp script job")
    stop_whatsapp_job()
    start_whatsapp_job()
def ensure_alert_directories():
    os.makedirs("alerts", exist_ok=True)

def load_alert_cache():
    ensure_alert_directories()
    try:
        with open(ALERT_CACHE_PATH, 'r', encoding='utf-8') as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return {}

def save_alert_cache(cache):
    ensure_alert_directories()
    with open(ALERT_CACHE_PATH, 'w', encoding='utf-8') as f:
        json.dump(cache, f, indent=2)

def generate_alert_hash(alert_type, vehicle_id, additional_data=None):
    today = datetime.now().strftime('%Y-%m-%d')
    current_hour = datetime.now().strftime('%H')
    
    if alert_type == "IDLE":
        content = f"{alert_type}_{vehicle_id}_{additional_data}_{today}_{current_hour}"
    elif alert_type == "UNAUTHORIZED_GEOFENCE":
        content = f"{alert_type}_{vehicle_id}_{additional_data}_{today}_{current_hour}"
    elif alert_type == "EARLY_RETURN":
        content = f"{alert_type}_{vehicle_id}_{today}_{current_hour}"
    elif alert_type == "VIOLATION":
        content = f"{alert_type}_{vehicle_id}_{additional_data}_{today}_{current_hour}"
    elif alert_type == "ROUTE_DEVIATION":
        content = f"{alert_type}_{vehicle_id}_{additional_data}_{today}_{current_hour}"
    else:
        content = f"{alert_type}_{vehicle_id}_{today}_{current_hour}"
    
    return hashlib.md5(content.encode()).hexdigest()

def is_duplicate_alert(alert_hash):
    cache = load_alert_cache()
    current_time = datetime.now()
    current_key = current_time.strftime('%Y-%m-%d-%H')
    
    if alert_hash in cache:
        last_sent = cache[alert_hash]
        if last_sent == current_key:
            return True
    
    cache[alert_hash] = current_key
    
    expired_keys = []
    for key, timestamp in cache.items():
        try:
            cached_time = datetime.strptime(timestamp, '%Y-%m-%d-%H')
            if (current_time - cached_time).total_seconds() > 3600:
                expired_keys.append(key)
        except:
            expired_keys.append(key)
    
    for key in expired_keys:
        del cache[key]
    
    save_alert_cache(cache)
    return False

def load_alert_logs():
    ensure_alert_directories()
    try:
        with open(ALERT_LOGS_PATH, 'r', encoding='utf-8') as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return {
            "daily_logs": {},
            "last_cleared": datetime.now().strftime('%Y-%m-%d')
        }

def save_alert_logs(logs):
    ensure_alert_directories()
    with open(ALERT_LOGS_PATH, 'w', encoding='utf-8') as f:
        json.dump(logs, f, indent=2)

def load_driver_violations():
    ensure_alert_directories()
    try:
        with open(DRIVER_VIOLATION_LOGS_PATH, 'r', encoding='utf-8') as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return {}

def save_driver_violations(violations):
    ensure_alert_directories()
    with open(DRIVER_VIOLATION_LOGS_PATH, 'w', encoding='utf-8') as f:
        json.dump(violations, f, indent=2)

def load_route_deviation_logs():
    ensure_alert_directories()
    try:
        with open(ROUTE_DEVIATION_LOGS_PATH, 'r', encoding='utf-8') as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return {}

def save_route_deviation_logs(logs):
    ensure_alert_directories()
    with open(ROUTE_DEVIATION_LOGS_PATH, 'w', encoding='utf-8') as f:
        json.dump(logs, f, indent=2)

def load_sent_alerts():
    ensure_alert_directories()
    try:
        with open(SENT_ALERTS_PATH, 'r', encoding='utf-8') as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return {}

def save_sent_alerts(alerts):
    ensure_alert_directories()
    with open(SENT_ALERTS_PATH, 'w', encoding='utf-8') as f:
        json.dump(alerts, f, indent=2)

def generate_alert_key(alert_type, vehicle_id, additional_data=None):
    today = datetime.now().strftime('%Y-%m-%d')
    current_hour = datetime.now().strftime('%H')
    
    if alert_type == "IDLE":
        return f"IDLE_{vehicle_id}_{additional_data}_{today}_{current_hour}"
    elif alert_type == "UNAUTHORIZED_GEOFENCE":
        return f"GEOFENCE_{vehicle_id}_{additional_data}_{today}_{current_hour}"
    elif alert_type == "EARLY_RETURN":
        return f"EARLY_{vehicle_id}_{today}_{current_hour}"
    elif alert_type == "VIOLATION":
        return f"VIOLATION_{vehicle_id}_{additional_data}_{today}_{current_hour}"
    elif alert_type == "ROUTE_DEVIATION":
        return f"ROUTE_DEV_{vehicle_id}_{additional_data}_{today}_{current_hour}"
    
    return f"{alert_type}_{vehicle_id}_{today}_{current_hour}"

def is_alert_already_sent(alert_key):
    sent_alerts = load_sent_alerts()
    current_time = datetime.now()
    
    if alert_key in sent_alerts:
        try:
            last_sent = datetime.strptime(sent_alerts[alert_key], '%Y-%m-%d %H:%M:%S')
            if (current_time - last_sent).total_seconds() < 3600:
                return True
        except:
            pass
    
    return False

def mark_alert_as_sent(alert_key):
    sent_alerts = load_sent_alerts()
    sent_alerts[alert_key] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    
    current_time = datetime.now()
    expired_keys = []
    
    for key, timestamp in sent_alerts.items():
        try:
            sent_time = datetime.strptime(timestamp, '%Y-%m-%d %H:%M:%S')
            if (current_time - sent_time).total_seconds() > 86400:
                expired_keys.append(key)
        except:
            expired_keys.append(key)
    
    for key in expired_keys:
        del sent_alerts[key]
    
    save_sent_alerts(sent_alerts)

def log_alert(alert_type, recipient_phone, recipient_name, message, vehicle_id=None, driver_name=None):
    logs = load_alert_logs()
    today = datetime.now().strftime('%Y-%m-%d')
    current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    
    if today not in logs["daily_logs"]:
        logs["daily_logs"][today] = []
    
    alert_entry = {
        "timestamp": current_time,
        "alert_type": alert_type,
        "recipient_phone": recipient_phone,
        "recipient_name": recipient_name,
        "message": message,
        "vehicle_id": vehicle_id,
        "driver_name": driver_name
    }
    
    logs["daily_logs"][today].append(alert_entry)
    save_alert_logs(logs)

def clear_old_logs():
    logs = load_alert_logs()
    current_date = datetime.now()
    
    keys_to_delete = []
    for date_key in logs["daily_logs"].keys():
        try:
            log_date = datetime.strptime(date_key, '%Y-%m-%d')
            if (current_date - log_date).days > 7:
                keys_to_delete.append(date_key)
        except:
            keys_to_delete.append(date_key)
    
    for key in keys_to_delete:
        del logs["daily_logs"][key]
    
    logs["last_cleared"] = current_date.strftime('%Y-%m-%d')
    save_alert_logs(logs)
    
    save_route_deviation_logs({})
    save_sent_alerts({})
    save_alert_cache({})
    print("DEBUG: Alert logs cleared")

def get_vehicle_location(vehicle_id):
    try:
        df = pd.read_csv(TRAVEL_REPORT_PATH, dtype={
            'Vehicle No': str,
            'Status': str,
            'Address': str,
            'Speed': float,
            'Odometer': float,
            'Panic': str,
            'Latitude': float,
            'Longitude': float
        }, parse_dates=['DateTime'])
        
        vehicle_data = df[df['Vehicle No'] == str(vehicle_id)].sort_values('DateTime', ascending=False)
        if not vehicle_data.empty:
            latest = vehicle_data.iloc[0]
            return {
                'latitude': latest['Latitude'],
                'longitude': latest['Longitude'],
                'address': latest['Address']
            }
    except Exception as e:
        print(f"DEBUG: Error getting vehicle location: {e}")
    return None

def create_google_maps_link(latitude, longitude, link_type="map"):
    if link_type == "street":
        return f"https://www.google.com/maps/@?api=1&map_action=pano&viewpoint={latitude},{longitude}"
    else:
        return f"https://www.google.com/maps/@?api=1&map_action=map&center={latitude},{longitude}&zoom=15"

def send_whatsapp_alert(phone_number, message):
    try:
        settings = load_settings()
        whatsapp_url = settings.get('whatsapp_server_url')
        if not whatsapp_url:
            return False
        
        response = requests.post(
            f"{whatsapp_url}/send-text",
            json={
                "number": phone_number + "@c.us",
                "message": message
            },
            timeout=30
        )
        return response.status_code == 200
    except Exception as e:
        print(f"DEBUG: WhatsApp send error: {e}")
        return False

def check_idle_alerts():
    try:
        if not os.path.exists(EXIDLE_REPORT_PATH):
            return True
            
        df = pd.read_csv(EXIDLE_REPORT_PATH, dtype={
            'Vehicle Number': str,
            'Vehicle Model': str,
            'Driver': str,
            'Location': str
        }, parse_dates=['Idle From', 'Idle Till'])
        
        if df.empty:
            return True
        
        df['Duration'] = pd.to_timedelta(df['Duration'])
        idle_threshold = timedelta(minutes=IDLE_THRESHOLD_MINUTES)
        
        vehicle_aliases = load_vehicle_aliases()
        phone_numbers = load_phone_numbers()
        
        for _, row in df.iterrows():
            if row['Duration'] > idle_threshold:
                vehicle_id = str(row['Vehicle Number'])
                alias = vehicle_aliases.get(vehicle_id, vehicle_id)
                
                idle_from_str = row['Idle From'].strftime('%Y-%m-%d %H:%M') if pd.notna(row['Idle From']) else 'N/A'
                alert_hash = generate_alert_hash("IDLE", vehicle_id, idle_from_str)
                
                if is_duplicate_alert(alert_hash):
                    continue
                
                alert_key = generate_alert_key("IDLE", vehicle_id, idle_from_str)
                
                if is_alert_already_sent(alert_key):
                    continue
                
                location_info = get_vehicle_location(vehicle_id)
                location_text = row['Location']
                
                if location_info and pd.notna(location_info['latitude']) and pd.notna(location_info['longitude']):
                    maps_link = create_google_maps_link(location_info['latitude'], location_info['longitude'])
                    street_link = create_google_maps_link(location_info['latitude'], location_info['longitude'], "street")
                    location_text = f"{location_info['address']}\nMAP: {maps_link}\nSTREET: {street_link}"
                
                duration_str = str(row['Duration']).split('.')[0]
                idle_from = row['Idle From'].strftime('%H:%M') if pd.notna(row['Idle From']) else 'N/A'
                idle_till = row['Idle Till'].strftime('%H:%M') if pd.notna(row['Idle Till']) else 'N/A'
                
                message = f"🚨 IDLE ALERT\n{alias} stopped {duration_str}\nFrom: {idle_from} To: {idle_till}\nAt: {location_text}"
                
                alert_sent = False
                for contact in phone_numbers['phone_numbers']:
                    if contact['category'] == 'Admin' and contact.get('alerts', False):
                        if send_whatsapp_alert(contact['phone'], message):
                            log_alert("IDLE", contact['phone'], contact['name'], message, vehicle_id, row['Driver'])
                            alert_sent = True
                    
                    elif (contact['category'] == 'Driver' and 
                          contact.get('vehicle_id') == vehicle_id and 
                          contact.get('alerts', False)):
                        if send_whatsapp_alert(contact['phone'], message):
                            log_alert("IDLE", contact['phone'], contact['name'], message, vehicle_id, row['Driver'])
                            alert_sent = True
                
                if alert_sent:
                    mark_alert_as_sent(alert_key)
        
        return True
    except Exception as e:
        print(f"DEBUG: Idle alerts error: {e}")
        return False

def check_driver_performance_alerts():
    try:
        if not os.path.exists(DRIVER_PERFORMANCE_PATH):
            return True
            
        df = pd.read_csv(DRIVER_PERFORMANCE_PATH, dtype={
            'Driver': str,
            'No of Vehicles': str,
            'KM': float,
            'Harsh Break': 'Int64',
            'Harsh Acceleration': 'Int64',
            'Over Speed': 'Int64',
            'Max Speed': 'Int64',
            'Exceed Road Speed': str
        }, parse_dates=['Login Time', 'Logout Time'])
        
        if df.empty:
            return True
        
        vehicle_aliases = load_vehicle_aliases()
        phone_numbers = load_phone_numbers()
        violation_logs = load_driver_violations()
        
        for _, row in df.iterrows():
            driver_name = row['Driver']
            vehicle_id = str(row['No of Vehicles'])
            
            harsh_braking = int(row['Harsh Break']) if pd.notna(row['Harsh Break']) else 0
            harsh_accel = int(row['Harsh Acceleration']) if pd.notna(row['Harsh Acceleration']) else 0
            over_speed = int(row['Over Speed']) if pd.notna(row['Over Speed']) else 0
            
            total_violations = harsh_braking + harsh_accel + over_speed
            
            if total_violations > VIOLATION_THRESHOLD:
                alert_hash = generate_alert_hash("VIOLATION", vehicle_id, total_violations)
                
                if is_duplicate_alert(alert_hash):
                    continue
                
                alert_key = generate_alert_key("VIOLATION", vehicle_id, total_violations)
                
                if is_alert_already_sent(alert_key):
                    continue
                
                driver_key = f"{driver_name}_{vehicle_id}"
                previous_violations = violation_logs.get(driver_key, 0)
                
                if total_violations > previous_violations:
                    violation_logs[driver_key] = total_violations
                    save_driver_violations(violation_logs)
                    
                    message = (f"⚠️ VIOLATION ALERT\n{driver_name} ({vehicle_aliases.get(vehicle_id, vehicle_id)})\n"
                             f"Total: {total_violations} (HB:{harsh_braking} HA:{harsh_accel} OS:{over_speed})")
                    
                    alert_sent = False
                    for contact in phone_numbers['phone_numbers']:
                        if contact['category'] == 'Admin' and contact.get('alerts', False):
                            if send_whatsapp_alert(contact['phone'], message):
                                log_alert("VIOLATION", contact['phone'], contact['name'], message, vehicle_id, driver_name)
                                alert_sent = True
                        elif (contact['category'] == 'Driver' and 
                              contact.get('vehicle_id') == vehicle_id and 
                              contact.get('alerts', False)):
                            if send_whatsapp_alert(contact['phone'], message):
                                log_alert("VIOLATION", contact['phone'], contact['name'], message, vehicle_id, driver_name)
                                alert_sent = True
                    
                    if alert_sent:
                        mark_alert_as_sent(alert_key)
        
        return True
    except Exception as e:
        print(f"DEBUG: Performance alerts error: {e}")
        return False

def check_route_deviation_alerts():
    try:
        now_utc = datetime.now(timezone.utc)
        uae_now = (now_utc + timedelta(hours=4)).strftime('%Y-%m-%d')
        
        vehicle_ids = list(load_vehicle_aliases().keys())
        settings = load_settings()
        
        map_html, comparison_data = generate_route_comparison(
            vehicle_ids=vehicle_ids,
            csv_path_current=settings["csv_path_current"],
            csv_path_past=settings["csv_path_past"],
            geojson_path=settings["geojson_path"],
            date_current=uae_now,
            t_start_current="00:00:00",
            t_end_current="23:59:59",
            date_past=None,
            t_start_past=None,
            t_end_past=None,
            generate_map=False
        )
        
        vehicle_aliases = load_vehicle_aliases()
        phone_numbers = load_phone_numbers()
        deviation_logs = load_route_deviation_logs()
        
        for key, stats in comparison_data.items():
            vehicle_id = key[:-8] if key.endswith('_Current') else key
            if vehicle_id not in vehicle_aliases:
                continue
                
            max_dev = stats.get('maximum_route_deviation', 0)
            visit_percent = stats.get('visit_percentage', 0.0)
            route_alignment = stats.get('actual_route_alignment', 0)
            route_coverage = stats.get('planned_route_coverage', 0)
            actual_km = stats.get('actual_distance', 0)
            planned_km = stats.get('planned_distance', 0)
            
            if max_dev > ROUTE_DEVIATION_THRESHOLD:
                alert_hash = generate_alert_hash("ROUTE_DEVIATION", vehicle_id, max_dev)
                
                if is_duplicate_alert(alert_hash):
                    continue
                
                alert_key = generate_alert_key("ROUTE_DEVIATION", vehicle_id, max_dev)
                
                if is_alert_already_sent(alert_key):
                    continue
                
                vehicle_key = f"{vehicle_id}_{uae_now}"
                
                if vehicle_key not in deviation_logs:
                    deviation_logs[vehicle_key] = max_dev
                    save_route_deviation_logs(deviation_logs)
                    
                    alias = vehicle_aliases[vehicle_id]
                    message = (f"🛤️ ROUTE DEVIATION\n{alias} exceeded {ROUTE_DEVIATION_THRESHOLD}m\n"
                             f"Max deviation: {max_dev:.0f}m\n"
                             f"Route alignment: {route_alignment:.1f}%\n"
                             f"Route coverage: {route_coverage:.1f}%\n"
                             f"Distance: {actual_km:.1f}km (Planned: {planned_km:.1f}km)\n"
                             f"Visits: {stats.get('visited_customer_points', 0)}/{stats.get('total_customer_points', 0)} ({visit_percent:.1f}%)")
                    
                    alert_sent = False
                    for contact in phone_numbers['phone_numbers']:
                        if contact['category'] == 'Admin' and contact.get('alerts', False):
                            if send_whatsapp_alert(contact['phone'], message):
                                log_alert("ROUTE_DEVIATION", contact['phone'], contact['name'], message, vehicle_id)
                                alert_sent = True
                    
                    if alert_sent:
                        mark_alert_as_sent(alert_key)
        
        return True
    except Exception as e:
        print(f"DEBUG: Route deviation alerts error: {e}")
        return False

def check_early_return_alerts():
    try:
        if not os.path.exists(GEOFENCE_REPORT_PATH):
            return True
            
        df = pd.read_csv(GEOFENCE_REPORT_PATH, dtype={
            'Vehicle No': str,
            'Driver': str,
            'Geofence': str,
            'Type': str
        }, parse_dates=['In Time', 'Out Time'])
        
        if df.empty:
            return True
        
        if not os.path.exists(DRIVER_PERFORMANCE_PATH):
            return True
            
        performance_df = pd.read_csv(DRIVER_PERFORMANCE_PATH, dtype={
            'Driver': str,
            'No of Vehicles': str,
            'KM': float,
            'Harsh Break': 'Int64',
            'Harsh Acceleration': 'Int64',
            'Over Speed': 'Int64'
        }, parse_dates=['Login Time', 'Logout Time'])
        
        current_time = datetime.now()
        
        if not (9 <= current_time.hour < 16):
            return True
            
        vehicle_aliases = load_vehicle_aliases()
        phone_numbers = load_phone_numbers()
        
        for _, row in df.iterrows():
            if (row['Geofence'] == 'Oxy Office' and 
                pd.notna(row['In Time']) and pd.notna(row['Out Time'])):
                
                vehicle_id = str(row['Vehicle No'])
                driver_name = row['Driver']
                
                in_time = pd.to_datetime(row['In Time'])
                out_time = pd.to_datetime(row['Out Time'])
                
                if (out_time.hour >= 9 and in_time.hour < 16 and 
                    in_time.date() == out_time.date()):
                    
                    alert_hash = generate_alert_hash("EARLY_RETURN", vehicle_id, in_time.strftime('%Y-%m-%d %H:%M'))
                    
                    if is_duplicate_alert(alert_hash):
                        continue
                    
                    early_return_key = generate_alert_key("EARLY_RETURN", vehicle_id, in_time.strftime('%Y-%m-%d %H:%M'))
                    
                    if is_alert_already_sent(early_return_key):
                        continue
                    
                    perf_row = performance_df[performance_df['No of Vehicles'] == vehicle_id]
                    if not perf_row.empty:
                        perf = perf_row.iloc[0]
                        
                        uae_now = current_time.strftime('%Y-%m-%d')
                        settings = load_settings()
                        
                        _, comparison_data = generate_route_comparison(
                            vehicle_ids=[vehicle_id],
                            csv_path_current=settings["csv_path_current"],
                            csv_path_past=settings["csv_path_past"],
                            geojson_path=settings["geojson_path"],
                            date_current=uae_now,
                            t_start_current="00:00:00",
                            t_end_current="23:59:59",
                            date_past=None,
                            t_start_past=None,
                            t_end_past=None,
                            generate_map=False
                        )
                        
                        customers_visited = 0
                        comparison_key = f"{vehicle_id}_Current"
                        if comparison_key in comparison_data:
                            customers_visited = comparison_data[comparison_key].get('visited_customer_points', 0)
                        
                        alias = vehicle_aliases.get(vehicle_id, vehicle_id)
                        hb = int(perf['Harsh Break']) if pd.notna(perf['Harsh Break']) else 0
                        ha = int(perf['Harsh Acceleration']) if pd.notna(perf['Harsh Acceleration']) else 0
                        overspeed = int(perf['Over Speed']) if pd.notna(perf['Over Speed']) else 0
                        
                        message = (f"⏰ EARLY RETURN\n{alias} ({driver_name}) returned at {in_time.strftime('%H:%M')}\n"
                                 f"Customers: {customers_visited}\nViolations: HB:{hb} HA:{ha} OS:{overspeed}")
                        
                        alert_sent = False
                        for contact in phone_numbers['phone_numbers']:
                            if contact['category'] == 'Admin' and contact.get('alerts', False):
                                if send_whatsapp_alert(contact['phone'], message):
                                    log_alert("EARLY_RETURN", contact['phone'], contact['name'], message, vehicle_id, driver_name)
                                    alert_sent = True
                        
                        if alert_sent:
                            mark_alert_as_sent(early_return_key)
        
        return True
    except Exception as e:
        print(f"DEBUG: Early return alerts error: {e}")
        return False

def check_unauthorized_geofence_alerts():
    try:
        if not os.path.exists(GEOFENCE_REPORT_PATH):
            return True
            
        df = pd.read_csv(GEOFENCE_REPORT_PATH, dtype={
            'Vehicle No': str,
            'Driver': str,
            'Geofence': str,
            'Type': str,
            'Elapsed Time Inside The Geofence': str
        }, parse_dates=['In Time', 'Out Time'])
        
        if df.empty:
            return True
        
        vehicle_aliases = load_vehicle_aliases()
        phone_numbers = load_phone_numbers()
        authorized_geofences = ['Oxy Office', 'Staff Accomodation']
        
        for _, row in df.iterrows():
            if (pd.isna(row['Out Time']) and 
                row['Geofence'] not in authorized_geofences and
                pd.notna(row['Geofence'])):
                
                vehicle_id = str(row['Vehicle No'])
                geofence_name = row['Geofence']
                in_time_str = row['In Time'].strftime('%Y-%m-%d %H:%M') if pd.notna(row['In Time']) else 'N/A'
                
                alert_hash = generate_alert_hash("UNAUTHORIZED_GEOFENCE", vehicle_id, f"{geofence_name}_{in_time_str}")
                
                if is_duplicate_alert(alert_hash):
                    continue
                
                alert_key = generate_alert_key("UNAUTHORIZED_GEOFENCE", vehicle_id, f"{geofence_name}_{in_time_str}")
                
                if is_alert_already_sent(alert_key):
                    continue
                
                alias = vehicle_aliases.get(vehicle_id, vehicle_id)
                driver_name = row['Driver']
                
                location_info = get_vehicle_location(vehicle_id)
                location_text = geofence_name
                
                if location_info and pd.notna(location_info['latitude']) and pd.notna(location_info['longitude']):
                    maps_link = create_google_maps_link(location_info['latitude'], location_info['longitude'])
                    street_link = create_google_maps_link(location_info['latitude'], location_info['longitude'], "street")
                    location_text = f"{geofence_name}\nMAP: {maps_link}\nSTREET: {street_link}"
                
                in_time = row['In Time'].strftime('%H:%M') if pd.notna(row['In Time']) else 'Unknown'
                elapsed_time = row['Elapsed Time Inside The Geofence'] if pd.notna(row['Elapsed Time Inside The Geofence']) else 'Unknown'
                
                message = f"🚩 UNAUTHORIZED AREA\n{alias} ({driver_name}) in competitor area\nLocation: {location_text}\nSince: {in_time}\nDuration: {elapsed_time}"
                
                alert_sent = False
                for contact in phone_numbers['phone_numbers']:
                    if contact['category'] == 'Admin' and contact.get('alerts', False):
                        if send_whatsapp_alert(contact['phone'], message):
                            log_alert("UNAUTHORIZED_GEOFENCE", contact['phone'], contact['name'], message, vehicle_id, driver_name)
                            alert_sent = True
                    
                    elif (contact['category'] == 'Driver' and 
                          contact.get('vehicle_id') == vehicle_id and 
                          contact.get('alerts', False)):
                        if send_whatsapp_alert(contact['phone'], message):
                            log_alert("UNAUTHORIZED_GEOFENCE", contact['phone'], contact['name'], message, vehicle_id, driver_name)
                            alert_sent = True
                
                if alert_sent:
                    mark_alert_as_sent(alert_key)
        
        return True
    except Exception as e:
        print(f"DEBUG: Unauthorized geofence alerts error: {e}")
        return False

def generate_daily_report():
    try:
        now_utc = datetime.now(timezone.utc)
        uae_now = (now_utc + timedelta(hours=4)).strftime('%Y-%m-%d')
        
        if not os.path.exists(DRIVER_PERFORMANCE_PATH):
            return True
        
        performance_df = pd.read_csv(DRIVER_PERFORMANCE_PATH, dtype={
            'Driver': str,
            'No of Vehicles': str,
            'KM': float,
            'Harsh Break': 'Int64',
            'Harsh Acceleration': 'Int64',
            'Over Speed': 'Int64'
        }, parse_dates=['Login Time', 'Logout Time'])
        
        vehicle_ids = list(load_vehicle_aliases().keys())
        settings = load_settings()
        
        _, comparison_data = generate_route_comparison(
            vehicle_ids=vehicle_ids,
            csv_path_current=settings["csv_path_current"],
            csv_path_past=settings["csv_path_past"],
            geojson_path=settings["geojson_path"],
            date_current=uae_now,
            t_start_current="00:00:00",
            t_end_current="23:59:59",
            date_past=None,
            t_start_past=None,
            t_end_past=None,
            generate_map=False
        )
        
        vehicle_aliases = load_vehicle_aliases()
        phone_numbers = load_phone_numbers()
        
        report_lines = [f"📊 DAILY REPORT - {uae_now}"]
        report_lines.append("=" * 40)
        
        for vehicle_id in vehicle_ids:
            alias = vehicle_aliases.get(vehicle_id, vehicle_id)
            
            customers_visited = 0
            total_customers = 0
            visit_percentage = 0
            route_alignment = 0
            route_coverage = 0
            actual_km = 0
            planned_km = 0
            
            comparison_key = f"{vehicle_id}_Current"
            if comparison_key in comparison_data:
                stats = comparison_data[comparison_key]
                customers_visited = stats.get('visited_customer_points', 0)
                total_customers = stats.get('total_customer_points', 0)
                visit_percentage = stats.get('visit_percentage', 0)
                route_alignment = stats.get('actual_route_alignment', 0)
                route_coverage = stats.get('planned_route_coverage', 0)
                actual_km = stats.get('actual_distance', 0)
                planned_km = stats.get('planned_distance', 0)
            
            violations_text = "None"
            perf_row = performance_df[performance_df['No of Vehicles'] == vehicle_id]
            if not perf_row.empty:
                row = perf_row.iloc[0]
                hb = int(row['Harsh Break']) if pd.notna(row['Harsh Break']) else 0
                ha = int(row['Harsh Acceleration']) if pd.notna(row['Harsh Acceleration']) else 0
                overspeed = int(row['Over Speed']) if pd.notna(row['Over Speed']) else 0
                if hb > 0 or ha > 0 or os > 0:
                    violations_text = f"HB:{hb} HA:{ha} OS:{overspeed}"
            
            report_lines.append(f"\n{alias}:")
            report_lines.append(f"Customers: {customers_visited}/{total_customers} ({visit_percentage:.1f}%)")
            report_lines.append(f"Route alignment: {route_alignment:.1f}%")
            report_lines.append(f"Route coverage: {route_coverage:.1f}%")
            report_lines.append(f"Distance: {actual_km:.1f}km (Planned: {planned_km:.1f}km)")
            report_lines.append(f"Violations: {violations_text}")
        
        report_message = "\n".join(report_lines)
        
        for contact in phone_numbers['phone_numbers']:
            if contact['category'] == 'Admin' and contact.get('alerts', False):
                if send_whatsapp_alert(contact['phone'], report_message):
                    log_alert("DAILY_REPORT", contact['phone'], contact['name'], report_message)
        
        return True
    except Exception as e:
        print(f"DEBUG: Daily report error: {e}")
        return False

def run_script_safely(module_name, func_name, *args):
    try:
        args_str = ", ".join(repr(arg) for arg in args)
        command = f'from {module_name} import {func_name}; {func_name}({args_str})'
        result = subprocess.run(
            [sys.executable, "-c", command],
            capture_output=False,
            text=True,
            check=True
        )
        return True
    except subprocess.CalledProcessError as e:
        print(f"ERROR: Subprocess failed with exit code {e.returncode}")
        return False
    except Exception as e:
        print(f"ERROR: Script execution failed: {e}")
        return False

def data_extraction_and_formatting_job():
    current_time = datetime.now().time()
    quiet_start = dtime(21, 0)
    quiet_end = dtime(8, 0)

    if quiet_start <= current_time or current_time < quiet_end:
        print("DEBUG: Skipping extraction job execution due to quiet hours [9 PM - 8 AM]")
        return False

    print("DEBUG: Starting data extraction")
    success1 = run_script_safely('extractdata', 'extract_all_data')
    print(f"DEBUG: Data extraction {'completed' if success1 else 'failed'}")
    
    if success1:
        print("DEBUG: Starting data formatting")
        success2 = run_script_safely('formatdata', 'format_everything')
        print(f"DEBUG: Data formatting {'completed' if success2 else 'failed'}")
        return success2
    return False

def preprocessing_job():
    success = run_script_safely('preprocess', 'preprocess_everything', 70)
    print(f"DEBUG: Preprocessing {'completed' if success else 'failed'}")

def whatsapp_clean_job():
    current_time = datetime.now().time()
    start_time = dtime(8, 0)
    end_time = dtime(23, 59)

    if not (start_time <= current_time <= end_time):
        print("DEBUG: Skipping WhatsApp clean job due to quiet hours")
        return

    print("DEBUG: Running WhatsApp clean job")
    
    try:
        result = subprocess.run(
            [sys.executable, "whatsappbot/clean.py"],
            capture_output=False,
            text=True,
            check=False
        )
        print(f"DEBUG: WhatsApp clean job {'completed' if result.returncode == 0 else 'failed silently'}")
    except Exception as e:
        print(f"DEBUG: WhatsApp clean job failed silently: {e}")

def alert_monitoring_job():
    current_time = datetime.now().time()
    quiet_start = dtime(21, 0)
    quiet_end = dtime(9, 0)

    if quiet_start <= current_time or current_time < quiet_end:
        print("DEBUG: Skipping alert monitoring due to quiet hours [9 PM - 9 AM]")
        return

    print("DEBUG: Starting alert monitoring")
    
    idle_alerts = check_idle_alerts()
    performance_alerts = check_driver_performance_alerts()
    route_alerts = check_route_deviation_alerts()
    early_return_alerts = check_early_return_alerts()
    geofence_alerts = check_unauthorized_geofence_alerts()
    
    print(f"DEBUG: Alert monitoring completed - idle:{idle_alerts}, performance:{performance_alerts}, "
          f"route:{route_alerts}, early_return:{early_return_alerts}, geofence:{geofence_alerts}")

def start_flask_app():
    global flask_process
    print("DEBUG: Starting Flask application")
    
    try:
        flask_process = subprocess.Popen(
            [sys.executable, 'app.py'],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL
        )
        print(f"DEBUG: Flask app started with PID {flask_process.pid}")
        return True
    except Exception as e:
        print(f"ERROR: Failed to start Flask app: {e}")
        return False

def stop_flask_app():
    global flask_process
    if flask_process and flask_process.poll() is None:
        try:
            flask_process.terminate()
            flask_process.wait(timeout=10)
            print("DEBUG: Flask app terminated gracefully")
        except subprocess.TimeoutExpired:
            flask_process.kill()
            print("DEBUG: Flask app killed forcefully")
        except Exception as e:
            print(f"DEBUG: Error stopping Flask app: {e}")

def flask_restart_job():
    print("DEBUG: Restarting Flask app")
    stop_flask_app()
    time.sleep(2)
    start_flask_app()

def test_all():
    print("=" * 60)
    print("TESTING ALL SCHEDULED JOBS")
    print("=" * 60)
    
    print("\n1. Testing data extraction and formatting...")
    try:
        result = data_extraction_and_formatting_job()
        print(f"Data extraction and formatting: {'PASSED' if result else 'FAILED'}")
    except Exception as e:
        print(f"Data extraction and formatting: FAILED - {e}")
    
    print("\n2. Testing preprocessing...")
    try:
        preprocessing_job()
        print("Preprocessing: PASSED")
    except Exception as e:
        print(f"Preprocessing: FAILED - {e}")
    
    print("\n3. Testing alert monitoring...")
    try:
        alert_monitoring_job()
        print("Alert monitoring: PASSED")
    except Exception as e:
        print(f"Alert monitoring: FAILED - {e}")
    
    print("\n4. Testing WhatsApp clean job...")
    try:
        whatsapp_clean_job()
        print("WhatsApp clean job: PASSED")
    except Exception as e:
        print(f"WhatsApp clean job: FAILED - {e}")
    
    print("\n5. Testing WhatsApp script job...")
    try:
        start_whatsapp_job()
        print("WhatsApp script job: PASSED")
    except Exception as e:
        print(f"WhatsApp script job: FAILED - {e}")
    
    print("\n6. Testing daily report generation...")
    try:
        result = generate_daily_report()
        print(f"Daily report generation: {'PASSED' if result else 'FAILED'}")
    except Exception as e:
        print(f"Daily report generation: FAILED - {e}")
    
    print("\n7. Testing log cleanup...")
    try:
        clear_old_logs()
        print("Log cleanup: PASSED")
    except Exception as e:
        print(f"Log cleanup: FAILED - {e}")
    
    print("\n" + "=" * 60)
    print("ALL TESTS COMPLETED")
    print("=" * 60)

def cleanup_handler(signum, frame):
    global monitoring_active
    print("DEBUG: Received shutdown signal, cleaning up...")
    monitoring_active = False
    stop_flask_app()
    sys.exit(0)

def initialize_system():
    print("DEBUG: Initializing alert monitoring system...")
    
    ensure_alert_directories()
    
    print("DEBUG: Testing system components...")
    try:
        load_settings()
        load_vehicle_aliases()
        load_phone_numbers()
        print("DEBUG: Configuration files loaded successfully")
    except Exception as e:
        print(f"ERROR: Failed to load configuration: {e}")
        return False
    
    print("DEBUG: Starting Flask application...")
    if not start_flask_app():
        print("DEBUG: FLASK APP START FAILED")
        return False
    print("DEBUG FLASK APP STARTED.\n")
    time.sleep(3)
    
    print("DEBUG: Running initial data extraction...")
    data_extraction_and_formatting_job()
    print("DEBUG: RAN WHATSAPP CLEAN JOB")
    whatsapp_clean_job()
    print("DEBUG: RAN WHATSAPP SCRIPT JOB")
    start_whatsapp_job()
    return True

def schedule_jobs():
    print("DEBUG: Scheduling jobs...")
    
    schedule.every(15).minutes.do(data_extraction_and_formatting_job)
    schedule.every(10).minutes.do(alert_monitoring_job)
    schedule.every(20).minutes.do(whatsapp_clean_job)
    schedule.every(20).minutes.do(whatsapp_restart_job)
    schedule.every().day.at("01:00").do(preprocessing_job)
    schedule.every().day.at("07:00").do(clear_old_logs)
    schedule.every().day.at("08:00").do(flask_restart_job)
    schedule.every().day.at("21:30").do(generate_daily_report)
    
    print("DEBUG: All jobs scheduled successfully")

def run_monitoring_loop():
    global monitoring_active
    start_time = datetime.now()
    max_runtime = timedelta(hours=15)
    print("DEBUG: Starting monitoring loop...")
    try:
        while monitoring_active:
            if datetime.now() - start_time > max_runtime:
                print("DEBUG: 15-hour cycle completed, exiting for restart")
                time.sleep(600)
                break
                
            schedule.run_pending()
            time.sleep(30)
    except KeyboardInterrupt:
        print("DEBUG: Received keyboard interrupt")
    except Exception as e:
        print(f"ERROR: Monitoring loop error: {e}")
    finally:
        cleanup_handler(None, None)

def main():
    print("=" * 60)
    print("ALERT MONITORING SYSTEM STARTING")
    print("=" * 60)
    
    signal.signal(signal.SIGTERM, cleanup_handler)
    signal.signal(signal.SIGINT, cleanup_handler)
    atexit.register(cleanup_handler, None, None)
    
    if len(sys.argv) > 1 and sys.argv[1] == "test":
        test_all()
        return 0
    
    if not initialize_system():
        print("ERROR: System initialization failed")
        return 1
    
    schedule_jobs()
    
    print("DEBUG: System initialized successfully")
    print("DEBUG: Alert monitoring system is now active")
    
    run_monitoring_loop()
    
    return 0

if __name__ == "__main__":
    #test_all()
    print("Final deploy version no lfs track")
    exit_code = main()