import json
import logging
import os
import threading
import time
from datetime import datetime, timedelta, UTC,timezone
from itertools import groupby
from typing import  Dict
from langchain_google_genai import ChatGoogleGenerativeAI
import google.generativeai as genai
from typing import Dict, List, Optional
from google.api_core.exceptions import ResourceExhausted

import folium
import numpy as np
import openrouteservice
import pandas as pd
import requests
import re
import schedule
from geopy import distance
from geopy.geocoders import Nominatim
from pyproj import Transformer
from scipy.spatial.distance import directed_hausdorff
from shapely.geometry import LineString, Point
from sklearn.cluster import KMeans

rag_system = None
_geolocator = None
SETTINGS_FILE = "config_data/app_settings.json"
PHONE_FILE = "config_data/phone_no.json"
DRIVER_NAMES = "config_data/vehicle_aliases.json"

CUSTOMER_POINTS_DIR = "analysis/customerpoints"
ROUTES_JSON_DIR = "analysis/routes_json"
EDITS_CSV_FILE = "analysis/customerinfo/customerinfo.csv"
TRAVEL_REPORT_DIR = "data/travelreport"
DEFAULT_SETTINGS = {
    "ors_api_key": "",
    "route_cache_dir": "",
    "csv_path_current": "",
    "csv_path_past": "",
    "geojson_path": "",
    "customer_points_path":"",
    "gemini_api_key": "",
    "gemini_model": "models/gemini-2.0-flash-exp",
    "alert_followup_url":"",
    "whatsapp_server_url":"",
    "openai_api_key":""
}
def load_whatsapp_customer_data():
    customers = []
    stats = {
        'total_customers': 0,
        'completed_customers': 0,
        'pending_customers': 0
    }
    
    try:
        whatsapp_dir = os.path.join(os.getcwd(), 'whatsappbot')
        
        contact_status_file = os.path.join(whatsapp_dir, 'contact_status.csv')
        extracted_data_file = os.path.join(whatsapp_dir, 'extracted_data.csv')
        
        if os.path.exists(contact_status_file) and os.path.exists(extracted_data_file):

            contact_df = pd.read_csv(
                contact_status_file, 
                dtype={'contact': str},
                encoding='utf-8'
            )
            extracted_df = pd.read_csv(
                extracted_data_file, 
                dtype={'contact': str},
                encoding='utf-8'
            )
            
            completed_contacts = contact_df[contact_df['status'] == 'COMPLETED']
            merged_df = pd.merge(
                completed_contacts, 
                extracted_df, 
                on=['contact', 'contact'], 
                how='inner',
                suffixes = ('','_drop')
            )

            merged_df['timestamp'] = pd.to_datetime(merged_df['timestamp'])
            merged_df['location_received_at'] = pd.to_datetime(merged_df['location_received_at'])
            merged_df['weekday'] = merged_df['timestamp'].dt.day_name()

            for _, row in merged_df.iterrows():
                customer = {
                    'customer_id': f"WA_{row['contact']}",
                    'customer_name': row['customer_name'],
                    'contact': row['contact'],
                    'latitude': float(row['latitude']),
                    'longitude': float(row['longitude']),
                    'location_description': row.get('location_description', ''),
                    'timestamp': row['timestamp'].strftime('%Y-%m-%d %H:%M:%S'),
                    'location_received_at': row['location_received_at'].strftime('%Y-%m-%d %H:%M:%S'),
                    'weekday': row['weekday'],
                    'vehicle_assigned': 'WHATSAPP',
                    'source': 'WhatsApp'
                }
                print(f"Appending customer")
                customers.append(customer)
            print(contact_df.head())
            stats['total_customers'] = len(contact_df)
            stats['completed_customers'] = len(merged_df)
            stats['pending_customers'] = len(contact_df[
                contact_df['status'].isin([
                    'PENDING', 'AWAITING_NAME', 'AWAITING_LOCATION', 'COLLECTING_LOCATION'
                ])
            ])
            
            print(f"Loaded {len(customers)} WhatsApp customers")
        
    except Exception as e:
        print(f"Error loading WhatsApp customer data: {e}")
        import traceback
        traceback.print_exc()
    return customers, stats
def load_vehicle_aliases():
    try:
        if os.path.exists(DRIVER_NAMES):
            with open(DRIVER_NAMES, 'r',encoding='utf-8') as f:
                return json.load(f)
        else:
            default_aliases = {
                "30915": "ghaffar/sr",
                "36346": "Majid",
                "30941": "Sayed",
                "30917": "Touqueer",
                "B-30942": "Sayed",
                "34261": "aboobakar"
            }
            with open(DRIVER_NAMES, 'w',encoding='utf-8') as f:
                json.dump(default_aliases, f, indent=2)
            return default_aliases
    except Exception as e:
        print(f"Error loading vehicle aliases: {e}")
        return {}

def load_actual_route(csv_path, vehicle_id, t_start, t_end):
    if not os.path.exists(csv_path):
        return []
    
    try:
        df = pd.read_csv(
            csv_path,
            dtype={
                'Vehicle No': str,
                'Status': str,
                'Address': str,
                'Panic':str,
                'Speed':float,
                'Odometer': float,
                'Latitude':float,
                'Longitude':float
            },
            parse_dates=['DateTime']
        )

        df['Vehicle No'] = df['Vehicle No'].astype(str).str.strip()
        vehicle_id_str = str(vehicle_id).strip()
        
        df = df[df["Vehicle No"] == vehicle_id_str]
        
        df = df[df["Latitude"].notnull() & df["Longitude"].notnull()]
        
        if "DateTime" in df.columns:
            df["DateTime"] = pd.to_datetime(df["DateTime"])
            df = df[(df["DateTime"] >= pd.to_datetime(t_start)) & (df["DateTime"] <= pd.to_datetime(t_end))]
        
        coords = df[["Longitude", "Latitude"]].drop_duplicates().values.tolist()
        return coords
        
    except Exception as e:
        return []
def load_geojson_route(geojson_data, vehicle_id, weekday):
    vehicle_id_str = str(vehicle_id).strip()
    weekday_lower = weekday.lower().strip()
    
    for feat in geojson_data["features"]:
        props = feat["properties"]
        geojson_vehicle = str(props.get("vehicle_id", "")).strip()
        geojson_weekday = str(props.get("weekday", "")).lower().strip()
                
        if geojson_vehicle == vehicle_id_str and geojson_weekday == weekday_lower:
            return feat["geometry"]["coordinates"]
    return []

def load_settings():
    if os.path.exists(SETTINGS_FILE):
        try:
            with open(SETTINGS_FILE, 'r',encoding='utf-8') as f:
                settings = json.load(f)
                for key, value in DEFAULT_SETTINGS.items():
                    if key not in settings:
                        settings[key] = value
                return settings
        except:
            pass
    return DEFAULT_SETTINGS.copy()

def save_settings(settings):
    try:
        with open(SETTINGS_FILE, 'w',encoding='utf-8') as f:
            json.dump(settings, f, indent=2)
        return True
    except:
        return False

def get_available_options():
    customer_files = [
        f for f in os.listdir(CUSTOMER_POINTS_DIR)
        if f.endswith(".csv")
    ]

    geojson_files = [
        f for f in os.listdir(ROUTES_JSON_DIR)
        if f.endswith(".geojson")
    ]

    return {
        "customer_points": customer_files,
        "geojson_paths": geojson_files
    }
def add_stop_points_to_map(map_obj, vehicle_ids, csv_path, date_val, vehicle_colors):
    if not date_val or not csv_path:
        return
    try:
        t_start = f"{date_val} 00:00:00"
        t_end = f"{date_val} 23:59:59"

        stop_points_df = extract_stop_points(csv_path, vehicle_ids, t_start, t_end)

        if not stop_points_df.empty:
            print(f"Found {len(stop_points_df)} stop points to display")
            for _, row in stop_points_df.iterrows():
                vehicle_id = str(row['Vehicle No'])

                color = vehicle_colors.get(vehicle_id, 'gray')
                
                stop_popup = f"""
                <div style="font-family: Arial; font-size: 12px; min-width: 200px;">
                    <b>Vehicle:</b> {load_vehicle_aliases().get(vehicle_id, vehicle_id)} ({vehicle_id})<br>
                    <b>Status:</b> {row['Status']}<br>
                    <b>Duration:</b> {row['DurationMinutes']} minutes<br>
                    <b>Start:</b> {row['StartTime'].strftime('%H:%M')}<br>
                    <b>End:</b> {row['EndTime'].strftime('%H:%M')}<br>
                    <b>Coordinates:</b> {row['Latitude']:.6f}, {row['Longitude']:.6f}<br>
                    <b>Address:</b> {row['Address']}<br>
                    <b>Date:</b> {date_val}<br>
                    <b>Type:</b> Stop/Idle Point<br><br>
                    <b>Map Views:</b><br>
                    <a href="https://www.google.com/maps?q={row['Latitude']:.6f},{row['Longitude']:.6f}" target="_blank">🗺️ View Location on 2D Map</a><br>
                    <a href="https://www.google.com/maps/@?api=1&map_action=pano&viewpoint={row['Latitude']:.6f},{row['Longitude']:.6f}" target="_blank">🌐 Explore in Street View (Geolocator)</a><br>
                </div>
                """

                tooltip_content = f"{load_vehicle_aliases().get(vehicle_id, vehicle_id)} - {row['Status']} ({row['DurationMinutes']} min) - {date_val}"

                marker_type = "idle_point" if row['Status'] == 'Idle' else "stop_point"
                icon_html = create_pin_marker(color, size=14, opacity=0.8, marker_type=marker_type)
                
                folium.Marker(
                    [row['Latitude'], row['Longitude']],
                    popup=folium.Popup(stop_popup, max_width=300),
                    tooltip=folium.Tooltip(tooltip_content, permanent=False),
                    icon=folium.DivIcon(
                        html=icon_html,
                        icon_size=(16, 16),
                        icon_anchor=(8, 16)
                    )
                ).add_to(map_obj)
        else:
            print("No stop points found for the specified date range")
    except Exception as e:
        print(f"Error extracting stop points: {e}")

def save_vehicle_aliases(aliases):
    try:
        with open(DRIVER_NAMES, 'w',encoding='utf-8') as f:
            json.dump(aliases, f, indent=2)
        return True
    except Exception as e:
        print(f"Error saving vehicle aliases: {e}")
        return False

def create_pin_marker(color, size=16, opacity=0.8, marker_type="default"):
    icon = {
        "customer_point": "👤",
        "stop_point": "🅿️",
        "idle_point": "🚚",
        "added_point": "✏️",
    }.get(marker_type, "📍")

    return f'''
    <div style="
        width: {size}px;
        height: {size}px;
        background-color: {color};
        border-radius: 50% 50% 50% 0;
        transform: rotate(-45deg);
        opacity: {opacity};
        filter: drop-shadow(1px 1px 2px rgba(0,0,0,0.5));
        border: 2px solid white;
        display: flex;
        align-items: center;
        justify-content: center;
        font-size: {int(size * 0.6)}px;
        color: white;
    ">
        <div style="transform: rotate(45deg);">{icon}</div>
    </div>
    '''

def haversine_distance(lat1, lon1, lat2, lon2):
    from math import radians, cos, sin, sqrt, atan2
    R = 6371.0
    dlon = radians(lon2 - lon1)
    dlat = radians(lat2 - lat1)
    a = sin(dlat/2)**2 + cos(radians(lat1)) * cos(radians(lat2)) * sin(dlon/2)**2
    c = 2 * atan2(sqrt(a), sqrt(1-a))
    return R * c * 1000

def compute_path_overlap(ref_path, target_path, tolerance_m=500):
    if not ref_path or not target_path:
        return 0
    ref_line = LineString(ref_path)
    count = sum(
        1 for lon, lat in target_path
        if ref_line.distance(Point(lon, lat)) * 111320 <= tolerance_m
    )
    return (count / len(target_path)) * 100 if target_path else 0

def extract_stop_points(csv_path, vehicle_ids, t_start, t_end):
    if not os.path.exists(csv_path):
        print(f"CSV file not found: {csv_path}")
        return pd.DataFrame()
    
    try:
        df = pd.read_csv(
            csv_path,
            dtype={
                'Vehicle No': str,
                'Status': str,
                'Address': str,
                'Odometer': float,
                'Panic': str,
                'Latitude':float,
                'Longitude':float
            },
            parse_dates=['DateTime']
        )
        print(f"Loaded {len(df)} records from {csv_path}")

        df['Vehicle No'] = df['Vehicle No'].astype(str).str.strip()
        vehicle_ids_str = [vid.strip() for vid in vehicle_ids]
        df = df[df['Vehicle No'].isin(vehicle_ids_str)]

        df['DateTime'] = pd.to_datetime(df['DateTime'])
        df = df[(df['DateTime'] >= pd.to_datetime(t_start)) & (df['DateTime'] <= pd.to_datetime(t_end))]

        df = df[df['Status'].isin(['Idle', 'Stopped'])]
        
        if df.empty:
            print("No stop/idle data found for the specified criteria")
            return pd.DataFrame()

        stop_points = []
        
        for vehicle_id in vehicle_ids_str:
            vehicle_data = df[df['Vehicle No'] == vehicle_id].copy()
            if vehicle_data.empty:
                continue

            vehicle_data = vehicle_data.sort_values('DateTime')

            current_group = []
            current_lat = None
            current_lon = None
            
            for _, row in vehicle_data.iterrows():
                lat, lon = row['Latitude'], row['Longitude']

                if current_lat is not None and current_lon is not None:
                    distance = haversine_distance(current_lat, current_lon, lat, lon)
                    if distance <= 50:
                        current_group.append(row)

                        current_lat = sum(r['Latitude'] for r in current_group) / len(current_group)
                        current_lon = sum(r['Longitude'] for r in current_group) / len(current_group)
                    else:

                        if len(current_group) >= 2:
                            start_time = current_group[0]['DateTime']
                            end_time = current_group[-1]['DateTime']
                            duration_minutes = (end_time - start_time).total_seconds() / 60
                            
                            if duration_minutes >= 2:
                                avg_lat = sum(r['Latitude'] for r in current_group) / len(current_group)
                                avg_lon = sum(r['Longitude'] for r in current_group) / len(current_group)
                                
                                stop_points.append({
                                    'Vehicle No': vehicle_id,
                                    'Latitude': avg_lat,
                                    'Longitude': avg_lon,
                                    'StartTime': start_time,
                                    'EndTime': end_time,
                                    'DurationMinutes': round(duration_minutes, 1),
                                    'Status': current_group[0]['Status'],
                                    'Address': current_group[0]['Address']
                                })

                        current_group = [row]
                        current_lat = lat
                        current_lon = lon
                else:
                    current_group = [row]
                    current_lat = lat
                    current_lon = lon

            if len(current_group) >= 2:
                start_time = current_group[0]['DateTime']
                end_time = current_group[-1]['DateTime']
                duration_minutes = (end_time - start_time).total_seconds() / 60
                
                if duration_minutes >= 2:
                    avg_lat = sum(r['Latitude'] for r in current_group) / len(current_group)
                    avg_lon = sum(r['Longitude'] for r in current_group) / len(current_group)
                    
                    stop_points.append({
                        'Vehicle No': vehicle_id,
                        'Latitude': avg_lat,
                        'Longitude': avg_lon,
                        'StartTime': start_time,
                        'EndTime': end_time,
                        'DurationMinutes': round(duration_minutes, 1),
                        'Status': current_group[0]['Status'],
                        'Address': current_group[0]['Address']
                    })
        
        result_df = pd.DataFrame(stop_points)
        print(f"Extracted {len(result_df)} stop points")
        return result_df
        
    except Exception as e:
        print(f"Error extracting stop points: {e}")
        return pd.DataFrame()

def get_geolocator():
    global _geolocator
    if _geolocator is None:
        _geolocator = Nominatim(user_agent="vehicle_tracker_app_1.0")
    return _geolocator

def get_address_from_coords(lat, lon):
    try:
        time.sleep(1)
        geolocator = get_geolocator()
        location = geolocator.reverse(f"{lat}, {lon}", exactly_one=True, timeout=10, language='en')
        if location and location.address:
            address = location.address
            if address and ',' in address:
                parts = address.split(',')
                if len(parts) > 2:
                    formatted_parts = parts[:3]
                    return ', '.join(formatted_parts).strip()
                else:
                    return address
            return address
        else:
            return "Unspecified Location"
    except Exception as e:
        print(f"Error getting address for {lat}, {lon}: {e}")
        return "Unspecified Location"

def get_available_vehicles():
    vehicles = set()
    
    for csv_path in [f"{TRAVEL_REPORT_DIR}/current.csv", f"{TRAVEL_REPORT_DIR}/history.csv"]:
        if os.path.exists(csv_path):
            df = pd.read_csv(
                csv_path,
                dtype={
                    'Vehicle No': str,
                    'Status': str,
                    'Odometer': float,
                    'Address': str,
                    'Panic': str,
                    'Latitude':float,
                    'Longitude':float
                },
                parse_dates=['DateTime']
            )
            if "Vehicle No" in df.columns:
                vehicles.update(df["Vehicle No"].astype(str).unique())
    
    return sorted(list(vehicles))
def get_appropriate_csv_path(date,csv_path_current,csv_path_past):
    try:
        uae_today = (datetime.now(UTC) + timedelta(hours=4)).date()
        target_date = datetime.strptime(date, "%Y-%m-%d").date()
        return csv_path_current if target_date >= uae_today else csv_path_past
    except:
        return csv_path_current
def get_unified_edits_df():
    if os.path.exists(EDITS_CSV_FILE):
        return pd.read_csv(
            EDITS_CSV_FILE,
            dtype={
                'customer_id': str,
                'latitude': float,
                'longitude': float,
                'vehicle_id': str,
                'weekday': str,
                'customer_name': str,
                'customer_contact': str,
                'description': str
            }
        )
    else:
        return pd.DataFrame(columns=[
            'customer_id', 'latitude', 'longitude', 'vehicle_id',
            'weekday', 'customer_name', 'customer_contact', 'description'
        ])

def load_and_process_customer_points(customer_cache_point):
    customer_points = pd.read_csv(
        customer_cache_point,
        dtype={
            'Vehicle No': str,
            'GeoCluster': int,
            'Weekday': str,
            'Address': str,
            'StopCount': int,
            'Latitude': float,
            'Longitude': float
        },
        parse_dates=['FirstVisit', 'LastVisit']
    )
    
    customer_points['customer_id'] = None
    
    if os.path.exists(EDITS_CSV_FILE):
        edits_df = get_unified_edits_df()
        
        for _, edit_row in edits_df.iterrows():
            mask = (
                (abs(customer_points['Latitude'] - edit_row['latitude']) < 0.000001) &
                (abs(customer_points['Longitude'] - edit_row['longitude']) < 0.000001) &
                (customer_points['Vehicle No'].astype(str) == str(edit_row['vehicle_id'])) &
                (customer_points['Weekday'] == edit_row['weekday'])
            )
            
            if mask.any():
                customer_points.loc[mask, 'customer_id'] = edit_row['customer_id']
            else:
                new_point = pd.DataFrame([{
                    'Vehicle No': edit_row['vehicle_id'],
                    'Latitude': edit_row['latitude'],
                    'Longitude': edit_row['longitude'],
                    'Weekday': edit_row['weekday'],
                    'GeoCluster': -1,
                    'StopCount': 1,
                    'Address': edit_row.get('description', 'Custom Customer Point'),
                    'FirstVisit': pd.NaT,
                    'LastVisit': pd.NaT,
                    'customer_id': edit_row['customer_id']
                }])
                customer_points = pd.concat([customer_points, new_point], ignore_index=True)
    
    return customer_points

def render_customer_points_to_map(map_object, filtered_df, vehicle_colors, show_edits_button=True):
    edits_df = get_unified_edits_df() if os.path.exists(EDITS_CSV_FILE) else pd.DataFrame()
    
    for _, row in filtered_df.iterrows():
        vehicle_no = row['Vehicle No']
        alias = load_vehicle_aliases().get(str(vehicle_no), str(vehicle_no))
        weekday = row['Weekday']
        color = vehicle_colors.get((vehicle_no, weekday)) or vehicle_colors.get(vehicle_no, 'gray')

        customer_id = row.get('customer_id')
        is_custom = pd.notna(customer_id)
        
        customer_name = ''
        customer_contact = ''
        display_address = row.get('Address', 'Unspecified Location')
        
        if is_custom and not edits_df.empty:
            customer_data = edits_df[edits_df['customer_id'] == customer_id]
            if not customer_data.empty:
                customer_record = customer_data.iloc[0]
                customer_name = customer_record.get('customer_name', '')
                customer_contact = customer_record.get('customer_contact', '')
                display_address = customer_record.get('description', display_address)

        if is_custom:
            icon_type = "custom_point"
            edit_button_class = "edit-custom-point-btn"
            remove_button_class = "remove-custom-point-btn"
            edit_button_text = "Edit Customer"
            remove_button_text = "Remove Customer"
        else:
            icon_type = "customer_point"
            edit_button_class = "edit-point-btn"
            remove_button_class = "remove-point-btn"
            edit_button_text = "Edit Point"
            remove_button_text = "Remove Point"

        popup_content = f"""
        <div style="font-family: Arial; font-size: 12px; min-width: 250px;">
            {f'<b>Customer ID : {customer_id}<br>' if customer_id else ''}
            <b>Vehicle:</b> {alias} ({vehicle_no})<br>
            {f'<b>Customer:</b> {customer_name}<br>' if customer_name else ''}
            {f'<b>Contact:</b> {customer_contact}<br>' if customer_contact else ''}
            {'<b>Cluster:</b> ' + str(row['GeoCluster']) + '<br>' if not is_custom else ''}
            <b>Day:</b> {row['Weekday']}<br>
            <b>Stop Count:</b> {row['StopCount']}<br>
            {'<b>First Visit:</b> ' + str(row.get('FirstVisit', 'N/A')) + '<br>' if not is_custom else ''}
            {'<b>Last Visit:</b> ' + str(row.get('LastVisit', 'N/A')) + '<br>' if not is_custom else ''}
            <b>Coordinates:</b> {row['Latitude']:.6f}, {row['Longitude']:.6f}<br>
            <b>Map Views:</b><br>
            <a href="https://www.google.com/maps?q={row['Latitude']:.6f},{row['Longitude']:.6f}" target="_blank">🗺️ View Location on 2D Map</a><br>
            <a href="https://www.google.com/maps/@?api=1&map_action=pano&viewpoint={row['Latitude']:.6f},{row['Longitude']:.6f}" target="_blank">🌐 Explore in Street View (Geolocator)</a><br>
            <b>Address:</b> {display_address}<br>
            {('<b><i class="fas fa-star"></i> Custom Point</b><br>' if is_custom else '')}
            <hr style="margin: 10px 0;">
            <div style="display: flex; gap: 8px; flex-wrap: wrap;">
                <button class="{edit_button_class}" 
                        data-customer-id="{customer_id if customer_id else ''}"
                        data-lat="{row['Latitude']}" 
                        data-lon="{row['Longitude']}" 
                        data-vehicle="{vehicle_no}" 
                        data-weekday="{row['Weekday']}"
                        data-name="{customer_name}"
                        data-contact="{customer_contact}"
                        data-description="{display_address}"
                        style="background: #007bff; color: white; border: none; padding: 5px 10px; border-radius: 4px; cursor: pointer; font-size: 11px; flex: 1; {('display: none;' if not show_edits_button else '')}">
                    <i class="fas fa-edit"></i> {edit_button_text}
                </button>
                <button class="{remove_button_class}" 
                        data-customer-id="{customer_id if customer_id else ''}"
                        data-lat="{row['Latitude']}" 
                        data-lon="{row['Longitude']}" 
                        data-vehicle="{vehicle_no}" 
                        data-weekday="{row['Weekday']}"
                        style="background: #dc3545; color: white; border: none; padding: 5px 10px; border-radius: 4px; cursor: pointer; font-size: 11px; flex: 1; {('display: none;' if not show_edits_button else '')}">
                    <i class="fas fa-trash"></i> {remove_button_text}
                </button>
            </div>
        </div>
        """
        
        tooltip_content = f"{alias} - {row['StopCount']} stops - {row['Weekday']}"
        if customer_name:
            tooltip_content = f"{customer_name} - {alias} - {row['Weekday']}"

        icon_html = create_pin_marker(color, size=18, opacity=0.8, marker_type=icon_type)
        
        folium.Marker(
            [row['Latitude'], row['Longitude']],
            popup=folium.Popup(popup_content, max_width=350),
            tooltip=folium.Tooltip(tooltip_content, permanent=False),
            icon=folium.DivIcon(
                html=icon_html,
                icon_size=(20, 20),
                icon_anchor=(10, 20)
            )
        ).add_to(map_object)
def render_vehicle_paths_to_map(map_object, assign_paths_addr, selected_weekdays, vehicle_colors):
    if all(isinstance(k, tuple) and len(k) == 2 for k in vehicle_colors.keys()):
        selected_vehicle_ids = {veh for (veh, _) in vehicle_colors.keys()}
    else:
        selected_vehicle_ids = set(vehicle_colors.keys())

    if assign_paths_addr and os.path.exists(assign_paths_addr):
        with open(assign_paths_addr, encoding="utf-8") as f:
            geojson_data = json.load(f)

        for feature in geojson_data["features"]:
            props = feature["properties"]
            veh = props.get("vehicle_id")
            day = props.get("weekday")

            if veh in selected_vehicle_ids and day in selected_weekdays:
                coords = feature["geometry"]["coordinates"]
                coords_latlon = [(lat, lon) for lon, lat in coords]

                if (veh, day) in vehicle_colors:
                    route_color = vehicle_colors[(veh, day)]
                else:
                    route_color = vehicle_colors.get(veh, 'blue')

                alias = load_vehicle_aliases().get(str(veh), str(veh))

                street_names = props.get('ordered_street_names', [])
                street_count = len(street_names)
                streets_preview = ', '.join(street_names[:3])
                if street_count > 3:
                    streets_preview += f"... (+{street_count - 3} more)"

                tooltip_content = f"{alias} - {day} - {props.get('total_distance_km', 0)} km"

                popup_html = f"""
                <div style="font-family: Arial; font-size: 12px; min-width: 250px;">
                    <b>Vehicle:</b> {alias} ({veh})<br>
                    <b>Day:</b> {day}<br>
                    <b>Distance:</b> {props.get('total_distance_km', 0)} km<br>
                    <b>Streets ({street_count}):</b><br>
                    <span style="font-size: 11px;">{streets_preview}</span>
                </div>
                """
                folium.PolyLine(
                    coords_latlon,
                    color=route_color,
                    weight=4,
                    opacity=0.8,
                    popup=folium.Popup(popup_html, max_width=300),
                    tooltip=folium.Tooltip(tooltip_content, permanent=False)
                ).add_to(map_object)
def create_map(vehicle_ids, weekdays, customer_points, assign_paths_addr=None,
               date=None, csv_path_current=None, csv_path_past=None):
    customer_points_df = load_and_process_customer_points(customer_points)
    if customer_points_df.empty:
        m = folium.Map(location=[25.276987, 55.296249], zoom_start=10)
        folium.Marker(
            [25.276987, 55.296249],
            popup="No data available. Please process data first.",
            icon=folium.Icon(color='red', icon='info-sign')
        ).add_to(m)
        return m, {}

    vehicle_ids = [str(vid.strip()) for vid in vehicle_ids if vid.strip()]
    weekdays = [day.strip() for day in weekdays if day.strip()]
    filtered_df = customer_points_df[
        (customer_points_df['Vehicle No'].isin(vehicle_ids)) &
        (customer_points_df['Weekday'].isin(weekdays))
    ]

    if filtered_df.empty:
        m = folium.Map(location=[25.276987, 55.276987], zoom_start=10)
        folium.Marker(
            [25.276987, 55.296249],
            popup="No data found for the selected filters.",
            icon=folium.Icon(color='red', icon='info-sign')
        ).add_to(m)
        return m, {}

    center_lat = filtered_df['Latitude'].mean()
    center_lon = filtered_df['Longitude'].mean()
    m = folium.Map(location=[center_lat, center_lon], zoom_start=12)

    colors = ['red', 'blue', 'green', 'purple', 'orange', 'darkred',
              'lightred', 'beige', 'darkblue', 'darkgreen', 'cadetblue',
              'darkpurple', 'pink', 'lightblue', 'lightgreen', 'gray', 'black']

    if len(vehicle_ids) == 1 and len(weekdays) > 1:
        vehicle_colors = {
            (vehicle_ids[0], day): colors[i % len(colors)] for i, day in enumerate(weekdays)
        }
    else:
        vehicle_colors = {
            vehicle: colors[i % len(colors)] for i, vehicle in enumerate(vehicle_ids)
        }
    if date:
        csv_path = get_appropriate_csv_path(date, csv_path_current, csv_path_past)
        add_stop_points_to_map(m, vehicle_ids, csv_path, date, vehicle_colors)
    render_customer_points_to_map(m, filtered_df, vehicle_colors)
    render_vehicle_paths_to_map(m, assign_paths_addr, weekdays, vehicle_colors)

    return m, vehicle_colors
def calculate_customer_points_analysis(vehicle_ids, customer_points, csv_path, date_val, t_start, t_end, vehicle_colors):
    analysis_results = {}
    
    if customer_points is None or customer_points.empty:
        for vehicle_id in vehicle_ids:
            analysis_results[str(vehicle_id)] = {
                'total_customer_points': 0,
                'visited_customer_points': 0,
                'unvisited_customer_points': 0,
                'visit_percentage': 0.0
            }
        return analysis_results

    weekday = pd.to_datetime(date_val).day_name()
    vehicle_customer_points = customer_points[
        (customer_points['Vehicle No'].isin([str(v) for v in vehicle_ids])) &
        (customer_points['Weekday'] == weekday)
    ]

    stop_points_df = extract_stop_points(csv_path, vehicle_ids, t_start, t_end)
    
    for vehicle_id in vehicle_ids:
        vehicle_id_str = str(vehicle_id)

        vehicle_cust_points = vehicle_customer_points[
            vehicle_customer_points['Vehicle No'] == vehicle_id_str
        ]
        
        total_customer_points = len(vehicle_cust_points)
        visited_count = 0
        
        if total_customer_points > 0 and not stop_points_df.empty:
            vehicle_stops = stop_points_df[stop_points_df['Vehicle No'] == vehicle_id_str]
            
            if not vehicle_stops.empty:
                for _, cust_point in vehicle_cust_points.iterrows():
                    cust_lat = cust_point['Latitude']
                    cust_lon = cust_point['Longitude']

                    is_visited = False
                    for _, stop_point in vehicle_stops.iterrows():
                        stop_lat = stop_point['Latitude']
                        stop_lon = stop_point['Longitude']

                        distance = haversine_distance(cust_lat, cust_lon, stop_lat, stop_lon)
                        
                        if distance <= 500:
                            is_visited = True
                            break
                    
                    if is_visited:
                        visited_count += 1
        
        unvisited_count = total_customer_points - visited_count
        visit_percentage = (visited_count / total_customer_points * 100) if total_customer_points > 0 else 0.0
        
        analysis_results[vehicle_id_str] = {
            'total_customer_points': total_customer_points,
            'visited_customer_points': visited_count,
            'unvisited_customer_points': unvisited_count,
            'visit_percentage': round(visit_percentage, 1)
        }
    
    return analysis_results

def generate_route_comparison(
    vehicle_ids,
    csv_path_current,
    csv_path_past,
    geojson_path, 
    date_current,
    t_start_current,
    t_end_current,
    date_past=None,
    t_start_past=None,
    t_end_past=None,
    generate_map=True
):

    colors = ['red', 'blue', 'green', 'purple', 'orange', 'darkred',
              'lightred', 'beige', 'darkblue', 'darkgreen', 'cadetblue',
              'darkpurple', 'pink', 'lightblue', 'lightgreen', 'gray', 'black']
    vehicle_colors = {vehicle_id:colors[i%len(colors)] for i,vehicle_id in enumerate(vehicle_ids)}
    comparison_data = {}

    if not os.path.exists(geojson_path):
        return None, {"error": "GeoJSON file not found"}
    
    try:
        with open(geojson_path, "r", encoding="utf-8") as f:
            geojson_data = json.load(f)
    except Exception:
        return None, {"error": "Failed to load GeoJSON file"}

    customer_points = None
    customer_cache_point = load_settings().get('customer_points_path', '')
    if customer_cache_point and os.path.exists(customer_cache_point):
        try:
            customer_points = load_and_process_customer_points(customer_cache_point)
        except Exception:
            customer_points = None

    m = None
    all_coords = []
    valid_routes_exist = False

    for label, csv_path, date_val, t_start, t_end in [
        ("Current", csv_path_current, date_current, t_start_current, t_end_current),
        ("Past", csv_path_past, date_past, t_start_past, t_end_past)
    ]:
        if not all([csv_path, date_val, t_start, t_end]):
            continue

        try:
            weekday = datetime.strptime(date_val, "%Y-%m-%d").strftime("%A")
        except Exception:
            continue

        for vehicle_id in vehicle_ids:
            alias = load_vehicle_aliases().get(str(vehicle_id), str(vehicle_id))
            color = vehicle_colors[str(vehicle_id)]

            try:
                actual_coords = load_actual_route(csv_path, vehicle_id, t_start, t_end)
            except Exception as e:
                print(f"FAILED TO LOAD actual_coords : {e}")
                actual_coords = None

            if date_past is None:
                try:
                    planned_coords = load_geojson_route(geojson_data, vehicle_id, weekday)
                except Exception:
                    planned_coords = None
                comparison_type = "vs Planned"
            else:
                planned_coords = None
                comparison_type = "vs Past Actual"

            comparison_data[f"{vehicle_id}_{label}"] = {
                "vehicle_id": vehicle_id,
                "alias": alias,
                "label": label,
                "date": datetime.strptime(date_val, "%Y-%m-%d").strftime("%B %d, %Y"),
                "weekday": weekday,
                "color": color,
                "comparison_type": comparison_type,
                "actual_distance": 0,
                "actual_coords": actual_coords if actual_coords else []
            }

            if actual_coords and len(actual_coords) > 1:
                valid_routes_exist = True
                try:
                    total_dist = sum(
                        haversine_distance(actual_coords[i-1][1], actual_coords[i-1][0], 
                                         actual_coords[i][1], actual_coords[i][0])
                        for i in range(1, len(actual_coords))
                    )
                    comparison_data[f"{vehicle_id}_{label}"]["actual_distance"] = round(total_dist/1000, 2)

                    if generate_map:
                        actual_latlon = [[lat, lon] for lon, lat in actual_coords]
                        all_coords.extend(actual_latlon)

                        if not m:
                            if all_coords:
                                center_lat = sum(coord[0] for coord in all_coords) / len(all_coords)
                                center_lon = sum(coord[1] for coord in all_coords) / len(all_coords)
                            else:
                                center_lat = 25.276987
                                center_lon = 55.296249
                            m = folium.Map(location=[center_lat, center_lon], zoom_start=12)

                        route_style = {"weight": 5, "opacity": 0.9} if label == "Current" else {"weight": 4, "opacity": 0.7, "dash_array": "5,10"}
                        
                        actual_popup = f"""
                        <div style="font-family: Arial; min-width: 200px;">
                            <h4 style="margin: 0; color: {color};">{alias} - {label} Route</h4>
                            <hr style="margin: 5px 0;">
                            <b>Date:</b> {comparison_data[f"{vehicle_id}_{label}"]["date"]}<br>
                            <b>Day:</b> {weekday}<br>
                            <b>Distance:</b> {round(total_dist/1000, 2)} km<br>
                            <b>Route Type:</b> Actual Path<br>
                        </div>
                        """

                        folium.PolyLine(
                            actual_latlon, 
                            color=color,
                            popup=folium.Popup(actual_popup, max_width=300),
                            tooltip=f"{alias} {label} - {round(total_dist/1000, 1)}km",
                            **route_style
                        ).add_to(m)

                        start_popup = f"""
                        <div style="font-family: Arial;">
                            <h4 style="margin: 0; color: {color};">{alias} - {label} Start</h4>
                            <b>Date:</b> {comparison_data[f"{vehicle_id}_{label}"]["date"]}<br>
                            <b>Time:</b> {t_start.split()[1] if ' ' in t_start else t_start}<br>
                            <b>Coords:</b> {actual_latlon[0][0]:.6f}, {actual_latlon[0][1]:.6f}<br>
                            <b>Map Views:</b><br>
                            <a href="https://www.google.com/maps?q={actual_latlon[0][0]:.6f},{actual_latlon[0][1]:.6f}" target="_blank">🗺️ View Location on 2D Map</a><br>
                            <a href="https://www.google.com/maps/@?api=1&map_action=pano&viewpoint={actual_latlon[0][0]:.6f},{actual_latlon[0][1]:.6f}" target="_blank">🌐 Explore in Street View (Geolocator)</a><br>
                        </div>
                        """

                        folium.Marker(
                            actual_latlon[0], 
                            icon=folium.DivIcon(
                                html=f'<div style="background-color: {color}; width: 12px; height: 12px; border-radius: 50%; border: 2px solid white; box-shadow: 0 0 4px rgba(0,0,0,0.5);"></div>',
                                icon_size=(12, 12),
                                icon_anchor=(6, 6)
                            ),
                            popup=folium.Popup(start_popup, max_width=250),
                            tooltip=f"{alias} {label} Start - {t_start.split()[1] if ' ' in t_start else t_start}"
                        ).add_to(m)

                        end_popup = f"""
                        <div style="font-family: Arial;">
                            <h4 style="margin: 0; color: {color};">{alias} - {label} End</h4>
                            <b>Date:</b> {comparison_data[f"{vehicle_id}_{label}"]["date"]}<br>
                            <b>Time:</b> {t_end.split()[1] if ' ' in t_end else t_end}<br>
                            <b>Coords:</b> {actual_latlon[-1][0]:.6f}, {actual_latlon[-1][1]:.6f}<br>
                            <b>Map Views:</b><br>
                            <a href="https://www.google.com/maps?q={actual_latlon[-1][0]:.6f},{actual_latlon[-1][1]:.6f}" target="_blank">🗺️ View Location on 2D Map</a><br>
                            <a href="https://www.google.com/maps/@?api=1&map_action=pano&viewpoint={actual_latlon[-1][0]:.6f},{actual_latlon[-1][1]:.6f}" target="_blank">🌐 Explore in Street View (Geolocator)</a><br>
                        </div>
                        """
                        
                        folium.Marker(
                            actual_latlon[-1], 
                            icon=folium.DivIcon(
                                html=f'<div style="background-color: {color}; width: 12px; height: 12px; border-radius: 2px; border: 2px solid white; box-shadow: 0 0 4px rgba(0,0,0,0.5);"></div>',
                                icon_size=(12, 12),
                                icon_anchor=(6, 6)
                            ),
                            popup=folium.Popup(end_popup, max_width=250),
                            tooltip=f"{alias} {label} End - {t_end.split()[1] if ' ' in t_end else t_end}"
                        ).add_to(m)
                except Exception:
                    pass

            if generate_map and planned_coords and len(planned_coords) > 1 and date_past is None:
                try:
                    planned_latlon = [[lat, lon] for lon, lat in planned_coords]
                    all_coords.extend(planned_latlon)
                    
                    planned_dist = sum(
                        haversine_distance(planned_coords[i-1][1], planned_coords[i-1][0], 
                                         planned_coords[i][1], planned_coords[i][0])
                        for i in range(1, len(planned_coords))
                    )

                    planned_popup = f"""
                    <div style="font-family: Arial; min-width: 200px;">
                        <h4 style="margin: 0; color: {color};">{alias} - Planned Route</h4>
                        <hr style="margin: 5px 0;">
                        <b>Day:</b> {weekday}<br>
                        <b>Distance:</b> {round(planned_dist/1000, 2)} km<br>
                        <b>Route Type:</b> Optimal Plan<br>
                    </div>
                    """
                    folium.PolyLine(
                        planned_latlon, 
                        color=color, 
                        weight=3,
                        opacity=0.7,
                        dash_array="8,12",
                        popup=folium.Popup(planned_popup, max_width=300),
                        tooltip=f"{alias} Planned - {round(planned_dist/1000, 1)}km"
                    ).add_to(m)
                except Exception:
                    pass

    if date_past is not None:
        try:
            project = Transformer.from_crs("epsg:4326", "epsg:3857", always_xy=True).transform
            
            for vehicle_id in vehicle_ids:
                current_key = f"{vehicle_id}_Current"
                past_key = f"{vehicle_id}_Past"
                
                if current_key in comparison_data and past_key in comparison_data:
                    current_coords = comparison_data[current_key].get("actual_coords", [])
                    past_coords = comparison_data[past_key].get("actual_coords", [])

                    comparison_data[current_key].update({
                        "maximum_route_deviation": 0,
                        "planned_route_coverage": 0,
                        "actual_route_alignment": 0,
                        "compared_distance": comparison_data[past_key]["actual_distance"]
                    })

                    comparison_data[past_key].update({
                        "maximum_route_deviation": 0,
                        "planned_route_coverage": 0,
                        "actual_route_alignment": 0,
                        "compared_distance": comparison_data[current_key]["actual_distance"]
                    })

                    if current_coords and past_coords and len(current_coords) > 1 and len(past_coords) > 1:
                        try:
                            current_proj = [project(lon, lat) for lon, lat in current_coords]
                            past_proj = [project(lon, lat) for lon, lat in past_coords]
                            
                            if len(current_proj) > 1 and len(past_proj) > 1:
                                A = np.array(current_proj)
                                B = np.array(past_proj)

                                hausdorff = min(directed_hausdorff(A, B)[0], directed_hausdorff(B, A)[0])
                                overlap = compute_path_overlap(current_coords, past_coords)
                                reverse_overlap = compute_path_overlap(past_coords, current_coords)

                                comparison_data[current_key].update({
                                    "maximum_route_deviation": round(hausdorff, 1),
                                    "planned_route_coverage": round(overlap, 1),
                                    "actual_route_alignment": round(reverse_overlap, 1)
                                })
                                
                                comparison_data[past_key].update({
                                    "maximum_route_deviation": round(hausdorff, 1),
                                    "planned_route_coverage": round(reverse_overlap, 1),
                                    "actual_route_alignment": round(overlap, 1)
                                })
                        except Exception:
                            pass
        except Exception:
            pass
    else:
        try:
            project = Transformer.from_crs("epsg:4326", "epsg:3857", always_xy=True).transform
            
            for vehicle_id in vehicle_ids:
                current_key = f"{vehicle_id}_Current"
                if current_key in comparison_data:
                    weekday = comparison_data[current_key]["weekday"]
                    
                    try:
                        planned_coords = load_geojson_route(geojson_data, vehicle_id, weekday)
                    except Exception:
                        planned_coords = None
                    
                    actual_coords = comparison_data[current_key].get("actual_coords", [])

                    planned_distance = 0
                    if planned_coords and len(planned_coords) > 1:
                        try:
                            planned_distance = sum(
                                haversine_distance(planned_coords[i-1][1], planned_coords[i-1][0], 
                                                 planned_coords[i][1], planned_coords[i][0])
                                for i in range(1, len(planned_coords))
                            )
                            planned_distance = round(planned_distance/1000, 2)
                        except Exception:
                            planned_distance = 0
                    
                    comparison_data[current_key]["planned_distance"] = planned_distance

                    if actual_coords and planned_coords and len(actual_coords) > 1 and len(planned_coords) > 1:
                        try:
                            actual_proj = [project(lon, lat) for lon, lat in actual_coords]
                            planned_proj = [project(lon, lat) for lon, lat in planned_coords]
                            
                            if len(actual_proj) > 1 and len(planned_proj) > 1:
                                A = np.array(actual_proj)
                                B = np.array(planned_proj)

                                hausdorff = min(directed_hausdorff(A, B)[0], directed_hausdorff(B, A)[0])
                                overlap = compute_path_overlap(actual_coords, planned_coords)
                                reverse_overlap = compute_path_overlap(planned_coords, actual_coords)

                                comparison_data[current_key].update({
                                    "maximum_route_deviation": round(hausdorff, 1),
                                    "planned_route_coverage": round(overlap, 1),
                                    "actual_route_alignment": round(reverse_overlap, 1)
                                })
                            else:
                                comparison_data[current_key].update({
                                    "maximum_route_deviation": 0,
                                    "planned_route_coverage": 0,
                                    "actual_route_alignment": 0
                                })
                        except Exception:
                            comparison_data[current_key].update({
                                "maximum_route_deviation": 0,
                                "planned_route_coverage": 0,
                                "actual_route_alignment": 0
                            })
                    else:
                        comparison_data[current_key].update({
                            "maximum_route_deviation": 0,
                            "planned_route_coverage": 0,
                            "actual_route_alignment": 0
                        })
        except Exception:
            pass

    if generate_map and customer_points is not None and not customer_points.empty:
        try:
            filter_date = date_past if date_past is not None else date_current
            
            vehicle_customer_points = customer_points[
                (customer_points['Vehicle No'].isin([str(v) for v in vehicle_ids])) &
                (customer_points['Weekday'] == pd.to_datetime(filter_date).day_name())
            ]
            render_customer_points_to_map(m,vehicle_customer_points,vehicle_colors,show_edits_button = False)
        except Exception:
            pass

    if generate_map:
        try:
            add_stop_points_to_map(m, vehicle_ids, csv_path_current,date_current, vehicle_colors)
            add_stop_points_to_map(m, vehicle_ids, csv_path_past, date_past,vehicle_colors)
        except Exception as e:
            print(f"Encountered error in add_stop_points_to_map function in generate_routes_comparison as {e}")

    try:
        current_analysis = calculate_customer_points_analysis(
            vehicle_ids, customer_points, csv_path_current,
            date_current, t_start_current, t_end_current, vehicle_colors
        )
        for vehicle_id in vehicle_ids:
            current_key = f"{vehicle_id}_Current"
            if current_key in comparison_data:
                vehicle_id_str = str(vehicle_id)
                if vehicle_id_str in current_analysis:
                    comparison_data[current_key].update({
                        'total_customer_points': current_analysis[vehicle_id_str]['total_customer_points'],
                        'visited_customer_points': current_analysis[vehicle_id_str]['visited_customer_points'],
                        'unvisited_customer_points': current_analysis[vehicle_id_str]['unvisited_customer_points'],
                        'visit_percentage': current_analysis[vehicle_id_str]['visit_percentage']
                    })
    except Exception:
        pass

    if date_past is not None and csv_path_past and t_start_past and t_end_past:
        try:
            past_analysis = calculate_customer_points_analysis(
                vehicle_ids, customer_points, csv_path_past, 
                date_past, t_start_past, t_end_past, vehicle_colors
            )

            for vehicle_id in vehicle_ids:
                past_key = f"{vehicle_id}_Past"
                if past_key in comparison_data:
                    vehicle_id_str = str(vehicle_id)
                    if vehicle_id_str in past_analysis:
                        comparison_data[past_key].update({
                            'total_customer_points': past_analysis[vehicle_id_str]['total_customer_points'],
                            'visited_customer_points': past_analysis[vehicle_id_str]['visited_customer_points'],
                            'unvisited_customer_points': past_analysis[vehicle_id_str]['unvisited_customer_points'],
                            'visit_percentage': past_analysis[vehicle_id_str]['visit_percentage']
                        })
        except Exception:
            pass

    for key in comparison_data:
        if "actual_coords" in comparison_data[key]:
            del comparison_data[key]["actual_coords"]

    if generate_map:
        if not m or not valid_routes_exist:
            m = folium.Map(location=[25.276987, 55.296249], zoom_start=12)
            folium.Marker(
                [25.276987, 55.296249],
                popup=folium.Popup("No route data available for the selected criteria", max_width=300),
                icon=folium.Icon(color='red', icon='info-sign')
            ).add_to(m)
        
    else:
        map_html = None
    if generate_map:
        whatsapp_customers, whatsapp_stats = load_whatsapp_customer_data()
        for customer in whatsapp_customers:
            print(f"Assigning customer to map{customer}")
            whatsapp_icon = folium.DivIcon(
                html=f'''
                <div style="
                    background: linear-gradient(45deg, #25D366, #128C7E);
                    width: 20px;
                    height: 20px;
                    border-radius: 50%;
                    border: 3px solid white;
                    display: flex;
                    align-items: center;
                    justify-content: center;
                    box-shadow: 0 2px 8px rgba(0,0,0,0.3);
                ">
                    <i class="fab fa-whatsapp" style="color: white; font-size: 10px;"></i>
                </div>
                ''',
                icon_size=(26, 26),
                icon_anchor=(13, 13)
            )

            popup_html = f'''
            <div style="font-family: Arial; font-size: 12px; min-width: 250px;">
                <h4 style="margin: 0 0 10px 0; color: #25D366;">
                    <i class="fab fa-whatsapp"></i> WhatsApp Customer
                </h4>
                <b>Name:</b> {customer['customer_name']}<br>
                <b>Contact:</b> {customer['contact']}<br>
                <b>Day:</b> {customer['weekday']}<br>
                <b>Location Received:</b> {customer['location_received_at']}<br>
                <b>Coordinates:</b> {customer['latitude']:.6f}, {customer['longitude']:.6f}<br>
                <b>Map Views:</b><br>
                <a href="https://www.google.com/maps?q={customer['latitude']:.6f},{customer['longitude']:.6f}" target="_blank">🗺️ View Location on 2D Map</a><br>
                <a href="https://www.google.com/maps/@?api=1&map_action=pano&viewpoint={customer['latitude']:.6f},{customer['longitude']:.6f}" target="_blank">🌐 Explore in Street View (Geolocator)</a><br>
                <b>Address:</b> {customer['location_description'] or 'WhatsApp Location'}<br>
                <b>Source:</b> OxyPlus WhatsApp Bot<br>
            </div>
            '''
            
            folium.Marker(
                location=[customer['latitude'], customer['longitude']],
                popup=folium.Popup(popup_html, max_width=350),
                tooltip=f"{customer['customer_name']} - WhatsApp - {customer['weekday']}",
                icon=whatsapp_icon
            ).add_to(m)
        map_html = m._repr_html_()
    return map_html, comparison_data

def generate_routes(csv_path, geojson_path, key=None):
    try:
        if not os.path.exists(csv_path):
            print(f"CSV file not found: {csv_path}")
            return None
            
        print(f"Loading data from: {csv_path}")
        '''
        df = pd.read_csv(
            csv_path,
            dtype={
                'Vehicle No': str,
                'GeoCluster': int,
                'Weekday': str,
                'Address': str,
                'StopCount': int,
                'Latitude':float,
                'Longitude':float
            },
            parse_dates=['FirstVisit', 'LastVisit']
        )
        '''
        df = load_and_process_customer_points(csv_path)
        if df.empty:
            print("CSV file is empty")
            return None
            
        print(f"Loaded {len(df)} records")

        oxy_rows = df[df["Address"] == "Oxy Office"]
        if oxy_rows.empty:
            print("No Address Oxy Office found in data.")
            return None
            
        oxy_row = oxy_rows.iloc[0]
        oxy_coords = (oxy_row["Latitude"], oxy_row["Longitude"])
        oxy_coord_lonlat = (oxy_row["Longitude"], oxy_row["Latitude"])
        print(f"Oxy office coordinates: {oxy_coords}")

        df["DistanceFromStartKM"] = df.apply(
            lambda row: distance.distance((row["Latitude"], row["Longitude"]), oxy_coords).km,
            axis=1
        )

        grouped = df.groupby(["Vehicle No", "Weekday"])
        total_groups = len(grouped)
        print(f"Processing {total_groups} vehicle-day combinations")
        
        try:
            client = openrouteservice.Client(key=key)
        except Exception as e:
            print(f"Failed to initialize ORS client: {e}")
            return None
            
        features = []
        group_count = 0

        def snap_and_validate_coordinates(coords_list, api_key, profile="driving-car"):
            if not coords_list:
                return [], []
            
            try:
                url = f'https://api.openrouteservice.org/v2/snap/{profile}'
                
                payload = {
                    "locations": coords_list,
                    "radius": 350
                }
                
                headers = {
                    'Authorization': api_key,
                    'Content-Type': 'application/json'
                }
                
                response = requests.post(url, json=payload, headers=headers)
                
                if response.status_code != 200:
                    raise Exception(f"Snap API failed ({response.status_code}): {response.text}")
                
                data = response.json()
                locations = data.get('locations', [])
                
                valid_coords = []
                invalid_indices = []
                
                for i, location in enumerate(locations):
                    if location is not None and 'location' in location:
                        valid_coords.append(location['location'])
                    else:
                        invalid_indices.append(i)
                        print(f"WARNING: Coordinate {i} ({coords_list[i]}) could not be snapped to road network")
                
                if invalid_indices:
                    print(f"INFO: {len(invalid_indices)} out of {len(coords_list)} coordinates were invalid")
                
                return valid_coords, invalid_indices
                
            except Exception as e:
                print(f"ERROR: Snap endpoint failed: {e}")
                return coords_list, []

        for (veh_id, weekday), group in grouped:
            group_count += 1
            print(f"PROCESSING: Group {group_count}/{total_groups}: Vehicle {veh_id}, {weekday}")
            
            sorted_group = group.sort_values("DistanceFromStartKM")
            coords = [(row["Longitude"], row["Latitude"]) for _, row in sorted_group.iterrows()]
            veh_id_str = str(veh_id)
            full_coords = [oxy_coord_lonlat] + coords + [oxy_coord_lonlat]
            
            print(f"   INFO: Total waypoints before validation: {len(full_coords)}")

            valid_coords, invalid_indices = snap_and_validate_coordinates(full_coords, key)
            
            if len(valid_coords) < 2:
                print(f"   ERROR: Not enough valid coordinates for routing ({len(valid_coords)} valid)")
                continue
                
            print(f"   INFO: Valid waypoints after snapping: {len(valid_coords)}")

            all_street_names = []
            total_distance = 0
            all_coordinates = []
            chunk_count = 0

            for i in range(0, len(valid_coords), 50):
                chunk = valid_coords[i:i + 50]
                if len(chunk) < 2:
                    continue
                
                chunk_count += 1
                max_retries = 3
                retry_count = 0

                while retry_count < max_retries:
                    try:
                        route = client.directions(chunk, profile="driving-car", format="geojson", instructions=True)

                        if not route.get("features") or not route["features"]:
                            print(f"   WARNING: Empty route response for chunk {chunk_count}")
                            break
                            
                        feature_geo = route["features"][0]["geometry"]["coordinates"]
                        props = route["features"][0]["properties"]

                        if not props.get("summary"):
                            print(f"   WARNING: No summary in route response for chunk {chunk_count}")
                            break
                            
                        summary = props["summary"]
                        chunk_distance = summary.get("distance", 0) / 1000
                        total_distance += chunk_distance

                        if feature_geo:
                            all_coordinates.extend(feature_geo)

                        segments = props.get("segments", [])
                        if segments:
                            steps = segments[0].get("steps", [])
                            chunk_street_names = []
                            for step in steps:
                                name = step.get("name", "").strip()
                                if name and name != "-" and name not in ["Destination", "Start"]:
                                    chunk_street_names.append(name)
                            all_street_names.extend(chunk_street_names)

                        print(f"SUCCESS: Chunk {chunk_count}: {chunk_distance:.2f} km | {len(chunk_street_names) if 'chunk_street_names' in locals() else 0} streets")
                        break
                        
                    except Exception as e:
                        retry_count += 1
                        print(f"   WARNING: Chunk {chunk_count} attempt {retry_count} failed: {e}")
                        if retry_count >= max_retries:
                            print(f"   ERROR: Chunk {chunk_count} failed after {max_retries} attempts")
                        time.sleep(4)

                time.sleep(1)

            ordered_street_names = [k for k, _ in groupby(all_street_names)]
            final_street_names = []
            seen = set()
            for name in ordered_street_names:
                if name not in seen:
                    final_street_names.append(name)
                    seen.add(name)

            if all_coordinates:
                feature = {
                    "type": "Feature",
                    "properties": {
                        "vehicle_id": veh_id_str,
                        "weekday": weekday,
                        "total_distance_km": round(total_distance, 2),
                        "ordered_street_names": final_street_names,
                        "street_count": len(final_street_names),
                        "original_waypoints": len(full_coords),
                        "valid_waypoints": len(valid_coords),
                        "invalid_waypoints": len(invalid_indices)
                    },
                    "geometry": {
                        "type": "LineString",
                        "coordinates": all_coordinates
                    }
                }
                features.append(feature)
                print(f"   COMPLETED: Route: {total_distance:.2f} km | {len(final_street_names)} unique streets")
            else:
                print(f"   WARNING: No coordinates collected for {veh_id_str} {weekday}")

        if not features:
            print("ERROR: No routes were generated")
            return None

        os.makedirs(os.path.dirname(geojson_path), exist_ok=True)
        
        geojson_obj = {
            "type": "FeatureCollection",
            "features": features
        }

        with open(geojson_path, "w", encoding="utf-8") as f:
            json.dump(geojson_obj, f, indent=2, ensure_ascii=False)

        print(f"SUCCESS: GeoJSON saved to: {geojson_path}")
        print(f"INFO: Total routes generated: {len(features)}")
        
        return geojson_path
        
    except Exception as e:
        print(f"ERROR: Function failed with exception: {e}")
        return None

def process_customer_data(min_duration_minutes=4, min_stop_count=5, segment_areas=False):
    raw_file_path = f'{CUSTOMER_POINTS_DIR}/idlepoints.csv'
    if os.path.exists(raw_file_path):
        df_raw = pd.read_csv(
            raw_file_path,
            dtype={
                'Vehicle No': str,
                'GeoCluster': int,
            },
            parse_dates=['Date', 'StartTime', 'EndTime']
        )
        df_raw['Duration'] = pd.to_timedelta(df_raw['Duration'], errors='coerce')
        print(f"Loaded {len(df_raw)} raw idle points")

    filtered_df = df_raw[df_raw['Duration'] >= pd.Timedelta(minutes=min_duration_minutes)].copy()
    print(f"After duration filter ({min_duration_minutes} min): {len(filtered_df)} records")

    
    filtered_df['Weekday'] = filtered_df['Date'].dt.strftime('%A')
    print(filtered_df['StartTime'].dtype)

    grouped = filtered_df.groupby(['Vehicle No', 'GeoCluster', 'Weekday'])
    summary = grouped.agg(
        StopCount=('GeoCluster', 'size'),
        Latitude=('Latitude', 'mean'),
        Longitude=('Longitude', 'mean'),
        Address=('Address', 'first'),
        FirstVisit=('StartTime', 'min'),
        LastVisit=('StartTime', 'max')
    ).reset_index()
    
    print(f"After groupby aggregation: {len(summary)} records")

    summary['FirstVisit'] = pd.to_datetime(summary['FirstVisit'], errors='coerce').dt.strftime('%Y-%m-%d %H:%M:%S')
    summary['LastVisit'] = pd.to_datetime(summary['LastVisit'], errors='coerce').dt.strftime('%Y-%m-%d %H:%M:%S')

    frequent_stops = summary[summary['StopCount'] >= min_stop_count].reset_index(drop=True)
    print(f"After stop count filter ({min_stop_count} stops): {len(frequent_stops)} records")

    frequent_stops = frequent_stops.dropna(subset=['Vehicle No', 'Latitude', 'Longitude'])
    print(f"After dropna: {len(frequent_stops)} records")
    
    if 'Vehicle No' in frequent_stops.columns:
        frequent_stops['Vehicle No'] = frequent_stops['Vehicle No'].astype(str).str.strip()
    if 'Weekday' in frequent_stops.columns:
        frequent_stops['Weekday'] = frequent_stops['Weekday'].astype(str).str.strip()

    if segment_areas:
        frequent_stops = segment_vehicle_areas(frequent_stops)

    return frequent_stops

def segment_vehicle_areas(df):
    clustering_data = df[df['Weekday'] != 'Friday'].copy()
    
    if clustering_data.empty:
        return df

    clustering_data['Vehicle No'] = clustering_data['Vehicle No'].astype(str)
    
    unique_coords = clustering_data.groupby(['Latitude', 'Longitude']).agg({
        'Vehicle No': 'nunique',
        'StopCount': 'sum'
    }).reset_index()

    n_vehicles = clustering_data['Vehicle No'].nunique()
    n_clusters = min(n_vehicles, len(unique_coords))
    
    if n_clusters <= 1:
        return df
    
    coords = unique_coords[['Latitude', 'Longitude']].values
    weights = unique_coords['StopCount'].values

    kmeans = KMeans(n_clusters=n_clusters, random_state=42, n_init=10)

    weighted_coords = []
    for i, (lat, lng, weight) in enumerate(zip(coords[:, 0], coords[:, 1], weights)):
        repeat_count = min(int(weight), 50)
        for _ in range(max(1, repeat_count)):
            weighted_coords.append([lat, lng])
    
    weighted_coords = np.array(weighted_coords)
    cluster_labels = kmeans.fit_predict(weighted_coords)

    cluster_centers = kmeans.cluster_centers_
    original_clusters = []
    
    for lat, lng in coords:
        distances = np.sqrt(np.sum((cluster_centers - [lat, lng])**2, axis=1))
        closest_cluster = np.argmin(distances)
        original_clusters.append(closest_cluster)
    
    unique_coords['AreaCluster'] = original_clusters

    clustering_data = clustering_data.merge(
        unique_coords[['Latitude', 'Longitude', 'AreaCluster']], 
        on=['Latitude', 'Longitude'], 
        how='left'
    )

    cluster_vehicle_mapping = {}
    for cluster_id in clustering_data['AreaCluster'].unique():
        if pd.isna(cluster_id):
            continue
        cluster_data = clustering_data[clustering_data['AreaCluster'] == cluster_id]
        vehicle_votes = cluster_data.groupby('Vehicle No')['StopCount'].sum()
        winning_vehicle = vehicle_votes.idxmax()
        cluster_vehicle_mapping[cluster_id] = winning_vehicle

    coord_to_vehicle = {}
    for cluster_id, assigned_vehicle in cluster_vehicle_mapping.items():
        cluster_coords = unique_coords[unique_coords['AreaCluster'] == cluster_id]
        for _, row in cluster_coords.iterrows():
            coord_key = (row['Latitude'], row['Longitude'])
            coord_to_vehicle[coord_key] = assigned_vehicle
    
    def assign_vehicle_to_coord(row):
        coord_key = (row['Latitude'], row['Longitude'])
        return coord_to_vehicle.get(coord_key, row['Vehicle No'])
    
    result_df = df.copy()

    result_df['Vehicle No'] = result_df['Vehicle No'].astype(str)
    result_df['Original_Vehicle'] = result_df['Vehicle No']
    result_df['Vehicle No'] = result_df.apply(assign_vehicle_to_coord, axis=1)

    result_df = result_df.drop('Original_Vehicle', axis=1)
    
    if 'AreaCluster' in result_df.columns:
        result_df = result_df.drop('AreaCluster', axis=1)
    
    return result_df

class VehicleDataWrapper:
    def __init__(self, base_path: str = ".", config_path: str = "config_data"):
        self.base_path = base_path
        self.config_path = config_path
        self.data_sources = {
            "data/idlereport": {"current": None, "history": None},
            "data/exidlereport": {"current": None, "history": None},
            "data/travelreport": {"current": None, "history": None},
            "data/geofence": {"current": None, "history": None},
            "data/driverperformance": {"current": None, "history": None}
        }
        self._vehicle_aliases = None
        self._customer_info = None
        self._alert_logs = None
        
        self.load_all_data()
        self.load_customer_info()
        self.load_alert_logs()
    
    def load_config(self, config_file: str) -> Dict:
        config_path = os.path.join(self.config_path, config_file)
        try:
            with open(config_path, 'r', encoding='utf-8') as f:
                return json.load(f)
        except FileNotFoundError:
            print(f"Config file not found: {config_path}")
            return {}
        except json.JSONDecodeError:
            print(f"Invalid JSON in config file: {config_path}")
            return {}
    
    def load_customer_info(self):
        customer_file = "analysis/customerinfo/customerinfo.csv"
        try:
            if os.path.exists(customer_file):
                self._customer_info = pd.read_csv(customer_file, dtype={
                    'customer_id': str,
                    'latitude': float,
                    'longitude': float,
                    'vehicle_id': str,
                    'weekday': str,
                    'customer_name': str,
                    'customer_contact': str,
                    'description': str
                })
                print(f"Loaded {len(self._customer_info)} customer records")
            else:
                print(f"Customer info file not found: {customer_file}")
                self._customer_info = pd.DataFrame()
        except Exception as e:
            print(f"Error loading customer info: {str(e)}")
            self._customer_info = pd.DataFrame()
    
    def load_alert_logs(self):
        alert_files = {
            "alert_logs": "alerts/alert_logs.json",
            "driver_violations": "alerts/driver_violations.json", 
            "route_deviation_logs": "alerts/route_deviation_logs.json",
            "sent_alerts": "alerts/sent_alerts.json"
        }
        
        self._alert_logs = {}
        for log_type, file_path in alert_files.items():
            try:
                if os.path.exists(file_path):
                    with open(file_path, 'r', encoding='utf-8') as f:
                        self._alert_logs[log_type] = json.load(f)
                        print(f"Loaded {log_type} from {file_path}")
                else:
                    self._alert_logs[log_type] = {}
                    print(f"Alert file not found: {file_path}")
            except Exception as e:
                print(f"Error loading {log_type}: {str(e)}")
                self._alert_logs[log_type] = {}
    
    def load_all_data(self):
        for source in self.data_sources.keys():
            for time_type in ["current", "history"]:
                file_path = os.path.join(self.base_path, source, f"{time_type}.csv")
                try:
                    if os.path.exists(file_path):
                        df = pd.read_csv(
                            file_path,
                            low_memory = False
                        )
                        df = self._parse_datetime_columns(df, source)
                        self.data_sources[source][time_type] = df
                        print(f"Loaded {len(df)} records from {source}/{time_type}")
                    else:
                        print(f"File not found: {file_path}")
                except Exception as e:
                    print(f"Error loading {file_path}: {str(e)}")
                    self.data_sources[source][time_type] = None
    
    def _parse_datetime_columns(self, df: pd.DataFrame, source: str) -> pd.DataFrame:
        datetime_columns = {
            "data/travelreport": ["DateTime"],
            "data/idlereport": ["Idle From", "Idle Till"],
            "data/exidlereport": ["Idle From", "Idle Till"],
            "data/geofence": ["In Time", "Out Time"],
            "data/driverperformance": ["Login Time", "Logout Time"]
        }
        
        if source in datetime_columns:
            for col in datetime_columns[source]:
                if col in df.columns:
                    print(f"DEBUG: Converting {col} to datetime for {source}")
                    df[col] = pd.to_datetime(df[col], errors='coerce')
                    print(f"DEBUG: {col} dtype after conversion: {df[col].dtype}")
        
        return df
    
    def get_filtered_alerts(self, start_date: datetime, end_date: datetime, 
                           vehicle_nos: Optional[List[str]] = None,
                           driver_names: Optional[List[str]] = None) -> Dict:
        filtered_alerts = {}
        
        if not self._alert_logs:
            return filtered_alerts
        
        start_str = start_date.strftime('%Y-%m-%d')
        end_str = end_date.strftime('%Y-%m-%d')
        
        if "alert_logs" in self._alert_logs and "daily_logs" in self._alert_logs["alert_logs"]:
            daily_logs = self._alert_logs["alert_logs"]["daily_logs"]
            
            for date_str, alerts in daily_logs.items():
                if start_str <= date_str <= end_str:
                    filtered_day_alerts = []
                    
                    for alert in alerts:
                        include_alert = True
                        
                        if vehicle_nos and alert.get("vehicle_id"):
                            if str(alert["vehicle_id"]) not in [str(v) for v in vehicle_nos]:
                                include_alert = False
                        
                        if driver_names and alert.get("driver_name"):
                            if alert["driver_name"] not in driver_names:
                                include_alert = False
                        
                        if include_alert:
                            filtered_day_alerts.append(alert)
                    
                    if filtered_day_alerts:
                        filtered_alerts[date_str] = filtered_day_alerts
        
        return filtered_alerts
    
    def get_customer_info_for_vehicles(self, vehicle_nos: Optional[List[str]] = None) -> pd.DataFrame:
        if self._customer_info is None or self._customer_info.empty:
            return pd.DataFrame()
        
        if vehicle_nos:
            vehicle_str_list = [str(v) for v in vehicle_nos]
            return self._customer_info[self._customer_info['vehicle_id'].isin(vehicle_str_list)]
        
        return self._customer_info
    
    def get_filtered_data(self, start_date: datetime, end_date: datetime, 
                         vehicle_no: Optional[str] = None, 
                         reports: Optional[List[str]] = None) -> Dict:
        self.reload_data()
        filtered_data = {}
        
        if reports:
            report_mapping = {
                "idlereport": "data/idlereport",
                "exidlereport": "data/exidlereport", 
                "travelreport": "data/travelreport",
                "geofence": "data/geofence",
                "driverperformance": "data/driverperformance"
            }
            sources_to_process = [report_mapping.get(r, r) for r in reports if report_mapping.get(r, r) in self.data_sources]
        else:
            sources_to_process = list(self.data_sources.keys())
        
        for source in sources_to_process:
            if source not in self.data_sources:
                print(f"Unknown source: {source}")
                continue
                
            for time_type in ["current", "history"]:
                df = self.data_sources[source][time_type]
                if df is None or df.empty:
                    continue
                
                datetime_col_map = {
                    "data/travelreport": "DateTime",
                    "data/idlereport": "Idle From",
                    "data/exidlereport": "Idle From",
                    "data/geofence": "In Time",
                    "data/driverperformance": "Login Time"
                }
                
                datetime_col = datetime_col_map.get(source)
                if not datetime_col or datetime_col not in df.columns:
                    print(f"DEBUG: {source} - Datetime column '{datetime_col}' not found in columns: {list(df.columns)}")
                    continue
                
                # Additional datetime conversion just in case
                if df[datetime_col].dtype == 'object':
                    print(f"DEBUG: {source} - Converting {datetime_col} from object to datetime")
                    df[datetime_col] = pd.to_datetime(df[datetime_col], errors='coerce')
                
                print(f"DEBUG: {source} - Using datetime column: {datetime_col} (dtype: {df[datetime_col].dtype})")
                
                try:
                    mask = (df[datetime_col] >= start_date) & (df[datetime_col] <= end_date)
                    filtered_df = df[mask].copy()
                    
                    print(f"DEBUG: {source} - Before date filter: {len(df)} records, After: {len(filtered_df)} records")
                    
                    # Ensure vehicle IDs are treated as strings
                    if vehicle_no:
                        vehicle_col = self._get_vehicle_column(filtered_df)
                        if vehicle_col:
                            before_vehicle_filter = len(filtered_df)
                            # Get available vehicles before filtering
                            available_vehicles = df[vehicle_col].astype(str).str.strip().unique() if len(df) > 0 else []
                            # Convert both sides to strings for comparison
                            filtered_df = filtered_df[
                                filtered_df[vehicle_col].astype(str).str.strip() == str(vehicle_no).strip()
                            ]
                            print(f"DEBUG: {source} - Vehicle filter ({vehicle_col}={vehicle_no}): {before_vehicle_filter} -> {len(filtered_df)} records")
                            if len(filtered_df) == 0 and before_vehicle_filter > 0:
                                print(f"DEBUG: {source} - Available vehicles: {available_vehicles}")
                        else:
                            print(f"DEBUG: {source} - No vehicle column found in: {list(filtered_df.columns)}")
                    
                    if source == "data/travelreport":
                        if 'Status' in filtered_df.columns:
                            before_status_filter = len(filtered_df)
                            filtered_df = filtered_df[filtered_df['Status'] != 'Moving']
                            print(f"DEBUG: {source} - Status filter (!=Moving): {before_status_filter} -> {len(filtered_df)} records")
                    
                    if not filtered_df.empty:
                        key = f"{source}_{time_type}"
                        filtered_data[key] = filtered_df
                        print(f"Filtered {source}_{time_type}: {len(filtered_df)} records")
                
                except Exception as e:
                    print(f"Error filtering {source}_{time_type}: {str(e)}")
                    continue
        
        return filtered_data
    
    def _get_vehicle_column(self, df: pd.DataFrame) -> Optional[str]:
        possible_columns = ["Vehicle No", "Vehicle Number", "VehicleNo", "VehicleNumber", "No of Vehicles"]
        for col in possible_columns:
            if col in df.columns:
                return col
        return None
    
    def get_aggregated_data(self, start_date: datetime, end_date: datetime, 
                           vehicle_nos: Optional[List[str]] = None,
                           reports: Optional[List[str]] = None) -> Dict:
        if not vehicle_nos:
            return self.get_filtered_data(start_date, end_date, reports=reports)

        all_data = {}
        
        for vehicle_no in vehicle_nos:
            vehicle_data = self.get_filtered_data(start_date, end_date, vehicle_no, reports)
            
            for key, df in vehicle_data.items():
                if key not in all_data:
                    all_data[key] = []
                all_data[key].append(df)

        combined_data = {}
        for key, df_list in all_data.items():
            if df_list:
                try:
                    combined_df = pd.concat(df_list, ignore_index=True)
                    source = key.split('_')[0]
                    datetime_col_map = {
                        "data/travelreport": "DateTime",
                        "data/idlereport": "Idle From",
                        "data/exidlereport": "Idle From",
                        "data/geofence": "In Time",
                        "data/driverperformance": "Login Time"
                    }
                    
                    datetime_col = datetime_col_map.get(source)
                    if datetime_col and datetime_col in combined_df.columns:
                        combined_df = combined_df.sort_values(datetime_col)
                    
                    combined_data[key] = combined_df
                    print(f"Combined {key}: {len(combined_df)} total records")
                    
                except Exception as e:
                    print(f"Error combining {key}: {str(e)}")
                    continue
        
        return combined_data
    
    def get_data_summary(self) -> Dict:
        summary = {}
        
        for source in self.data_sources.keys():
            summary[source] = {}
            for time_type in ["current", "history"]:
                df = self.data_sources[source][time_type]
                if df is not None:
                    summary[source][time_type] = {
                        "records": len(df),
                        "columns": list(df.columns),
                        "date_range": self._get_date_range(df, source)
                    }
                else:
                    summary[source][time_type] = {"records": 0, "columns": [], "date_range": None}
        
        return summary
    
    def _get_date_range(self, df: pd.DataFrame, source: str) -> Optional[Dict]:
        datetime_col_map = {
            "data/travelreport": "DateTime",
            "data/idlereport": "Idle From",
            "data/exidlereport": "Idle From",
            "data/geofence": "In Time",
            "data/driverperformance": "Login Time"
        }
        
        datetime_col = datetime_col_map.get(source)
        if datetime_col and datetime_col in df.columns:
            try:
                return {
                    "start": df[datetime_col].min(),
                    "end": df[datetime_col].max()
                }
            except:
                return None
        return None
    
    def reload_data(self):
        print("DEBUG: Reloading all data...")
        self._vehicle_aliases = None
        self._customer_info = None
        self._alert_logs = None
        self.load_all_data()
        self.load_customer_info()
        self.load_alert_logs()
        print("DEBUG: Data reload completed")

class VehicleRAGSystem:
    def __init__(self, data_wrapper: VehicleDataWrapper, gemini_api_key: str, 
                 model_name: str = "models/gemini-2.5-flash-preview-04-17-thinking"):
        self.data_wrapper = data_wrapper
        self.gemini_api_key = gemini_api_key
        self.model_name = model_name
        self.max_context_size = 32000
        self.conversations = {}
        
        genai.configure(api_key=gemini_api_key)
        
        self.llm = ChatGoogleGenerativeAI(
            model=model_name,
            google_api_key=gemini_api_key,
            temperature=0.3,
            max_retries = 0
        )
    
    def update_model(self, model_name: str):
        self.model_name = model_name
        self.llm = ChatGoogleGenerativeAI(
            model=model_name,
            google_api_key=self.gemini_api_key,
            temperature=0.3,
            max_retries = 0
        )
    
    def update_api_key(self, api_key: str):
        self.gemini_api_key = api_key
        genai.configure(api_key=api_key)
        self.llm = ChatGoogleGenerativeAI(
            model=self.model_name,
            google_api_key=api_key,
            temperature=0.3
        )
    
    def get_conversation_history(self, session_id: str) -> List[Dict]:
        if session_id not in self.conversations:
            self.conversations[session_id] = []
        return self.conversations[session_id]
    
    def add_to_conversation(self, session_id: str, user_message: str, assistant_response: str):
        if session_id not in self.conversations:
            self.conversations[session_id] = []
        
        self.conversations[session_id].append({
            "user": user_message,
            "assistant": assistant_response,
            "timestamp": datetime.now()
        })
    
    def clear_conversation(self, session_id: str = None):
        if session_id:
            if session_id in self.conversations:
                del self.conversations[session_id]
        else:
            self.conversations.clear()
    
    def clear_all_conversations(self):
        self.conversations.clear()
        print(f"Cleared all conversations at {datetime.now()}")
    
    def _prepare_limited_context(self, filtered_data: Dict, alerts_data: Dict, customer_data: pd.DataFrame, 
                                vehicle_no: str = None, max_records: int = 50) -> str:
        current_time = datetime.now()
        context = f"CURRENT TIME: {current_time.strftime('%Y-%m-%d %H:%M:%S')}\n\n"
        
        # Helper function to safely get string values from pandas rows
        def safe_get(row, column, default='N/A'):
            value = row.get(column, default)
            if pd.isna(value):
                return default
            return str(value)
        
        if not customer_data.empty:
            context += "=== CUSTOMER ASSIGNMENTS ===\n"
            for _, row in customer_data.head(20).iterrows():
                context += f"V{row['vehicle_id']}: {row['customer_name']} ({row['weekday']}) - {row['description'][:50]}\n"
            context += "\n"
        
        if alerts_data:
            context += "=== ALERTS RECEIVED ===\n"
            alert_count = 0
            for date_str, daily_alerts in alerts_data.items():
                for alert in daily_alerts:
                    if alert_count >= 20:
                        break
                    alert_type = alert.get('alert_type', 'UNKNOWN')
                    vehicle_id = alert.get('vehicle_id', 'N/A')
                    driver_name = alert.get('driver_name', 'N/A')
                    timestamp = alert.get('timestamp', 'N/A')
                    recipient = alert.get('recipient_name', 'Admin')
                    
                    context += f"{timestamp}: {alert_type} - V{vehicle_id} ({driver_name}) -> {recipient}\n"
                    alert_count += 1
                if alert_count >= 20:
                    break
            context += "\n"
        
        total_records = 0
        
        for source_key, df in filtered_data.items():
            if total_records >= max_records:
                break
                
            source, time_type = source_key.split('_')
            available_records = min(len(df), max_records - total_records)
            
            # Convert back to display format
            display_source = source.replace('data/', '')
            context += f"=== {display_source.upper()} ({time_type}) - {available_records} records ===\n"
            
            df_sample = df.head(available_records)
            
            if source == "data/travelreport":
                for idx, row in df_sample.iterrows():
                    vehicle_num = safe_get(row, 'Vehicle No')
                    alias = load_vehicle_aliases().get(str(vehicle_num), str(vehicle_num))
                    status = safe_get(row, 'Status')
                    address = safe_get(row, 'Address')[:50]
                    datetime_val = safe_get(row, 'DateTime')
                    context += f"V{vehicle_num}({alias}) {status} at {address} | {datetime_val}\n"
            
            elif source == "data/idlereport":
                for idx, row in df_sample.iterrows():
                    vehicle_num = safe_get(row, 'Vehicle Number')
                    alias = load_vehicle_aliases().get(str(vehicle_num), str(vehicle_num))
                    location = safe_get(row, 'Location')[:30]
                    idle_from = safe_get(row, 'Idle From')
                    idle_till = safe_get(row, 'Idle Till')
                    duration = safe_get(row, 'Duration')
                    context += f"V{vehicle_num}({alias}) idle at {location} | {idle_from} to {idle_till} | {duration}\n"
            
            elif source == "data/exidlereport":
                for idx, row in df_sample.iterrows():
                    vehicle_num = safe_get(row, 'Vehicle Number')
                    alias = load_vehicle_aliases().get(str(vehicle_num), str(vehicle_num))
                    location = safe_get(row, 'Location')[:30]
                    duration = safe_get(row, 'Duration')
                    context += f"V{vehicle_num}({alias}) excessive idle at {location} | {duration}\n"
            
            elif source == "data/driverperformance":
                for idx, row in df_sample.iterrows():
                    driver = safe_get(row, 'Driver')
                    km = safe_get(row, 'KM')
                    harsh_break = safe_get(row, 'Harsh Break')
                    harsh_accel = safe_get(row, 'Harsh Acceleration')
                    over_speed = safe_get(row, 'Over Speed')
                    context += f"Driver {driver} | {km}km | HB:{harsh_break} HA:{harsh_accel} OS:{over_speed}\n"
            
            elif source == "data/geofence":
                for idx, row in df_sample.iterrows():
                    vehicle_num = safe_get(row, 'Vehicle No')
                    alias = load_vehicle_aliases().get(str(vehicle_num), str(vehicle_num))
                    geofence = safe_get(row, 'Geofence')
                    in_time = safe_get(row, 'In Time')
                    out_time = safe_get(row, 'Out Time')
                    context += f"V{vehicle_num}({alias}) {geofence} | In:{in_time} Out:{out_time}\n"
            
            context += "\n"
            total_records += available_records
        
        if total_records == 0 and not alerts_data and customer_data.empty:
            context += "No data found for the specified criteria.\n"
        
        return context
    
    def _generate_structured_query(self, user_query: str) -> str:
        current_time = datetime.now()
        with open(DRIVER_NAMES,'r',encoding = 'utf-8') as f:
            vehicle_list_str = f.read()
        query_generation_prompt = f"""Current Time: {current_time.strftime('%Y-%m-%d %H:%M:%S')}
        Available Vehicles: {vehicle_list_str}
        User Query: {user_query}

        You must analyze the user query and return a **structured response** in exactly one of the following two formats:

        **1. Direct Answer Format**  
        Use this when the user's query can be answered without needing to look up data.  
        Return:
        {{{{  
        "type": "answer",  
        "text": "Your direct answer here."  
        }}}}

        **2. Data Query Format**  
        Use this when the query requires checking vehicle activity, reports, alerts, or data filtering.  
        Return:
        {{{{  
        "type": "query",  
        "start": "YYYY-MM-DD 08:00:00",  
        "end": "YYYY-MM-DD 22:00:00",  
        "ids": [list of vehicle numbers as strings],  
        "reports": [list of relevant reports],
        "include_alerts": true/false,
        "include_customers": true/false  
        }}}}

        **General Rules**  
        1. The time range must be from YYYY-MM-HH 08:00:00 to YYYY-MM-HH 22:00:00 by default.  
        2. The date range must be within **one calendar day only**.  
        3. If the query specifies no vehicle, assume all vehicles `"ids"` and make a full list [vehicle_id1 .. vehicle_idn].  
        4. Use today's date unless if any time not mentioned, basically current time, else figure out the desired date. 
        5. The `"reports"` field must only contain relevant sources from: ["idlereport", "exidlereport", "travelreport", "driverperformance", "geofence"]  
        6. Set `"include_alerts": true` for queries about alerts, violations, warnings, or notifications
        7. Set `"include_customers": true` for queries about deliveries, customer visits, or assignments
        8. The output must always be valid **JSON** (use double quotes).  
        9. Vehicle IDs must be strings and must match the available vehicles exactly.
        10. No explanatory text—return only the JSON object.

        **Examples**

        - *"What alerts did driver X receive today?"* →  
        {{{{  
        "type": "query",  
        "start": "{current_time.strftime('%Y-%m-%d')} 08:00:00",  
        "end": "{current_time.strftime('%Y-%m-%d')} 22:00:00",  
        "ids": ["123"],  
        "reports": ["driverperformance"],
        "include_alerts": true,
        "include_customers": false  
        }}}}

        - *"Which customers did vehicle 123 visit yesterday?"* →  
        {{{{  
        "type": "query",  
        "start": "{(current_time - timedelta(days=1)).strftime('%Y-%m-%d')} 08:00:00",  
        "end": "{(current_time - timedelta(days=1)).strftime('%Y-%m-%d')} 22:00:00",  
        "ids": ["123"],  
        "reports": ["travelreport", "idlereport"],
        "include_alerts": false,
        "include_customers": true  
        }}}}

        - *"Show me all delivery stops and alerts for all vehicles today"* →  
        {{{{  
        "type": "query",  
        "start": "{current_time.strftime('%Y-%m-%d')} 08:00:00",  
        "end": "{current_time.strftime('%Y-%m-%d')} 22:00:00",  
        "ids": [123,456,... all vehicle id's],  
        "reports": ["idlereport", "travelreport"],
        "include_alerts": true,
        "include_customers": true  
        }}}}

        Now generate only one of the two response types above, in **valid JSON** format.
        """
        try:
            response = self.llm.invoke(query_generation_prompt)
            print(f"DEBUG: Generated structured query: {response.content}")
            return response.content
        except Exception as e:
            print(f"DEBUG: Query generation failed: {e}")
            return f'{{"type": "answer", "text": "Sorry, I encountered an error processing your query."}}'
    
    def query(self, question: str, session_id: str = 'default') -> str:
        conversation_history = self.get_conversation_history(session_id)
        
        context_from_history = ""
        if conversation_history:
            context_from_history = "Recent conversation history:\n"
            for entry in conversation_history[-5:]:
                context_from_history += f"User: {entry['user']}\nAssistant: {entry['assistant']}\n\n"
        
        enhanced_question = f"{context_from_history}\nCurrent question: {question}"
        
        structured_query = self._generate_structured_query(enhanced_question)
        
        try:
            start_index = structured_query.find('{')
            end_index = structured_query.rfind('}') + 1
            
            if start_index == -1 or end_index == 0:
                raise ValueError("No valid JSON found in response")
            
            json_part = structured_query[start_index:end_index]
            structured = json.loads(json_part)
            
            if structured.get("type") == "answer":
                response = structured.get("text", "No answer text provided.")
                self.add_to_conversation(session_id, question, response)
                return response
            elif structured.get("type") == "query":
                start_date = datetime.strptime(structured["start"], "%Y-%m-%d %H:%M:%S")
                end_date = datetime.strptime(structured["end"], "%Y-%m-%d %H:%M:%S")
                vehicles = structured.get("ids", [])
                reports = structured.get("reports", [])
                include_alerts = structured.get("include_alerts", False)
                include_customers = structured.get("include_customers", False)
                
                print(f"DEBUG: Parsed query - Date range: {start_date} to {end_date}, Vehicles: {vehicles}, Reports: {reports}, Alerts: {include_alerts}, Customers: {include_customers}")
                
                if vehicles:
                    all_data = self.data_wrapper.get_aggregated_data(
                        start_date=start_date, 
                        end_date=end_date, 
                        vehicle_nos=vehicles, 
                        reports=reports
                    )
                else:
                    all_data = self.data_wrapper.get_filtered_data(
                        start_date=start_date, 
                        end_date=end_date, 
                        reports=reports
                    )
                
                alerts_data = {}
                if include_alerts:
                    alerts_data = self.data_wrapper.get_filtered_alerts(
                        start_date=start_date,
                        end_date=end_date,
                        vehicle_nos=vehicles
                    )
                
                customer_data = pd.DataFrame()
                if include_customers:
                    customer_data = self.data_wrapper.get_customer_info_for_vehicles(vehicles)
                
                print(f"DEBUG: Retrieved data sources: {list(all_data.keys())}, Alerts: {len(alerts_data)}, Customers: {len(customer_data)}")
                
                if not all_data and not alerts_data and customer_data.empty:
                    response = "No data found for the specified time period and criteria."
                    self.add_to_conversation(session_id, question, response)
                    return response

                data_context = self._prepare_limited_context(all_data, alerts_data, customer_data, vehicles, max_records=2000)

                analysis_prompt = f"""Based on the following data, answer the user's question concisely. Make business assumptions about deliveries and driver activities.

                BUSINESS CONTEXT:
                - When drivers stop/idle,under 20 min they are likely making deliveries to customers else be suspicious
                - Idle reports can show both delivery stops and for considering time wastage threshold is 20 min
                - Alert system monitors driver performance and route compliance
                - Each vehicle has assigned customers for specific weekdays
                
                {context_from_history}

                Data Context:
                {data_context}

                User Question: {question}

                Provide a concise business-focused response covering:
                1. Driver delivery activities and customer visits
                2. Alert notifications received (if relevant)
                3. Performance insights and route compliance
                4. Specific patterns or delivery efficiency

                Answer:"""
                        
                try:
                    response = self.llm.invoke(analysis_prompt)
                    final_response = response.content
                    self.add_to_conversation(session_id, question, final_response)
                    return final_response
                except Exception as e:
                    print(f"DEBUG: LLM analysis failed: {e}")
                    error_response = "Sorry, I encountered an error generating the response."
                    self.add_to_conversation(session_id, question, error_response)
                    return error_response
            else:
                raise ValueError("Unknown structured type")
        except Exception as e:
            print(f"DEBUG: Query processing failed: {e}")
            import traceback
            traceback.print_exc()
            response = "Sorry, I encountered an error processing your query."
            self.add_to_conversation(session_id, question, response)
            return response
def initialize_rag_system():
    app_settings = load_settings()
    gemini_api_key = app_settings.get('gemini_api_key')
    if not gemini_api_key:
        print("Warning: No Gemini API key found in settings")
        return None
    data_wrapper = VehicleDataWrapper()
    rag_system = VehicleRAGSystem(data_wrapper, gemini_api_key)
    return rag_system

def schedule_memory_reset():
    def reset_memory():
        global rag_system
        if rag_system:
            rag_system.clear_all_conversations()
    
    schedule.every().hour.do(reset_memory)
    
    while True:
        schedule.run_pending()
        time.sleep(60)

def start_scheduler():
    scheduler_thread = threading.Thread(target=schedule_memory_reset, daemon=True)
    scheduler_thread.start()
def load_phone_numbers():
    with open(PHONE_FILE,'r',encoding = 'utf-8') as f:
        phone_data = json.load(f)
    return phone_data