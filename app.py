from flask import Flask, render_template, request, jsonify,send_file
from flask import session,redirect, url_for, flash,Response
import pandas as pd
import os
import google.generativeai as genai
import requests
import traceback
from google.api_core.exceptions import ResourceExhausted
from urllib.parse import quote_plus, urlencode
from authlib.integrations.flask_client import OAuth
from functools import wraps
import secrets
from dotenv import load_dotenv
import re
from urllib.parse import urlencode
from werkzeug.utils import secure_filename
from flask import Response
import pandas as pd
load_dotenv()
from utils import (
    rag_system,
    _geolocator,
    Nominatim,
    initialize_rag_system,
    start_scheduler,
    load_settings,
    create_map,
    generate_route_comparison,
    DEFAULT_SETTINGS,
    save_settings,
    save_vehicle_aliases,
    load_vehicle_aliases,
    json,
    PHONE_FILE,
    DRIVER_NAMES,
    ROUTES_JSON_DIR,
    CUSTOMER_POINTS_DIR,
    EDITS_CSV_FILE,
    get_available_options,
    process_customer_data,
    generate_routes,
    get_available_vehicles,
    get_appropriate_csv_path,
    get_address_from_coords,
    get_unified_edits_df,
    load_whatsapp_customer_data,
    folium
)

app = Flask(__name__)
app.secret_key = secrets.token_hex(32)
_geolocator = Nominatim(user_agent="vehicle_tracker_app_1.0")
rag_system = initialize_rag_system()

start_scheduler()

def load_credentials():
    try:
        with open('config_data/credentials.json', 'r') as f:
            return json.load(f)
    except FileNotFoundError:
        return {"username": "oxyplusDWS", "password": "oxyplusDWS@2024#"}

AUTH0_CLIENT_ID = os.environ.get('AUTH0_CLIENT_ID', 'your_auth0_client_id')
AUTH0_CLIENT_SECRET = os.environ.get('AUTH0_CLIENT_SECRET', 'your_auth0_client_secret')
AUTH0_DOMAIN = os.environ.get('AUTH0_DOMAIN', 'your_domain.auth0.com')

oauth = OAuth(app)
auth0 = oauth.register(
    'auth0',
    client_id=AUTH0_CLIENT_ID,
    client_secret=AUTH0_CLIENT_SECRET,
    server_metadata_url=f'https://{AUTH0_DOMAIN}/.well-known/openid-configuration',
    client_kwargs={
        'scope': 'openid profile email',
    },
)

def requires_auth(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if 'profile' not in session:
            return redirect(url_for('login'))
        return f(*args, **kwargs)
    return decorated_function

@app.route('/')
def index():
    if 'profile' in session:
        return render_template('dashboard.html', 
                             user=session['profile'],
                             pretty=json.dumps(session['profile'], indent=4))
    return redirect(url_for('login'))

@app.route('/login')
def login():
    if 'profile' in session:
        return redirect(url_for('index'))

    if request.args.get('fallback') == 'true':
        return render_template('fallback_login.html')
    
    return render_template('login.html', auth0_domain=AUTH0_DOMAIN)

@app.route('/auth0-login')
def auth0_login():
    try:
        url_sendy = url_for('callback', _external=True, _scheme='https')
        return auth0.authorize_redirect(
            redirect_uri=url_sendy
        )
    except Exception as e:
        print(f"Auth0 login error: {str(e)}")
        return f"Authentication error: {str(e)}", 500

@app.route('/callback')
def callback():
    token = auth0.authorize_access_token()
    session['profile'] = token['userinfo']
    session['jwt_payload'] = token
    return redirect(url_for('index'))

@app.route('/fallback-login', methods=['POST'])
def fallback_login():
    credentials = load_credentials()
    username = request.form.get('username')
    password = request.form.get('password')
    
    if username == credentials['username'] and password == credentials['password']:
        session['profile'] = {
            'name': 'OxyPlus Admin',
            'email': 'admin@oxypluswater.com',
            'picture': '/static/images/default-avatar.png',
            'sub': 'fallback|admin',
            'auth_method': 'fallback'
        }
        flash('Successfully logged in!', 'success')
        return redirect(url_for('index'))
    else:
        flash('Invalid credentials!', 'error')
        return redirect(url_for('login', fallback='true'))

@app.route('/change-password', methods=['GET', 'POST'])
@requires_auth
def change_password():
    if request.method == 'POST':
        if session['profile'].get('auth_method') == 'fallback':
            current_password = request.form.get('current_password', '').strip()
            new_password = request.form.get('new_password', '').strip()
            confirm_password = request.form.get('confirm_password', '').strip()
            
            if not all([current_password, new_password, confirm_password]):
                flash('All password fields are required!', 'error')
            else:
                try:
                    credentials = load_credentials()
                    
                    if current_password != credentials.get('password', ''):
                        flash('Current password is incorrect!', 'error')
                    elif new_password != confirm_password:
                        flash('New passwords do not match!', 'error')
                    elif len(new_password) < 8:
                        flash('Password must be at least 8 characters long!', 'error')
                    elif not re.search(r'[A-Z]', new_password):
                        flash('Password must contain at least one uppercase letter!', 'error')
                    elif not re.search(r'[a-z]', new_password):
                        flash('Password must contain at least one lowercase letter!', 'error')
                    elif not re.search(r'\d', new_password):
                        flash('Password must contain at least one number!', 'error')
                    elif not re.search(r'[!@#$%^&*(),.?":{}|<>]', new_password):
                        flash('Password must contain at least one special character!', 'error')
                    else:
                        credentials['password'] = new_password
                        os.makedirs('config_data', exist_ok=True)
                        with open('config_data/credentials.json', 'w') as f:
                            json.dump(credentials, f, indent=2)
                        flash('Password changed successfully!', 'success')
                        return redirect(url_for('index'))
                except Exception as e:
                    flash('An error occurred while updating the password. Please try again.', 'error')
        else:
            password_reset_url = f"https://{AUTH0_DOMAIN}/dbconnections/change_password"
            return redirect(f"{password_reset_url}?client_id={AUTH0_CLIENT_ID}&connection=Username-Password-Authentication")
    
    return render_template('change_password.html', 
                         user=session['profile'],
                         auth0_domain=AUTH0_DOMAIN,
                         client_id=AUTH0_CLIENT_ID)

@app.route('/logout')
def logout():
    session.clear()
    return redirect(
        f'https://{AUTH0_DOMAIN}/v2/logout?'
        + urlencode({
            'returnTo': url_for('login', _external=True, _scheme = 'https'),
            'client_id': AUTH0_CLIENT_ID
        }, quote_via=quote_plus)
    )

@app.route('/fallback-logout')
def fallback_logout():
    session.clear()
    flash('Successfully logged out!', 'success')
    return redirect(url_for('login'))

@app.route('/api/vehicle-aliases', methods=['GET', 'POST'])
@requires_auth
def api_vehicle_aliases():
    if request.method == 'GET':
        return jsonify({"aliases": load_vehicle_aliases()})
    
    elif request.method == 'POST':
        try:
            data = request.get_json()
            new_aliases = data.get('aliases', {})

            validated_aliases = {}
            for vehicle_id, alias in new_aliases.items():
                if isinstance(alias, str) and alias.strip():
                    validated_aliases[str(vehicle_id).strip()] = alias.strip()
            
            if save_vehicle_aliases(validated_aliases):
                with open (DRIVER_NAMES,'w',encoding = 'utf-8') as f:
                    json.dump(validated_aliases,f)
                return jsonify({"success": True, "message": "Settings updated successfully"})
            else:
                return jsonify({"success": False, "message": "Failed to save vehicle aliases"}), 500
                
        except Exception as e:
            return jsonify({"success": False, "message": str(e)}), 400

@app.route('/chat')
@requires_auth
def chat_page():
    return render_template('chat.html')

@app.route('/api/gemini-models', methods=['GET'])
@requires_auth
def get_models():
    try:
        app_settings = load_settings()
        api_key = os.getenv("GEMINI_API_KEY") or app_settings.get('gemini_api_key')
        genai.configure(api_key=api_key)
        models = genai.list_models()
        model_data = []
        for model in models:
            if 'generateContent' in model.supported_generation_methods:
                model_name = model.name.lower()
                is_deprecated = any(deprecated in model_name for deprecated in ['gemini-1.5'])
                is_recommended = 'gemini-2.0' in model_name
                model_data.append({
                    "name": model.name,
                    "token_limit": getattr(model, 'input_token_limit', 'N/A'),
                    "is_deprecated": is_deprecated,
                    "is_recommended": is_recommended
                })
        return jsonify({"models": model_data})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/api/chat', methods=['POST'])
@requires_auth
def api_chat():
    global rag_system
    try:
        data = request.get_json()
        message = data.get('message', '')
        provider = data.get('provider', 'gemini').lower()
        selected_model = data.get('model')
        session_id = data.get('session_id', 'default')

        if not message:
            return jsonify({"error": "Message is required"}), 400

        api_key_field = f"{provider}_api_key"
        if not load_settings().get(api_key_field):
            return jsonify({"error": f"{provider.title()} API key not configured"}), 400

        if provider == 'gemini':
            try:
                if not rag_system:
                    rag_system = initialize_rag_system()
                if not rag_system:
                    return jsonify({"error": "RAG system initialization failed"}), 500

                current_api_key = load_settings().get('gemini_api_key')
                if current_api_key != rag_system.gemini_api_key:
                    rag_system.update_api_key(current_api_key)

                if selected_model:
                    rag_system.update_model(selected_model)
                response = rag_system.query(message, session_id)
                return jsonify({"response": response})

            except ResourceExhausted as e:
                return jsonify({
                    "error": "Quota exhausted or Gemini model not available in your current plan.",
                    "details": str(e)
                }), 429

            except Exception as e:
                return jsonify({
                    "error": f"Internal error in Gemini interaction: {str(e)}",
                    "traceback": traceback.format_exc()
                }), 500

        return jsonify({"error": f"Provider '{provider}' is not supported"}), 400

    except Exception as e:
        return jsonify({
            "error": str(e),
            "traceback": traceback.format_exc()
        }), 500

@app.route('/api/edit-original-point', methods=['POST'])
@requires_auth
def api_edit_original_point():
    try:
        data = request.get_json()
        
        if not data.get('customer_id'):
            return jsonify({"success": False, "error": "Customer ID is required"}), 400

        edits_df = get_unified_edits_df()
        
        new_edit = {
            'customer_id': str(data['customer_id']),
            'vehicle_id': str(data['vehicle_id']),
            'weekday': str(data['weekday']),
            'latitude': float(data['latitude']),
            'longitude': float(data['longitude']),
            'customer_contact': data.get('customer_contact', ''),
            'customer_name': data.get('customer_name', ''),
            'description': data.get('description', '')
        }
        
        edits_df = pd.concat([edits_df, pd.DataFrame([new_edit])], ignore_index=True)
        save_unified_edits_df(edits_df)
        
        return jsonify({"success": True, "message": "Original point edited and saved to customer info"})
        
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500
@app.route('/api/remove-original-point', methods=['POST'])
@requires_auth
def api_remove_original_point():
    try:
        data = request.get_json()

        min_duration = int(request.args.get('min_duration', 4))
        min_stop_count = int(request.args.get('min_stop_count', 5))
        segment_areas = request.args.get('segment_areas') == 'on'

        customer_cache_point = f'{CUSTOMER_POINTS_DIR}/cust_{int(segment_areas)}_min{min_duration}_stop{min_stop_count}_points.csv'
        
        if not os.path.exists(customer_cache_point):
            return jsonify({"success": False, "error": "Customer points file not found"}), 404

        df = pd.read_csv(
            customer_cache_point,
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
        mask = (
            (abs(df['Latitude'] - data['latitude']) < 0.000001) &
            (abs(df['Longitude'] - data['longitude']) < 0.000001) &
            (df['Vehicle No'].astype(str) == str(data['vehicle_id'])) &
            (df['Weekday'] == data['weekday'])
        )
        
        if not mask.any():
            return jsonify({"success": False, "error": "Original point not found"}), 404

        df = df[~mask]
        df.to_csv(customer_cache_point, index=False)
        
        return jsonify({"success": True, "message": "Original point permanently deleted"})
        
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500

def get_current_customer_cache_point():
    min_duration = 4
    min_stop_count = 5
    segment_areas = False
    return f'{CUSTOMER_POINTS_DIR}/cust_{int(segment_areas)}_min{min_duration}_stop{min_stop_count}_points.csv'

@app.route('/weekly-customers')
@requires_auth
def weekly_customers():
    min_duration = int(request.args.get('min_duration', 4))
    min_stop_count = int(request.args.get('min_stop_count', 5))
    selected_vehicles = request.args.getlist('vehicles')
    selected_weekdays = request.args.getlist('weekdays')
    segment_areas = request.args.get('segment_areas') == 'on'
    assign_paths = request.args.get('assign_paths') == 'on'
    force_assign_path = request.args.get('force_assign_path') == 'on'
    make_edits = request.args.get('edit_lists') == 'on'
    
    stop_date = request.args.get('stop_date')
    show_stop_points = request.args.get('show_stop_points') == 'on'

    show_confirmed_customers = request.args.get('show_confirmed_customers') == 'on'
    
    customer_cache_point = f'{CUSTOMER_POINTS_DIR}/cust_{int(segment_areas)}_min{min_duration}_stop{min_stop_count}_points.csv'
    customer_cache_paths = f'{ROUTES_JSON_DIR}/path_{int(segment_areas)}_min{min_duration}_stop{min_stop_count}_routes.geojson'
    
    if os.path.exists(customer_cache_point):
        current_processed = pd.read_csv(customer_cache_point,
            dtype = {
                'Vehicle No':str,
                'GeoCluster':int,
                'Weekday':str,
                'Address':str,
                'StopCount':int,
                'Latitude':float,
                'Longitude':float
            },
            parse_dates = ['FirstVisit','LastVisit']
        )
        print('points_found_from_cache')
    else:
        current_processed = process_customer_data(min_duration, min_stop_count, segment_areas)
        current_processed.to_csv(customer_cache_point, index=False)
    
    if assign_paths == False:
        customer_cache_paths = None
    elif force_assign_path or not os.path.exists(customer_cache_paths):
        print('routes_not_found_from_cache or force regeneration enabled, generating...')
        generate_routes(customer_cache_point, customer_cache_paths, load_settings()['ors_api_key'])
    else:
        print('routes_found_from_cache')
    
    csv_path_current = load_settings().get("csv_path_current", "")
    csv_path_past = load_settings().get("csv_path_past", "")

    whatsapp_customers = []
    whatsapp_stats = {
        'total_customers': 0,
        'completed_customers': 0,
        'pending_customers': 0
    }
    
    if show_confirmed_customers:
        print("show_confirmed_customers enabled")
        whatsapp_customers, whatsapp_stats = load_whatsapp_customer_data()

    map_obj, vehicle_colors = create_map(
        selected_vehicles, 
        selected_weekdays, 
        assign_paths_addr=customer_cache_paths,
        customer_points=customer_cache_point,
        date=stop_date if show_stop_points and stop_date else None,
        csv_path_current=csv_path_current,
        csv_path_past=csv_path_past,
    )
    print(f"WHATSAPP CUSTOMERS LOOK LIKE: {whatsapp_customers}")
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
        ).add_to(map_obj)
    map_html = map_obj.get_root().render()
    vehicles = sorted(current_processed['Vehicle No'].astype(str).unique()) if not current_processed.empty else []
    weekdays = ['Saturday','Sunday','Monday','Tuesday','Wednesday','Thursday','Friday']
    total_points = len(current_processed)
    total_vehicles = len(vehicles)
    total_clusters = len(current_processed['GeoCluster'].unique()) if not current_processed.empty else 0
    avg_stops = round(current_processed['StopCount'].mean()) if not current_processed.empty else 0

    if show_confirmed_customers:
        total_points += whatsapp_stats['completed_customers']
    
    return render_template(
        "weekly_customers.html",
        vehicles=vehicles,
        weekdays=weekdays,
        selected_vehicles=selected_vehicles,
        selected_weekdays=selected_weekdays,
        map_html=map_html,
        vehicle_colors=vehicle_colors,
        min_duration=min_duration,
        min_stop_count=min_stop_count,
        total_points=total_points,
        total_vehicles=total_vehicles,
        total_clusters=total_clusters,
        avg_stops=avg_stops,
        segment_areas=segment_areas,
        assign_paths=assign_paths,
        force_assign_path=force_assign_path,
        make_edits=make_edits,
        stop_date=stop_date,
        show_stop_points=show_stop_points,
        show_confirmed_customers=show_confirmed_customers,
        whatsapp_stats=whatsapp_stats,
        vehicle_aliases=load_vehicle_aliases()
    )

@app.route('/daily')
@requires_auth
def daily_route_comparison():
    available_vehicles = get_available_vehicles()
    return render_template('daily.html', vehicles=available_vehicles, vehicle_aliases=load_vehicle_aliases())

@app.route('/api/compare-routes', methods=['POST'])
@requires_auth
def api_compare_routes():
    data = request.get_json()

    vehicle_ids = data.get('vehicle_ids', [])
    date_current = data.get('date_current')
    date_past = data.get('date_past')

    t_start_current = data.get('t_start_current')
    t_end_current = data.get('t_end_current')
    t_start_past = data.get('t_start_past')
    t_end_past = data.get('t_end_past')
    print(date_current)
    print(t_start_current)
    print(t_end_current)
    print(date_past)
    print(t_start_past)
    print(t_end_past)
    if not t_start_current:
        t_start_current = f"{date_current} 00:00:00"
    if not t_end_current:
        t_end_current = f"{date_current} 23:59:59"
    
    if date_past and not t_start_past:
        t_start_past = f"{date_past} 00:00:00"
    if date_past and not t_end_past:
        t_end_past = f"{date_past} 23:59:59"
    
    csv_path_current = load_settings()["csv_path_current"]
    csv_path_past = load_settings()["csv_path_past"]
    geojson_path = load_settings()["geojson_path"]
    print(csv_path_current)
    print(csv_path_past)
    print(geojson_path)

    csv_path_current = get_appropriate_csv_path(date_current, load_settings()["csv_path_current"], load_settings()["csv_path_past"])
    
    map_html, comparison_data = generate_route_comparison(
        vehicle_ids,
        csv_path_current,
        csv_path_past,
        geojson_path,
        date_current,
        t_start_current,
        t_end_current,
        date_past,
        t_start_past,
        t_end_past,
        generate_map = True
    )

    return jsonify({
        "success": True,
        "map_html": map_html,
        "comparison_data": comparison_data,
        "time_ranges": {
            "current": {"start": t_start_current, "end": t_end_current},
            "past": {"start": t_start_past, "end": t_end_past} if date_past else None
        }
    })

@app.route('/api/available-vehicles')
@requires_auth
def api_available_vehicles():
    try:
        vehicles = get_available_vehicles()
        vehicle_data = []
        
        for vehicle_id in vehicles:
            alias = load_vehicle_aliases().get(vehicle_id, vehicle_id)
            vehicle_data.append({
                "id": vehicle_id,
                "alias": alias,
                "display_name": f"{alias} ({vehicle_id})"
            })
        
        return jsonify({
            "success": True,
            "vehicles": vehicle_data
        })
    
    except Exception as e:
        return jsonify({
            "success": False,
            "error": str(e)
        }), 500

@app.route('/api/process-data')
@requires_auth
def process_data_api():
    min_duration = int(request.args.get('min_duration', 4))
    min_stop_count = int(request.args.get('min_stop_count', 5))
    
    result = process_customer_data(min_duration, min_stop_count)
    
    return jsonify({
        'success': True,
        'total_records': len(result),
        'vehicles': sorted(result['Vehicle No'].unique().tolist()) if not result.empty else [],
        'weekdays': sorted(result['Weekday'].unique().tolist()) if not result.empty else []
    })

@app.route('/api/address')
@requires_auth
def get_address_api():
    lat = request.args.get('lat', type=float)
    lon = request.args.get('lon', type=float)
    
    if lat is None or lon is None:
        return jsonify({"error": "Missing 'lat' or 'lon' query parameters."}), 400
        
    address = get_address_from_coords(lat, lon)
    return jsonify({'address': address, 'coordinates': f"{lat}, {lon}"})

def save_unified_edits_df(df):
    os.makedirs('edits', exist_ok=True)
    df.to_csv(EDITS_CSV_FILE, index=False)

@app.route('/api/add-customer-point', methods=['POST'])
@requires_auth
def api_add_customer_point():
    try:
        data = request.get_json()
        
        if not data.get('customer_id'):
            return jsonify({"success": False, "error": "Customer ID is required"}), 400
        
        edits_df = get_unified_edits_df()
        
        new_point = {
            'customer_id': str(data['customer_id']),
            'vehicle_id': str(data['vehicle_id']),
            'weekday': str(data['weekday']),
            'latitude': float(data['latitude']),
            'longitude': float(data['longitude']),
            'customer_contact': data.get('customer_contact', ''),
            'customer_name': data.get('customer_name', ''),
            'description': data.get('description', 'Manually added point')
        }
        
        edits_df = pd.concat([edits_df, pd.DataFrame([new_point])], ignore_index=True)
        save_unified_edits_df(edits_df)
        
        return jsonify({
            "success": True, 
            "customer_id": new_point['customer_id'],
            "point": new_point
        })
        
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500

@app.route('/api/edit-customer-point', methods=['POST'])
@requires_auth
def api_edit_customer_point():
    try:
        data = request.get_json()
        customer_id = data.get('customer_id')
        
        if not customer_id:
            return jsonify({"success": False, "error": "Customer ID required"}), 400
        
        edits_df = get_unified_edits_df()
        
        mask = edits_df['customer_id'] == customer_id
        if not mask.any():
            return jsonify({"success": False, "error": "Customer not found"}), 404

        for field in ['latitude', 'longitude', 'vehicle_id', 'weekday', 'customer_name', 'customer_contact', 'description']:
            if field in data:
                if field in ['latitude', 'longitude']:
                    edits_df.loc[mask, field] = float(data[field])
                else:
                    edits_df.loc[mask, field] = str(data[field])
        
        save_unified_edits_df(edits_df)
        
        updated_point = edits_df[mask].iloc[0].to_dict()
        
        return jsonify({
            "success": True, 
            "point": updated_point
        })
        
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500

@app.route('/api/remove-customer-point', methods=['POST'])
@requires_auth
def api_remove_customer_point():
    try:
        data = request.get_json()
        customer_id = data.get('customer_id')
        
        if not customer_id:
            return jsonify({"success": False, "error": "Customer ID required"}), 400
        
        edits_df = get_unified_edits_df()
        
        mask = edits_df['customer_id'] == customer_id
        if not mask.any():
            return jsonify({"success": False, "error": "Customer not found"}), 404
        
        removed_point = edits_df[mask].iloc[0].to_dict()
        edits_df = edits_df[~mask]
        save_unified_edits_df(edits_df)
        
        return jsonify({
            "success": True, 
            "removed_point": removed_point
        })
        
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500

@app.route('/api/get-customer-points', methods=['GET'])
@requires_auth
def api_get_customer_points():
    try:
        edits_df = get_unified_edits_df()
        
        vehicle_ids = request.args.getlist('vehicles')
        weekdays = request.args.getlist('weekdays')
        
        if vehicle_ids:
            edits_df = edits_df[edits_df['vehicle_id'].isin(vehicle_ids)]
        
        if weekdays:
            edits_df = edits_df[edits_df['weekday'].isin(weekdays)]
        
        points = edits_df.to_dict('records')
        
        return jsonify({
            "success": True,
            "points": points,
            "count": len(points)
        })
        
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500

@app.route('/api/clear-all-edits', methods=['POST'])
@requires_auth
def api_clear_all_edits():
    try:
        if os.path.exists(EDITS_CSV_FILE):
            os.remove(EDITS_CSV_FILE)
        
        return jsonify({"success": True, "message": "All customer points cleared"})
        
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500
@app.route('/settings')
@requires_auth
def settings_page():
    available = get_available_options()
    return render_template('settings.html', settings=load_settings(), available=available)
@app.route('/api/settings', methods=['GET', 'POST'])
@requires_auth
def api_settings():
    app_settings = load_settings()

    if request.method == 'GET':
        safe_settings = app_settings.copy()
        for key in ['gemini_api_key', 'ors_api_key','openai_api_key']:
            if safe_settings.get(key):
                safe_settings[key] = '*' * 20
        return jsonify(safe_settings)
    
    elif request.method == 'POST':
        try:
            new_settings = request.get_json()

            for key, value in new_settings.items():
                if key in DEFAULT_SETTINGS and value and not value.startswith('*'):
                    app_settings[key] = value
            
            if save_settings(app_settings):
                return jsonify({"success": True, "message": "Settings updated successfully"})
            else:
                return jsonify({"success": False, "message": "Failed to save settings"}), 500
                
        except Exception as e:
            return jsonify({"success": False, "message": str(e)}), 400

@app.route('/api/phone-numbers', methods=['GET'])
@requires_auth
def get_phone_numbers():
    if not os.path.exists(PHONE_FILE):
        return jsonify({"phone_numbers": []})
    with open(PHONE_FILE, 'r') as f:
        return jsonify(json.load(f))

@app.route('/api/phone-numbers', methods=['POST'])
@requires_auth
def save_phone_numbers():
    data = request.get_json()
    if "phone_numbers" not in data:
        return jsonify({"success": False, "message": "Missing phone_numbers key"}), 400

    for entry in data["phone_numbers"]:
        if entry["category"] == "Driver" and not entry.get("vehicle_id"):
            return jsonify({"success": False, "message": "Driver entries must have a vehicle_id"}), 400
        if not entry.get("name") or not entry.get("phone"):
            return jsonify({"success": False, "message": "All entries must have name and phone"}), 400
    
    with open(PHONE_FILE, 'w') as f:
        json.dump(data, f, indent=2)
    return jsonify({"success": True, "message": "Phone numbers updated successfully"})

@app.route("/whatsapp-status")
@requires_auth
def whatsapp_status():
    print("WhatsApp status call.")
    try:
        response = requests.get(f'{load_settings()["whatsapp_server_url"]}/status', timeout=15)
        
        if response.status_code == 200:
            status_data = response.json()
            
            if status_data.get("sessionRestarting"):
                return {"status": "restarting", "message": "Session is restarting"}
            
            if status_data.get("hasQR") and status_data.get("sessionState") == "QR_READY":
                qr_response = requests.get(f'{load_settings()["whatsapp_server_url"]}/qr', timeout=10)
                if qr_response.status_code == 200:
                    qr_data = qr_response.json()
                    return {
                        "status": "ready", 
                        "qr": qr_data.get("qr"),
                        "sessionState": qr_data.get("sessionState"),
                        "ageMs": qr_data.get("ageMs")
                    }
                elif qr_response.status_code == 202:
                    return {"status": "restarting", "message": "Session is restarting"}
                elif qr_response.status_code == 410:
                    return {"status": "expired", "message": "QR code expired"}
            
            session_state = status_data.get("sessionState", "UNKNOWN")
            if session_state == "CONNECTED":
                return {"status": "authenticated", "message": "WhatsApp is connected"}
            elif session_state == "CONNECTING":
                return {"status": "initializing", "message": "WhatsApp is starting up"}
            elif session_state == "RESTARTING":
                return {"status": "restarting", "message": "Session is restarting"}
            elif session_state == "DISCONNECTED":
                return {"status": "disconnected", "message": "WhatsApp is disconnected"}
            else:
                return {"status": "waiting", "message": "Waiting for QR code"}
                
        else:
            return {"status": "error", "message": f"Server responded with {response.status_code}"}

    except requests.exceptions.Timeout:
        print("Timeout getting WhatsApp status")
        return {"status": "timeout", "message": "WhatsApp server timeout"}
    except requests.exceptions.RequestException as e:
        print(f"Error getting WhatsApp status: {e}")
        return {"status": "offline", "message": "WhatsApp server unreachable"}

@app.route("/restart-whatsapp")
@requires_auth
def restart_whatsapp():
    try:
        response = requests.post(f'{load_settings()["whatsapp_server_url"]}/restart-session', timeout=30)
        if response.status_code == 200:
            return {"status": "success", "message": "WhatsApp session restarted"}, 200
        elif response.status_code == 429:
            return {"status": "warning", "message": "Restart already in progress"}, 200
        else:
            return {"status": "error", "message": "Failed to restart session"}, 500
    except requests.exceptions.Timeout:
        return {"status": "error", "message": "Restart request timed out"}, 500
    except requests.exceptions.RequestException as e:
        print(f"Error restarting WhatsApp: {e}")
        return {"status": "error", "message": "WhatsApp server unreachable"}, 500
@app.route('/api/files/upload', methods=['POST'])
@requires_auth
def upload_file():
    if 'file' not in request.files:
        return jsonify({"success": False, "message": "No file provided"}), 400
    
    file = request.files['file']
    file_type = request.form.get('type')
    
    if file.filename == '':
        return jsonify({"success": False, "message": "No file selected"}), 400
    
    try:
        if file_type == 'geojson':
            upload_dir = 'analysis/routes_json'
            allowed_extensions = ['.geojson', '.json']
        elif file_type == 'customer_points':
            upload_dir = 'analysis/customerpoints'
            allowed_extensions = ['.csv']
        else:
            return jsonify({"success": False, "message": "Invalid file type"}), 400
        
        os.makedirs(upload_dir, exist_ok=True)
        
        filename = secure_filename(file.filename)
        file_ext = os.path.splitext(filename)[1].lower()
        
        if file_ext not in allowed_extensions:
            return jsonify({"success": False, "message": f"Invalid file extension. Allowed: {', '.join(allowed_extensions)}"}), 400
        
        file_path = os.path.join(upload_dir, filename)
        
        if os.path.exists(file_path):
            base, ext = os.path.splitext(filename)
            counter = 1
            while os.path.exists(file_path):
                new_filename = f"{base}_{counter}{ext}"
                file_path = os.path.join(upload_dir, new_filename)
                counter += 1
            filename = os.path.basename(file_path)
        
        file.save(file_path)
        
        if file_type == 'geojson':
            try:
                with open(file_path, 'r') as f:
                    json.load(f)
            except json.JSONDecodeError:
                os.remove(file_path)
                return jsonify({"success": False, "message": "Invalid JSON/GeoJSON format"}), 400
        elif file_type == 'customer_points':
            try:
                df = pd.read_csv(file_path)
                required_cols = ['customer_id', 'latitude', 'longitude']
                missing_cols = [col for col in required_cols if col not in df.columns]
                if missing_cols:
                    os.remove(file_path)
                    return jsonify({"success": False, "message": f"Missing required columns: {', '.join(missing_cols)}"}), 400
            except Exception as e:
                os.remove(file_path)
                return jsonify({"success": False, "message": f"Invalid CSV format: {str(e)}"}), 400
        
        return jsonify({"success": True, "message": f"File uploaded successfully as {filename}"})
        
    except Exception as e:
        return jsonify({"success": False, "message": f"Upload failed: {str(e)}"}), 500

@app.route('/api/files/<file_type>', methods=['DELETE'])
@requires_auth
def delete_file(file_type):
    data = request.get_json()
    file_path = data.get('file_path')
    
    if not file_path:
        return jsonify({"success": False, "message": "No file path provided"}), 400
    
    try:
        if file_type == 'geojson':
            if not file_path.startswith('analysis/routes_json/'):
                return jsonify({"success": False, "message": "Invalid file path"}), 400
        elif file_type == 'customer_points':
            if not file_path.startswith('analysis/customerpoints/'):
                return jsonify({"success": False, "message": "Invalid file path"}), 400
        else:
            return jsonify({"success": False, "message": "Invalid file type"}), 400
        
        if not os.path.exists(file_path):
            return jsonify({"success": False, "message": "File not found"}), 404
        
        current_settings = load_settings()
        if (file_type == 'geojson' and current_settings.get('geojson_path') == file_path) or \
           (file_type == 'customer_points' and current_settings.get('customer_points_path') == file_path):
            return jsonify({"success": False, "message": "Cannot delete currently selected file"}), 400
        
        os.remove(file_path)
        return jsonify({"success": True, "message": "File deleted successfully"})
        
    except Exception as e:
        return jsonify({"success": False, "message": f"Delete failed: {str(e)}"}), 500

@app.route('/api/edits/export', methods=['GET'])
@requires_auth
def export_edits():
    try:
        edits_df = get_unified_edits_df()
        
        if edits_df.empty:
            temp_df = pd.DataFrame(columns=[
                'customer_id', 'latitude', 'longitude', 'vehicle_id',
                'weekday', 'customer_name', 'customer_contact', 'description'
            ])
            csv_content = temp_df.to_csv(index=False)
        else:
            csv_content = edits_df.to_csv(index=False)
        
        return Response(
            csv_content,
            mimetype='text/csv',
            headers={'Content-Disposition': 'attachment; filename=edits.csv'}
        )
        
    except Exception as e:
        return jsonify({"success": False, "message": f"Export failed: {str(e)}"}), 500

@app.route('/api/edits/import', methods=['POST'])
@requires_auth
def import_edits():
    if 'file' not in request.files:
        return jsonify({"success": False, "message": "No file provided"}), 400
    
    file = request.files['file']
    
    if file.filename == '':
        return jsonify({"success": False, "message": "No file selected"}), 400
    
    if not file.filename.lower().endswith('.csv'):
        return jsonify({"success": False, "message": "File must be a CSV"}), 400
    
    try:
        df = pd.read_csv(file)
        
        required_columns = [
            'customer_id', 'latitude', 'longitude', 'vehicle_id',
            'weekday', 'customer_name', 'customer_contact', 'description'
        ]
        
        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            return jsonify({
                "success": False, 
                "message": f"Missing required columns: {', '.join(missing_columns)}"
            }), 400
        
        for col in ['latitude', 'longitude']:
            df[col] = pd.to_numeric(df[col], errors='coerce')
            if df[col].isna().any():
                return jsonify({
                    "success": False, 
                    "message": f"Invalid numeric values in {col} column"
                }), 400
        
        for col in ['customer_id', 'vehicle_id', 'weekday', 'customer_name', 'customer_contact', 'description']:
            df[col] = df[col].astype(str).fillna('')
        
        os.makedirs(os.path.dirname(EDITS_CSV_FILE), exist_ok=True)
        df.to_csv(EDITS_CSV_FILE, index=False)
        
        return jsonify({
            "success": True, 
            "message": f"Successfully imported {len(df)} records to edits file"
        })
        
    except pd.errors.EmptyDataError:
        return jsonify({"success": False, "message": "CSV file is empty"}), 400
    except Exception as e:
        return jsonify({"success": False, "message": f"Import failed: {str(e)}"}), 500
@app.route('/api/contacts/export', methods=['GET'])
@requires_auth
def export_contacts():
    try:
        contacts_file = os.path.join('whatsappbot', 'contacts.txt')
        if os.path.exists(contacts_file):
            with open(contacts_file, 'r', encoding='utf-8') as f:
                content = f.read()
        else:
            content = ""
            
        return Response(
            content,
            mimetype='text/plain',
            headers={'Content-Disposition': 'attachment; filename=contacts.txt'}
        )
    except Exception as e:
        return jsonify({"success": False, "message": f"Export failed: {str(e)}"}), 500

@app.route('/api/contacts/import', methods=['POST'])
@requires_auth
def import_contacts():
    if 'file' not in request.files:
        return jsonify({"success": False, "message": "No file provided"}), 400
    
    file = request.files['file']
    
    if file.filename == '':
        return jsonify({"success": False, "message": "No file selected"}), 400
    
    if not file.filename.lower().endswith('.txt'):
        return jsonify({"success": False, "message": "File must be a .txt file"}), 400
    
    try:
        content = file.read().decode('utf-8')

        os.makedirs('whatsappbot', exist_ok=True)

        contacts_file = os.path.join('whatsappbot', 'contacts.txt')
        with open(contacts_file, 'w', encoding='utf-8') as f:
            f.write(content)

        line_count = len([line for line in content.split('\n') if line.strip()])
        
        return jsonify({
            "success": True,
            "message": f"Successfully imported contacts.txt with {line_count} lines"
        })
        
    except Exception as e:
        return jsonify({"success": False, "message": f"Import failed: {str(e)}"}), 500

@app.route('/api/contact-status/export', methods=['GET'])
@requires_auth
def export_contact_status():
    try:
        contact_status_file = os.path.join('whatsappbot', 'contact_status.csv')
        if os.path.exists(contact_status_file):
            return send_file(contact_status_file, as_attachment=True, download_name='contact_status.csv')
        else:
            return jsonify({"success": False, "message": "contact_status.csv file not found"}), 404
    except Exception as e:
        return jsonify({"success": False, "message": f"Export failed: {str(e)}"}), 500

@app.route('/api/contact-status/import', methods=['POST'])
@requires_auth
def import_contact_status():
    if 'file' not in request.files:
        return jsonify({"success": False, "message": "No file provided"}), 400
    
    file = request.files['file']
    
    if file.filename == '':
        return jsonify({"success": False, "message": "No file selected"}), 400
    
    if not file.filename.lower().endswith('.csv'):
        return jsonify({"success": False, "message": "File must be a .csv file"}), 400
    
    try:
        df = pd.read_csv(file)
        
        os.makedirs('whatsappbot', exist_ok=True)
        contact_status_file = os.path.join('whatsappbot', 'contact_status.csv')
        df.to_csv(contact_status_file, index=False)
        
        return jsonify({
            "success": True,
            "message": f"Successfully imported contact_status.csv with {len(df)} records"
        })
        
    except Exception as e:
        return jsonify({"success": False, "message": f"Import failed: {str(e)}"}), 500

@app.route('/api/extracted-data/export', methods=['GET'])
@requires_auth
def export_extracted_data():
    try:
        extracted_data_file = os.path.join('whatsappbot', 'extracted_data.csv')
        if os.path.exists(extracted_data_file):
            return send_file(extracted_data_file, as_attachment=True, download_name='extracted_data.csv')
        else:
            return jsonify({"success": False, "message": "extracted_data.csv file not found"}), 404
    except Exception as e:
        return jsonify({"success": False, "message": f"Export failed: {str(e)}"}), 500

@app.route('/api/extracted-data/import', methods=['POST'])
@requires_auth
def import_extracted_data():
    if 'file' not in request.files:
        return jsonify({"success": False, "message": "No file provided"}), 400
    
    file = request.files['file']
    
    if file.filename == '':
        return jsonify({"success": False, "message": "No file selected"}), 400
    
    if not file.filename.lower().endswith('.csv'):
        return jsonify({"success": False, "message": "File must be a .csv file"}), 400
    
    try:
        df = pd.read_csv(file)
        
        os.makedirs('whatsappbot', exist_ok=True)
        extracted_data_file = os.path.join('whatsappbot', 'extracted_data.csv')
        df.to_csv(extracted_data_file, index=False)
        
        return jsonify({
            "success": True,
            "message": f"Successfully imported extracted_data.csv with {len(df)} records"
        })
        
    except Exception as e:
        return jsonify({"success": False, "message": f"Import failed: {str(e)}"}), 500





@app.route('/health')
def health():
    return 'OK', 200
if __name__ == "__main__":
    try:

        app.run(debug=False, host='0.0.0.0', port=5231)

    except Exception as e:
        print(f"error {e}")
        raise