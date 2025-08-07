import requests
import pandas as pd
import time
import json
import os
import re
import csv
import asyncio
import aiohttp
import threading
from datetime import datetime, timedelta
from typing import Dict, List, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed
from queue import Queue
import random

def reverse_geocode(latitude: float, longitude: float) -> str:
    try:
        url = f"https://nominatim.openstreetmap.org/reverse"
        params = {
            "lat": latitude,
            "lon": longitude,
            "format": "json",
            "addressdetails": 1
        }
        headers = {
            "User-Agent": "OxyPlusWaterDeliveryBot/1.0"
        }
        response = requests.get(url, params=params, headers=headers, timeout=10)
        if response.status_code == 200:
            data = response.json()
            return data.get("display_name", "")
        else:
            print(f"Reverse geocode failed: {response.status_code}")
            return ""
    except Exception as e:
        print(f"Error in reverse_geocode: {e}")
        return ""

def load_settings():
    json_path = "../config_data/app_settings.json"
    with open(json_path, 'r', encoding='utf-8') as f:
        return json.load(f)

class EnhancedWhatsAppCollector:
    def __init__(self):
        self.contact_status_file = "contact_status.csv"
        self.extracted_data_file = "extracted_data.csv"
        self.processed_messages_file = "processed_messages.json"
        self.rate_limit_file = "rate_limits.json"
        self.outreach_state_file = "outreach_state.json"

        self.message_queue = Queue()
        self.processing_threads = []
        self.is_running = False
        self.max_workers = 5

        self.last_message_time = self.load_rate_limits()
        self.min_reply_interval = 5
        self.reply_delay_range = (8, 25)

        self.base_outreach_delay = 90
        self.outreach_delay_variance = 30
        self.max_daily_outreach = 960
        
        self.last_global_message = 0
        self.outreach_state = self.load_outreach_state()
        
        self.processed_messages = self.load_processed_messages()
        self._initialize_csv_files()
        print("Enhanced WhatsApp Collector initialized with human-like timing")
        print(f"Daily outreach limit: {self.max_daily_outreach} messages")
        print(f"Outreach delay: {self.base_outreach_delay}±{self.outreach_delay_variance} seconds")
    
    def load_outreach_state(self) -> dict:
        try:
            if os.path.exists(self.outreach_state_file):
                with open(self.outreach_state_file, 'r') as f:
                    data = json.load(f)
                    last_date = data.get('date', '')
                    today = datetime.now().strftime('%Y-%m-%d')
                    if last_date != today:
                        return {
                            'date': today,
                            'sent_count': 0,
                            'last_sent_time': 0,
                            'current_batch_index': 0
                        }
                    return data
            return {
                'date': datetime.now().strftime('%Y-%m-%d'),
                'sent_count': 0,
                'last_sent_time': 0,
                'current_batch_index': 0
            }
        except Exception as e:
            print(f"Error loading outreach state: {e}")
            return {
                'date': datetime.now().strftime('%Y-%m-%d'),
                'sent_count': 0,
                'last_sent_time': 0,
                'current_batch_index': 0
            }
    
    def save_outreach_state(self):
        try:
            with open(self.outreach_state_file, 'w') as f:
                json.dump(self.outreach_state, f)
        except Exception as e:
            print(f"Error saving outreach state: {e}")
    
    def get_human_like_outreach_delay(self) -> float:
        delay = self.base_outreach_delay + random.randint(-self.outreach_delay_variance, self.outreach_delay_variance)

        if random.random() < 0.1:
            delay += random.randint(300, 900)
            print(f"Taking a human-like break: {delay/60:.1f} minutes")
        
        return delay
    
    def get_human_like_reply_delay(self) -> float:
        return random.randint(self.reply_delay_range[0], self.reply_delay_range[1])
    
    def can_send_outreach_message(self) -> bool:
        current_time = time.time()

        if self.outreach_state['sent_count'] >= self.max_daily_outreach:
            return False

        time_since_last = current_time - self.outreach_state.get('last_sent_time', 0)
        required_delay = self.get_human_like_outreach_delay()
        
        return time_since_last >= required_delay
    
    def get_openai_client(self):
        openai_key = load_settings()['openai_api_key']
        if openai_key:
            import openai
            return openai.OpenAI(api_key=openai_key)
        return None
    
    def get_whatsapp_url(self):
        return load_settings()['whatsapp_server_url'].rstrip('/')
    
    def load_rate_limits(self) -> dict:
        try:
            if os.path.exists(self.rate_limit_file):
                with open(self.rate_limit_file, 'r') as f:
                    data = json.load(f)
                    current_time = time.time()
                    return {
                        phone: timestamp 
                        for phone, timestamp in data.items() 
                        if current_time - timestamp < 3600
                    }
            return {}
        except Exception as e:
            print(f"Error loading rate limits: {e}")
            return {}
    
    def save_rate_limits(self):
        try:
            with open(self.rate_limit_file, 'w') as f:
                json.dump(self.last_message_time, f)
        except Exception as e:
            print(f"Error saving rate limits: {e}")
    
    def load_processed_messages(self) -> set:
        try:
            if os.path.exists(self.processed_messages_file):
                with open(self.processed_messages_file, 'r') as f:
                    data = json.load(f)
                    processed_ids = set(data.get('processed_ids', []))
                    if 'timestamps' in data:
                        current_time = time.time()
                        valid_ids = set()
                        for msg_id in processed_ids:
                            timestamp = data['timestamps'].get(msg_id, 0)
                            if current_time - timestamp < 86400:
                                valid_ids.add(msg_id)
                        return valid_ids
                    return processed_ids
            return set()
        except:
            return set()
    
    def save_processed_messages(self):
        try:
            current_time = time.time()
            data = {
                'processed_ids': list(self.processed_messages),
                'timestamps': {msg_id: current_time for msg_id in self.processed_messages},
                'last_updated': current_time
            }
            with open(self.processed_messages_file, 'w') as f:
                json.dump(data, f)

            self.save_rate_limits()
            self.save_outreach_state()
        except Exception as e:
            print(f"Error saving processed messages: {e}")
    
    def _initialize_csv_files(self):
        if not os.path.exists(self.contact_status_file):
            with open(self.contact_status_file, 'w', newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
                writer.writerow(['contact', 'status', 'customer_name', 'message_sent_at', 'location_received_at', 'name_collected_at'])
        
        if not os.path.exists(self.extracted_data_file):
            with open(self.extracted_data_file, 'w', newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
                writer.writerow(['customer_name', 'latitude', 'longitude', 'contact', 'location_description', 'timestamp'])
    
    def load_contacts_from_txt(self, txt_file_path: str) -> List[str]:
        try:

            with open(txt_file_path, 'r', encoding='utf-8') as f:
                contacts = [
                    re.sub(r'[^\d]', '', line.strip())
                    for line in f if line.strip()
                ]

            print(f"Loaded {len(contacts)} contacts from {txt_file_path}")
            return contacts

        except Exception as e:
            print(f"Error loading contacts: {e}")
            return []
    
    def create_contact_status_csv(self, contacts: List[str]):
        try:
            existing_contacts = set()
            if os.path.exists(self.contact_status_file):
                df = pd.read_csv(self.contact_status_file)
                existing_contacts = set(df['contact'].astype(str))
            
            new_contacts = []
            for contact in contacts:
                contact = contact.strip()
                if contact and contact not in existing_contacts:
                    new_contacts.append({
                        'contact': contact,
                        'status': 'PENDING',
                        'customer_name': '',
                        'message_sent_at': '',
                        'location_received_at': '',
                        'name_collected_at': ''
                    })
            
            if new_contacts:
                df_new = pd.DataFrame(new_contacts)
                if os.path.exists(self.contact_status_file):
                    df_new.to_csv(self.contact_status_file, mode='a', header=False, index=False)
                else:
                    df_new.to_csv(self.contact_status_file, index=False)
                print(f"Added {len(new_contacts)} new contacts")
            
        except Exception as e:
            print(f"Error creating contact status CSV: {e}")
    
    def check_whatsapp_status(self) -> bool:
        try:
            response = requests.get(f"{self.get_whatsapp_url()}/status", timeout=10)
            if response.status_code == 200:
                status_data = response.json()
                is_connected = status_data.get('sessionState') == 'CONNECTED'
                if not is_connected:
                    print(f"WhatsApp not connected. State: {status_data.get('sessionState')}")
                return is_connected
            return False
        except Exception as e:
            print(f"Error checking WhatsApp status: {e}")
            return False
    
    def can_send_reply(self, phone_number: str) -> bool:
        current_time = time.time()

        if phone_number in self.last_message_time:
            time_since_last = current_time - self.last_message_time[phone_number]
            if time_since_last < self.min_reply_interval:
                return False
        
        return True
    
    def _send_reply_with_rate_limit(self, phone_number: str, message: str) -> bool:
        if not self.can_send_reply(phone_number):
            print(f"Reply rate limited for {phone_number}")
            return False

        delay = self.get_human_like_reply_delay()
        print(f"Human-like reply delay: {delay} seconds for {phone_number}")
        time.sleep(delay)
        
        success = self._send_message(phone_number, message)
        if success:
            current_time = time.time()
            self.last_message_time[phone_number] = current_time
            self.save_rate_limits()
        
        return success
    
    def _send_outreach_with_rate_limit(self, phone_number: str, message: str) -> bool:
        """NEW: Separate rate limiting for outreach messages"""
        if not self.can_send_outreach_message():
            return False
        
        success = self._send_message(phone_number, message)
        if success:
            current_time = time.time()
            self.outreach_state['last_sent_time'] = current_time
            self.outreach_state['sent_count'] += 1
            self.save_outreach_state()
            print(f"Outreach sent ({self.outreach_state['sent_count']}/{self.max_daily_outreach})")
        
        return success
    
    def send_initial_location_request(self, phone_number: str) -> bool:
        message = """Hello! This is OxyPlus Water Delivery

We need to coordinate your premium water service. Please help us with:

1️⃣ Your name for our records
2️⃣ Your current location using WhatsApp's location feature

📍 Tap attachment → Location → Send Current Location

This helps us deliver fresh water directly to your door!

What's your name?"""
        
        return self._send_outreach_with_rate_limit(phone_number, message)  # UPDATED: Use outreach rate limiting
    
    def send_location_request_after_name(self, phone_number: str, customer_name: str) -> bool:
        message = f"""Thank you {customer_name}! 

Now please share your current location using WhatsApp's location feature:

📍 Tap the attachment button
📍 Select "Location" 
📍 Choose "Send Current Location"

This helps our delivery team find you quickly and ensure timely service!"""
        
        return self._send_reply_with_rate_limit(phone_number, message)
    
    def generate_intelligent_response(self, message_text: str, phone_number: str, customer_name: str = "") -> str:
        openai_client = self.get_openai_client()
        max_retries = 3
        retry_count = 0
        
        while retry_count < max_retries:
            try:
                if not openai_client:
                    openai_client = self.get_openai_client()
                    if not openai_client:
                        return "Sorry, I'm having technical difficulties. Please try again later."
                
                context = f"Customer name: {customer_name}" if customer_name else "Customer name not collected yet"
                
                prompt = f"""You are a professional customer service representative for OxyPlus Water Delivery in UAE we are oxypluswater.com.

Customer message: "{message_text}"
Context: {context}

Generate a helpful, professional response (max 100 words) that:
1. Addresses their specific concern
2. Explains we only need location for water delivery
3. Reassures about privacy and legitimacy
4. Encourages sharing name/location
5. Stays friendly and professional
6. If name is not collected please use Dear Customer or something similar

If they refuse or have privacy concerns, be understanding but persistent.
If they ask questions, answer briefly and redirect to sharing info.
If they seem confused, explain clearly what we need.
Also tell them doing location and then name won't work the proper order is name first and location.
"""
                
                response = openai_client.chat.completions.create(
                    model="gpt-4o-mini",
                    messages=[{"role": "user", "content": prompt}],
                    max_tokens=150,
                    temperature=1.0
                )
                
                return response.choices[0].message.content.strip()
                
            except Exception as e:
                retry_count += 1
                print(f"OpenAI API error (attempt {retry_count}/{max_retries}): {e}")
                if retry_count < max_retries:
                    time.sleep(2 ** retry_count)
                
        return "Sorry, couldn't understand what you said. Can you be more brief?"
    
    def analyze_message_for_name(self, message_text: str) -> Optional[str]:
        if not message_text or len(message_text.strip()) < 2:
            return None
        
        openai_client = self.get_openai_client()
        if openai_client:
            try:
                prompt = f"""Extract a person's name from this message: "{message_text}"

Rules:
- Return ONLY the name if it's clearly a person's name
- Return "NO_NAME" if it's not a name
- Names should be 2-30 characters
- Ignore questions, complaints, addresses, or service-related text

Examples:
"My name is Ahmed" → Ahmed
"I am Sarah Khan" → Sarah Khan  
"Mohammed" → Mohammed
"What is this service?" → NO_NAME
"I don't want this" → NO_NAME
"Ahmad Ali" → Ahmad Ali
"Hey" -> NO_NAME
"""
                
                response = openai_client.chat.completions.create(
                    model="gpt-4o-mini",
                    messages=[{"role": "user", "content": prompt}],
                    max_tokens=50,
                    temperature=0.4
                )

                result = response.choices[0].message.content.strip()
                if result and result != "NO_NAME" and 2 <= len(result) <= 30:
                    return result.title()
                    
            except Exception as e:
                print(f"Error with OpenAI name extraction: {e}")
        
        return None
    
    def _send_message(self, phone_number: str, message: str) -> bool:
        try:
            payload = {"number": phone_number, "message": message}
            response = requests.post(f"{self.get_whatsapp_url()}/send-text", json=payload, timeout=30)
            
            if response.status_code == 200:
                print(f"Message sent to {phone_number}")
                return True
            else:
                print(f"Failed to send message to {phone_number}: {response.text}")
                return False
        except Exception as e:
            print(f"Error sending message to {phone_number}: {e}")
            return False
    
    def update_contact_status(self, phone_number: str, status: str, **kwargs):
        try:
            df = pd.read_csv(self.contact_status_file)
            df['contact'] = df['contact'].astype(str)
            
            mask = df['contact'] == str(phone_number)
            if mask.any():
                df.loc[mask, 'status'] = status
                for key, value in kwargs.items():
                    if key in df.columns:
                        df.loc[mask, key] = value
                
                df.to_csv(self.contact_status_file, index=False)
                print(f"Updated {phone_number} status to {status}")
        except Exception as e:
            print(f"Error updating contact status: {e}")
    
    def get_contacts_by_status(self, status: str) -> List[Dict]:
        try:
            df = pd.read_csv(self.contact_status_file)
            contacts = df[df['status'] == status].to_dict('records')
            return [{k: str(v) for k, v in contact.items()} for contact in contacts]
        except Exception as e:
            print(f"Error getting contacts by status: {e}")
            return []
    
    def check_location_for_contact(self, phone_number: str) -> Optional[Dict]:
        try:
            clean_number = phone_number.replace('@c.us', '')
            response = requests.get(f"{self.get_whatsapp_url()}/location/{clean_number}", timeout=10)
            
            if response.status_code == 200:
                return response.json()
            return None
        except Exception as e:
            print(f"Error checking location for {phone_number}: {e}")
            return None
    
    def save_location_data(self, phone_number: str, customer_name: str, location_data: Dict):
        try:
            location_info = location_data.get('location', {})
            latitude = location_info.get('latitude', '')
            longitude = location_info.get('longitude', '')
            
            location_description = reverse_geocode(float(latitude), float(longitude)) if latitude and longitude else ""

            row_data = {
                'customer_name': customer_name,
                'latitude': latitude,
                'longitude': longitude,
                'contact': phone_number,
                'location_description': location_description,
                'timestamp': location_info.get('timestamp', datetime.now().isoformat())
            }
            
            df = pd.DataFrame([row_data])
            if os.path.exists(self.extracted_data_file):
                df.to_csv(self.extracted_data_file, mode='a', header=False, index=False)
            else:
                df.to_csv(self.extracted_data_file, index=False)
            
            print(f"LOCATION SAVED: {customer_name} ({phone_number}) - {location_description}")
        except Exception as e:
            print(f"Error saving location data: {e}")
    
    def send_messages_to_pending_contacts_gradually(self):
        if not self.check_whatsapp_status():
            print("WhatsApp not connected - cannot send messages")
            return
        
        pending_contacts = self.get_contacts_by_status('PENDING')

        start_index = self.outreach_state.get('current_batch_index', 0)
        remaining_contacts = pending_contacts[start_index:]
        
        if not remaining_contacts:
            print("No pending contacts to process")
            return

        remaining_daily_quota = self.max_daily_outreach - self.outreach_state['sent_count']
        contacts_to_process = min(len(remaining_contacts), remaining_daily_quota)
        
        print(f"Processing {contacts_to_process} contacts (Daily quota: {self.outreach_state['sent_count']}/{self.max_daily_outreach})")
        
        success_count = 0
        processed_count = 0
        
        for i, contact_info in enumerate(remaining_contacts[:contacts_to_process]):
            phone_number = contact_info['contact']
            
            if not self.can_send_outreach_message():
                print(f"Daily outreach limit reached or waiting for next send window")
                break
            
            print(f"Sending initial message to {phone_number} ({processed_count+1}/{contacts_to_process})")
            
            if self.send_initial_location_request(phone_number):
                self.update_contact_status(
                    phone_number, 
                    'AWAITING_NAME', 
                    message_sent_at=datetime.now().isoformat()
                )
                success_count += 1
            
            processed_count += 1
            self.outreach_state['current_batch_index'] = start_index + processed_count
            self.save_outreach_state()

            if i < contacts_to_process - 1:
                delay = self.get_human_like_outreach_delay()
                print(f"Waiting {delay/60:.1f} minutes before next message...")
                time.sleep(delay)
        
        print(f"Outreach session completed: {success_count}/{processed_count} sent successfully")
        print(f"Daily progress: {self.outreach_state['sent_count']}/{self.max_daily_outreach}")

        if start_index + processed_count >= len(pending_contacts):
            self.outreach_state['current_batch_index'] = 0
            self.save_outreach_state()
            print("All pending contacts processed. Batch index reset.")
    
    def process_single_message(self, message_data: dict, active_contacts: dict) -> bool:
        try:
            phone_number = message_data.get('from', '').replace('@c.us', '')
            message_body = message_data.get('body', '')
            message_type = message_data.get('type', '')
            msg_id = message_data.get('id')
            
            if phone_number not in active_contacts:
                return False
            
            contact_info = active_contacts[phone_number]
            current_status = contact_info['status']
            customer_name = contact_info.get('customer_name', '')
            
            print(f"PROCESSING: {phone_number} ({current_status}): {message_body[:50]}")
            
            if message_type == 'location':
                if current_status in ['AWAITING_LOCATION', 'COLLECTING_LOCATION'] and customer_name:
                    location_data = self.check_location_for_contact(phone_number)
                    if location_data:
                        self.save_location_data(phone_number, customer_name, location_data)
                        
                        self.update_contact_status(
                            phone_number,
                            'COMPLETED',
                            location_received_at=datetime.now().isoformat()
                        )
                        
                        thank_you_msg = f"Perfect! Thank you {customer_name}! Your location has been saved. Our OxyPlus team will contact you shortly to coordinate your premium water delivery. Thanks for choosing OxyPlus!"
                        self._send_reply_with_rate_limit(phone_number, thank_you_msg)  # UPDATED
                        
                        print(f"COMPLETED: {customer_name} ({phone_number})")
                        return True
            
            elif current_status == 'AWAITING_NAME' and message_body:
                extracted_name = self.analyze_message_for_name(message_body)
                
                if extracted_name:
                    self.update_contact_status(
                        phone_number,
                        'AWAITING_LOCATION',
                        customer_name=extracted_name,
                        name_collected_at=datetime.now().isoformat()
                    )
                    
                    self.send_location_request_after_name(phone_number, extracted_name)
                    print(f"NAME COLLECTED: {extracted_name} ({phone_number})")
                    return True
                else:
                    response_msg = self.generate_intelligent_response(message_body, phone_number, customer_name)
                    self._send_reply_with_rate_limit(phone_number, response_msg)  # UPDATED
                    print(f"INTELLIGENT RESPONSE sent to {phone_number}")
                    return True
            
            elif current_status in ['AWAITING_LOCATION', 'COLLECTING_LOCATION'] and message_body:
                response_msg = self.generate_intelligent_response(message_body, phone_number, customer_name)
                self._send_reply_with_rate_limit(phone_number, response_msg)  # UPDATED
                print(f"INTELLIGENT RESPONSE sent to {phone_number}")
                return True
            
            return False
            
        except Exception as e:
            print(f"Error processing message from {phone_number}: {e}")
            return False
    
    def graceful_shutdown(self):
        print("Initiating graceful shutdown...")
        self.is_running = False

        self.save_processed_messages()
        self.save_rate_limits()
        self.save_outreach_state()  # NEW

        for thread in self.processing_threads:
            if thread.is_alive():
                thread.join(timeout=30)
        
        print("Graceful shutdown completed")
    
    def process_messages_parallel(self, messages: List[dict], active_contacts: dict):
        if not messages:
            return

        new_messages = [
            msg for msg in messages 
            if msg.get('id') not in self.processed_messages and not msg.get('fromMe', False)
        ]
        
        if not new_messages:
            return
        
        print(f"Processing {len(new_messages)} new messages in parallel...")

        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            future_to_message = {
                executor.submit(self.process_single_message, msg, active_contacts): msg 
                for msg in new_messages
            }

            processed_count = 0
            for future in as_completed(future_to_message):
                message = future_to_message[future]
                try:
                    success = future.result()
                    if success:
                        processed_count += 1
                    self.processed_messages.add(message.get('id'))
                except Exception as e:
                    print(f"Error in parallel processing: {e}")
                    self.processed_messages.add(message.get('id'))

                time.sleep(1)
        
        print(f"Parallel processing completed: {processed_count} messages processed successfully")
        
        if new_messages:
            self.save_processed_messages()
    
    def process_incoming_messages(self):
        print("Starting enhanced message monitoring with parallel processing")
        print(f"Loaded {len(self.processed_messages)} previously processed messages")
        print(f"Loaded rate limits for {len(self.last_message_time)} contacts")
        
        self.is_running = True
        
        try:
            while self.is_running:
                try:
                    if not self.check_whatsapp_status():
                        print("WhatsApp not connected - waiting for connection")
                        time.sleep(30)
                        continue

                    response = requests.get(f"{self.get_whatsapp_url()}/messages?limit=50", timeout=10)
                    if response.status_code != 200:
                        print("Failed to fetch messages")
                        time.sleep(30)
                        continue
                    
                    messages_data = response.json()
                    messages = messages_data.get('messages', [])

                    active_contacts = {}
                    for status in ['AWAITING_NAME', 'AWAITING_LOCATION', 'COLLECTING_LOCATION']:
                        contacts = self.get_contacts_by_status(status)
                        for contact in contacts:
                            active_contacts[contact['contact']] = contact

                    self.process_messages_parallel(messages, active_contacts)

                    self.send_reminders_to_stalled_contacts()

                    time.sleep(5)
                    
                except KeyboardInterrupt:
                    print("Monitoring stopped by user")
                    break
                except Exception as e:
                    print(f"Error in message processing: {e}")
                    time.sleep(30)
        finally:
            self.graceful_shutdown()
    
    def send_reminders_to_stalled_contacts(self):
        """UPDATED: Use reply rate limiting for reminders"""
        try:
            awaiting_location_contacts = self.get_contacts_by_status('AWAITING_LOCATION')
            
            for contact_info in awaiting_location_contacts:
                phone_number = contact_info['contact']
                customer_name = contact_info.get('customer_name', '')
                name_collected_at = contact_info.get('name_collected_at', '')
                
                if name_collected_at and customer_name:
                    try:
                        collected_time = datetime.fromisoformat(name_collected_at)
                        time_diff = datetime.now() - collected_time

                        if 300 <= time_diff.total_seconds() <= 360:
                            reminder_msg = f"""Hi {customer_name}! We're still waiting for your location to complete your OxyPlus water delivery setup.

Please share your location using:
📍 Attachment button → Location → Send Current Location

This is required to provide you with our premium water delivery service."""
                            
                            if self._send_reply_with_rate_limit(phone_number, reminder_msg):  # UPDATED
                                self.update_contact_status(phone_number, 'COLLECTING_LOCATION')
                                print(f"REMINDER sent to {customer_name}")
                    except:
                        pass
        except Exception as e:
            print(f"Error sending reminders: {e}")
    
    def run_continuous_outreach(self):
        """NEW: Continuous outreach management with human-like timing"""
        print("Starting continuous outreach with human-like timing...")
        
        while self.is_running:
            try:
                if not self.check_whatsapp_status():
                    print("WhatsApp not connected for outreach - waiting...")
                    time.sleep(60)
                    continue

                today = datetime.now().strftime('%Y-%m-%d')
                if self.outreach_state['date'] != today:
                    print(f"New day detected. Resetting outreach counters.")
                    self.outreach_state = {
                        'date': today,
                        'sent_count': 0,
                        'last_sent_time': 0,
                        'current_batch_index': 0
                    }
                    self.save_outreach_state()

                if self.outreach_state['sent_count'] >= self.max_daily_outreach:
                    print(f"Daily outreach limit reached ({self.max_daily_outreach}). Waiting for next day...")
                    time.sleep(3600)
                    continue

                self.send_messages_to_pending_contacts_gradually()

                time.sleep(300)
                
            except KeyboardInterrupt:
                print("Outreach stopped by user")
                break
            except Exception as e:
                print(f"Error in continuous outreach: {e}")
                time.sleep(300)
    
    def get_status_summary(self) -> Dict:
        try:
            df = pd.read_csv(self.contact_status_file)
            status_counts = df['status'].value_counts().to_dict()
            
            extracted_count = 0
            if os.path.exists(self.extracted_data_file):
                df_extracted = pd.read_csv(self.extracted_data_file)
                extracted_count = len(df_extracted)
            
            return {
                'total_contacts': len(df),
                'pending': status_counts.get('PENDING', 0),
                'awaiting_name': status_counts.get('AWAITING_NAME', 0),
                'awaiting_location': status_counts.get('AWAITING_LOCATION', 0),
                'collecting_location': status_counts.get('COLLECTING_LOCATION', 0),
                'completed': status_counts.get('COMPLETED', 0),
                'locations_extracted': extracted_count,
                'processed_messages_count': len(self.processed_messages),
                'daily_outreach_sent': self.outreach_state['sent_count'],
                'daily_outreach_limit': self.max_daily_outreach,
                'outreach_progress': f"{self.outreach_state['sent_count']}/{self.max_daily_outreach}"
            }
        except Exception as e:
            print(f"Error getting status summary: {e}")
            return {}

def main():
    print(f"Absolute path : {os.path.abspath(os.curdir)}")
    os.chdir(os.path.join(os.path.abspath(os.curdir),'whatsappbot'))

    openai_key = load_settings()['openai_api_key']
    while not openai_key:
        print("No OpenAI api key found please enter it...")
        openai_key = load_settings()['openai_api_key']
        time.sleep(30)
    
    collector = EnhancedWhatsAppCollector()
    
    txt_file = 'contacts.txt'
    if not os.path.exists(txt_file):
        print(f"Please create {txt_file} with phone numbers (one per line)")
        return
    
    contacts = collector.load_contacts_from_txt(txt_file)
    if not contacts:
        print("No contacts loaded")
        return
    
    collector.create_contact_status_csv(contacts)
    
    print("=" * 60)
    print("ENHANCED WHATSAPP AUTOMATION WITH HUMAN-LIKE TIMING")
    print("=" * 60)
    print(f"Total contacts loaded: {len(contacts)}")
    print(f"Daily outreach limit: {collector.max_daily_outreach} messages")
    print(f"Outreach delay: {collector.base_outreach_delay}±{collector.outreach_delay_variance} seconds")
    print(f"Reply delay: {collector.reply_delay_range[0]}-{collector.reply_delay_range[1]} seconds")
    print(f"Reply workers: {collector.max_workers}")
    print("=" * 60)

    import threading
    
    def reply_processor():
        print("Starting reply monitoring thread...")
        collector.process_incoming_messages()

    def outreach_processor():
        print("Starting outreach management thread...")
        collector.run_continuous_outreach()

    reply_thread = threading.Thread(target=reply_processor, daemon=True)
    outreach_thread = threading.Thread(target=outreach_processor, daemon=True)
    
    reply_thread.start()
    time.sleep(2)
    outreach_thread.start()
    
    print("Both threads started!")
    print("Reply processing: Real-time with human-like delays")
    print("Outreach processing: Gradual with 1.5±0.5 minute intervals")
    print("Press Ctrl+C to stop")
    
    try:
        while reply_thread.is_alive() or outreach_thread.is_alive():
            time.sleep(10)
            status = collector.get_status_summary()
            print(f"""
   STATUS UPDATE:
   Pending: {status.get('pending', 0)}
   Awaiting Name: {status.get('awaiting_name', 0)}
   Awaiting Location: {status.get('awaiting_location', 0)}
   Completed: {status.get('completed', 0)}
   Daily Outreach: {status.get('outreach_progress', '0/0')}
            """)
    except KeyboardInterrupt:
        print("\nShutting down...")
        collector.graceful_shutdown()

if __name__ == "__main__":
    main()