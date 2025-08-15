import requests
import pandas as pd
import time
import json
import os
import re
import csv
import threading
from datetime import datetime, timedelta
from typing import Dict, List, Optional
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

def is_business_hours():
    now = datetime.now()
    return 6 <= now.hour < 21

class EnhancedWhatsAppCollector:
    def __init__(self):
        self.contact_status_file = "contact_status.csv"
        self.extracted_data_file = "extracted_data.csv"
        self.processed_messages_file = "processed_messages.json"
        self.contacted_today_file = "contacted_today.json"

        self.is_running = False
        
        # Fixed timing intervals as requested
        self.outreach_delay_range = (300, 1200)  # 5-20 minutes between outreach messages
        self.reply_delay_range = (60, 180)       # 1-3 minutes for replies
        
        self.last_message_time = {}
        self.contacted_today = self.load_contacted_today()
        self.processed_messages = self.load_processed_messages()
        self._initialize_csv_files()
    
    def load_contacted_today(self) -> set:
        try:
            if os.path.exists(self.contacted_today_file):
                with open(self.contacted_today_file, 'r') as f:
                    data = json.load(f)
                    today = datetime.now().strftime('%Y-%m-%d')
                    if data.get('date') == today:
                        return set(data.get('contacted', []))
            return set()
        except Exception as e:
            print(f"Error loading contacted today: {e}")
            return set()
    
    def save_contacted_today(self):
        try:
            today = datetime.now().strftime('%Y-%m-%d')
            data = {
                'date': today,
                'contacted': list(self.contacted_today)
            }
            with open(self.contacted_today_file, 'w') as f:
                json.dump(data, f)
        except Exception as e:
            print(f"Error saving contacted today: {e}")
    
    def load_processed_messages(self) -> set:
        try:
            if os.path.exists(self.processed_messages_file):
                with open(self.processed_messages_file, 'r') as f:
                    data = json.load(f)
                    return set(data.get('processed_ids', []))
            return set()
        except:
            return set()
    
    def save_processed_messages(self):
        try:
            data = {
                'processed_ids': list(self.processed_messages),
                'last_updated': time.time()
            }
            with open(self.processed_messages_file, 'w') as f:
                json.dump(data, f)
        except Exception as e:
            print(f"Error saving processed messages: {e}")
    
    def _initialize_csv_files(self):
        if not os.path.exists(self.contact_status_file):
            with open(self.contact_status_file, 'w', newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
                writer.writerow(['contact', 'status', 'customer_name', 'message_sent_at', 'location_received_at', 'name_collected_at', 'last_follow_up'])
        
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
                        'name_collected_at': '',
                        'last_follow_up': ''
                    })
            
            if new_contacts:
                df_new = pd.DataFrame(new_contacts)
                if os.path.exists(self.contact_status_file):
                    df_new.to_csv(self.contact_status_file, mode='a', header=False, index=False)
                else:
                    df_new.to_csv(self.contact_status_file, index=False)
            
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
    
    def get_openai_client(self):
        openai_key = load_settings()['openai_api_key']
        if openai_key:
            import openai
            return openai.OpenAI(api_key=openai_key)
        return None
    
    def get_whatsapp_url(self):
        return load_settings()['whatsapp_server_url'].rstrip('/')
    
    def generate_ai_message(self, message_type: str, customer_name: str = "", user_message: str = "") -> str:
        openai_client = self.get_openai_client()
        
        try:
            if not openai_client:
                return "Sorry, I'm having technical difficulties. Please call 6005-69699 for assistance."
            
            if message_type == "initial_outreach":
                prompt = """Generate a brief, friendly WhatsApp message (max 50 words) for OxyPlus Water Delivery introducing our premium water service. Ask for their name and mention we'll need location for delivery. Sound natural and professional. Don't use bullet points or emojis."""
            
            elif message_type == "location_request":
                prompt = f"""Generate a brief message (max 40 words) asking {customer_name} to share their location using WhatsApp's location feature for OxyPlus water delivery. Be friendly and explain it's needed for delivery. Don't use emojis."""
            
            elif message_type == "completion":
                prompt = f"""Generate a brief thank you message (max 30 words) for {customer_name} confirming we received their location and will contact them soon for OxyPlus water delivery."""
            
            elif message_type == "follow_up":
                prompt = f"""Generate a brief follow-up message (max 40 words) for OxyPlus water delivery. Ask {customer_name if customer_name else 'the customer'} if they're still interested and mention toll-free 6005-69699."""
            
            elif message_type == "redirect_to_support":
                prompt = f"""Generate a brief message (max 30 words) politely redirecting the customer to call toll-free 6005-69699 for detailed questions about OxyPlus water service."""
            
            else:
                # General response - always redirect to support for questions
                prompt = f"""Generate a brief response (max 40 words) for OxyPlus Water Delivery that directs them to call 6005-69699 for questions, or asks for their name/location if not collected yet."""
            
            response = openai_client.chat.completions.create(
                model="gpt-4o-mini",
                messages=[{"role": "user", "content": prompt}],
                max_tokens=80,
                temperature=0.7
            )
            
            return response.choices[0].message.content.strip()
            
        except Exception as e:
            print(f"OpenAI API error: {e}")
            return "Sorry, please call 6005-69699 for assistance."
    
    def analyze_message_for_name(self, message_text: str) -> Optional[str]:
        if not message_text or len(message_text.strip()) < 2:
            return None
        
        openai_client = self.get_openai_client()
        if openai_client:
            try:
                prompt = f"""Extract a person's name from: "{message_text}"
Return ONLY the name if it's clearly a person's name, otherwise return "NO_NAME".
Examples: "Ahmed" → Ahmed, "I am Sarah" → Sarah, "What service?" → NO_NAME"""
                
                response = openai_client.chat.completions.create(
                    model="gpt-4o-mini",
                    messages=[{"role": "user", "content": prompt}],
                    max_tokens=30,
                    temperature=0.3
                )

                result = response.choices[0].message.content.strip()
                if result and result != "NO_NAME" and 2 <= len(result) <= 25:
                    return result.title()
                    
            except Exception as e:
                print(f"Error with name extraction: {e}")
        
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
        except Exception as e:
            print(f"Error saving location data: {e}")
    
    def should_send_follow_up(self, contact_info: Dict) -> bool:
        try:
            status = contact_info['status']
            last_follow_up = contact_info.get('last_follow_up', '')
            message_sent_at = contact_info.get('message_sent_at', '')
            
            if status not in ['AWAITING_NAME', 'AWAITING_LOCATION']:
                return False
            
            if not message_sent_at:
                return False
            
            sent_time = datetime.fromisoformat(message_sent_at)
            now = datetime.now()
            days_since_sent = (now - sent_time).days
            
            if days_since_sent < 2:
                return False
            
            if last_follow_up:
                last_follow_time = datetime.fromisoformat(last_follow_up)
                days_since_follow = (now - last_follow_time).days
                if days_since_follow < 2:
                    return False
            
            return True
        except Exception as e:
            print(f"Error checking follow-up eligibility: {e}")
            return False
    
    def send_outreach_messages(self):
        """Send outreach messages to PENDING contacts during business hours"""
        if not is_business_hours():
            print("Outside business hours, skipping outreach")
            return
        
        if not self.check_whatsapp_status():
            print("WhatsApp not connected")
            return
        
        # Reset contacted_today if it's a new day
        today = datetime.now().strftime('%Y-%m-%d')
        try:
            if os.path.exists(self.contacted_today_file):
                with open(self.contacted_today_file, 'r') as f:
                    data = json.load(f)
                    if data.get('date') != today:
                        self.contacted_today = set()
        except:
            self.contacted_today = set()
        
        # Get pending contacts that haven't been contacted today
        pending_contacts = self.get_contacts_by_status('PENDING')
        pending_contacts = [c for c in pending_contacts if c['contact'] not in self.contacted_today]
        
        if not pending_contacts:
            print("No pending contacts to reach out to")
            return
        
        print(f"Found {len(pending_contacts)} pending contacts to reach out to")
        
        # Send to ONE contact per run (restart-friendly approach)
        contact_info = pending_contacts[0]  # Take the first pending contact
        phone_number = contact_info['contact']
        
        # Generate and send initial message
        message = self.generate_ai_message("initial_outreach")
        
        if self._send_message(phone_number, message):
            # Update contact status
            self.update_contact_status(
                phone_number, 
                'AWAITING_NAME', 
                message_sent_at=datetime.now().isoformat()
            )
            
            # Mark as contacted today
            self.contacted_today.add(phone_number)
            self.save_contacted_today()
            
            print(f"Outreach sent to {phone_number}")
            
            # Wait 5-20 minutes before allowing next outreach (this delay is preserved even if script restarts)
            delay = random.randint(self.outreach_delay_range[0], self.outreach_delay_range[1])
            print(f"Waiting {delay//60} minutes before next outreach can be sent...")
            time.sleep(delay)
        else:
            print(f"Failed to send outreach to {phone_number}")
    
    def send_follow_up_messages(self):            
        try:
            awaiting_contacts = self.get_contacts_by_status('AWAITING_NAME') + self.get_contacts_by_status('AWAITING_LOCATION')
            
            for contact_info in awaiting_contacts:
                if self.should_send_follow_up(contact_info):
                    phone_number = contact_info['contact']
                    customer_name = contact_info.get('customer_name', '')
                    
                    follow_up_msg = self.generate_ai_message("follow_up", customer_name)
                    
                    if self._send_message(phone_number, follow_up_msg):
                        self.update_contact_status(
                            phone_number, 
                            contact_info['status'],
                            last_follow_up=datetime.now().isoformat()
                        )
                        print(f"Follow-up sent to {phone_number}")
                        
                        # Wait 1-3 minutes between follow-ups
                        delay = random.randint(self.reply_delay_range[0], self.reply_delay_range[1])
                        time.sleep(delay)
                        
        except Exception as e:
            print(f"Error sending follow-up messages: {e}")
    
    def delayed_reply(self, phone_number, message, delay):
        def send_after_delay():
            time.sleep(delay)
            self._send_message(phone_number, message)
        
        thread = threading.Thread(target=send_after_delay)
        thread.daemon = True
        thread.start()

    def process_incoming_messages(self):
        self.is_running = True
        
        try:
            while self.is_running:
                try:
                    if not self.check_whatsapp_status():
                        time.sleep(30)
                        continue

                    response = requests.get(f"{self.get_whatsapp_url()}/messages?limit=200", timeout=10)
                    if response.status_code != 200:
                        time.sleep(30)
                        continue
                    
                    messages_data = response.json()
                    messages = messages_data.get('messages', [])

                    active_contacts = {}
                    for status in ['AWAITING_NAME', 'AWAITING_LOCATION']:
                        contacts = self.get_contacts_by_status(status)
                        for contact in contacts:
                            active_contacts[contact['contact']] = contact

                    for message_data in messages:
                        if message_data.get('fromMe', False):
                            continue
                            
                        msg_id = message_data.get('id')
                        if msg_id in self.processed_messages:
                            continue
                        
                        phone_number = message_data.get('from', '').replace('@c.us', '')
                        message_body = message_data.get('body', '')
                        message_type = message_data.get('type', '')
                        
                        if phone_number not in active_contacts:
                            self.processed_messages.add(msg_id)
                            continue
                        
                        contact_info = active_contacts[phone_number]
                        current_status = contact_info['status']
                        customer_name = contact_info.get('customer_name', '')

                        if message_type == 'location':
                            if current_status == 'AWAITING_LOCATION' and customer_name:
                                location_data = self.check_location_for_contact(phone_number)
                                if location_data:
                                    self.save_location_data(phone_number, customer_name, location_data)
                                    
                                    self.update_contact_status(
                                        phone_number,
                                        'COMPLETED',
                                        location_received_at=datetime.now().isoformat()
                                    )

                                    delay = random.randint(self.reply_delay_range[0], self.reply_delay_range[1])
                                    thank_you_msg = self.generate_ai_message("completion", customer_name)
                                    self.delayed_reply(phone_number, thank_you_msg, delay)

                        elif current_status == 'AWAITING_NAME' and message_body:
                            extracted_name = self.analyze_message_for_name(message_body)
                            
                            if extracted_name:
                                self.update_contact_status(
                                    phone_number,
                                    'AWAITING_LOCATION',
                                    customer_name=extracted_name,
                                    name_collected_at=datetime.now().isoformat()
                                )

                                delay = random.randint(self.reply_delay_range[0], self.reply_delay_range[1])
                                location_msg = self.generate_ai_message("location_request", extracted_name)
                                self.delayed_reply(phone_number, location_msg, delay)
                            else:
                                delay = random.randint(self.reply_delay_range[0], self.reply_delay_range[1])
                                response_msg = self.generate_ai_message("redirect_to_support")
                                self.delayed_reply(phone_number, response_msg, delay)

                        elif current_status == 'AWAITING_LOCATION' and message_body:
                            delay = random.randint(self.reply_delay_range[0], self.reply_delay_range[1])
                            response_msg = self.generate_ai_message("redirect_to_support")
                            self.delayed_reply(phone_number, response_msg, delay)
                        
                        self.processed_messages.add(msg_id)
                    
                    self.save_processed_messages()
                    time.sleep(10)
                    
                except KeyboardInterrupt:
                    break
                except Exception as e:
                    print(f"Error in message processing: {e}")
                    time.sleep(30)
        finally:
            self.is_running = False
    
    def run_outreach_loop(self):
        """Main outreach loop - continuously sends messages to pending contacts"""
        while self.is_running:
            try:
                print("Running outreach cycle...")
                self.send_outreach_messages()
                
                # Send follow-ups occasionally
                if random.random() < 0.3:  # 30% chance to check follow-ups
                    self.send_follow_up_messages()
                
                # Short wait before checking for more contacts (restart-friendly)
                print("Waiting 2 minutes before next check...")
                time.sleep(10)  # 2 minutes - allows script to restart without losing progress
                
            except KeyboardInterrupt:
                break
            except Exception as e:
                print(f"Error in outreach loop: {e}")
                time.sleep(120)  # Wait 2 minutes on error

def main():
    os.chdir(os.path.join(os.path.abspath(os.curdir),'whatsappbot'))

    openai_key = load_settings()['openai_api_key']
    while not openai_key:
        print("Waiting for OpenAI API key...")
        openai_key = load_settings()['openai_api_key']
        time.sleep(30)
    
    collector = EnhancedWhatsAppCollector()
    
    txt_file = 'contacts.txt'
    if not os.path.exists(txt_file):
        print("contacts.txt not found")
        return
    
    contacts = collector.load_contacts_from_txt(txt_file)
    if not contacts:
        print("No contacts found in contacts.txt")
        return
    
    print(f"Loaded {len(contacts)} contacts")
    collector.create_contact_status_csv(contacts)
    
    # Start both threads
    def message_processor():
        print("Starting message processor...")
        collector.process_incoming_messages()

    def outreach_processor():
        print("Starting outreach processor...")
        collector.run_outreach_loop()

    message_thread = threading.Thread(target=message_processor, daemon=True)
    outreach_thread = threading.Thread(target=outreach_processor, daemon=True)
    
    collector.is_running = True
    
    message_thread.start()
    time.sleep(5)  # Start outreach after message processor
    outreach_thread.start()
    
    print("Bot started successfully!")
    print("- Message processing: Running")
    print("- Outreach system: Running") 
    print("- Business hours: 9 AM - 6 PM")
    print("- Outreach delay: 5-20 minutes")
    print("- Reply delay: 1-3 minutes")
    
    try:
        while message_thread.is_alive() or outreach_thread.is_alive():
            time.sleep(30)
    except KeyboardInterrupt:
        print("Shutting down...")
        collector.is_running = False

if __name__ == "__main__":
    main()