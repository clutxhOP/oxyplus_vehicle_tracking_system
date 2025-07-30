import requests
import pandas as pd
import time
import json
import os
import re
import csv
from datetime import datetime, timedelta
from typing import Dict, List, Optional

def load_settings():
    json_path = "../config_data/app_settings.json"
    with open(json_path, 'r', encoding='utf-8') as f:
        return json.load(f)

class IntelligentWhatsAppCollector:
    def __init__(self):
        self.contact_status_file = "contact_status.csv"
        self.extracted_data_file = "extracted_data.csv"
        self.processed_messages_file = "processed_messages.json"
        
        self.processed_messages = self.load_processed_messages()
        self._initialize_csv_files()
        print("Intelligent WhatsApp Collector initialized")
    
    def get_openai_client(self):
        openai_key = load_settings()['openai_api_key']
        if openai_key:
            import openai
            return openai.OpenAI(api_key=openai_key)
        return None
    
    def get_whatsapp_url(self):
        return load_settings()['whatsapp_server_url'].rstrip('/')
    
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
            with open(self.processed_messages_file, 'w') as f:
                json.dump({'processed_ids': list(self.processed_messages)}, f)
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
                contacts = [line.strip() for line in f if line.strip()]
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
    
    def send_initial_location_request(self, phone_number: str) -> bool:
        message = """Hello! This is OxyPlus Water Delivery

We need to coordinate your premium water service. Please help us with:

1️⃣ Your name for our records
2️⃣ Your current location using WhatsApp's location feature

📍 Tap attachment → Location → Send Current Location

This helps us deliver fresh water directly to your door!

What's your name?"""
        
        return self._send_message(phone_number, message)
    
    def send_location_request_after_name(self, phone_number: str, customer_name: str) -> bool:
        message = f"""Thank you {customer_name}! 

Now please share your current location using WhatsApp's location feature:

📍 Tap the attachment button
📍 Select "Location" 
📍 Choose "Send Current Location"

This helps our delivery team find you quickly and ensure timely service!"""
        
        return self._send_message(phone_number, message)
    
    def generate_intelligent_response(self, message_text: str, phone_number: str, customer_name: str = "") -> str:
        openai_client = self.get_openai_client()
        if not openai_client:
            return self.get_fallback_response(message_text, customer_name)
        
        try:
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

If they refuse or have privacy concerns, be understanding but persistent.
If they ask questions, answer briefly and redirect to sharing info.
If they seem confused, explain clearly what we need.
Also tell them doing location and then name won't work the proper order is name first and location.
"""
            
            response = openai_client.chat.completions.create(
                model="gpt-4o-mini",
                messages=[{"role": "user", "content": prompt}],
                max_tokens=100,
                temperature=1.0
            )
            
            return response.choices[0].message.content.strip()
            
        except Exception as e:
            print(f"Error with OpenAI response generation: {e}")
            return self.get_fallback_response(message_text, customer_name)
    
    def get_fallback_response(self, message_text: str, customer_name: str = "") -> str:
        message_lower = message_text.lower()
        
        if any(word in message_lower for word in ['privacy', 'why location', 'personal', 'safe']):
            return """We completely understand your privacy concerns! 

OxyPlus only uses your location to deliver water to your exact address. Your location is NEVER shared with third parties and is only used for delivery coordination.

This is completely safe and secure. Would you like to proceed with sharing your location?"""
        
        elif any(word in message_lower for word in ['who is this', 'scam', 'fake', 'spam', 'suspicious']):
            return """This is 100% legitimate - we're OxyPlus Water Delivery!

✅ Licensed water delivery service in UAE
✅ Visit our website: oxypluswater.com  
✅ Thousands of satisfied customers

We only need your location for delivery purposes. Completely safe and legitimate service!"""
        
        elif any(word in message_lower for word in ['what', 'dont understand', 'confused', 'explain']):
            return """Let me explain clearly:

🏢 OxyPlus = Premium Water Delivery Service
📍 We need location = To deliver water to your address
💧 What we do = Bring fresh drinking water to your door

Just share your name and location using WhatsApp's location feature to get started!"""
        
        elif any(word in message_lower for word in ['no', 'not interested', 'stop', 'remove', 'dont want', 'wont', 'aint']):
            return """We understand your hesitation! 

OxyPlus provides premium water delivery service across UAE. We only need location to serve you better - completely safe and only used for delivery.

Many customers were initially hesitant but are now happy with our service. Would you like to give us a try?"""
        
        elif any(word in message_lower for word in ['how much', 'price', 'cost', 'what service', 'expensive']):
            return """Great question! OxyPlus offers:

💧 Premium filtered water delivery
🚚 Fast delivery across UAE  
💰 Competitive and transparent pricing
⚡ Same-day delivery available

To see our complete service menu and pricing, please share your name and location first!"""
        
        else:
            if customer_name:
                return f"""Thank you for your message {customer_name}! 

To proceed with your OxyPlus water delivery, we just need your location using WhatsApp's location feature.

📍 Tap attachment → Location → Send Current Location

This helps us coordinate your delivery efficiently!"""
            else:
                return """Thank you for your message! 

To proceed with OxyPlus water delivery service, we need:
1️⃣ Your name 
2️⃣ Your location (using WhatsApp location feature)

Could you please share your name first?"""
    
    def analyze_message_for_name(self, message_text: str) -> Optional[str]:
        if not message_text or len(message_text.strip()) < 2:
            return None
        
        message_clean = message_text.strip()
        message_lower = message_clean.lower()
        
        ignore_patterns = [
            'what', 'why', 'when', 'where', 'how', 'ok', 'yes', 'no', 'sure',
            'location', 'address', 'deliver', 'water', 'service', 'oxypluswater',
            'hello', 'hi', 'thanks', 'thank you', 'please', 'help', 'understand',
            'privacy', 'scam', 'fake', 'spam', 'stop', 'remove', 'price', 'cost'
        ]
        
        if any(pattern in message_lower for pattern in ignore_patterns):
            return None
        
        if len(message_clean) > 50:
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
"Ahmad Ali" → Ahmad Ali"""
                
                response = openai_client.chat.completions.create(
                    model="gpt-4o-mini",
                    messages=[{"role": "user", "content": prompt}],
                    max_tokens=50,
                    temperature=1.0
                )

                result = response.choices[0].message.content.strip()
                print("OPEN AI RESPONSE GENERATED")
                if result and result != "NO_NAME" and 2 <= len(result) <= 30:
                    return result.title()
                    
            except Exception as e:
                print(f"Error with OpenAI name extraction: {e}")
        
        words = message_clean.split()
        if len(words) == 1 and 2 <= len(words[0]) <= 20 and words[0].isalpha():
            return words[0].title()
        elif len(words) == 2 and all(2 <= len(word) <= 15 and word.replace('-', '').isalpha() for word in words):
            return ' '.join(word.title() for word in words)
        
        name_patterns = [
            r'(?:my name is|i am|this is|call me)\s+([a-zA-Z\s]{2,30})',
            r'^([A-Z][a-z]+(?:\s+[A-Z][a-z]+)?)$'
        ]
        
        for pattern in name_patterns:
            match = re.search(pattern, message_clean, re.IGNORECASE)
            if match:
                potential_name = match.group(1).strip().title()
                if 2 <= len(potential_name) <= 30:
                    return potential_name
        
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
            
            row_data = {
                'customer_name': customer_name,
                'latitude': location_info.get('latitude', ''),
                'longitude': location_info.get('longitude', ''),
                'contact': phone_number,
                'location_description': location_info.get('description', ''),
                'timestamp': location_info.get('timestamp', datetime.now().isoformat())
            }
            
            df = pd.DataFrame([row_data])
            if os.path.exists(self.extracted_data_file):
                df.to_csv(self.extracted_data_file, mode='a', header=False, index=False)
            else:
                df.to_csv(self.extracted_data_file, index=False)
            
            print(f"LOCATION SAVED: {customer_name} ({phone_number})")
        except Exception as e:
            print(f"Error saving location data: {e}")
    
    def send_messages_to_pending_contacts(self):
        if not self.check_whatsapp_status():
            print("WhatsApp not connected - cannot send messages")
            return
        
        pending_contacts = self.get_contacts_by_status('PENDING')
        print(f"Found {len(pending_contacts)} pending contacts")
        
        for contact_info in pending_contacts:
            phone_number = contact_info['contact']
            print(f"Sending initial message to {phone_number}")
            
            if self.send_initial_location_request(phone_number):
                self.update_contact_status(
                    phone_number, 
                    'AWAITING_NAME', 
                    message_sent_at=datetime.now().isoformat()
                )
                time.sleep(3)
        
        print("Finished sending initial messages")
    
    def process_incoming_messages(self):
        print("Starting message monitoring - only processing NEW messages")
        
        while True:
            try:
                if not self.check_whatsapp_status():
                    print("WhatsApp not connected - waiting for connection")
                    time.sleep(30)
                    continue
                
                response = requests.get(f"{self.get_whatsapp_url()}/messages?limit=20", timeout=10)
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
                
                new_messages_processed = 0
                
                for message in messages:
                    msg_id = message.get('id')
                    
                    if msg_id in self.processed_messages or message.get('fromMe', False):
                        continue
                    
                    phone_number = message.get('from', '').replace('@c.us', '')
                    message_body = message.get('body', '')
                    message_type = message.get('type', '')
                    
                    if phone_number in active_contacts:
                        contact_info = active_contacts[phone_number]
                        current_status = contact_info['status']
                        customer_name = contact_info.get('customer_name', '')
                        
                        print(f"NEW MESSAGE from {phone_number} ({current_status}): {message_body[:100]}")
                        
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
                                    self._send_message(phone_number, thank_you_msg)
                                    
                                    print(f"COMPLETED: {customer_name} ({phone_number})")
                        
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
                            else:
                                response_msg = self.generate_intelligent_response(message_body, phone_number, customer_name)
                                self._send_message(phone_number, response_msg)
                                print(f"INTELLIGENT RESPONSE sent to {phone_number}")
                        
                        elif current_status in ['AWAITING_LOCATION', 'COLLECTING_LOCATION'] and message_body:
                            response_msg = self.generate_intelligent_response(message_body, phone_number, customer_name)
                            self._send_message(phone_number, response_msg)
                            print(f"INTELLIGENT RESPONSE sent to {phone_number}")
                        
                        self.processed_messages.add(msg_id)
                        new_messages_processed += 1
                
                if new_messages_processed > 0:
                    self.save_processed_messages()
                    print(f"Processed {new_messages_processed} new messages")
                
                self.send_reminders_to_stalled_contacts()
                time.sleep(20)
                
            except KeyboardInterrupt:
                print("Monitoring stopped by user")
                break
            except Exception as e:
                print(f"Error in message processing: {e}")
                time.sleep(30)
    
    def send_reminders_to_stalled_contacts(self):
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
                            
                            self._send_message(phone_number, reminder_msg)
                            self.update_contact_status(phone_number, 'COLLECTING_LOCATION')
                            print(f"REMINDER sent to {customer_name}")
                    except:
                        pass
        except Exception as e:
            print(f"Error sending reminders: {e}")
    
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
                'locations_extracted': extracted_count
            }
        except Exception as e:
            print(f"Error getting status summary: {e}")
            return {}

def main():

    print(f"Absolute path : {os.path.abspath(os.curdir)}")
    os.chdir(os.path.join(os.path.abspath(os.curdir),'whatsappbot'))
    openai_key = load_settings()['openai_api_key']
    if not openai_key:
        print("No OpenAI API key found. Using fallback responses only.")
    collector = IntelligentWhatsAppCollector()
    
    txt_file = 'contacts.txt'
    if not os.path.exists(txt_file):
        print(f"Please create {txt_file} with phone numbers (one per line)")
        return
    
    contacts = collector.load_contacts_from_txt(txt_file)
    if not contacts:
        print("No contacts loaded")
        return
    
    collector.create_contact_status_csv(contacts)
    
    print("Starting intelligent workflow")
    collector.send_messages_to_pending_contacts()
    
    print("Starting message monitoring - Press Ctrl+C to stop")
    collector.process_incoming_messages()

if __name__ == "__main__":
    main()