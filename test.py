import os
import pandas as pd
whatsapp_dir = 'whatsappbot'
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
        how='inner'
    )
    print(merged_df.head())