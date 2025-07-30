import pandas as pd
from geopy.distance import geodesic
from datetime import datetime, timedelta
days = 70
df = pd.read_csv(
    "data/travelreport/history.csv",
    dtype={
        'Vehicle No': str,
        'Status': 'category',
        'Address': str,
        'Speed': float,
        'Odometer': float,
        'Panic': str,
        'Latitude': float,
        'Longitude': float
    },
    parse_dates=['DateTime'],
    low_memory=False
)
print("Data loaded successfully")
cutoff_date = datetime.now() - timedelta(days=days)
print(f"cutoff_date is : {cutoff_date}")
df = df[df['DateTime'] >= cutoff_date]
print("cutoff date applied to dataframe")
df.to_csv('data/travelreport/history.csv', index=False)