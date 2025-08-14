import pandas as pd
from geopy.distance import geodesic
from datetime import datetime, timedelta

def preprocess_everything(days: int = 70):
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
    print("data/travelreport/history.csv file truncated for the last {day} days.")
    df = df[df['Status'].isin(['Stopped', 'Idle'])].copy()

    df = df.sort_values(by=['Vehicle No', 'DateTime']).reset_index(drop=True)

    df['GroupChange'] = (
        (df['Vehicle No'] != df['Vehicle No'].shift()) |
        (df['Status'] != df['Status'].shift()) |
        (df['Latitude'] != df['Latitude'].shift()) |
        (df['Longitude'] != df['Longitude'].shift())
    ).cumsum()
    print("Done grouping")
    collapsed = df.groupby('GroupChange').agg({
        'Vehicle No': 'first',
        'Status': 'first',
        'Latitude': 'first',
        'Longitude': 'first',
        'Address': 'first',
        'DateTime': ['first', 'last']
    })
    collapsed.columns = ['Vehicle No', 'Status', 'Latitude', 'Longitude', 'Address', 'StartTime', 'EndTime']
    collapsed = collapsed.reset_index(drop=True)

    collapsed['Duration'] = collapsed['EndTime'] - collapsed['StartTime']
    print("Collapsed all points time to find customer points")
    def split_by_day(row):
        results = []
        start, end = row['StartTime'], row['EndTime']
        current = start

        while current.date() < end.date():
            midnight = pd.Timestamp.combine(current.date() + pd.Timedelta(days=1), pd.Timestamp.min.time())
            results.append({
                **row,
                'StartTime': current,
                'EndTime': midnight,
                'Duration': midnight - current,
                'Date': current.date()
            })
            current = midnight

        results.append({
            **row,
            'StartTime': current,
            'EndTime': end,
            'Duration': end - current,
            'Date': current.date()
        })
        return results

    split_records = []
    for _, row in collapsed.iterrows():
        split_records.extend(split_by_day(row))

    final = pd.DataFrame(split_records)
    print("Splitted all the records")
    def assign_geo_clusters(group, radius_meters=25):
        if len(group) <= 1:
            group['GeoCluster'] = 0
            return group
        
        cluster_id = 0
        assigned = [None] * len(group)
        coords = group[['Latitude', 'Longitude']].values

        for i in range(len(group)):
            if assigned[i] is not None:
                continue

            assigned[i] = cluster_id
            for j in range(i + 1, len(group)):
                if assigned[j] is None:
                    dist = geodesic(coords[i], coords[j]).meters
                    if dist <= radius_meters:
                        assigned[j] = cluster_id
            cluster_id += 1

        group['GeoCluster'] = assigned
        return group
    print("Making geoclusters heaviest operation in entire deploy.")
    final = (
        final.groupby(['Vehicle No', 'Date', 'Status'], group_keys=False)
            .apply(assign_geo_clusters)
            .reset_index(drop=True)
    )
    print("Finally made geoclusters")
    final = final.groupby(['Vehicle No', 'Status', 'Date', 'GeoCluster']).agg({
        'Latitude': 'first',
        'Longitude': 'first', 
        'Address': 'first',
        'StartTime': 'min',
        'EndTime': 'max',
        'Duration': lambda x: pd.to_timedelta(x).sum()
    }).reset_index()

    final['StartTime'] = final['StartTime'].dt.strftime('%Y-%m-%d %H:%M:%S')
    final['EndTime'] = final['EndTime'].dt.strftime('%Y-%m-%d %H:%M:%S')
    
    final['Duration'] = pd.to_timedelta(final['Duration']).apply(
        lambda x: str(x).split(' ')[-1] if 'day' in str(x) else str(x)
    )
    print("Analysis updated now you can make fresh customer points by deleting old ones from settings.")
    final.to_csv("analysis/customerpoints/idlepoints.csv", index=False)
    return