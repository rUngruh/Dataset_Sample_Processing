######################################################################

# This script filters the listening events of users based on the genres of artists.
# It retains only those listening events where the artist's genre is present in the provided list of genres.
# The script also calculates the age of users at the time of interaction and saves the filtered data to a new file.
# Further, only listening events within a specified date range are retained.

######################################################################

import os
import pandas as pd
import numpy as np
import argparse


parser = argparse.ArgumentParser(description='Filter listening events by genre')
parser.add_argument('--start_date', type=str, default='2009-01-01', help='Start date for filtering (YYYY-MM-DD)')
parser.add_argument('--end_date', type=str, default='2013-12-31', help='End date for filtering (YYYY-MM-DD)')
parser.add_argument('--dataset_dir', type=str, help="Path to the dataset directory. Only needed if not using the .env file.", default='data')

args = parser.parse_args()

start_date = pd.to_datetime(args.start_date)
end_date = pd.to_datetime(args.end_date)
dataset_dir = args.dataset_dir


sample_directory = dataset_dir + '/processed/MLHD_sampled'
sample_filtered_directory = dataset_dir + '/processed/MLHD_sampled_filtered'

listening_events_save_path = os.path.join(sample_filtered_directory, 'interactions_verbose.tsv.bz2')
artists_allmusic_path = os.path.join(sample_directory, 'artists_AM_genres.tsv')
artists_save_path = os.path.join(sample_filtered_directory, 'artists_verbose.tsv')
les_paths = [os.path.join(sample_directory, f'listening_events-{i}.tsv.bz2') for i in list(range(0, 10)) + list(map(chr, range(ord('a'), ord('f')+1)))]
users_path = os.path.join(sample_directory, 'users.tsv')
users_save_path = os.path.join(sample_filtered_directory, 'users_verbose.tsv')

le_save_paths = [os.path.join(sample_filtered_directory, f'interactions_verbose-{i}.tsv.bz2') for i in list(range(0, 10)) + list(map(chr, range(ord('a'), ord('f')+1)))]


data_collection_date = pd.to_datetime('2014-01-01') # This date is the day we assume each user turned the reported age


batch_size = 10000000
compressed = True if listening_events_save_path.endswith('.bz2') else False

artists_w_genres = pd.read_csv(artists_allmusic_path, sep="\t")
artists = set(artists_w_genres['artist_id'].unique())
del artists_w_genres

users_df = pd.read_csv(users_path, sep="\t")
user_age_dict = dict(zip(users_df['user_id'], users_df['age']))

if not os.path.exists(sample_filtered_directory):
    os.makedirs(sample_filtered_directory)

if os.path.exists(listening_events_save_path):
    os.remove(listening_events_save_path)

for save_path in le_save_paths:
    if os.path.exists(save_path):
        os.remove(save_path)

seen_users = set()
seen_artists = set()

def custom_year_diff(date, base_date):
    # Calculate the year difference based on the base_date year
    year_diff = date.year - base_date.year
    
    # Determine if the date is before the custom year start in the given year
    current_year_start = pd.Timestamp(year=date.year, month=base_date.month, day=base_date.day)
    if date <= current_year_start:
        year_diff -= 1
    return float(year_diff)



def filter_artists(artist_str):
    return ','.join([artist for artist in artist_str.split(',') if artist in artists])

print(f'Loading listening events in batches...')
for path, save_path in zip(les_paths, le_save_paths):
    # load listening events in batches
    for i, chunk in enumerate(pd.read_csv(path, sep="\t", chunksize=batch_size, header=None, compression='bz2' if path.endswith('.bz2') else None)):
        if i == 0:
            print(chunk.head(), path)
        if i % 1 == 0:
            print(f'Processing chunk {i}...')
        chunk.columns = ['user_id', 'timestamp', 'artist_id', 'item_id']
        chunk['timestamp'] = pd.to_datetime(chunk['timestamp'], unit='s')
        # Filter out items where we don't have an artist with matching genre for, and where the time of listen is not in the range of interest
        
        chunk['artist_id'] = chunk['artist_id'].apply(lambda x: filter_artists(x))
        
        chunk = chunk[(chunk['artist_id'] != '') & 
                      (chunk['timestamp'] >= start_date) & (chunk['timestamp'] <= end_date)].copy()
        
        
        if chunk.empty:
            continue
        data_collection_date = pd.to_datetime('2014-01-01') # This is the age returned by user_age_dict and the day a user turned the given age. chunk['timestamp'] is the time of listen.
        
        chunk.loc[:, 'age_at_interaction'] = chunk['timestamp'].map(lambda date: custom_year_diff(date, data_collection_date)) + \
            chunk['user_id'].map(user_age_dict)
            
        chunk = chunk[chunk['age_at_interaction'].notna()]
        
        seen_users.update(chunk['user_id'].unique())
        
        chunk_artists = set(
            artist
            for artist_list in chunk['artist_id'].dropna().str.split(',')
            for artist in artist_list
        )
        seen_artists.update(chunk_artists)
        
        chunk.to_csv(save_path, mode='a', sep='\t', index=False, header=False, compression='bz2' if compressed else None)
        print(f'Processed chunk {i} and saved to {save_path}')
        
if os.path.exists(users_save_path):
    os.remove(users_save_path)

if os.path.exists(artists_save_path):
    os.remove(artists_save_path)

users_df = pd.read_csv(users_path, sep="\t")
users_df = users_df[users_df['user_id'].isin(seen_users)]
users_df.to_csv(users_save_path, sep="\t", index=False)

artists_df = pd.read_csv(artists_allmusic_path, sep="\t")
artists_df = artists_df[artists_df['artist_id'].isin(seen_artists)]
artists_df.rename(columns={'am_genres': 'genres'}, inplace=True)
artists_df.to_csv(artists_save_path, sep="\t", index=False)