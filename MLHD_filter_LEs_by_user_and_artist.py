######################################################################

# This script samples the listening events from the MLHD dataset based on the sampled users and artists.
# Make sure to add the sampled users and artists from the Samples directory to the data directory as specified in the README.
# Running this script with the specified samples avoids the need to re-run the sampling and genre annotation process.

######################################################################


import os
import pandas as pd
import zstandard as zstd
import io
from pathlib import Path
import tarfile

import argparse

argparser = argparse.ArgumentParser(description="Filter MLHD listening events by user and artist.")
argparser.add_argument('--dataset_dir', type=str, help="Path to the dataset directory.", default='data')
argparser.add_argument('-start_fresh', action='store_true', help="If set, will start fresh and ignore the savestate file.")
argparser.add_argument('-sample_les', action='store_true', help="If set, will sample the listening events.")
argparser.add_argument('-save_users', action='store_true', help="If set, will save the sampled users to a file.")
argparser.add_argument('-save_artists', action='store_true', help="If set, will save the sampled artists to a file.")

args = argparser.parse_args()

dataset_dir = args.dataset_dir
sample_les = args.sample_les
combine_les = args.combine_les
save_users = args.save_users
save_artists = args.save_artists
start_fresh = args.start_fresh

raw_directory = dataset_dir + '/raw/MLHD+'
sample_directory = dataset_dir + '/processed/MLHD_sampled'
processed_directory = dataset_dir + '/processed/MLHD_sampled'

raw_user_path = os.path.join(raw_directory, 'MLHD_demographics.csv')
artist_path = os.path.join(processed_directory, 'artists.tsv')
user_path = os.path.join(processed_directory, 'users.tsv')

listening_events_path = os.path.join(sample_directory, 'listening_events.tsv')
savestate_path = os.path.join(sample_directory, 'processing_savestate.txt')

artists_allmusic_path = os.path.join(sample_directory, 'artists_AM_genres.tsv')
users_save_path = os.path.join(sample_directory, 'users.tsv')

sampled_users = set(pd.read_csv(user_path, sep="\t")['user_id'].unique())
sampled_artists = set(pd.read_csv(artist_path, sep="\t")['artist_id'].unique())

mlhd_datasets = [os.path.join(raw_directory, f'mlhdplus-complete-{i}.tar') for i in list(range(0, 10)) + list(map(chr, range(ord('a'), ord('f')+1)))]
save_paths = [os.path.join(sample_directory, f'listening_events-{i}.tsv.bz2') for i in list(range(0, 10)) + list(map(chr, range(ord('a'), ord('f')+1)))]

if start_fresh:
    if os.path.exists(savestate_path):
        os.remove(savestate_path)
        print(f"Removed existing savestate file: {savestate_path}")
    for path in save_paths:
        if os.path.exists(path):
            os.remove(path)
            print(f"Removed existing listening events file: {path}")
    else:
        print(f"No existing savestate file found: {savestate_path}")

print("Loaded necessary files and directories.")
print("Sampled users:", len(sampled_users))
print("Sampled artists:", len(sampled_artists))

found_users = 0

processed = set()

if os.path.exists(savestate_path):
    with open(savestate_path, 'r') as f:
        processed = set(line.strip() for line in f if line.strip())
        print(f"Loaded {len(processed)} processed members from savestate file.")
        
        
def process_tar_file(tar_path, save_path):
    tar_name = os.path.basename(tar_path)
    global found_users
    
    print(f"Processing {tar_name}...")

    with tarfile.open(tar_path, 'r') as tar:
        write_lines = []
        savestate_members = []

        
        for member in tar:
            
            if member.name in processed:
                continue
            
            if not member.name.endswith('.txt.zst'):
                continue
            
            savestate_members.append(member)
            user_id = member.name.split('.')[0].split('/')[-1]
            
            if user_id in sampled_users: # Check if user_id is in sampled users
                #print(f"  -> {user_id}")
                found_users += 1
                if found_users % 100 == 0:
                    print(f"    {found_users} users out of {len(sampled_users)} found, i.e. {found_users/len(sampled_users)*100:.2f}%")
                f = tar.extractfile(member)
                if f is None:
                    print(f"    Error extracting {member.name}")
                    continue

                dctx = zstd.ZstdDecompressor()
                with dctx.stream_reader(f) as reader:
                    text_stream = io.TextIOWrapper(reader, encoding='utf-8')
                    for i, line in enumerate(text_stream):   
                        item = [user_id] + line.strip().split('\t')
                        artists = item[2].split(',')  # Split artist_id string into a list
                        # Check if any artist_id in the list is in sampled artists; for initial processing, further steps are accomplished in "MLHD_filter_LEs_by_genre.py"
                        if any(artist in sampled_artists for artist in artists):              
                            write_lines.append(item[:3] + item[4:]) # Saved columns: user_id, timestamp, artist_id, item_id

            
            if len(savestate_members) >= 10000:
                print(f'Saving {len(savestate_members)} members to savestate...')
                
                with open(savestate_path, 'a') as log:
                    for member in savestate_members:
                        log.write(member.name + '\n')
                savestate_members = []

                print(f'Saving {len(write_lines)} lines to {listening_events_path}...')
                if write_lines:
                    save_batch(write_lines, save_path, compressed=True)
                    write_lines = []
                
        print('Last batch processing...')
        print(f'Saving {len(savestate_members)} members to savestate...')
        if savestate_members:
            with open(savestate_path, 'a') as log:
                for member in savestate_members:
                    log.write(member.name + '\n')
                    
        print(f'Saving {len(write_lines)} lines to {listening_events_path}...')
        if write_lines:
            save_batch(write_lines, save_path, compressed=True)
            write_lines = []
        savestate_members = []


def save_batch(lines, save_path, compressed=True):
    # mode = 'at' if compressed else 'a'
    # open_func = bz2.open if compressed else open
    
    lines_df = pd.DataFrame(lines)
    lines_df.to_csv(save_path, mode='a', sep='\t', index=False, header=False, compression='bz2' if compressed else None)

    # with open_func(listening_events_path, mode, encoding='utf-8') as out_file:
    #     for line in lines:
    #         out_file.write(line + '\n')
            
    print('Saved batch.')

if sample_les:
    print("Starting batch processing")
    for tar_file, save_file in zip(mlhd_datasets, save_paths):
        process_tar_file(tar_file, save_file)
        print(f"Finished processing {tar_file}.")
    print("Processed all datasets.")
    
if save_users:
    print('Saving sampled users...')
    raw_users = pd.read_csv(raw_user_path, sep="\t")
    raw_users = raw_users[raw_users['uuid'].isin(sampled_users)]
    sampled_users = sampled_users[['uuid', 'age', 'country', 'gender']]
    sampled_users.rename(columns={'uuid':'user_id'}, inplace=True)
    sampled_users.to_csv(users_save_path, sep="\t", index=False)
    print('Saved sampled users to:', users_save_path)
    
if save_artists:
    print('Saving sampled artists...')
    artists = pd.read_csv(artist_path, sep="\t")
    artists_allmusic = artists[(artists['am_genres'].notnull()) & (artists['am_genres'] != '')] 
    artists_allmusic = artists_allmusic[['artist_id', 'am_genres']]
    artists_allmusic.to_csv(artists_allmusic_path, sep="\t", index=False)
    print('Saved sampled artists to:', artists_allmusic_path)