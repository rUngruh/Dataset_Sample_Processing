######################################################################

# This script processes interactions from the Book-Crossing dataset.
# It filters out items without genres and saves the files in a format consistent with other datasets used in the project.

######################################################################

import pandas as pd
import os


import argparse
argparser = argparse.ArgumentParser(description="Process Book-Crossing.")
argparser.add_argument('--dataset_dir', type=str, help="Path to the dataset directory.", default='data')
args = argparser.parse_args()
dataset_dir = args.dataset_dir

bx_directory = dataset_dir + '/raw/Book-Crossing'
processed_directory = dataset_dir + '/processed/Book-Crossing'

books_path = os.path.join(processed_directory, 'books_w_genre.tsv')
books_save_path = os.path.join(processed_directory, 'books.tsv')
users_path = os.path.join(bx_directory, 'BX-Users.csv')
users_save_path = os.path.join(processed_directory, 'users.tsv')
interactions_path = os.path.join(bx_directory, 'BX-Book-Ratings.csv')
interactions_save_path = os.path.join(processed_directory, 'interactions.tsv.bz2')

print("Loading books...")
interactions = pd.read_csv(interactions_path, sep=';', encoding='latin-1') # "User-ID";"ISBN";"Book-Rating"
interactions.rename(columns={'ISBN': 'item_id', 'User-ID' : 'user_id', 'Book-Rating':'rating'}, inplace=True)

users = pd.read_csv(users_path, sep=';', encoding='latin-1') # "User-ID";"Location";"Age"
users.rename(columns={'User-ID': 'user_id', 'Location' : 'location', 'Age':'age'}, inplace=True)

books = pd.read_csv(books_path, sep='\t') #item_id, genres

print(f'Number of ratings in Bookcrossing: {len(interactions)}')
print(f"Number of users in Bookcrossing: {len(users)}")

interactions = interactions[interactions['item_id'].isin(books['item_id'])].copy()
valid_users = users[(users['age'].notna()) & (users['age'] > 11) & (users['age'] < 65)]['user_id'].unique()
interactions = interactions[interactions['user_id'].isin(valid_users)].copy()


users = users[users['user_id'].isin(interactions['user_id'])].copy()
books = books[books['item_id'].isin(interactions['item_id'])].copy()

print(f'Number of ratings in filtered Bookcrossing: {len(interactions)}')
print(f'Number of users in filtered Bookcrossing: {len(users)}')
print(f'Number of books in filtered Bookcrossing: {len(books)}')

interactions.to_csv(interactions_save_path, sep='\t', index=False, compression='bz2' if interactions_save_path.endswith('bz2') else None, header=False)
users.to_csv(users_save_path, sep='\t', index=False, header=True)
books.to_csv(books_save_path, sep='\t', index=False, header=True)

print("Saved all sets")