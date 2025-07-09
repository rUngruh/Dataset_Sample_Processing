# Dataset Sample Processing
This repository provides data samples and code to create datasets suitable recommender systems research. This includes the sampling of larger datasets as well as the annotation of datasets with additional metadata.
At the moment, the following functionality is included:
- **MLHD+**: Processing the large-scale dataset [MLHD+](https://musicbrainz.org/doc/MLHD+) to create a smaller subset. Annotate artist information with information from https://musicbrainz.org/
- **BX**: Match ISBN's from [Book-Crossing](https://www.kaggle.com/datasets/syedjaferk/book-crossing-dataset) with those from Goodreads to annotate books with genres as defined by [Goodreads](https://cseweb.ucsd.edu/~jmcauley/datasets/goodreads.html).


# Data Gathering
Gather the publicly available datasets and add them to a directory `data` as structured below.
- [MLHD+](https://musicbrainz.org/doc/MLHD+) and the respective demographical data from the original [MLHD](https://ddmal.music.mcgill.ca/research/The_Music_Listening_Histories_Dataset_(MLHD)/)
- [Book-Crossing](https://www.kaggle.com/datasets/syedjaferk/book-crossing-dataset)

```
├── data
│   ├── raw
│   │   ├── MLHD+
│   │   │   ├── MLHD_demographics
│   │   │   ├── mlhdplus-complete-0.tar
│   │   │   ├── mlhdplus-complete-1.tar
│   │   │   ├── ...
│   │   │   ├── mlhdplus-complete-f.tar
│   │   ├── Book-Crossing
│   │   │   ├── BX-Book-Ratings
│   │   │   ├── BX-Books
│   │   │   ├── BX-Users
│   ├── processed
│   │   ├── MLHD_processed
│   │   │   ├── artists.tsv
│   │   │   ├── users.tsv
│   │   ├── Book-Crossing
│   │   │   ├── books_w_genre.tsv
├── ...
```

# Environment Preparation
```
conda create -n datasetProcessing PYTHON=3.12
conda install pip
pip install -r requirements.txt
```


## MLHD
The MLHD dataset used in the [RecSys 2025 paper](https://doi.org/10.1145/3705328.3748160). This is a subset of the original MLHD+ dataset which closely reflects ages in the original set and includes genre information of each artist.
For the code used to generate the data samples, we refer to [the original code](https://github.com/rUngruh/2025_RecSys_Reproducibility)

### Process Datasets
 The dataset was created as follows:
- 45,000 users were selected in line with the age distribution in the complete dataset
- For each track these users interacted with, we gathered the artists and annotated the artists with genre information from MusicBrainz
- If no genre information was available, listening events were removed.
The resulting samples can be found in `data/processed/MLHD_processed`. To gather a subset of the listening events based on these user and artists samples, follow the instructions below.

### Processing Instructions
Download the required MLHD+ dataset. After that, run the following scripts:
```
python MLHD_filter_LEs_by_user_and_artist.py -start_fresh -sample_les -save_users -save_artists --dataset_dir data
python MLHD_filter_LEs_by_genre.py --dataset_dir data
```

### Citation
If this subset is used, please cite the original [MLHD dataset](https://ddmal.music.mcgill.ca/research/The_Music_Listening_Histories_Dataset_(MLHD)/), [MLHD+](https://musicbrainz.org/doc/MLHD+) and our paper (see below)
```
Robin Ungruh, Alejandro Bellogín, Dominik Kowald, and Maria Soledad Pera. 2025. Impacts of Mainstream-Driven Algorithms on Recommendations for Children Across Domains: A Reproducibility Study. In 19th ACM Conference on Recommender Systems (RecSys ’25), September 22–26, 2025, Prague, Czech Republic. ACM, New York, NY, USA, 9 pages. https://doi.org/10.1145/3705328.3748160
```


## Bookcrossing

The Bookcrossing dataset used in the [RecSys 2025 paper](https://doi.org/10.1145/3705328.3748160). This dataset includes an annotation of genres as per the Goodreads dataset for each book.
For the code used to generate the data samples, we refer to [the original code](https://github.com/rUngruh/2025_RecSys_Reproducibility)

### Process Datasets
 The dataset was created as follows:
- As a book can have multiple ISBN's depending on the version, we connected Bookcrossing with Goodreads using the [Book-Data](https://github.com/PIReTship/bookdata-tools/tree/main) tool 
- We annotate each book in Bookcrossing with at least one genre. Books without genres are removed
The resulting samples can be found in `data/processed/Book-Crossing`. To get the filtered dataset based on this sample, follow the instructions.

### Processing Instructions
Download the required Bookcrossing dataset. After that, run the following script:
```
python BX_filter_ratings.py --dataset_dir data
```

### Citation
If this subset is used, please cite the original [Book-Crossing](https://www.kaggle.com/datasets/syedjaferk/book-crossing-dataset), [Goodreads](https://cseweb.ucsd.edu/~jmcauley/datasets/goodreads.html), and our paper (see below)
```
Robin Ungruh, Alejandro Bellogín, Dominik Kowald, and Maria Soledad Pera. 2025. Impacts of Mainstream-Driven Algorithms on Recommendations for Children Across Domains: A Reproducibility Study. In 19th ACM Conference on Recommender Systems (RecSys ’25), September 22–26, 2025, Prague, Czech Republic. ACM, New York, NY, USA, 9 pages. https://doi.org/10.1145/3705328.3748160
```
