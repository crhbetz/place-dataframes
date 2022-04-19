### Note: place-analyzer
**If you're just looking for analysis of your personal data, I recommend also taking a look at [place-analyzer by Yannis4444](https://github.com/Yannis4444/place-analyzer).**

# place-dataframes

The broader idea of this project was to create a `PlaceData` class which could later be re-used for manual or other analysis or, for example,
to power a web form where one would put in a username and quickly receive results for that username.

It loads and keeps in memory dask DataFrames of both the official, reddit supplied pixel placement data, and an unofficial dataset found on
[The Internet Archive](https://archive.org/details/place2022-opl-raw) and runs its analysis on those DataFrames.

## RAM warning

First loading the csv data to DataFrames requires massive amounts of RAM - it barely fit into the 24GB on a free tier Oracle Cloud server. The data
will be dumped to and loaded from pickle files afterwards, which in my setup required up to 9GB RAM. Processing to find user information again used
up to around 22GB of RAM.

## Examples

So far, this project is able to produce a text summary and some nice images of a user's activity during r/place 2022 given their reddit username from an
interactive Python session.

```
example will be added
```

# Usage

## Preparation

1. Install required packages: `pip install -r requirements.txt` or whatever suits your environment
2. Copy config.ini.example to config.ini and fill in your preferences
3. Consider using the [.torrent file from The Internet Archive](https://archive.org/download/place2022-opl-raw/place2022-opl-raw_archive.torrent) to
download the unofficial dataset, saving bandwidth of The Internet Archive, and move all files called `details-*` to your previously configured [original] -> unofficial folder.
4. Run `python download-and-compress-official.py` and `python download-and-compress-unofficial.py` to (download and) process the required data.
5. Run `python -i place-dataframes.py`. Loading will take a while. Finally, you should have a python prompt where you will find
the `PlaceData()` object as the variable `data`:
```
[2022-04-19 13:46:08,240] [place-dataframes.py:876] [I] PlaceData object available as variable 'data'
>>> 
```

## Methods

Most likely, you will want to produces summaries and images for some users. You can do this with the `analyze_user` method:

`>>> data.analyze_user("Username")`

However, you can also pass a list of multiple usernames to analyze them as if they were one user:

`>>> data.analyze_user(["User1", "User2", "User3"])`

*to be continued ...*
