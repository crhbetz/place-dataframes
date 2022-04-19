import glob
import dask.dataframe as dd
import pandas as pd
import numpy as np
import pickle
import logging
import sys
import configparser
import os
from colory.color import Color
from datetime import datetime
from dateutil.relativedelta import relativedelta
from concurrent.futures import ThreadPoolExecutor
from concurrent import futures
from PIL import Image, ImageColor, ImageEnhance
from collections import Counter
from tqdm import tqdm
from dask.diagnostics import ProgressBar

# Enable logging
logFormat = ('[%(asctime)s] [%(filename)s:%(lineno)3d] [%(levelname).1s] %(message)s')
logging.basicConfig(format=logFormat, level=logging.INFO, stream=sys.stdout)
logger = logging.getLogger(__name__)

start = 1648771200 * 1000  # 2022-04-01 00:00:00 GMT in ms
whiteout = 1649112460186  # 2022-04-04 22:47:40.186 GMT in ms - last non-white pixel in dataset: 341260185
whiteout_short = whiteout - start


class Cache():
    # cache results of expensive operations from the PlaceData class

    def __init__(self, cwd=None):
        if not cwd:
            self.cwd = os.path.dirname(os.path.abspath(__file__))
        else:
            self.cwd = cwd

        logger.info("Initialize Cache ...")
        self.datatypes = ["ouid", "uuid", "hash", "first_pixels", "final_pixels"]
        try:
            logger.info("try to load pickle'd cache ...")
            self.data = pickle.load(open(os.path.join(self.cwd, "cache.p"), "rb"))
            logger.info("Cache loaded!")
        except Exception as e:
            logger.warning(f"Unable to load cache, start empty ... ({e})")
            self.data = {}

    def get(self, cachename=None, datatype=None):
        if cachename is None or datatype is None:
            raise ValueError("Error getting from cache: Missing one of: cachename, datatype")
        logger.debug(f"Requested {cachename}:{datatype} from cache")
        if datatype not in self.datatypes:
            raise ValueError(f"Invalid datatype {datatype} requested from cache! Valid types: {self.datatypes}")
        if cachename not in self.data:
            logger.debug(f"{cachename} not in cache")
            return False
        elif datatype not in self.data[cachename]:
            logger.debug(f"{datatype} not in cache for {cachename}")
            return False
        else:
            logger.debug(f"Return {datatype} data from {cachename} cache: {self.data[cachename][datatype]}")
            return self.data[cachename][datatype]

    def set(self, cachename=None, datatype=None, data=None):
        if cachename is None or datatype is None or data is None:
            raise ValueError("Error setting cache: Missing one of: cachename, datatype, data")
        if datatype not in self.datatypes:
            raise ValueError(f"Attempted to set invalid datatype {datatype} to cache! Valid types: {self.datatypes}")
        if datatype in ["ouid", "uuid"]:
            try:
                data = int(data)
            except Exception:
                raise ValueError(f"{type(data)} is invalid for {datatype} cache - requires int")
        elif datatype == "hash" and not isinstance(data, str):
            raise ValueError(f"{type(data)} is invalid for {datatype} cache - requires str")
        elif (datatype in ["final_pixels", "first_pixels"]
                and not (isinstance(data, dd.DataFrame) or isinstance(data, pd.DataFrame))):
            raise ValueError(f"{type(data)} is invalid for final_pixels cache - requires DataFrame")
        if cachename not in self.data:
            self.data[cachename] = {}
        self.data[cachename][datatype] = data
        logger.debug(f"Added {datatype} to {cachename} cache: {self.data[cachename][datatype]}")
        pickle.dump(self.data, open(os.path.join(self.cwd, "cache.p"), "wb"))
        return True

    def drop(self, cachename=None, datatype=None):
        if cachename is None:
            return False
        if datatype is not None and datatype not in self.datatypes:
            raise ValueError(f"Invalid datatype {datatype}! Valid types: {self.datatypes}")
        if datatype:
            if cachename in self.data and datatype in self.data[cachename]:
                del self.data[cachename][datatype]
                pickle.dump(self.data, open(os.path.join(self.cwd, "cache.p"), "wb"))
                return True
            else:
                return False
        else:
            if cachename in self.data:
                del self.data[cachename]
                pickle.dump(self.data, open(os.path.join(self.cwd, "cache.p"), "wb"))
                return True
            else:
                return False


class PlaceData():
    def __init__(self, config_file="config.ini"):
        pbar = ProgressBar()
        pbar.register()

        # load config
        config = configparser.ConfigParser()
        config.read(config_file)

        self.cwd = config.get("global", "dir", fallback=os.path.dirname(os.path.abspath(__file__)))
        self.imgdir = config.get("global", "imgdir", fallback=os.path.join(self.cwd, "images"))
        os.makedirs(self.imgdir, exist_ok=True)

        self.imgurl = config.get("global", "imgurl", fallback=None)
        self.uidworkers = int(config.get("global", "uidworkers", fallback=2))
        self.pixelworkers = int(config.get("global", "pixelworkers", fallback=4))

        self.official_compressed = config.get("compressed", "official",
                                              fallback=os.path.join(self.cwd, "official_compressed"))
        self.unofficial_compressed = config.get("compressed", "unofficial",
                                                fallback=os.path.join(self.cwd, "unofficial_compressed"))

        # loading from csv will drop the data objects to have more RAM available, thus we need this
        # weird loop to make sure we can finally load both from pickle
        both_loaded = False
        while not both_loaded:
            self.load_official(f"{self.official_compressed}/*.csv")
            self.load_unofficial(f"{self.unofficial_compressed}/*.csv")
            try:
                self.official
                self.unofficial
            except Exception:
                # try again
                pass
            else:
                both_loaded = True

        self.cache = Cache()

        hexmap = {
            "#000000": 0,
            "#FFB470": 1,
            "#2450A4": 2,
            "#FFA800": 3,
            "#D4D7D9": 4,
            "#493AC1": 5,
            "#00756F": 6,
            "#FFFFFF": 7,
            "#6D482F": 8,
            "#FFF8B8": 9,
            "#3690EA": 10,
            "#00CCC0": 11,
            "#51E9F4": 12,
            "#9C6926": 13,
            "#B44AC0": 14,
            "#009EAA": 15,
            "#FF4500": 16,
            "#BE0039": 17,
            "#811E9F": 18,
            "#00A368": 19,
            "#FF3881": 20,
            "#6A5CFF": 21,
            "#FFD635": 22,
            "#E4ABFF": 23,
            "#DE107F": 24,
            "#FF99AA": 25,
            "#515252": 26,
            "#94B3FF": 27,
            "#7EED56": 28,
            "#00CC78": 29,
            "#898D90": 30,
            "#6D001A": 31,
        }
        self.hexmap = {v: k for k, v in hexmap.items()}
        self.colormap = {v: Color(k, "xkcd") for k, v in hexmap.items()}
        pbar.unregister()

    # https://gist.github.com/enamoria/fa9baa906f23d1636c002e7186516a7b
    # This function is used to reduce memory of a pandas dataframe
    # The idea is cast the numeric type to another more memory-effective type
    # For ex: Features "age" should only need type='np.int8'
    # Source: https://www.kaggle.com/gemartin/load-data-reduce-memory-usage
    def reduce_mem_usage(self, df):
        """ iterate through all the columns of a dataframe and modify the data type
            to reduce memory usage.
        """
        start_mem = df.memory_usage().sum() / 1024**2
        logger.info('Memory usage of dataframe is {:.2f} MB - try to reduce it ...'.format(start_mem))

        for col in tqdm(df.columns):
            col_type = df[col].dtype

            if col_type != object and col_type.name != 'category' and 'datetime' not in col_type.name:
                c_min = df[col].min()
                c_max = df[col].max()
                if str(col_type)[:3] == 'int':
                    if c_min > np.iinfo(np.int8).min and c_max < np.iinfo(np.int8).max:
                        df[col] = df[col].astype(np.int8)
                    elif c_min > np.iinfo(np.int16).min and c_max < np.iinfo(np.int16).max:
                        df[col] = df[col].astype(np.int16)
                    elif c_min > np.iinfo(np.int32).min and c_max < np.iinfo(np.int32).max:
                        df[col] = df[col].astype(np.int32)
                    elif c_min > np.iinfo(np.int64).min and c_max < np.iinfo(np.int64).max:
                        df[col] = df[col].astype(np.int64)
                else:
                    if c_min > np.finfo(np.float16).min and c_max < np.finfo(np.float16).max:
                        df[col] = df[col].astype(np.float16)
                    elif c_min > np.finfo(np.float32).min and c_max < np.finfo(np.float32).max:
                        df[col] = df[col].astype(np.float32)
                    else:
                        df[col] = df[col].astype(np.float64)
            elif 'datetime' not in col_type.name:
                df[col] = df[col].astype('category')

        end_mem = df.memory_usage().sum() / 1024**2
        logger.info('Memory usage after optimization is: {:.2f} MB'.format(end_mem))
        logger.info('Decreased by {:.1f}%'.format(100 * (start_mem - end_mem) / start_mem))
        return df

    def load_official(self, file_glob=None):
        # load official data from pickle file or initialize from files
        try:
            logger.info("try to load official data from pickle file ...")
            self.official = pickle.load(open(os.path.join(self.cwd, "official.p"), "rb"))
            logger.info("Official data loaded from pickle file!")
            return True
        except Exception as e:
            logger.warning(f"Unable to load official data pickle file ({e}).. initialize!")

        if not file_glob:
            raise ValueError("Missing file_glob for official compressed files!")
        self.official = self.load_csv(file_glob, reduce_mem=True)
        try:
            self.official
        except Exception:
            raise ValueError(f"Unable to load official data from this glob: {file_glob} - is your [compressed] "
                             "official folder correctly configuredi and did you run the downloader?")
        else:
            logger.info("dump official data to official.p ...")
            pickle.dump(self.official, open(os.path.join(self.cwd, "official.p"), "wb"))
            return True

    def load_unofficial(self, file_glob=None):
        # load unofficial data from pickle file or initialize from files
        try:
            logger.info("try to load unofficial data from pickle file ....")
            self.unofficial = pickle.load(open(os.path.join(self.cwd, "unofficial.p"), "rb"))
            logger.info("Unofficial data loaded from pickle file!")
            return True
        except Exception as e:
            logger.warning(f"Unable to load unofficial data pickle file ({e}).. initialize!")

        if not file_glob:
            raise ValueError("Missing file_glob for unofficial compressed files!")
        self.unofficial = self.load_csv(file_glob, reduce_mem=True)
        try:
            self.unofficial
        except Exception:
            raise ValueError(f"Unable to load unofficial data from this glob: {file_glob} - is your [compressed]"
                             "unofficial folder correctly configured and did you run the downloader?")
        else:
            logger.info("dump unofficial data to unofficial.p ...")
            pickle.dump(self.unofficial, open(os.path.join(self.cwd, "unofficial.p"), "wb"))
            return True

    def load_csv(self, file_glob=None, reduce_mem=False):
        # load multiple csv files to dask DataFrames and concat them

        # this needs a LOT of RAM, so drop everything that's eating it up ...
        # there's a loop in __init__ to make sure everything is loaded in the end
        try:
            del self.official
            del self.unofficial
        except Exception:
            pass

        if not file_glob:
            return None
        dfs = []
        for f in sorted(glob.glob(file_glob)):
            logger.debug(f)
            df = dd.read_csv(f)
            dfs.append(df)
        logger.info("Compute DataFrames ...")
        if reduce_mem:
            return self.reduce_mem_usage(dd.concat(dfs).compute())
        else:
            return dd.concat(dfs).compute()

    def get_rows_by_username(self, username=None):
        # wrapper to get_rows_by_uid to get rows by one or multiple username(s)
        if isinstance(username, list):
            ouid = []
            for user in username:
                ouid.append(self.__internal_get_ouid(user))
        else:
            ouid = self.__internal_get_ouid(username)
        return self._get_rows_by_uid(ouid)

    def __internal_get_ouid(self, username):
        # get official uid from cache or pass to get_official_uid_by_username to determine and cache it
        ouid = self.cache.get(username, "ouid")
        if not ouid:
            ouid = self.get_official_uid_by_username(username)
            self.cache.set(username, "ouid", ouid)
        return ouid

    def _get_rows_by_uid(self, uid=None):
        # returns dataframe of official data rows for one or multiple user ids
        if isinstance(uid, list):
            return self.official[self.official.user_id.isin(uid)].sort_values(by="timestamp") if uid else pd.DataFrame()
        else:
            return self.official[(self.official["user_id"] == uid)].sort_values(by="timestamp") if uid else \
                pd.DataFrame()

    def get_rows_by_ts(self, ts=None):
        # returns dataframe of rows matched by timestamp
        return self.official[(self.official["timestamp"] == ts)] if ts else pd.DataFrame()

    def get_rows_by_coords(self, x=None, y=None):
        # returns dataframe of rows matched by coordinates
        if x and y:
            return self.official[(self.official["pixel_x"] == x) & (self.official["pixel_y"] == y)]
        elif x:
            return self.official[(self.official["pixel_x"] == x)]
        elif y:
            return self.official[(self.official["pixel_y"] == y)]
        else:
            return pd.DataFrame()

    def _check_rectangle(self, a, b):
        # verify rectangle format: two tuples of upper left and lower right coordinates
        warning = "Not a rectangle! Requires two tuples of upper left and lower right pixel coordinates"
        if not isinstance(a, tuple) or not len(a) == 2 or not isinstance(b, tuple) or not len(b) == 2:
            logger.warning(warning)
            return False
        xa, ya = a
        xb, yb = b
        if not xa <= xb or not ya <= yb:
            logger.warning(warning)
            return False
        return True

    def get_rows_by_rectangle(self, a=None, b=None):
        # returns dataframe of all rows within a rectangle, defined by tuples of upper left, lower right coordinates
        if not self._check_rectangle(a, b):
            return []
        xa, ya = a
        xb, yb = b
        query = f"pixel_x >= {xa} and pixel_x <= {xb} and pixel_y >= {ya} and pixel_y <= {yb}"
        df = self.official.query(query)
        return df

    def get_last_edit(self, x=None, y=None):
        # returns dataframe containing 1 row, which is the last edit of the given pixel (or empty if invalid input)
        if x is None or y is None:
            return pd.DataFrame()
        df = self.get_rows_by_coords(x, y).sort_values(by="timestamp")
        return df.iloc[-1:]

    def get_first_edit(self, x=None, y=None):
        # returns dataframe containing 1 row, which is the first edit of the given pixel (or empty if invalid input)
        if x is None or y is None:
            return pd.DataFrame()
        df = self.get_rows_by_coords(x, y).sort_values(by="timestamp")
        return df.iloc[0:1]

    def get_last_edit_before_whiteout(self, x=None, y=None):
        # returns dataframe containing 1 row, which is the last edit of the pixel before the whiteout started
        # (or empty if invalid input)
        if x is None or y is None:
            logger.warning(f"get_last_edit_before_whiteout: Invalid input! (x: {x}, y: {y})")
            return pd.DataFrame()
        df = self.get_rows_by_coords(x, y)
        query = f"timestamp < {whiteout_short}"
        before_whiteout = df.query(query).sort_values(by="timestamp")
        return before_whiteout.iloc[-1:]

    def get_unique_users_on_pixel(self, x=None, y=None):
        # returns list of unique user_ids who interacted with the given pixel
        if x is None or y is None:
            return []
        df = self.get_rows_by_coords(x, y)
        try:
            return df.user_id.unique()
        except Exception:
            return []

    def get_unique_users_in_rectangle(self, a=None, b=None):
        # returns list of unique user_ids who interacted with any of the pixels in the given rectangle
        if not self._check_rectangle(a, b):
            return []
        try:
            return self.get_rows_by_rectangle(a, b).user_id.unique()
        except Exception:
            return []

    def get_rows_by_expression(self, expression=None):
        # just an alias to df.query() for the official data
        # example: "pixel_x == 1 and pixel_y == 2"
        # use double quotes as outer quotes!
        return self.official.query(expression)

    def get_unofficial_rows_by_uid(self, uid):
        # returns dataframe of all rows by the given uid from the unofficial data
        return self.unofficial[(self.unofficial["user_id"] == uid)] if uid else pd.DataFrame()

    def get_official_uid_by_username(self, username):
        # wrapper around _get_official_uid to handle username(s) and caching
        if not isinstance(username, list):
            username = [username]
        ouid = []
        for user in username:
            cache = self.cache.get(user, "ouid")
            if cache:
                ouid.append(cache)
            else:
                ret = self._get_official_uid(self.get_unofficial_uid(user))
                if ret:
                    ouid.append(ret)
                    self.cache.set(user, "ouid", ret)
        if len(ouid) > 1:
            return ouid
        elif len(ouid) == 1:
            return ouid[0]
        else:
            return False

    def _get_official_uid(self, uid, find_all=False):
        # determine user id in the compressed official dataset given a user id from the unofficial data
        matches = []
        udf = self.get_unofficial_rows_by_uid(uid)
        with ThreadPoolExecutor(max_workers=self.uidworkers) as executor:
            # more than two workers caused 24GB RAM to run full sometimes
            jobs = []
            results = []
            loop = True
            i = 0
            while loop and i < 25:
                # 25 samples seem to be enough to be sure
                try:
                    dataset = udf.iloc[i]
                    # TODO: coordinates could potentially contain the canvas notation from the unofficial data,
                    # try to catch this here for now
                    try:
                        int(dataset.pixel_x)
                        int(dataset.pixel_y)
                    except Exception:
                        i += 1
                        continue
                    tslow = int(dataset.timestamp / 1000) * 1000
                    tshigh = tslow + 1000
                    # TODO: because I was too lazy to correctly parse the canvas notation in the unofficial dataset,
                    # the coordinates are only 0 <= x <= 1000 and could be on any of the four canvas parts
                    query = (f"timestamp >= {tslow} and timestamp < {tshigh} and "
                             f"(pixel_x == {dataset.pixel_x} or pixel_x == 1{dataset.pixel_x}) and "
                             f"(pixel_y == {dataset.pixel_y} or pixel_y == 1{dataset.pixel_x})")
                    logger.debug(f"string to query: {query}")
                    jobs.append(executor.submit(self.get_rows_by_expression, query))
                    i += 1
                except Exception as e:
                    logger.warning(f"Error searching official dataset for pixels by {uid}: {e}")
                    loop = False

            for job in tqdm(futures.as_completed(jobs), total=len(jobs), desc="Determining official user hash...",
                            leave=False):
                match = job.result()
                results.append(match)

        for match in results:
            if len(match.index) > 0:
                logger.debug(match.to_string())
                for index, row in match.iterrows():
                    ouid = row.user_id
                    logger.debug(f"Found: {ouid}")
                    matches.append(ouid)

        logger.info(f"matches: {matches}")
        if len(matches) > 0:
            # https://stackoverflow.com/a/6987358
            # determine the uid with the most matches and use it
            most_common, num_most_common = Counter(matches).most_common(1)[0]
            logger.info(f"Found {most_common} in {num_most_common}/{len(matches)} entries")
            return most_common
        else:
            return False

    def strip_username(self, username=None):
        # Sanitize usernames: remove slash-parts used on reddit
        if isinstance(username, str):
            return username.lstrip("/u/").lstrip("u/").lstrip("/").rstrip("/")
        elif isinstance(username, list):
            return [x.lstrip("/u/").lstrip("u/").lstrip("/").rstrip("/") for x in username]
        else:
            return None

    def printuser(self, username=None):
        # get a clean str for one or multiple usernames
        return "-".join(username) if isinstance(username, list) else str(username)

    def get_hash_by_username(self, username=None):
        # wrapper around get_hash_by_official_uid to return the full reddit supplied hash value for a given username,
        # utilizing cache where possible
        username = self.strip_username(username)
        cache = self.cache.get(username, "hash")
        if cache:
            return cache
        uid = self.get_unofficial_uid(username)
        if uid is None:
            return None
        logger.debug(f"get hash for uid {uid}")
        ouid = self.get_official_uid_by_username(username)
        _hash = self.get_hash_by_official_uid(ouid)
        self.cache.set(username, "hash", _hash)
        return _hash

    def get_unofficial_uid(self, username):
        # get the unofficial compressed user id for a given username
        # returns None if the username can't be found in the official dataset
        cache = self.cache.get(username, "uuid")
        if cache:
            return cache
        with open(os.path.join(self.unofficial_compressed, "users"), "r") as f:
            for line in f:
                if f"\"{username}\"" in line:
                    uid = int(line.split(":")[1].strip().rstrip(","))
                    logger.debug(f"found uid in unofficial usermap file: {uid}")
                    self.cache.set(username, "uuid", uid)
                    return uid
        return None

    def get_hash_by_official_uid(self, uid):
        # return the full reddit supplied hash value for a given compressed user id
        logger.debug(f"search hash for uid: {uid}")
        lnum = 0
        with open(os.path.join(self.official_compressed, "users"), "r") as f:
            for line in f:
                lnum += 1
                if lnum % 10000 == 0:
                    logger.debug(f"line {lnum} ...")
                if str(uid) in line:
                    hash = line.split(":")[0].strip().rstrip(",")
                    logger.debug(f"found hash in official usermap file line {lnum}: {hash}")
                    return hash
        return None

    def get_final_pixels_by_username(self, username):
        # wrapper to supply correct mode value to __internal_get_pixels
        return self.__internal_get_pixels(username, "final_before_whiteout")

    def get_first_pixels_by_username(self, username):
        # wrapper to supply correct mode value to __internal_get_pixels
        return self.__internal_get_pixels(username, "first")

    def __internal_get_pixels(self, username, mode=None):
        # get pixels where the user(s) was/were the first or last user(s) to place a pixel on
        # returns DataFrame
        cachenames = {"first": "first_pixels",
                      "final_before_whiteout": "final_pixels"}
        if mode not in ["first", "final_before_whiteout"]:
            return False

        if not isinstance(username, list):
            username = [username]
        pixels = []
        for user in username:
            cache = self.cache.get(user, cachenames[mode])
            if cache is not False and cache is not None:
                if not cache.empty:
                    pixels.append(cache)
                continue
            rows = self.get_rows_by_username(user)
            res = self._pixels_threaded(rows, self.get_official_uid_by_username(user), mode)
            if not res.empty:
                pixels.append(res)
            self.cache.set(user, cachenames[mode], res)
        if len(username) > 1 and len(pixels) > 1:
            try:
                return dd.concat(pixels).compute().sort_values(by="timestamp")
            except Exception as e:
                logger.debug("_pixels_threaded: Exception trying to return dask DataFrame. Return without compute. "
                             f"({e})")
                return dd.concat(pixels).sort_values(by="timestamp")
        elif len(pixels) == 1:
            return pixels[0].sort_values(by="timestamp")
        else:
            return pd.DataFrame()

    def _pixel_thread(self, pixel_x, pixel_y, mode):
        # to be used in _pixels_threaded to determine the requested edit of a given pixel
        # returns DataFrame
        logger.debug(f"Launch thread with pixel_x={pixel_x}, pixel_y={pixel_y}")
        if mode == "first":
            edit = self.get_first_edit(pixel_x, pixel_y)
        elif mode == "final":
            edit = self.get_last_edit(pixel_x, pixel_y)
        elif mode == "final_before_whiteout":
            edit = self.get_last_edit_before_whiteout(pixel_x, pixel_y)
        else:
            return pd.DataFrame()
        if edit.empty:
            return pd.DataFrame()
        edit = edit.iloc[0:]
        logger.debug(f"Thread with pixel_x={pixel_x}, pixel_y={pixel_y} finished!")
        return edit

    def _pixels_threaded(self, pixels, official_uid, mode=None):
        # return all the pixels that meet the requested condition for one or multiple given user(s)
        # possible conditions: user made the first edit, user made the last edit, user made the last edit before the
        # start of the whiteout
        # returns DataFrame
        if not official_uid:
            return pd.DataFrame()
        if not isinstance(official_uid, list):
            official_uid = [official_uid]
        if mode not in ["first", "final", "final_before_whiteout"]:
            return pd.DataFrame()
        ret_pixels = []
        with ThreadPoolExecutor(max_workers=self.pixelworkers) as executor:
            jobs = []
            results = []
            for index, row in pixels.iterrows():
                jobs.append(executor.submit(self._pixel_thread, row.pixel_x, row.pixel_y, mode))

            for job in tqdm(futures.as_completed(jobs), total=len(jobs), desc=f"Searching {mode} pixels ...",
                            leave=False):
                edit = job.result()
                results.append(edit)

            for edit in results:
                if edit.iloc[0].user_id in official_uid:
                    ret_pixels.append(edit)

        if len(ret_pixels) > 0:
            try:
                return dd.concat(ret_pixels).compute().value_counts(ascending=True).reset_index(name='count')
            except Exception as e:
                logger.debug("_pixels_threaded: Exception trying to return dask DataFrame. Return without compute. "
                             f"({e})")
                return dd.concat(ret_pixels).value_counts(ascending=True).reset_index(name='count')
        else:
            return pd.DataFrame()

    def analyze_user(self, username=None, list_pixels=False):
        # wrapper to get the text summary + all implemented pictures for one username or a list of usernames
        if username is None:
            return None
        username = self.strip_username(username)

        # get_summary returns bool depending on if username could be matched to official data or not
        ret = self.get_summary(username, list_pixels)
        if ret:
            print()
            self.generate_first_pixels_dark(username, True)
            self.generate_final_pixels_dark(username, True)
            self.generate_all_pixels_dark_pre_whiteout(username, True)
            self.generate_all_pixels_dark_during_whiteout(username, True)

    def get_summary(self, username=None, list_pixels=False):
        # print copy-pasteable summary to console
        # return True if username could be found, else return False

        # human readable pixel survival time
        # https://stackoverflow.com/a/11157649
        attrs = ['days', 'hours', 'minutes', 'seconds']
        human_readable = lambda delta: ['%d %s' % (getattr(delta, attr), attr if getattr(delta, attr) > 1
                                                   else attr[:-1]) for attr in attrs if getattr(delta, attr)]

        # initialize and print hash
        username = self.strip_username(username)
        official_uid = self.get_official_uid_by_username(username)
        if not official_uid:
            print(f"Unable to match {username} to the official dataset. No analysis possible. :(")
            return False
        print(f"\nSummary for {self.printuser(username)}")
        print("=" * int(12 + len(self.printuser(username))))
        logger.debug(f"Official ID: {official_uid}")
        if not isinstance(username, list):
            uhash = self.get_hash_by_official_uid(official_uid)
            print(f"\nReddit username hash: {uhash}")

        # get and count all pixels
        pixels = self.get_rows_by_username(username)
        npixels = len(pixels.index)
        if list_pixels:
            print(pixels.to_string())
        print(f"You placed {npixels} pixels!")

        # first and last pixels
        print("\n(color names from https://xkcd.com/color/rgb/)")
        first_pixel = pixels.iloc[0]
        first_timestring = datetime.fromtimestamp(int((first_pixel.timestamp + start) // 1000)).strftime("%Y-%m-%d "
                                                                                                         "%H:%M:%S")
        last_pixel = pixels.iloc[-1]
        last_timestring = datetime.fromtimestamp(int((last_pixel.timestamp + start) // 1000)).strftime("%Y-%m-%d "
                                                                                                       "%H:%M:%S")
        print(f"Your first pixel: {first_pixel.pixel_x},{first_pixel.pixel_y} placed at {first_timestring} GMT "
              f"with color {self.colormap[first_pixel.pixel_color].name} ({self.hexmap[first_pixel.pixel_color]})")
        print(f"Your last pixel: {last_pixel.pixel_x},{last_pixel.pixel_y} placed at {last_timestring} GMT "
              f"with color {self.colormap[last_pixel.pixel_color].name} ({self.hexmap[last_pixel.pixel_color]})")

        # pixels touched as first user
        first_pixels = self.get_first_pixels_by_username(username)
        print(f"\n{len(first_pixels.index)} pixels were first touched by you!")
        if len(first_pixels) > 0:
            for index, pixel in first_pixels.sort_values(by="timestamp").iterrows():
                timestring = datetime.fromtimestamp(int((pixel.timestamp + start) // 1000)).strftime("%Y-%m-%d "
                                                                                                     "%H:%M:%S")
                print(f"Pixel {pixel.pixel_x},{pixel.pixel_y} set at {timestring} GMT, color "
                      f"{self.colormap[pixel.pixel_color].name} ({self.hexmap[pixel.pixel_color]})")

        # pixels during whiteout
        during_whiteout = pixels.query(f"timestamp >= {whiteout_short} and pixel_color == 7")
        if not during_whiteout.empty:
            print(f"\nYou placed {len(during_whiteout.index)} pixels during the whiteout!")
            for index, pixel in during_whiteout.sort_values(by="timestamp").iterrows():
                timestring = datetime.fromtimestamp(int((pixel.timestamp + start) // 1000)).strftime("%Y-%m-%d "
                                                                                                     "%H:%M:%S")
                print(f"Pixel {pixel.pixel_x},{pixel.pixel_y} set at {timestring} GMT")
        else:
            print("\nYou did not place pixels during the whiteout.")

        # pixels on the final canvas before whiteout started
        final_pixels = self.get_final_pixels_by_username(username)
        print(f"\nYou have {len(final_pixels.index)} pixels on the final non-whitened canvas!")
        if len(final_pixels) > 0:
            for index, pixel in final_pixels.sort_values(by="timestamp").iterrows():
                timestring = datetime.fromtimestamp(int((pixel.timestamp + start) // 1000)).strftime("%Y-%m-%d "
                                                                                                     "%H:%M:%S")
                survived = human_readable(relativedelta(seconds=(whiteout_short - pixel.timestamp) / 1000))
                print(f"Pixel {pixel.pixel_x},{pixel.pixel_y} set at {timestring} GMT, color "
                      f"{self.colormap[pixel.pixel_color].name} ({self.hexmap[pixel.pixel_color]}) - survived "
                      f"{' '.join(survived)} until the whiteout!")
        return True

    def generate_first_pixels_dark(self, username=None, summary=False):
        # highlight the pixels the user(s) touched first on a darkened canvas
        username = self.strip_username(username)
        img = Image.open(os.path.join(self.cwd, "final_place.png"))
        sample = img.copy()
        enhancer = ImageEnhance.Brightness(img)
        img = enhancer.enhance(0.3)
        pixels = self.get_first_pixels_by_username(username)
        if pixels.empty:
            return False
        self.generate_image(sample, img, pixels, None, 2, 0, f"{self.printuser(username)}-first.png", summary)
        if summary:
            self.print_img_summary("Image of pixels you touched first: {}-first.png", username)

    def generate_final_pixels_dark(self, username=None, summary=False):
        # highlight the pixels the user(s) had placed that remained until before the whiteout, on a darkened canvas
        username = self.strip_username(username)
        img = Image.open(os.path.join(self.cwd, "final_place.png"))
        sample = img.copy()
        enhancer = ImageEnhance.Brightness(img)
        img = enhancer.enhance(0.3)
        pixels = self.get_final_pixels_by_username(username)
        if pixels.empty:
            return False
        self.generate_image(sample, img, pixels, None, 2, 0, f"{self.printuser(username)}-final.png", summary)
        if summary:
            self.print_img_summary("Image of pixels on the final canvas: {}-final.png", username)

    def generate_all_pixels_dark_pre_whiteout(self, username=None, summary=False):
        # highlight all pixels the user(s) ever touched, in the last color they used, before the whiteout,
        # on a darkened canvas
        username = self.strip_username(username)
        img = Image.open(os.path.join(self.cwd, "final_place.png"))
        sample = img.copy()
        enhancer = ImageEnhance.Brightness(img)
        img = enhancer.enhance(0.3)
        query = f"timestamp < {whiteout_short}"
        pixels = self.get_rows_by_username(username).query(query).value_counts(ascending=True).reset_index(name='count')
        if pixels.empty:
            return False
        logger.debug(pixels)
        self.generate_image(sample, img, pixels, None, 1, 0, f"{self.printuser(username)}-all.png", summary)
        if summary:
            self.print_img_summary("Image of all edited pixels before the whiteout (in the last color you placed): "
                                   "{}-all.png", username)

    def generate_all_pixels_dark_during_whiteout(self, username=None, summary=False):
        # highlight all pixels the user(s) replaced with white during the whiteout on a darkened (pre-whiteout) canvas
        username = self.strip_username(username)
        img = Image.open(os.path.join(self.cwd, "final_place.png"))
        sample = img.copy()
        enhancer = ImageEnhance.Brightness(img)
        img = enhancer.enhance(0.3)
        query = f"timestamp >= {whiteout_short}"
        pixels = self.get_rows_by_username(username).query(query).value_counts(ascending=True).reset_index(name='count')
        if pixels.empty:
            return False
        logger.debug(pixels)
        self.generate_image(sample, img, pixels, None, 1, 0, f"{self.printuser(username)}-whiteout.png", summary)
        if summary:
            self.print_img_summary("Image of all edited pixels during the whiteout: {}-whiteout.png", username)

    def print_img_summary(self, text, username):
        if self.imgurl:
            print(text.format(f"{self.imgurl}/{self.printuser(username)}"))
        else:
            print(text.format(f"{self.imgdir}/{self.printuser(username)}"))

    def generate_image(self, sample_img, edit_img, pixels, highlight_color, highlight_radius, highlight_border,
                       filepath, summary=False):
        # common image generator
        # sample_img: image to take unedited pixel colors from
        # edit_img: the image to edit
        # pixels: the pixels to process on edit_img
        # highlight_color: the RGB color to use for highlighting borders (can be None to disable)
        # highlight_radius: total pixel radius of the highlight square
        # highlight_border: thickness of the outer highlight border (ignored if highlight_color is None)
        # summary: True: disable logger output to get a clean print()ed summary from get_summary()
        for index, pixel in pixels.iterrows():
            x = pixel.pixel_x
            y = pixel.pixel_y
            color = ImageColor.getrgb(self.hexmap[pixel.pixel_color])
            edit_img = self.draw_highlight(sample_img, edit_img, x, y, color, highlight_color, highlight_radius,
                                           highlight_border)
            edit_img.putpixel((x, y), color + (255,))
        edit_img = edit_img.resize((16000, 16000), resample=Image.Resampling.NEAREST)
        edit_img.save(os.path.join(self.imgdir, filepath), 'PNG')
        if not summary:
            logger.info(f"Saved image to {filepath}!")

    def draw_highlight(self, sample_img, edit_img, x, y, color, highlight_color, radius, border_thickness=2):
        # draw a highlight around a pixel
        # sample_img: image to take unedited pixel colors from
        # edit_img: the image to edit
        # x, y: coordinates of the pixel
        # color: color of the pixel to be highlighted
        # highlight_color: color of the highlight (can be None to disable colored highlight borders)
        # radius: total radius of the highlight
        # border_thickness: thickness of the outer highlight border
        for r in range(radius, 0, -1):
            if not highlight_color or (r <= radius - border_thickness and not r == 1):
                # these are the fading boxes, appearing in between:
                # the center box: r = 1
                # the variable border: radius - border_thickness
                alpha = int(255 / (r + 0.5))
            else:
                # red border and center box
                alpha = int(255 / 1.1)
                color = (highlight_color + (alpha,))
            for xa in range(x - r, x + r + 1):
                for ya in range(y - r, y + r + 1):
                    try:
                        px = sample_img.getpixel((xa, ya))
                        mixed = self.mix_rgba(color + (alpha,), px)
                        edit_img.putpixel((xa, ya), (mixed[0], mixed[1], mixed[2], alpha))
                    except Exception:
                        # can happen when pixel would be outside of the image
                        pass
        return edit_img

    def mix_rgba(self, col_a, col_b):
        # mix two RGBA colors into one
        # col_a, col_b: tuples of (R, G, B, A)
        # returns tuple of (R, G, B, A)
        r = int(0.5 * col_a[0] + 0.5 * col_b[0])
        g = int(0.5 * col_a[1] + 0.5 * col_b[1])
        b = int(0.5 * col_a[2] + 0.5 * col_b[2])
        a = int(0.5 * col_a[3] + 0.5 * col_b[3])
        return (r, g, b, a)


if __name__ == "__main__":
    logger.info("initializing ...")
    data = PlaceData()
    logger.info("PlaceData object available as variable 'data'")
