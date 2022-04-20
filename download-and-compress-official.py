import gzip
import requests
import io
import os
import configparser
import csv
import json
from tqdm import tqdm
from datetime import datetime

# load config
config = configparser.ConfigParser()
config.read("config.ini")
output_dir = "/" + config['compressed']['official'].rstrip('/').lstrip('/')
os.makedirs(output_dir, exist_ok=True)

start = 1648771200 * 1000  # april 1, 12am timestamp in ms
user_map: dict[str, int] = {}
colors = {
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
user_i = 0

for i in tqdm(range(79), desc="Processing official files", position=0, leave=False):
    out = ["timestamp,user_id,pixel_color,pixel_x,pixel_y"]
    n = f"{i:02d}"
    url = f"https://placedata.reddit.com/data/canvas-history/2022_place_canvas_history-0000000000{n}.csv.gzip"
    r = requests.get(url, stream=True)

    bio = io.BytesIO()
    bar = tqdm(desc=f"Downloading {i}", total=int(r.headers.get('content-length', 0)), unit='b', unit_scale=True,
               unit_divisor=1024, position=1, leave=False)
    for chunk in r.iter_content(chunk_size=1024):
        bar.update(len(chunk))
        bio.write(chunk)
    bar.close()
    bio.seek(0)

    with gzip.open(bio, mode="rt") as uncompressed:
        reader = csv.DictReader(uncompressed, delimiter=',')
        for row in tqdm(reader, desc=f"Processing {i} ...", position=1, leave=False):
            user = row["user_id"]
            new_user: int | None = user_map.get(user)

            if new_user is None:
                new_user = user_i
                user_i += 1
                user_map[user] = new_user

            new_color = colors[row["pixel_color"]]
            if "." in row["timestamp"]:
                old_time = datetime.strptime(row["timestamp"], "%Y-%m-%d %H:%M:%S.%f UTC")
            else:
                old_time = datetime.strptime(row["timestamp"], "%Y-%m-%d %H:%M:%S UTC")
            time = old_time.timestamp() * 1000
            new_time = int(time - start)

            coords = row["coordinate"].replace('"', '').split(",")
            # drop admin edit rows, only complicating our data for no gain
            if len(coords) > 2:
                continue

            out.append(f"{new_time},{new_user},{new_color},{coords[0]},{coords[1]}")
        with open(os.path.join(output_dir, f"{n}.csv"), "w+") as f:
            f.write("\n".join(out))

print("save users file")
with open(os.path.join(output_dir, "users"), "w+") as f:
    json.dump(user_map, f, indent=4)
