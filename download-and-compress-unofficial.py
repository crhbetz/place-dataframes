import json
import re
import lzma
import os
import configparser
import requests
from tqdm import tqdm

# file list taken from https://github.com/Yannis4444/place-analyzer
INTERNET_ARCHIVE_URL = "https://archive.org/download/place2022-opl-raw/{}"
INTERNET_ARCHIVE_FILES = [
    "details-1648846499954.csv.xz",
    "details-1648846530849.csv.xz",
    "details-1648846617602.csv.xz",
    "details-1648846924973.csv.xz",
    "details-1648847646586.csv.xz",
    "details-1648850799528.csv.xz",
    "details-1648850852558.csv.xz",
    "details-1648851037616.csv.xz",
    "details-1648851076265.csv.xz",
    "details-1648851107891.csv.xz",
    "details-1648851186235.csv.xz",
    "details-1648851241128.csv.xz",
    "details-1648851317318.csv.xz",
    "details-1648851461813.csv.xz",
    "details-1648851871929.csv.xz",
    "details-1648851892660.csv.xz",
    "details-1648852318687.csv.xz",
    "details-1648852411672.csv.xz",
    "details-1648852464182.csv.xz",
    "details-1648852511064.csv.xz",
    "details-1648852523944.csv.xz",
    "details-1648852615620.csv.xz",
    "details-1648852723128.csv.xz",
    "details-1648852970444.csv.xz",
    "details-1648853031242.csv.xz",
    "details-1648853063579.csv.xz",
    "details-1648853162505.csv.xz",
    "details-1648853270653.csv.xz",
    "details-1648853527092.csv.xz",
    "details-1648853619041.csv.xz",
    "details-1648854129523.csv.xz",
    "details-1648854141138.csv.xz",
    "details-1648854155610.csv.xz",
    "details-1648854288198.csv.xz",
    "details-1648855986664.csv.xz",
    "details-1648856072103.csv.xz",
    "details-1648856225161.csv.xz",
    "details-1648856301087.csv.xz",
    "details-1648856756516.csv.xz",
    "details-1648856780638.csv.xz",
    "details-1648916686994.csv.xz",
    "details-1648916836150.csv.xz",
    "details-1648916935467.csv.xz",
    "details-1648916955962.csv.xz",
    "details-1648917094074.csv.xz",
    "details-1648917123519.csv.xz",
    "details-1648917297932.csv.xz",
    "details-1648917352825.csv.xz",
    "details-1648917414917.csv.xz",
    "details-1648917434118.csv.xz",
    "details-1648917503333.csv.xz",
    "details-1648918254005.csv.xz",
    "details-1648918409990.csv.xz",
    "details-1648918490475.csv.xz",
    "details-1648918654648.csv.xz",
    "details-1648918855244.csv.xz",
    "details-1648918987574.csv.xz",
    "details-1648919137613.csv.xz",
    "details-1648919143491.csv.xz",
    "details-1648919249576.csv.xz",
    "details-1648919329023.csv.xz",
    "details-1648919372640.csv.xz",
    "details-1648919391776.csv.xz",
    "details-1648919408534.csv.xz",
    "details-1648924148338.csv.xz",
    "details-1648924997299.csv.xz",
    "details-1648926051076.csv.xz",
    "details-1648926251287.csv.xz",
    "details-1648926325934.csv.xz",
    "details-1648926363072.csv.xz",
    "details-1648926662391.csv.xz",
    "details-1648926706681.csv.xz",
    "details-1648926724645.csv.xz",
    "details-1648926753057.csv.xz",
    "details-1648926763116.csv.xz",
    "details-1648926831306.csv.xz",
    "details-1648926903200.csv.xz",
    "details-1648927392699.csv.xz",
    "details-1648927436942.csv.xz",
    "details-1648927695648.csv.xz",
    "details-1648927721517.csv.xz",
    "details-1648927724975.csv.xz",
    "details-1648927749035.csv.xz",
    "details-1648927753104.csv.xz",
    "details-1648927842157.csv.xz",
    "details-1648928188597.csv.xz",
    "details-1648928217462.csv.xz",
    "details-1648928439474.csv.xz",
    "details-1648928456793.csv.xz",
    "details-1648928760261.csv.xz",
    "details-1648928832033.csv.xz",
    "details-1648928856857.csv.xz",
    "details-1648929134214.csv.xz",
    "details-1648929487533.csv.xz",
    "details-1648930345105.csv.xz",
    "details-1648930346391.csv.xz",
    "details-1648930391069.csv.xz",
    "details-1648930396540.csv.xz",
    "details-1648931370611.csv.xz",
    "details-1648932385637.csv.xz",
    "details-1648937731553.csv.xz",
    "details-1648937874674.csv.xz",
    "details-1648937919747.csv.xz",
    "details-1648937922467.csv.xz",
    "details-1648938003819.csv.xz",
    "details-1648938061222.csv.xz",
    "details-1648938090208.csv.xz",
    "details-1648940090527.csv.xz",
    "details-1648940241539.csv.xz",
    "details-1648940329389.csv.xz",
    "details-1648940479144.csv.xz",
    "details-1648940529484.csv.xz",
    "details-1648940535337.csv.xz",
    "details-1648940649165.csv.xz",
    "details-1648940698456.csv.xz",
    "details-1649013111520.csv.xz",
    "details-1649013157836.csv.xz",
    "details-1649013197899.csv.xz",
    "details-1649013222540.csv.xz",
    "details-1649013361028.csv.xz",
    "details-1649013429949.csv.xz",
    "details-1649090013255.csv.xz",
    "details-1649108974796.csv.xz"
]

# load config
config = configparser.ConfigParser()
config.read("config.ini")
input_dir = "/" + config['original']['unofficial'].rstrip('/').lstrip('/')
output_dir = "/" + config['compressed']['unofficial'].rstrip('/').lstrip('/')
os.makedirs(input_dir, exist_ok=True)
os.makedirs(output_dir, exist_ok=True)

start = 1648771200 * 1000  # april 1, 12am timestamp in ms
user_map: dict[str, int] = {}
user_i = 0


def parse_stream(r, filename):
    global user_map
    global user_i

    basename, _ = os.path.splitext(filename)
    out = ["timestamp,user_id,pixel_x,pixel_y"]
    for line in tqdm(r, position=1, leave=False, desc=f"Processing {filename}..."):
        new_line = re.sub(r',', '|', line.rstrip(), count=3)
        new_line = re.sub(r'\\', '', new_line)
        data = new_line.split("|")[3].lstrip('"').rstrip('"')
        try:
            j = json.loads(data)
        except Exception:
            continue
        try:
            for elem in j["data"]:
                try:
                    user = j["data"][elem]["data"][0]["data"]["userInfo"]["username"]
                    new_user: int | None = user_map.get(user)

                    if new_user is None:
                        new_user = user_i
                        user_i += 1
                        user_map[user] = new_user

                    pixels = elem.lstrip("p").replace("x", ",")
                    x, y = pixels.split(",")

                    if "c" in y:
                        y, canvas = y.split("c")
                        if int(canvas) in [1, 3]:
                            x = int(x) + 1000
                        if int(canvas) in [2, 3]:
                            y = int(y) + 1000

                    ts = float(j["data"][elem]["data"][0]["data"]["lastModifiedTimestamp"])
                    new_time = int(ts - start)

                    out.append(f"{new_time},{new_user},{int(x)},{int(y)}")
                except Exception:
                    continue
        except Exception:
            continue

    with open(os.path.join(output_dir, basename), "w+") as f:
        f.write("\n".join(out))


for filename in tqdm(INTERNET_ARCHIVE_FILES, desc="Processing unofficial files", position=0, leave=False):
    if not os.path.isfile(os.path.join(input_dir, filename)):
        url = INTERNET_ARCHIVE_URL.format(filename)
        r = requests.get(url, stream=True)

        # https://stackoverflow.com/a/70400902
        with open(os.path.join(input_dir, filename), 'wb') as f:
            pbar = tqdm(unit="B", unit_scale=True, unit_divisor=1024, total=int(r.headers['Content-Length']),
                        desc=f"Download {filename}", position=1, leave=False)
            pbar.clear()
            for chunk in r.iter_content(chunk_size=1024):
                if chunk:
                    pbar.update(len(chunk))
                    f.write(chunk)
            pbar.close()

    with lzma.open(os.path.join(input_dir, filename), mode='rt') as r:
        parse_stream(r, filename)

with open(os.path.join(output_dir, "users"), "w+") as f:
    json.dump(user_map, f, indent=4)
