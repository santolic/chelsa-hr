#!/usr/bin/env python3
"""
chelsa_hr.py - CHELSA -> Hrvatska

Lista datoteke direktno iz javnih S3 bucketa (chelsav2, chelsa02, chelsav1),
za svaku cita samo prozor koji pokriva Hrvatsku (GeoTIFF/SAGA preko HTTP
range zahtjeva; NetCDF se privremeno skine cijeli) i sprema ga lokalno kao
malen, komprimiran GeoTIFF.

Instalacija (jednom):
    pip install rasterio numpy

Primjeri:
    python chelsa_hr.py --out D:\\chelsa_hr --dry-run        # samo popis, broj datoteka, globalna velicina, slobodan prostor
    python chelsa_hr.py --out D:\\chelsa_hr --estimate       # izreze uzorak i procijeni stvarnu velicinu za Hrvatsku
    python chelsa_hr.py --out D:\\chelsa_hr                  # skidanje (default grupe, bez dnevnih)
    python chelsa_hr.py --out D:\\chelsa_hr --groups daily   # dnevni podaci (puno datoteka, ide posebno)
    python chelsa_hr.py --out D:\\chelsa_hr --groups extra   # dodatni set (SPEI/SPI, CHELSAcruts, EUR11, DEM...)
    python chelsa_hr.py --out D:\\chelsa_hr --only "bio"     # samo kljucevi koji sadrze 'bio'

Prekinuto skidanje se nastavlja istom naredbom (postojece datoteke se preskacu).
"""
import argparse
import csv
import os
import re
import shutil
import sys
import tempfile
import threading
import time
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

# GDAL/HTTP postavke moraju biti u okolini prije importa rasterija
os.environ.setdefault("GDAL_DISABLE_READDIR_ON_OPEN", "EMPTY_DIR")
os.environ.setdefault("CPL_VSIL_CURL_ALLOWED_EXTENSIONS", ".tif,.sdat,.sgrd,.prj")
os.environ.setdefault("GDAL_HTTP_MAX_RETRY", "5")
os.environ.setdefault("GDAL_HTTP_RETRY_DELAY", "2")
os.environ.setdefault("GDAL_INGESTED_BYTES_AT_OPEN", "65536")
os.environ.setdefault("VSI_CACHE", "TRUE")
os.environ.setdefault("GDAL_HTTP_MULTIPLEX", "YES")

V2 = "https://os.zhdk.cloud.switch.ch/chelsav2/"
V1 = "https://os.zhdk.cloud.switch.ch/chelsav1/"
C02 = "https://os.unil.cloud.switch.ch/chelsa02/"
BASE = V2

# Hrvatska (13.49-19.45 E, 42.39-46.56 N) + ~0.2 stupnja margine
BBOX = (13.3, 42.2, 19.65, 46.75)  # west, south, east, north


def src(base, prefix, include=None, rename=(r"^GLOBAL/", "")):
    """Izvor: bucket, prefiks, opcionalni regex filter, (pattern, repl) za lokalnu putanju."""
    return {"base": base, "prefix": prefix, "include": include, "rename": rename}


GROUPS = {
    "clim_present": [src(V2, "GLOBAL/climatologies/1981-2010/")],
    "clim_future": [
        src(V2, "GLOBAL/climatologies/2011-2040/"),
        src(V2, "GLOBAL/climatologies/2041-2070/"),
        src(V2, "GLOBAL/climatologies/2071-2100/"),
    ],
    "monthly": [src(V2, "GLOBAL/monthly/")],
    "annual": [src(V2, "GLOBAL/annual/")],
    "daily_normals": [src(V2, "GLOBAL/daily_normals/")],
    "daily": [src(V2, "GLOBAL/daily/")],
    # --- dodatni set ---
    # chelsa02: SPEI/SPI i mjeseci 2020-2021 (1979-2019 vec imamo iz chelsav2)
    "monthly_new": [src(C02, "chelsa/global/monthly/", r"/(spei12|spi12)/|/(2020|2021)/",
                        (r"^chelsa/global/monthly/([^/]+)/\d{4}/", r"monthly/\1/"))],
    "annual_veg": [src(C02, "chelsa/global/annual/", r"/(mymd|mymd10|qkndvi)/",
                       (r"^chelsa/global/annual/([^/]+)/\d{4}/", r"annual/\1/"))],
    "cruts": [src(V1, "chelsa_cruts/", None, (r"^chelsa_cruts/", "chelsacruts/"))],
    "eur11": [src(V2, "EUR11/",
                  r"^EUR11/(obs/annual/V(1\.1|2\.1)/|obs/\d{4}-\d{4}/bio/|obs/normals/[^/]+\.nc$|rcp)",
                  (r"^EUR11/", "EUR11/"))],
    "static": [
        src(V2, "GLOBAL/input/", r"/(dem_latlong|landseamask|expocor)\.sdat$", (r"^GLOBAL/input/", "input/")),
        src(C02, "chelsa/global/input/static/", r"/Wind Exposition_latlong\.sdat$",
            (r"^chelsa/global/input/static/", "input/")),
    ],
}
ALIASES = {"extra": ["monthly_new", "annual_veg", "cruts", "eur11", "static"]}
DEFAULT_GROUPS = ["clim_present", "clim_future", "monthly", "annual", "daily_normals"]
SKIP_PARTS = ("/5km/", "/ncdf/")  # 5 km kopija i NetCDF duplikati
EXTS = (".tif", ".nc", ".sdat")
NC_SLOTS = threading.Semaphore(int(os.environ.get("CHELSA_NC_SLOTS", "2")))  # NetCDF se skida cijeli


# ----------------------------------------------------------------- listing
def http_get(url, retries=5, timeout=60):
    last = None
    for attempt in range(retries):
        try:
            with urllib.request.urlopen(url, timeout=timeout) as r:
                return r.read()
        except Exception as e:  # noqa: BLE001
            last = e
            time.sleep(2 ** attempt)
    raise RuntimeError(f"GET {url} nije uspio: {last}")


def parse_listing(xml_bytes):
    """Vraca (lista (key, size), is_truncated, next_marker)."""
    root = ET.fromstring(xml_bytes)
    ns = root.tag.split("}")[0] + "}" if root.tag.startswith("{") else ""
    items = []
    for c in root.iter(ns + "Contents"):
        items.append((c.findtext(ns + "Key"), int(c.findtext(ns + "Size") or 0)))
    truncated = (root.findtext(ns + "IsTruncated") or "false").lower() == "true"
    return items, truncated, root.findtext(ns + "NextMarker")


def list_keys(base, prefix):
    marker = ""
    while True:
        q = {"prefix": prefix, "max-keys": "1000"}
        if marker:
            q["marker"] = marker
        items, truncated, nxt = parse_listing(http_get(base + "?" + urllib.parse.urlencode(q)))
        yield from items
        if not truncated or not items:
            break
        marker = nxt or items[-1][0]


def collect(groups, only):
    """Vraca {grupa: [(url, size, lokalna_rel_putanja), ...]}."""
    rx = re.compile(only) if only else None
    plan = {}
    for g in groups:
        files = []
        for s in GROUPS[g]:
            inc = re.compile(s["include"]) if s["include"] else None
            for key, size in list_keys(s["base"], s["prefix"]):
                if not key.lower().endswith(EXTS):
                    continue
                if any(p in key for p in SKIP_PARTS):
                    continue
                if inc and not inc.search(key):
                    continue
                if rx and not rx.search(key):
                    continue
                rel = re.sub(s["rename"][0], s["rename"][1], key)
                rel = os.path.splitext(rel)[0] + ".tif"
                files.append((s["base"] + urllib.parse.quote(key), size, rel))
        plan[g] = files
    return plan


# -------------------------------------------------------------------- crop
def crop_raster(src_path, dst_path, bbox):
    import rasterio
    from rasterio.windows import Window, from_bounds

    with rasterio.open(src_path) as src:
        win = from_bounds(*bbox, transform=src.transform)
        win = win.round_offsets(op="floor").round_lengths(op="ceil")
        win = win.intersection(Window(0, 0, src.width, src.height))
        data = src.read(window=win)
        nodata = src.nodata
        if nodata is None and data.dtype.name == "int16" and (data == -32768).any():
            nodata = -32768  # CHELSAcruts nema upisan nodata; more je -32768
        profile = dict(
            driver="GTiff",
            height=data.shape[1],
            width=data.shape[2],
            count=src.count,
            dtype=data.dtype,
            crs=src.crs or "EPSG:4326",
            transform=src.window_transform(win),
            nodata=nodata,
            tiled=True,
            blockxsize=256,
            blockysize=256,
            compress="deflate",
            predictor=3 if data.dtype.kind == "f" else 2,
            zlevel=9,
        )
        if src.count > 1:
            profile["interleave"] = "band"
        with rasterio.open(dst_path, "w", **profile) as dst:
            dst.write(data)
            dst.scales = src.scales  # CHELSA cuva vrijednosti kao int + scale/offset
            dst.offsets = src.offsets
            dst.update_tags(**src.tags())
            for i in range(1, src.count + 1):
                if src.descriptions[i - 1]:
                    dst.set_band_description(i, src.descriptions[i - 1])
                dst.update_tags(i, **src.tags(i))


def nc_variables(path):
    """Podatkovne varijable u NetCDF-u (bez koordinata, granica i crs-a)."""
    import rasterio

    with rasterio.open(path) as s:
        subs = list(s.subdatasets)
    if not subs:
        return [(path, "")]
    skip = re.compile(r":(time|lat|lon|latitude|longitude|crs|x|y)$|_bnds$|_bounds$", re.I)
    return [(sd, sd.rsplit(":", 1)[-1]) for sd in subs if not skip.search(sd)]


def crop_nc(url, dst, bbox):
    """NetCDF: skini cijeli u privremenu mapu, izrezi svaku varijablu, obrisi."""
    with NC_SLOTS:
        tmpdir = tempfile.mkdtemp(prefix="chelsa_nc_")
        try:
            local = os.path.join(tmpdir, "src.nc")
            with urllib.request.urlopen(url, timeout=120) as r, open(local, "wb") as f:
                shutil.copyfileobj(r, f, 8 << 20)
            variables = nc_variables(local)
            for path, var in variables:
                out = dst if len(variables) == 1 else dst.with_name(f"{dst.stem}_{var}{dst.suffix}")
                part = out.with_name(out.name + ".part")
                crop_raster(path, str(part), bbox)
                os.replace(part, out)
            return len(variables)
        finally:
            shutil.rmtree(tmpdir, ignore_errors=True)


def crop_key(url, rel, out_root, bbox, retries=4):
    dst = Path(out_root) / rel
    done = dst.with_name(dst.name + ".done")  # NetCDF s vise varijabli -> vise datoteka
    if dst.exists() and dst.stat().st_size > 0:
        return "skip", dst.stat().st_size, ""
    if done.exists():
        return "skip", 0, ""
    dst.parent.mkdir(parents=True, exist_ok=True)
    tmp = dst.parent / (dst.name + ".part")
    is_nc = urllib.parse.unquote(url).lower().endswith(".nc")
    err = ""
    for attempt in range(retries):
        try:
            if is_nc:
                n = crop_nc(url, dst, bbox)
                if not dst.exists():
                    done.write_text(f"{n} varijabli\n")
                    return "ok", sum(p.stat().st_size for p in dst.parent.glob(dst.stem + "_*.tif")), ""
            else:
                crop_raster("/vsicurl/" + url, str(tmp), bbox)
                os.replace(tmp, dst)
            return "ok", dst.stat().st_size, ""
        except Exception as e:  # noqa: BLE001
            err = str(e).replace("\n", " ")[:200]
            try:
                tmp.unlink()
            except FileNotFoundError:
                pass
            time.sleep(2 ** attempt)
    return "fail", 0, err


# ------------------------------------------------------------------- utils
def gb(n):
    return f"{n / 1e9:,.1f} GB"


def free_space(path):
    p = Path(path).resolve()
    while not p.exists() and p.parent != p:
        p = p.parent
    return shutil.disk_usage(p).free


def print_plan(plan, out_root):
    total_n = total_b = 0
    print(f"\n{'grupa':<16}{'datoteka':>10}{'globalno (ne skida se)':>26}")
    for g, files in plan.items():
        n, b = len(files), sum(s for _, s, _ in files)
        total_n += n
        total_b += b
        print(f"{g:<16}{n:>10,}{gb(b):>26}")
    print(f"{'UKUPNO':<16}{total_n:>10,}{gb(total_b):>26}")
    print(f"\nSlobodno na disku ({out_root}): {gb(free_space(out_root))}")
    return total_n


def estimate(plan, bbox, samples=4):
    """Izrezuje uzorak datoteka iz svake grupe i ekstrapolira velicinu za Hrvatsku."""
    tmpdir = tempfile.mkdtemp(prefix="chelsa_est_")
    grand = 0.0
    print("\nProcjena velicine za Hrvatsku (uzorak):")
    try:
        for g, files in plan.items():
            if not files:
                continue
            step = max(1, len(files) // samples)
            picks = files[::step][:samples]
            sizes = []
            for url, _, rel in picks:
                st, sz, err = crop_key(url, rel, tmpdir, bbox)
                if st == "fail":
                    print(f"  ! {rel}: {err}")
                else:
                    sizes.append(sz)
            if sizes:
                avg = sum(sizes) / len(sizes)
                total = avg * len(files)
                grand += total
                print(f"  {g:<16}{len(files):>8,} datoteka  ~{avg / 1e6:5.2f} MB/dat.  ->  ~{gb(total)}")
        print(f"  {'UKUPNO':<16}{'':>8}              ~{gb(grand)}")
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)


def run(plan, out_root, bbox, workers, limit):
    jobs = [(url, rel) for files in plan.values() for url, _, rel in files]
    if limit:
        jobs = jobs[:limit]
    out_root = Path(out_root)
    out_root.mkdir(parents=True, exist_ok=True)
    manifest = out_root / "_manifest.csv"
    new = not manifest.exists()
    lock = threading.Lock()
    done = ok = skipped = failed = 0
    nbytes = 0
    t0 = time.time()
    with open(manifest, "a", newline="", encoding="utf-8") as mf:
        w = csv.writer(mf)
        if new:
            w.writerow(["key", "status", "bytes", "error"])
        ex = ThreadPoolExecutor(max_workers=workers)
        futs = {ex.submit(crop_key, url, rel, out_root, bbox): rel for url, rel in jobs}
        try:
            for f in as_completed(futs):
                rel = futs[f]
                st, sz, err = f.result()
                with lock:
                    done += 1
                    nbytes += sz
                    ok += st == "ok"
                    skipped += st == "skip"
                    failed += st == "fail"
                    if st != "skip":
                        w.writerow([rel, st, sz, err])
                        mf.flush()
                    if done % 50 == 0 or done == len(jobs):
                        rate = done / max(time.time() - t0, 1e-6)
                        eta = (len(jobs) - done) / rate if rate else 0
                        print(f"[{done:,}/{len(jobs):,}] ok={ok:,} skip={skipped:,} fail={failed:,} "
                              f"{rate:.1f} dat/s  ETA {eta / 60:,.0f} min  na disku {gb(nbytes)}", flush=True)
        except KeyboardInterrupt:
            print("\nPrekidam - ponovno pokreni istu naredbu za nastavak.")
            ex.shutdown(wait=False, cancel_futures=True)
            sys.exit(1)
        ex.shutdown()
    print(f"\nGotovo. ok={ok:,} preskoceno={skipped:,} neuspjelo={failed:,}. "
          f"Neuspjele ponovno pokreni istom naredbom (vidi {manifest}).")


def main():
    ap = argparse.ArgumentParser(description="CHELSA -> Hrvatska (streaming crop)")
    ap.add_argument("--out", required=True, help="odredisna mapa, npr. D:\\chelsa_hr")
    ap.add_argument("--groups", default=",".join(DEFAULT_GROUPS),
                    help=f"zarezom odvojeno; moguce: {', '.join(list(GROUPS) + list(ALIASES))} "
                         "(default bez 'daily')")
    ap.add_argument("--bbox", type=float, nargs=4, metavar=("W", "S", "E", "N"), default=BBOX)
    ap.add_argument("--only", help="regex filter na S3 kljuc, npr. 'ssp370|ssp585' ili '/bio/'")
    ap.add_argument("--workers", type=int, default=6)
    ap.add_argument("--limit", type=int, help="samo prvih N datoteka (test)")
    ap.add_argument("--shard", help="I/N - obradi samo svaku N-tu datoteku pocevsi od I (paralelni jobovi)")
    ap.add_argument("--dry-run", action="store_true", help="samo popis i slobodan prostor")
    ap.add_argument("--estimate", action="store_true", help="izrezi uzorak i procijeni velicinu")
    a = ap.parse_args()

    groups = []
    for g in (g.strip() for g in a.groups.split(",") if g.strip()):
        groups += ALIASES.get(g, [g])
    bad = [g for g in groups if g not in GROUPS]
    if bad:
        ap.error(f"nepoznata grupa: {bad}")

    print("Listam bucket...")
    plan = collect(groups, a.only)
    if a.shard:
        i, n = (int(x) for x in a.shard.split("/"))
        flat = sorted((rel, url, s, g) for g, files in plan.items() for url, s, rel in files)[i::n]
        plan = {g: [(url, s, rel) for rel, url, s, gg in flat if gg == g] for g in plan}
    print_plan(plan, a.out)
    if a.dry_run:
        return
    if a.estimate:
        estimate(plan, tuple(a.bbox))
        return
    run(plan, a.out, tuple(a.bbox), a.workers, a.limit)


if __name__ == "__main__":
    main()
