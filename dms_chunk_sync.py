#!/usr/bin/env python3
import os
import sys
import time
import hashlib
import subprocess
import re
import argparse
import warnings
import shutil
import xml.etree.ElementTree as ET

# Optional imports – used only in certain download modes
try:
    import requests
except ImportError:
    requests = None

# =========================
# Common configuration
# =========================

DMS_BASE = "https://dms.uom.lk/remote.php/webdav/"
CHUNKS_DIR_REMOTE = "chunks/"      # folder in WebDAV
CHUNKS_DIR_LOCAL = "./chunks"      # local temp directory for chunks
MANIFEST_NAME = "manifest.txt"

# Chunk size: 1 GiB
CHUNK_SIZE_BYTES = 1 * 1024 * 1024 * 1024

# =========================
# Progress bar helper
# =========================

def draw_progress(prefix, current, total, bar_length=40):
    if total is None or total <= 0:
        pct_clamped = 0.0
    else:
        pct = current / total
        pct_clamped = max(0.0, min(1.0, pct))
    filled = int(bar_length * pct_clamped)
    bar = "█" * filled + "░" * (bar_length - filled)
    sys.stdout.write(f"\r{prefix} [{bar}] {pct_clamped*100:5.1f}%")
    sys.stdout.flush()
    if total is not None and current >= total:
        sys.stdout.write("\n")
        sys.stdout.flush()

# =========================
# Curl helpers
# =========================

def build_login_detail(username, password):
    # Wrap in quotes to be used directly in -u
    return f'"{username}:{password}"'

def execute_curl_with_progress(curl_command, label):
    """
    Run curl, parsing stderr for "XX.X%" progress and showing a per-file bar.
    """
    process = subprocess.Popen(
        curl_command,
        shell=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        universal_newlines=True,
    )
    try:
        for line in process.stderr:
            match = re.search(r'(\d+(\.\d+)?)%', line)
            if match:
                progress = float(match.group(1))
                draw_progress(label, progress, 100.0)
        return_code = process.wait()
        if return_code == 0:
            print(f"{label}: done.")
        else:
            print(f"{label}: curl returned non-zero exit code {return_code}")
    except Exception as e:
        print(f"{label}: error running curl: {e}")

def curl_http_code(curl_command):
    """
    Run curl and return only the HTTP status code as string.
    """
    try:
        result = subprocess.check_output(curl_command, shell=True)
        return result.decode().strip()
    except subprocess.CalledProcessError:
        return ""

# =========================
# DMS / WebDAV helpers
# =========================

def dms_exists(login_detail, url):
    curl_command = (
        f'curl -u {login_detail} -o /dev/null -s '
        f'-w "%{{http_code}}" -I "{url}"'
    )
    status_code = curl_http_code(curl_command)
    return status_code == "200"

def dms_mkcol(login_detail, remote_dir_url):
    curl_command = f'curl -u {login_detail} -s -o /dev/null -X MKCOL "{remote_dir_url}"'
    subprocess.call(curl_command, shell=True)

def dms_delete(login_detail, url, label=None):
    curl_command = f'curl -u {login_detail} -s -o /dev/null -X DELETE "{url}"'
    try:
        subprocess.check_output(curl_command, shell=True)
        if label:
            print(f"Deleted from DMS: {label}")
    except Exception as e:
        print(f"Error deleting from DMS {label or url}: {e}")

def dms_upload_file(login_detail, local_path, remote_url, label):
    curl_command = (
        f'curl -u {login_detail} --progress-bar '
        f'-T "{local_path}" "{remote_url}"'
    )
    execute_curl_with_progress(curl_command, f"Upload {label}")

def dms_download_file(login_detail, remote_url, local_path, label):
    curl_command = (
        f'curl -u {login_detail} --progress-bar '
        f'-o "{local_path}" "{remote_url}"'
    )
    execute_curl_with_progress(curl_command, f"Download {label}")

# =========================
# Checksum helpers
# =========================

def sha256_file(path):
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for block in iter(lambda: f.read(4096), b""):
            h.update(block)
    return h.hexdigest()

# =========================
# Storage & quota helpers
# =========================

def get_dms_quota(login_detail):
    """
    Fetch DMS quota info via WebDAV PROPFIND.
    Returns (used_bytes, available_bytes) or (-1, -1) if failed.
    """
    quota_url = DMS_BASE
    headers = ' -H "Depth: 0" -H "Content-Type: application/xml" '
    data = "'<?xml version=\"1.0\"?><propfind xmlns=\"DAV:\"><prop><quota-available-bytes/><quota-used-bytes/></prop></propfind>'"
    curl_cmd = (
        f"curl -u {login_detail} -s {headers} "
        f"-X PROPFIND --data {data} \"{quota_url}\""
    )
    try:
        output = subprocess.check_output(curl_cmd, shell=True).decode()
        tree = ET.fromstring(output)
        ns = {"d": "DAV:"}
        used = tree.find('.//d:quota-used-bytes', ns)
        avail = tree.find('.//d:quota-available-bytes', ns)
        used_bytes = int(used.text) if used is not None else -1
        avail_bytes = int(avail.text) if avail is not None else -1
        return used_bytes, avail_bytes
    except Exception as e:
        print("Failed to fetch DMS quota:", e)
        return -1, -1

def get_vps_free_space():
    """Return free disk space on VPS in bytes."""
    return shutil.disk_usage("/").free

def get_remote_file_size(url):
    """Return Content-Length for HTTP/HTTPS download links, or -1 if unknown."""
    if requests is None:
        return -1
    try:
        r = requests.head(url, allow_redirects=True, timeout=10)
        if "Content-Length" in r.headers:
            return int(r.headers["Content-Length"])
        return -1
    except Exception:
        return -1

# =========================
# Producer: download original file on VPS
# =========================

def download_regular_file(download_link, file_name):
    if requests is None:
        raise RuntimeError("requests not installed; needed for regular HTTP download.")

    cwd = os.getcwd()
    full_output_path = os.path.join(cwd, file_name)

    max_retries = 3
    for attempt in range(max_retries):
        try:
            resp = requests.get(download_link, stream=True)
            resp.raise_for_status()
            total = int(resp.headers.get("Content-Length", 0)) or None
            downloaded = 0

            with open(full_output_path, "wb") as f:
                for chunk in resp.iter_content(chunk_size=51200):
                    if chunk:
                        f.write(chunk)
                        downloaded += len(chunk)
                        if total:
                            draw_progress("Download original", downloaded, total)
            print(f"\nFile downloaded successfully: {full_output_path}")
            return full_output_path
        except Exception as e:
            print(f"Download attempt {attempt+1} failed: {e}")
            if attempt < max_retries - 1:
                print("Retrying in 5 seconds...")
                time.sleep(5)
            else:
                print("Failed to download after multiple attempts.")
                raise

def download_torrent_magnet(download_link, file_name_hint):
    """
    Minimal torrent download using libtorrent.
    """
    try:
        import libtorrent as lt # type: ignore
    except ImportError:
        raise RuntimeError("libtorrent not installed; torrent mode not available on VPS.")

    with warnings.catch_warnings():
        warnings.simplefilter("ignore", category=DeprecationWarning)
        ses = lt.session()
        params = {
            "save_path": "./",
            "storage_mode": lt.storage_mode_t(2),
        }
        handle = lt.add_magnet_uri(ses, download_link, params)
        sys.stdout.write("Downloading metadata...\n")
        sys.stdout.flush()
        while not handle.has_metadata():
            time.sleep(1)

        info = handle.get_torrent_info()
        torrent_name = info.name()
        sys.stdout.write("Metadata downloaded, starting torrent download...\n")
        sys.stdout.flush()

        while handle.status().state != lt.torrent_status.seeding:
            s = handle.status()
            state_str = [
                "queued", "checking", "downloading metadata",
                "downloading", "finished", "seeding",
                "allocating", "checking fastresume",
            ]
            progress = (
                f"{s.progress * 100:.2f}% complete "
                f"(down: {s.download_rate/1000:.1f} kB/s "
                f"up: {s.upload_rate/1000:.1f} kB/s "
                f"peers: {s.num_peers} {state_str[s.state]})"
            )
            sys.stdout.write("\r" + progress)
            sys.stdout.flush()
            time.sleep(1)

        sys.stdout.write("\nTorrent download complete.\n")
        sys.stdout.flush()

        # If multiple files, just zip folder; keep this simple
        out_name = file_name_hint or torrent_name
        if os.path.isdir(torrent_name):
            zip_name = out_name + ".zip"
            print("Zipping torrent folder into:", zip_name)
            subprocess.run(["zip", "-r", zip_name, torrent_name], check=True)
            return zip_name
        else:
            # Single file torrent
            return torrent_name

# =========================
# Chunking + manifest
# =========================

def split_file_into_chunks(file_path, chunks_dir, chunk_size=CHUNK_SIZE_BYTES):
    os.makedirs(chunks_dir, exist_ok=True)
    base = os.path.basename(file_path)
    size = os.path.getsize(file_path)
    print(f"Splitting '{file_path}' ({size} bytes) into {chunk_size}-byte chunks...")

    chunk_num = 0
    total_bytes_read = 0

    with open(file_path, "rb") as f_in:
        while True:
            chunk = f_in.read(chunk_size)
            if not chunk:
                break
            chunk_filename = f"{base}.part{chunk_num:03d}"
            chunk_path = os.path.join(chunks_dir, chunk_filename)
            with open(chunk_path, "wb") as f_out:
                f_out.write(chunk)
            total_bytes_read += len(chunk)
            draw_progress("Chunking", total_bytes_read, size)
            print(f"  Created chunk: {chunk_path}")
            chunk_num += 1

    print(f"File '{file_path}' split into {chunk_num} chunks in '{chunks_dir}'.")
    return chunk_num

def create_manifest(chunks_dir, manifest_path):
    files = [
        f for f in os.listdir(chunks_dir)
        if os.path.isfile(os.path.join(chunks_dir, f))
    ]
    files = sorted(files)
    print(f"Creating manifest for {len(files)} chunks...")

    with open(manifest_path, "w") as mf:
        for fname in files:
            full_path = os.path.join(chunks_dir, fname)
            checksum = sha256_file(full_path)
            mf.write(f"{checksum}  {fname}\n")
    print(f"Manifest saved: {manifest_path}")
    return files

def load_manifest(manifest_path):
    manifest = {}
    with open(manifest_path, "r") as mf:
        for line in mf:
            line = line.strip()
            if not line:
                continue
            checksum, fname = line.split(maxsplit=1)
            manifest[fname.strip()] = checksum.strip()
    return manifest

# =========================
# Producer mode (VPS)
# =========================

def producer_mode():
    print("=== PRODUCER MODE (VPS) ===")

    dms_user = input("DMS Username: ").strip()
    dms_pass = input("DMS Password: ").strip()
    login_detail = build_login_detail(dms_user, dms_pass)

    # ----- SHOW DMS QUOTA -----
    used_dms, avail_dms = get_dms_quota(login_detail)
    if used_dms >= 0 and avail_dms >= 0:
        print(f"\nDMS Used     : {used_dms/1e9:.2f} GB")
        print(f"DMS Available: {avail_dms/1e9:.2f} GB\n")
    else:
        print("\nWarning: could not fetch DMS quota (skipping DMS space checks).\n")

    # ----- SHOW VPS FREE STORAGE -----
    vps_free = get_vps_free_space()
    print(f"VPS Free Storage: {vps_free/1e9:.2f} GB\n")

    # Step 1 — Ask for download link
    download_link = input("Download link (magnet/http/https): ").strip()

    remote_size = -1
    if not download_link.startswith("magnet:?"):
        remote_size = get_remote_file_size(download_link)
        if remote_size > 0:
            print(f"Remote file size: {remote_size/1e9:.2f} GB")
        else:
            print("⚠ Could not determine remote file size.")

    # Step 2 — Ask file name
    file_name = input("FileName to use (base name, no path, e.g., movie.mkv): ").strip()
    if not file_name:
        print("FileName is required.")
        sys.exit(1)

    # ----- STORAGE CHECKS -----
    # VPS: we need space for at least the file OR one chunk, whichever is larger
    required_vps = CHUNK_SIZE_BYTES
    if remote_size > 0:
        required_vps = max(remote_size, CHUNK_SIZE_BYTES)
    if required_vps >= vps_free:
        print("\n❌ ERROR: VPS does not have enough space.")
        print(f"Required (approx): {required_vps/1e9:.2f} GB")
        print(f"Available       : {vps_free/1e9:.2f} GB")
        sys.exit(1)

    # DMS: must be able to hold at least one chunk
    if avail_dms >= 0 and CHUNK_SIZE_BYTES >= avail_dms:
        print("\n❌ ERROR: DMS cannot hold even one chunk.")
        print(f"Chunk size: {CHUNK_SIZE_BYTES/1e9:.2f} GB")
        print(f"DMS free : {avail_dms/1e9:.2f} GB")
        sys.exit(1)

    print("\nStorage checks passed. Proceeding...\n")

    # Step 3: Download original file on VPS
    if download_link.startswith("magnet:?xt=urn:btih:"):
        original_path = download_torrent_magnet(download_link, file_name)
    else:
        original_path = download_regular_file(download_link, file_name)

    # Step 4: Chunk + manifest
    chunks_dir = CHUNKS_DIR_LOCAL
    split_file_into_chunks(original_path, chunks_dir, CHUNK_SIZE_BYTES)
    manifest_path = os.path.join(chunks_dir, MANIFEST_NAME)
    chunk_files = create_manifest(chunks_dir, manifest_path)

    # Step 5: Upload manifest + chunks one-by-one to DMS, wait for consumer
    remote_chunks_url = DMS_BASE + CHUNKS_DIR_REMOTE
    dms_mkcol(login_detail, remote_chunks_url)

    # Upload manifest first
    remote_manifest_url = remote_chunks_url + MANIFEST_NAME
    dms_upload_file(login_detail, manifest_path, remote_manifest_url, MANIFEST_NAME)

    print("Manifest uploaded. Now uploading chunks one by one...")

    total_bytes = sum(os.path.getsize(os.path.join(chunks_dir, f)) for f in chunk_files)
    uploaded_bytes = 0

    for fname in chunk_files:
        local_chunk_path = os.path.join(chunks_dir, fname)
        remote_chunk_url = remote_chunks_url + fname

        # Before uploading, ensure DMS has enough free space for this chunk.
        chunk_size = os.path.getsize(local_chunk_path)
        while True:
            counter = 0
            used_dms, avail_dms = get_dms_quota(login_detail)
            # If we couldn't determine DMS quota, warn and proceed with upload
            if avail_dms < 0:
                print(f"Warning: could not determine DMS free space; proceeding to upload {fname}.")
                break
            if avail_dms >= chunk_size:
                # Enough space — proceed to upload
                break
            # Not enough space — wait for consumer to delete chunks
            if counter % 5 == 0:
                print(
                    f"Not enough DMS space to upload {fname}: need {chunk_size/1e9:.2f} GB, "
                    f"available {avail_dms/1e9:.2f} GB. Waiting for consumer to free space..."
                )
            time.sleep(10)
            counter += 1

        # Upload
        dms_upload_file(login_detail, local_chunk_path, remote_chunk_url, fname)

        # Wait for consumer to delete
        print(f"Waiting for consumer to delete {fname} from DMS...")
        while dms_exists(login_detail, remote_chunk_url):
            time.sleep(5)

        # Update global progress
        uploaded_bytes += os.path.getsize(local_chunk_path)
        draw_progress("Total upload", uploaded_bytes, total_bytes)

    print("\nAll chunks uploaded and acknowledged by consumer.")

    # Cleanup on VPS
    try:
        os.remove(original_path)
        print(f"Removed original file: {original_path}")
    except Exception:
        pass
    shutil.rmtree(chunks_dir, ignore_errors=True)
    print("Producer cleanup done.")

# =========================
# Consumer mode (Laptop)
# =========================

def consumer_mode():
    print("=== CONSUMER MODE (Laptop) ===")

    dms_user = input("DMS Username: ").strip()
    dms_pass = input("DMS Password: ").strip()
    login_detail = build_login_detail(dms_user, dms_pass)

    # Step 1: Download manifest from DMS
    remote_chunks_url = DMS_BASE + CHUNKS_DIR_REMOTE
    remote_manifest_url = remote_chunks_url + MANIFEST_NAME
    local_manifest_path = MANIFEST_NAME

    print("Waiting for manifest.txt to appear on DMS...")
    while not dms_exists(login_detail, remote_manifest_url):
        time.sleep(5)

    dms_download_file(login_detail, remote_manifest_url, local_manifest_path, MANIFEST_NAME)

    manifest = load_manifest(local_manifest_path)
    chunk_files = sorted(manifest.keys())

    os.makedirs(CHUNKS_DIR_LOCAL, exist_ok=True)

    total_chunks = len(chunk_files)
    chunks_done = 0

    for fname in chunk_files:
        remote_chunk_url = remote_chunks_url + fname
        local_chunk_path = os.path.join(CHUNKS_DIR_LOCAL, fname)
        expected_hash = manifest[fname]

        print(f"\n=== Handling chunk: {fname} ===")
        print("Waiting for chunk to appear on DMS...")
        while not dms_exists(login_detail, remote_chunk_url):
            time.sleep(5)

        # Download + verify with retries
        max_attempts = 3
        ok = False
        for attempt in range(1, max_attempts + 1):
            dms_download_file(login_detail, remote_chunk_url, local_chunk_path, fname)
            actual_hash = sha256_file(local_chunk_path)
            if actual_hash.lower() == expected_hash.lower():
                print(f"Checksum OK for {fname}")
                ok = True
                break
            else:
                print(f"Checksum mismatch for {fname} (attempt {attempt})")
                os.remove(local_chunk_path)
                time.sleep(3)

        if not ok:
            print(f"Failed to obtain valid chunk {fname} after {max_attempts} attempts. Aborting.")
            sys.exit(1)

        # Delete from DMS
        dms_delete(login_detail, remote_chunk_url, label=fname)

        # Update global "combined" progress
        chunks_done += 1
        draw_progress("Total download", chunks_done, total_chunks)

    print("\nAll chunks downloaded and verified. Merging...")

    # Step 3: Merge - group chunks by base filename
    if not chunk_files:
        print("No chunks in manifest. Nothing to merge.")
        return

    # Group chunks by base filename (e.g., super.zip, supernatural_S03.zip)
    chunk_groups = {}
    for fname in chunk_files:
        if ".part" in fname:
            base_name = fname.rsplit(".part", 1)[0]
        else:
            base_name = fname
        
        if base_name not in chunk_groups:
            chunk_groups[base_name] = []
        chunk_groups[base_name].append(fname)
    
    # Sort chunks in each group
    for base_name in chunk_groups:
        chunk_groups[base_name].sort()
    
    # Merge each group separately
    for base_name, group_files in chunk_groups.items():
        output_path = base_name
        print(f"\nMerging {len(group_files)} chunks into: {output_path}")
        
        with open(output_path, "wb") as out_f:
            for fname in group_files:
                chunk_path = os.path.join(CHUNKS_DIR_LOCAL, fname)
                with open(chunk_path, "rb") as in_f:
                    while True:
                        data = in_f.read(1024 * 1024)
                        if not data:
                            break
                        out_f.write(data)
        
        print(f"✓ Merged file written to: {output_path}")

    # Cleanup on laptop
    shutil.rmtree(CHUNKS_DIR_LOCAL, ignore_errors=True)
    try:
        os.remove(local_manifest_path)
    except Exception:
        pass

    print("Consumer cleanup done.")

# =========================
# Main
# =========================

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", choices=["producer", "consumer"], required=True,
                        help="producer (run on VPS) or consumer (run on laptop)")
    args = parser.parse_args()

    if args.mode == "producer":
        producer_mode()
    else:
        consumer_mode()

if __name__ == "__main__":
    main()
