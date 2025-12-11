
# DMS Chunks Download & Merge — Overview 📦

This repository contains two concise Python utilities to split/produce, upload/download, verify, and merge large files using chunked uploads and a SHA256 manifest. The tools are designed to be reliable and easy to integrate into small workflows.

Files
- `dms_chunk_sync.py` — Dual-mode tool:
  - `producer` 🔧 (VPS): download an original file (HTTP or magnet), split into chunk files, create `manifest.txt`, and upload chunks + manifest to a DMS WebDAV folder.
  - `consumer` 💻 (Laptop): download `manifest.txt` from DMS, fetch each chunk, verify checksums, delete remote chunks after successful download, and merge chunks into final file(s).
- `merge_chunks.py` 🧩 — Local utility that reads `manifest.txt`, verifies SHA256 checksums for listed chunks, groups chunk files by base filename (e.g., `super.zip.part*` → `super.zip`), and merges each group into a separate output file.
- `manifest.txt` 📜 — Manifest file with SHA256 checksum and filename pairs.
- `chunks/` — Directory to hold chunk files.

Quickstart ▶️

1) (Optional) Create a virtual environment and install `requests` (required for HTTP downloads in `dms_chunk_sync.py`):

```bash
python3 -m venv venv
source venv/bin/activate
pip install requests
```

2) Producer (on VPS) 🔧

Run the producer to download a file, split into chunks, generate `manifest.txt`, and upload to the DMS WebDAV folder. The script prompts for DMS credentials and the download link.

```bash
python3 dms_chunk_sync.py --mode producer
```

3) Consumer (on your laptop) 💻

Run the consumer to wait for `manifest.txt` on the DMS, download and verify each chunk, then merge them into final file(s).

```bash
python3 dms_chunk_sync.py --mode consumer
```

4) Local merge using the manifest 🧩

If you already have `chunks/` and `manifest.txt` locally, use `merge_chunks.py` to verify and merge files. This command groups chunks by base filename and creates separate merged outputs.

```bash
# defaults: manifest.txt in current dir and chunks in ./chunks
python3 merge_chunks.py

# explicit paths
python3 merge_chunks.py --manifest manifest.txt --chunks-dir chunks --output-dir .
```

Manifest format 📜

Each line in the manifest should contain the SHA256 checksum, two spaces, then the chunk filename. Example:

```
916e6338...  super.zip.part000
94df2197...  super.zip.part001
```

Behavior notes ⚠️
- Both merging tools operate only on files listed in `manifest.txt`.
- When multiple independent files are present in the manifest (for example `super.zip.part*` and `supernatural_S03.zip.part*`), the consumer and `merge_chunks.py` will merge them into separate outputs named after their base filenames (e.g., `super.zip`, `supernatural_S03.zip`).
- `dms_chunk_sync.py` consumer deletes chunks from the DMS after a successful download and verification to free remote storage.

Dependencies & optional components ⚙️
- `requests` — optional but required for HTTP downloads in `dms_chunk_sync.py`.
- For torrent (magnet) downloads you may need `libtorrent`; installation varies by OS and may require system packages.

Troubleshooting & tips 🛠️
- If checksums fail during merging, re-check that all chunk files are fully downloaded and match the manifest.
- If `dms_chunk_sync.py` cannot contact the DMS, verify credentials and network access to the WebDAV server.

License & credits 🔒
- Provided as-is for personal or internal use — adapt to your needs.

Want more? ✅
- I can add a `requirements.txt`, a small test harness to simulate chunk creation, or colorized terminal output. Tell me which you'd prefer.

Special note — attribution ✨
- Parts of this codebase were contributed by a senior colleague(Patric anna), and other parts were generated with the help of AI tools. Please review and test before using in production environments.
