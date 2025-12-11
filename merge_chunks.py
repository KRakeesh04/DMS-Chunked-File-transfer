#!/usr/bin/env python3
"""
Merge chunk files based on manifest.txt checksums.
Only merges files that are listed in the manifest with valid checksums.
"""

import os
import hashlib
from pathlib import Path


def calculate_sha256(filepath):
    """Calculate SHA256 checksum of a file."""
    sha256_hash = hashlib.sha256()
    with open(filepath, "rb") as f:
        for byte_block in iter(lambda: f.read(4096), b""):
            sha256_hash.update(byte_block)
    return sha256_hash.hexdigest()


def read_manifest(manifest_path):
    """Read manifest file and return dict of {filename: expected_checksum}."""
    manifest = {}
    with open(manifest_path, 'r') as f:
        for line in f:
            line = line.strip()
            if line:
                parts = line.split()
                if len(parts) == 2:
                    checksum, filename = parts
                    manifest[filename] = checksum
    return manifest


def get_base_filename(chunk_name):
    """Extract base filename from chunk name (e.g., super.zip.part000 -> super.zip)."""
    # Remove .partXXX extension
    if '.part' in chunk_name:
        return chunk_name.rsplit('.part', 1)[0]
    return chunk_name


def merge_chunks(manifest_path, chunks_dir, output_dir=None):
    """
    Merge chunk files based on manifest.
    
    Args:
        manifest_path: Path to manifest.txt file
        chunks_dir: Directory containing chunk files
        output_dir: Directory to save merged files (default: same as manifest)
    """
    # Set output directory
    if output_dir is None:
        output_dir = Path(manifest_path).parent
    else:
        output_dir = Path(output_dir)
    
    output_dir.mkdir(parents=True, exist_ok=True)
    chunks_dir = Path(chunks_dir)
    
    # Read manifest
    print(f"Reading manifest from: {manifest_path}")
    manifest = read_manifest(manifest_path)
    print(f"Found {len(manifest)} chunks in manifest\n")
    
    # Group chunks by base filename
    chunk_groups = {}
    for chunk_name in manifest.keys():
        base_name = get_base_filename(chunk_name)
        if base_name not in chunk_groups:
            chunk_groups[base_name] = []
        chunk_groups[base_name].append(chunk_name)
    
    # Sort chunks in each group
    for base_name in chunk_groups:
        chunk_groups[base_name].sort()
    
    print(f"Found {len(chunk_groups)} file(s) to merge:")
    for base_name, chunks in chunk_groups.items():
        print(f"  - {base_name} ({len(chunks)} chunks)")
    print()
    
    # Merge each group
    for base_name, chunk_list in chunk_groups.items():
        print(f"Processing: {base_name}")
        output_file = output_dir / base_name
        
        # Verify all chunks exist and have correct checksums
        all_valid = True
        for chunk_name in chunk_list:
            chunk_path = chunks_dir / chunk_name
            
            if not chunk_path.exists():
                print(f"  ✗ Missing chunk: {chunk_name}")
                all_valid = False
                continue
            
            print(f"  Verifying: {chunk_name}...", end=" ")
            actual_checksum = calculate_sha256(chunk_path)
            expected_checksum = manifest[chunk_name]
            
            if actual_checksum == expected_checksum:
                print("✓")
            else:
                print("✗ CHECKSUM MISMATCH")
                print(f"    Expected: {expected_checksum}")
                print(f"    Got:      {actual_checksum}")
                all_valid = False
        
        if not all_valid:
            print(f"  Skipping {base_name} due to errors\n")
            continue
        
        # Merge chunks
        print(f"  Merging into: {output_file}")
        with open(output_file, 'wb') as outfile:
            for chunk_name in chunk_list:
                chunk_path = chunks_dir / chunk_name
                with open(chunk_path, 'rb') as infile:
                    outfile.write(infile.read())
        
        print(f"  ✓ Successfully merged {base_name}\n")


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Merge chunk files based on manifest")
    parser.add_argument(
        "--manifest",
        default="manifest.txt",
        help="Path to manifest file (default: manifest.txt)"
    )
    parser.add_argument(
        "--chunks-dir",
        default="chunks",
        help="Directory containing chunk files (default: chunks)"
    )
    parser.add_argument(
        "--output-dir",
        default=None,
        help="Directory to save merged files (default: same as manifest location)"
    )
    
    args = parser.parse_args()
    
    merge_chunks(args.manifest, args.chunks_dir, args.output_dir)
