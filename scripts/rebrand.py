#!/usr/bin/env python3
"""Rebrand OtoCPA → OtoCPA across the entire codebase."""

import os
import re
import shutil
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

SKIP_DIRS = {'.git', '__pycache__', '.venv', 'node_modules', 'dist', 'build'}
TARGET_EXTS = {'.py', '.json', '.md', '.txt', '.bat', '.sh', '.html',
               '.cfg', '.ini', '.toml', '.yml', '.yaml', '.ps1'}
SKIP_FILES = {'tests/test_results.json'}

# Ordered longest-first to avoid partial replacements
REPLACEMENTS = [
    ("OtoCPA Portail Client", "OtoCPA Portail Client"),
    ("OtoCPA Dashboard", "OtoCPA Dashboard"),
    ("install.otocpa.com", "install.otocpa.com"),
    ("support@otocpa.com", "support@otocpa.com"),
    ("otocpa_agent.db", "otocpa_agent.db"),
    ("otocpa.config.json", "otocpa.config.json"),
    ("OtoCPA", "OtoCPA"),
    ("OtoCPA", "OtoCPA"),
    ("OtoCPA", "OtoCPA"),
    ("otocpa.com", "otocpa.com"),
    ("otocpa.com", "otocpa.com"),
    ("otocpa", "otocpa"),
    ("OTOCPA", "OTOCPA"),
]

FILE_RENAMES = [
    ("otocpa.config.json", "otocpa.config.json"),
    ("data/otocpa_agent.db", "data/otocpa_agent.db"),
    ("docs/OtoCPA_User_Manual_EN.pdf", "docs/OtoCPA_User_Manual_EN.pdf"),
    ("docs/OtoCPA_Manuel_Utilisateur_FR.pdf", "docs/OtoCPA_Manuel_Utilisateur_FR.pdf"),
]


def should_skip(path):
    rel = os.path.relpath(path, ROOT).replace('\\', '/')
    # Skip exports/ directory
    if rel.startswith('exports/') or rel.startswith('exports\\'):
        return True
    for sf in SKIP_FILES:
        if rel == sf or rel.endswith('/' + sf):
            return True
    return False


def replace_contents():
    replaced_files = 0
    total_replacements = 0
    for dirpath, dirnames, filenames in os.walk(ROOT):
        dirnames[:] = [d for d in dirnames if d not in SKIP_DIRS]
        for fname in filenames:
            ext = os.path.splitext(fname)[1].lower()
            if ext not in TARGET_EXTS:
                continue
            fpath = os.path.join(dirpath, fname)
            if should_skip(fpath):
                continue
            try:
                with open(fpath, 'r', encoding='utf-8', errors='ignore') as f:
                    content = f.read()
            except (OSError, PermissionError):
                continue

            new_content = content
            for old, new in REPLACEMENTS:
                new_content = new_content.replace(old, new)

            if new_content != content:
                try:
                    with open(fpath, 'w', encoding='utf-8', newline='') as f:
                        f.write(new_content)
                    count = 0
                    for old, new in REPLACEMENTS:
                        count += content.count(old)
                    replaced_files += 1
                    total_replacements += count
                    rel = os.path.relpath(fpath, ROOT)
                    print(f"  Updated: {rel} ({count} replacements)")
                except (OSError, PermissionError) as e:
                    print(f"  SKIP (write error): {fpath}: {e}")
    return replaced_files, total_replacements


def rename_files():
    renamed = 0
    for old_rel, new_rel in FILE_RENAMES:
        old_path = os.path.join(ROOT, old_rel)
        new_path = os.path.join(ROOT, new_rel)
        if os.path.exists(old_path):
            os.makedirs(os.path.dirname(new_path), exist_ok=True)
            shutil.move(old_path, new_path)
            print(f"  Renamed: {old_rel} -> {new_rel}")
            renamed += 1
        else:
            print(f"  Skip rename (not found): {old_rel}")
    return renamed


def verify():
    total = 0
    files_with_refs = []
    for dirpath, dirnames, filenames in os.walk(ROOT):
        dirnames[:] = [d for d in dirnames if d not in SKIP_DIRS]
        for fname in filenames:
            ext = os.path.splitext(fname)[1].lower()
            if ext not in TARGET_EXTS:
                continue
            fpath = os.path.join(dirpath, fname)
            try:
                with open(fpath, 'r', encoding='utf-8', errors='ignore') as f:
                    content = f.read()
                count = len(re.findall(r'[Ll]edger[Ll]ink|OTOCPA', content))
                if count > 0:
                    total += count
                    rel = os.path.relpath(fpath, ROOT)
                    files_with_refs.append((rel, count))
            except (OSError, PermissionError):
                continue
    return total, files_with_refs


def main():
    print("=" * 60)
    print("  OtoCPA -> OtoCPA Rebrand Script")
    print("=" * 60)

    print("\n[1/3] Replacing text in files...")
    files_updated, total_reps = replace_contents()
    print(f"  -> {files_updated} files updated, {total_reps} replacements")

    print("\n[2/3] Renaming files...")
    renamed = rename_files()
    print(f"  -> {renamed} files renamed")

    print("\n[3/3] Verifying...")
    remaining, files_with_refs = verify()
    print(f"  Remaining OtoCPA references: {remaining}")
    if files_with_refs:
        print("  Files with remaining references:")
        for rel, count in sorted(files_with_refs, key=lambda x: -x[1]):
            print(f"    {rel}: {count}")

    print("\n" + "=" * 60)
    if remaining < 20:
        print(f"  SUCCESS: {remaining} remaining references (target: < 20)")
    else:
        print(f"  WARNING: {remaining} remaining references (target: < 20)")
    print("=" * 60)


if __name__ == '__main__':
    main()
