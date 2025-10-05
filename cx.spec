# -*- mode: python ; coding: utf-8 -*-

"""
PyInstaller spec file for the Syncropel Shell (`cx`).
This is configured for a true single-file executable build.
"""

added_files = [
    ('src/cx_shell/assets', 'cx_shell/assets'),
    ('src/cx_shell/interactive/grammar', 'cx_shell/interactive/grammar')
]

# Explicitly tell PyInstaller to bundle all modules that are imported
# dynamically or as part of optional dependency sets. This ensures that
# features like MSSQL, Trino, and Git connectivity work in the packaged binary.
hidden_imports = [
    'aioodbc',
    'trino',
    'git'
]

block_cipher = None

a = Analysis(
    ['src/cx_shell/main.py'],
    pathex=[],
    binaries=[],
    datas=added_files,
    hiddenimports=hidden_imports, # Use the hiddenimports list here
    hookspath=[],
    hooksconfig={},
    runtime_hooks=[],
    excludes=[],
    win_no_prefer_redirects=False,
    win_private_assemblies=False,
    cipher=block_cipher,
    noarchive=False,
)
pyz = PYZ(a.pure, a.zipped_data, cipher=block_cipher)

exe = EXE(
    pyz,
    a.scripts,
    [],
    name='cx',
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    upx=True,
    runtime_tmpdir=None,
    console=True,
)