# /cx.spec

# -*- mode: python ; coding: utf-8 -*-

"""
PyInstaller spec file for the Syncropel Shell (`cx`).
This is configured for a true single-file executable build with minimal dependencies.
"""

# Explicitly add non-code assets that MUST be bundled.
added_files = [
    ('src/cx_shell/assets', 'cx_shell/assets'),
    ('src/cx_shell/interactive/grammar', 'cx_shell/interactive/grammar')
]

# Explicitly exclude heavy, optional dependencies that will be downloaded on-demand.
hidden_imports = [
    # We might need a few tricky-to-find imports here, but start with none.
]

# A list of modules to explicitly EXCLUDE from the binary. This is the key to slimming it down.
excluded_modules = [
    'pandas',
    'numpy',
    'sqlalchemy',
    'aioodbc',
    'trino',
    'lancedb',
    'fastembed',
    'tiktoken',
    'instructor',
    'playwright',
    # Exclude test libraries
    'pytest',
    'pytest_mock',
]

a = Analysis(
    ['src/cx_shell/main.py'],
    pathex=['src'], # <-- ADD THIS LINE to explicitly add 'src' to the path
    binaries=[],
    datas=added_files,
    hiddenimports=hidden_imports,
    hookspath=[],
    runtime_hooks=[],
    excludes=excluded_modules,
    win_no_prefer_redirects=False,
    win_private_assemblies=False,
    noarchive=False
)
pyz = PYZ(a.pure, a.zipped_data)

exe = EXE(
    pyz,
    a.scripts,
    [],
    name='cx',
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    upx=True, # Use UPX to compress the final binary
    console=True,
    runtime_tmpdir=None
)