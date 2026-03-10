# -*- mode: python ; coding: utf-8 -*-
import os
# 获取当前目录
spec_dir = os.path.dirname(os.path.abspath(SPEC))

a = Analysis(
    ['src/quick_matching_tool/__main__.py'],
    pathex=[spec_dir, os.path.dirname(spec_dir), os.path.join(spec_dir, 'src')],
    binaries=[],
    datas=[
        ('config/credentials.json', 'config'),
        (os.path.join(spec_dir, 'src'), 'src'),
    ],
    hiddenimports=[
        'DrissionPage',
        'quick_matching_tool',
        'quick_matching_tool.core.matching_service',
        'quick_matching_tool.core.data_prep',
        'quick_matching_tool.infra.google_drive',
        'quick_matching_tool.infra.google_sheets',
        'quick_matching_tool.infra.google_auth',
        'quick_matching_tool.infra.remote_update',
        'quick_matching_tool.infra.shopee_api',
        'quick_matching_tool.ui.app',
        'quick_matching_tool.ui.threads',
        'gspread',
        'gspread_dataframe',
        'oauth2client',
        'oauth2client.service_account',
        'googleapiclient',
        'googleapiclient.discovery',
        'googleapiclient.http',
        'pyquery',
        'aiohttp',
        'pandas',
        'packaging',  # 版本比较
        'packaging.version',
    ],
    hookspath=[],
    hooksconfig={},
    runtime_hooks=['pyinstaller_runtime_hook.py'],
    excludes=[],
    noarchive=False,
    optimize=0,
)
pyz = PYZ(a.pure)

exe = EXE(
    pyz,
    a.scripts,
    [],
    exclude_binaries=True,
    name='QuickMatchingTool',
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    upx=False,
    console=False,
    disable_windowed_traceback=False,
    argv_emulation=False,
    target_arch=None,
    codesign_identity=None,
    entitlements_file=None,
)
coll = COLLECT(
    exe,
    a.binaries,
    a.datas,
    strip=False,
    upx=False,
    upx_exclude=[],
    name='QuickMatchingTool',
)
