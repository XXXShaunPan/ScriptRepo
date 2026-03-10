import PyInstaller.__main__

PyInstaller.__main__.run([
    'src/quick_matching_tool/__main__.py',
    '--onefile',
    '--windowed',
    '--name=QuickMatchingTool',
    '--hidden-import=DrissionPage',
    '--paths=src',
    '--noupx',
    '--clean',
    '--noconfirm',
])