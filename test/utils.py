import os
from typing import List

def rmFiles(files: List[str]):
    for file in files:
        if os.path.exists(file):
            os.remove(file)

def assertFile(filePath: str, content: str):
    with open(filePath, 'r') as f:
        s = f.read()
        assert s == content
