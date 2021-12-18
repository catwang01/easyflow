import sys
import json
import os
from easyflow.resolver import JsonResolver
thisDir = os.path.dirname(__file__)
import glob
from test.utils import assertFile, rmFiles

class TestResolver:

    def test_csv(self):
        rmFiles(glob.glob("output*.txt"))
        with open(os.path.join(thisDir, "wdef.json")) as f:
            wdef = json.load(f)
        resolver = JsonResolver()
        workflow = resolver.resolve(wdef)
        workflow.run()
        assertFile('output5.txt', "output2 \noutput1  \noutput1  \n")
        