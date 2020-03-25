import sys
import os
import importlib
# Auto select all runcards
runcard_files = os.listdir(os.path.dirname(os.path.abspath(__file__)))
runcards = [rc.replace(".py", "") for rc in runcard_files if rc.endswith(".py")
            # Or you can hard code them here...
            # runcards = ["triple_differential"]
            and "template" not in rc and "finalise" not in rc]


# Disable
def blockPrint():
    sys.stdout = open(os.devnull, 'w')


# Restore
def enablePrint():
    sys.stdout = sys.__stdout__


# RUNCARDS USED ONLY FOR PULLING DATA WITH FINALISE.PY
print("Pulling data for runcard(s): {0}".format(" ".join(i for i in runcards)))
blockPrint()
modules = [importlib.import_module(
    "runcards.{0}".format(rc)) for rc in runcards]
enablePrint()

dictCard = {}
for x in modules:
    dictCard.update(x.dictCard)
