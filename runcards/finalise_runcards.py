import sys, os, importlib
from collections import defaultdict

#runcards = ["CMS_8TeV_StandardCuts"]
# Auto select all runcards
runcard_files = os.listdir(os.path.dirname(os.path.abspath(__file__)))
runcards = [rc.replace(".py","") for rc in runcard_files if rc.endswith(".py") 
            and "template" not in rc and "finalise" not in rc and "#" not in rc]
# Disable
def blockPrint():
    sys.stdout = open(os.devnull, 'w')

# Restore
def enablePrint():
    sys.stdout = sys.__stdout__

# RUNCARDS USED ONLY FOR PULLING DATA WITH FINALISE.PY 
print("Pulling data for runcard(s): \n{0}.py".format(".py ".join(i for i in runcards)))

blockPrint()
modules = [importlib.import_module("runcards.{0}".format(rc)) for rc in runcards]
enablePrint()

dictCard = defaultdict(list)
for x in modules:
    for a,b in x.dictCard.items():
        dictCard[a].append(b)
