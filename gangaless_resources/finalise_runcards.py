import sys, os, importlib
#runcards = ["WpJ_TC", "ppb_nnlo", "Njetti", "Njetti_CT14nnlo"]
#runcards = ["WpJ_TC", "ppb_nnlo", "Njetti_CT14nnlo"]
#runcards = ["CCDIS","Z_trip_diff","LHCb"]
runcards = ["LHCb"]

# Disable
def blockPrint():
    sys.stdout = open(os.devnull, 'w')

# Restore
def enablePrint():
    sys.stdout = sys.__stdout__

# RUNCARDS USED ONLY FOR PULLING DATA WITH FINALISE.PY 
blockPrint()
modules = [importlib.import_module(rc) for rc in runcards]
enablePrint()

dictCard = {}
for x in modules:
    dictCard.update(x.dictCard)
