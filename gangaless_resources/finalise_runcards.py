import sys, os

# Disable
def blockPrint():
    sys.stdout = open(os.devnull, 'w')

# Restore
def enablePrint():
    sys.stdout = sys.__stdout__

# RUNCARDS USED ONLY FOR PULLING DATA WITH FINALISE.PY 
blockPrint()
import WpJ, WmJ, WpJ_TC
enablePrint()

dictCard = {}
for x in [WpJ, WmJ, WpJ_TC]:
    dictCard.update(x.dictCard)
