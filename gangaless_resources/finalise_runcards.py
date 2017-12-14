import sys, os

# Disable
def blockPrint():
    sys.stdout = open(os.devnull, 'w')

# Restore
def enablePrint():
    sys.stdout = sys.__stdout__

# RUNCARDS USED ONLY FOR PULLING DATA WITH FINALISE.PY 
blockPrint()
import Wp, Wm, Z, WpJ, WmJ, ZJ, WpJ_TC
enablePrint()

dictCard = {}
for x in [Wp, Wm, Z, WpJ, WmJ, ZJ]:
    dictCard.update(x.dictCard)
