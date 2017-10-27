import os

#####################################################
#                   Plot Config                     #
#####################################################

col = ["#006400", "#FF4500", "#00BFFF", "#000000", "#FFA500"]

#####################################################
#                  Output Files                     #
#####################################################

outgnu = "plt.gnu"
outtex = "plt.tex"
outputdir = "plotter_output/"
outputloc = os.path.join(os.getcwd(), outputdir)

#####################################################
#               NNLOJET Parameters                  #
#####################################################

NNLOJET_scale = 1  # Scale to plot from NNLOJET histogram files
NNLOJET_chan = "tot"  # Channel to plot from NNLOJET histogram files
