#!/home/jumax9/Lairen_hg/PhD_code/NNLOJET_utilities/pyScaleCheck.py

n = 0
plotFl = 1

muR = [125.0, 250.0, 62.0]

slo = {
     125.0  : [4009.0866228, 1.2067997612],
     250.0  : [4009.0866228, 1.2067997612],
      62.0  : [4009.0866228, 1.2067997612],
}

sv = {
     125.0  : [-584.80267235, 0.50725195285],
     250.0  : [-531.86172322, 0.46133150635],
      62.0  : [-650.62717718, 0.56434746592],
}

sr = {
     125.0  : [525.70377852, 1.4530031522],
     250.0  : [478.11292726, 1.3214658498],
      62.0  : [584.87620119, 1.6165509907],
}

svv = {
     125.0  : [-199.79014743, 0.34372137798],
     250.0  : [-211.31889863, 0.31304950223],
      62.0  : [-177.56464398, 0.39037135352],
}

srv = {
     125.0  : [83.009557779, 1.3740858048],
     250.0  : [110.24240586, 1.3005612707],
      62.0  : [39.801089519, 1.5727914276],
}

snlo = sr
snnlo = srv

if snlo == sv and snnlo == svv:
    plot_basetitle = "Born Virtual Double-Virtual"
    png_basename = "scales_born"
    legend = "upper right"
elif snlo == sr and snnlo == srv:
    plot_basetitle = "Real Real-Virtual"
    png_basename = "scales_real"
    legend = "lower right"
else:
    raise("Wron't selection of cross sections")

