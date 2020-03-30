""" Module to allow for quick and dirty gnuplotting in the dumb terminal"""


def do_plot(x, y, title=None, xlabel=None, ylabel=None):
    import subprocess
    gnuplot = subprocess.Popen(["/usr/bin/gnuplot"],
                               stdin=subprocess.PIPE)
    gnuplot.stdin.write("set term dumb 79 35\n".encode("utf-8"))
    if title is not None:
        gnuplot.stdin.write("set title '{0}' \n;".format(title).encode("utf-8"))
    if xlabel is not None:
        gnuplot.stdin.write(
            "set xlabel '{0}' \n;".format(xlabel).encode("utf-8"))
    if ylabel is not None:
        gnuplot.stdin.write(
            "set ylabel '{0}' \n;".format(ylabel).encode("utf-8"))
    gnuplot.stdin.write("set key horizontal left \n;".encode("utf-8"))

    gnuplot.stdin.write(
        ("plot '-' using 1:2 title 'Number of jobs' with histep \n"
         ).encode("utf-8"))
    for i, j in zip(x, y):
        gnuplot.stdin.write(("%f %f\n" % (i, j)).encode("utf-8"))
    gnuplot.stdin.write("e\n".encode("utf-8"))
    gnuplot.stdin.flush()
