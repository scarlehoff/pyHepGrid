#
# Utilities to deal with ARC and Dirac proxys
#


def arcProxy(validity, password=None):
    from subprocess import call
    cmdbase = ["arcproxy"]
    args = ["-S", "pheno", "-c", "validityPeriod=" +
            validity, "-c", "vomsACvalidityPeriod="+validity]
    if password:
        pass
    call(cmdbase + args)
    return 0


def arcProxyWiz():
    return arcProxy("24h")


def diracProxy(password=None):
    from subprocess import call
    call(["which", "dirac-proxy-init"])
    cmdb = ["dirac-proxy-init", "-g", "pheno_user", "-M"]
    if password:
        pass
    call(cmdb)
    return 0
