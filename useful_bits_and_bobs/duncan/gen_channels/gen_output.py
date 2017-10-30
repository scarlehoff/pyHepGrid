import channel as ch


def write_unique_partons(allchans):
    in_parton_pairs = list(set([(i.IP1, i.IP2) for i in allchans]))
    in_parton_pairs.sort(key=lambda x: (x[0], x[1]))
    for i in in_parton_pairs:
        print((i[0]+", ").ljust(4)+i[1])


def write_unique_PDFs(allchans):
    in_parton_pairs = list(set([(i.PDF1, i.PDF2) for i in allchans]))
    in_parton_pairs.sort(key=lambda x: (x[0], x[1]))
    for i in in_parton_pairs:
        print((i[0]+", ").ljust(4)+i[1])


def write_channels(allchans, key="IP"):
    try:
        allchans.sort(key=lambda x: (int(getattr(x, key)), int(x.IP)))
    except Exception as e:
        allchans.sort(key=lambda x: (getattr(x, key), int(x.IP)))
    for i in allchans:
        print(i)


def write_table(allchans, args):
    """ Writes a table for direct copy and paste into emacs org mode """
    allchans.sort(key=lambda x: int(x.IP))
    ch.Channel.print_table_header(args)
    for i in allchans:
        i.print_table_line(args)


def write_output_ip(allchans):
    ips = sorted([int(i.IP) for i in allchans])
    retstr = " ".join(str(i) for i in ips)
    print(retstr)


def write_full_ME(allchans):
    retstr = ""
    first = True
    for i in allchans:
        tmpbuffer = ""
        if not first:
            tmpbuffer += "+"
        first = False
        tmpbuffer += i.ME
        tmpbuffer += i.args
        tmpbuffer += "*"
        tmpbuffer += i.FAC
        tmpbuffer += " \n"
        retstr += tmpbuffer
    print(retstr[:-2])  # removes last newline


def write_LO_test(allchans, args):
    if args.LOtestfuncname is not None:
        name = args.LOtestfuncname
    else:
        name = "func"
    retval = "module "+name+"_mod\n"
    retval += "contains\n"
    retval += "function "+name+"()\n"
    retval += "  use KinData_mod\n  implicit none\n"
    retval += "  integer, parameter :: dp     =  kind(1.d0)\n"
    retval += "  integer, parameter :: nc     =  3\n"

    MElist = list(set(chan.ME for chan in allchans))
    for ME in MElist:
        retval += "  real(dp) :: "+ME+"\n"
    retval += "  real(dp) :: ave,amzw,stw,g2qcd,nup,ndown,nf\n"
    retval += "  real(dp) :: "+name+"\n"
    retval += "  real(dp), parameter :: pi     =  3.141592653589793238462643383279502884d0\n"
    retval += "  integer  :: nfB1\n"
    retval += "  common /BZFlav/ nfB1\n"
    retval += "  !$omp threadprivate(/BZFlav/)\n"
    retval += "  integer  :: nfC1,nfC2\n"
    retval += "  common /CZFlav/ nfC1,nfC2\n"
    retval += "  !$omp threadprivate(/CZFlav/)\n"
    retval += "  common /eweakW/ amzW,stw\n"
    retval += "  !$omp threadprivate(/eweakW/)\n"
    retval += "  common /Wmass/emw, ewwidth\n"
    retval += "  real(dp) :: emw, ewwidth\n"
    retval += "  nup = 2\n"
    retval += "  ndown = 3\n"
    retval += "  nf = 5\n"
    retval += "  emw =   80.419002445756163d0\n"
    retval += "  ewwidth = 2.0476000000000001d0\n"
    retval += "  ave = 0.25d0/(nc**2-1d0)**2\n"
    retval += "  amzw = 7.5467711139788835D-003\n"
    retval += "  stw = 0.22224648578577766d0\n"
    retval += "  g2qcd = 4*pi*0.11799999999999999d0\n\n"
    retval += "  "+name+" = 0d0\n"
    for idx, chan in enumerate(allchans):
        retval += "! "+chan.IP+": "+chan.partons+"\n"
        retval += "      ave = 0.25d0"
        if "g" in chan.IP1:
            retval += "/(nc**2-1d0)"
        else:
            retval += "/nc"
        if "g" in chan.IP2:
            retval += "/(nc**2-1d0)"
        else:
            retval += "/nc"
        retval += "\n"
        if chan.B1 != "-":
            retval += "      nfB1 = "+chan.B1+"\n"
        if chan.C1 != "-":
            retval += "      nfC1 = "+chan.C1+"\n"
        if chan.C2 != "-":
            retval += "      nfC2 = "+chan.C2+"\n"
        for x in chan.args:  # Loop over colour orderings
            splitfactor = splitfac(chan.FULLFAC)
            retval += "      "+name+" = "+name+"+"+chan.ME+x+"*"+splitfactor+"\n"
    retval += "  return\n"
    retval += "end function "+name+"\n\n\n"
    retval += "end module "+name+"_mod\n"
    with open(args.LOtest, "w") as outfile:
        outfile.write(retval)

def splitfac(instring):
    """ Splits a factor into new lines if it gets too long. Length hardcoded here."""
    sub_bits = instring.split("*")
    qlines = []
    currstr = sub_bits[0]
    for bit in sub_bits[1:]:
        newstr = currstr + "*" + bit
        if len(newstr) > 40:
            qlines.append(currstr)
            currstr = bit
        else:
            currstr = newstr
    qlines.append(currstr)
    assert instring == "*".join(i for i in qlines)  # Check string isn't fundamentally altered
    return " &\n        ".join(i for i in qlines)
