import channel as ch

chanlookup = {'tree level': 'LO', 'real ME SNLO': "R", 'virtual ME TNLO': "V",
              'double real ME S': "RR", 'real virtual ME T': "RV",
              'double virtual ME U': "VV"}


def parse_infile(scfile):
    """Generates a list of Channel objects from a given selectchannel file"""
    channels = []
    with open(scfile) as infile:
        chantype = None
        for line in infile:
            if line[0] == "c":
                if " -- " in line:
                    tmp = " ".join(i for i in line.split()[2:])
                    chantype = chanlookup[tmp]
                if "ip(" in line:
                    channels.append(ch.Channel(line, chantype))
    return channels


def parse_qcdnorm(qcdfile, chans):
    """Generates a list of prefactors from a given qcdnorm file
    """
    factags = ["facB=", "facR=", "facV=", "facRR=", "facRV=", "facVV="]
    facdict = {}
    with open(qcdfile) as infile:
        for line in infile:
            for tag in factags:
                if tag in line:
                    lineval = line.split("=")[-1]
                    facdict[tag[:-1]] = lineval.replace(
                        tag[:-1], lineval + "*").replace("\n", "")
            if "tree level" in line:
                break

        for chan in chans:
            found = False
            for line in infile:
                if found:
                    chan.FAC = (line.split()[0]).split("=")[1]
                    FULLFAC = chan.FAC
                    for fac in facdict:
                        FULLFAC = FULLFAC.replace(fac, facdict[fac] + "*")
                    for fac in facdict:
                        FULLFAC = FULLFAC.replace(fac, facdict[fac] + "*")
                    if FULLFAC[-1] == "*":
                        FULLFAC = FULLFAC[:-1]
                    chan.FULLFAC = FULLFAC.replace("\n", "")
                    break
                if ("case(" + chan.IP + ")") in line:
                    found = True
    for chan in chans:
        for tag in sorted(factags, key=lambda x: len(x), reverse=False):
            if tag[:-1] in chan.FAC:
                chan.rmFAC = chan.FAC.replace(
                    tag[:-1], "1").replace("d0", "")
                if chan.rmFAC[-2:] == "*1":
                    chan.rmFAC = chan.rmFAC[:-2]
    for chan in chans:
        chan.rmFAC = chan.combine_powers(chan.rmFAC, "nf")
        chan.rmFAC = chan.combine_powers(chan.rmFAC, "nup")
        chan.rmFAC = chan.combine_powers(chan.rmFAC, "ndown")
        if ch.Channel.rmfac:
            chan.FAC = chan.rmFAC
        chan._get_NC_NF_order()


class sigchan():
    def __init__(self, lines, ME=False):
        import re
        ipline = [line for line in lines if "(ip(" in line][0]
        self.IP = ipline.split("(")[2].split(")")[0]
        if not ME:
            ip1line = [line for line in lines if "ip1" in line][0]
            self.PDF1 = ip1line.split("=")[-1].strip()
            if self.PDF1[0] != "-":
                self.PDF1 = "+"+self.PDF1
            ip2line = [line for line in lines if "ip2" in line][0]
            self.PDF2 = ip2line.split("=")[-1].strip()
            if self.PDF2[0] != "-":
                self.PDF2 = "+"+self.PDF2
            flav_lines = [line for line in lines if "nf" in line]
            self.FLAVOUR = ""
            for i in flav_lines:
                self.FLAVOUR += i.strip().replace(" = ", ": ")+" "
        elif ME:
            args = re.compile(r'\((([0-9],)+[0-9])\)')
            MElines = [line for line in lines if "wt" in line]
            MElines += [line for line in lines if ".   " in line]
            MElines = [line for line in MElines if "(" in line]
            MElines = [line for line in MElines if "bino" not in line]
            MElines = [line for line in MElines if "idmapMuR_scl" not in line]
            MElines = [line for line in MElines if "rewgt" not in line]
            ME_args = []
            for line in MElines:
                line = line.replace(",muR2_scl(i))", ")")
                ME_args.append(args.search(line).group()[:])
            self.ME_args = ME_args


def add_ip_details(ip_details, lines, ME=False):
    a = sigchan(lines, ME)
    ip_details[a.IP] = a
    return ip_details


def parse_sig(inpath, ip_details, ME=False):
    with open(inpath) as infile:
        buff = False
        ip = []
        for line in infile:
            if "c---" in line:
                if buff:
                    ip_details = add_ip_details(ip_details, ip, ME)
                    ip = [line.strip()]
                buff = True
            elif "rnorm" in line and buff:
                buff = False
                ip_details = add_ip_details(ip_details, ip, ME)
            elif buff:
                ip.append(line.strip())


def parse_MEs(MEfiles, chans):
    ip_details = {}
    for ME in MEfiles:
        parse_sig(ME.fullpath, ip_details, ME=True)
    for chan in chans:
        try:
            chan.add_MEfile_info(ip_details[chan.IP])
        except KeyError as e:
            pass  # Channel not present in ME file - subtraction only term
    return chans


def parse_sigs(sigfiles, chans):
    ip_details = {}
    for s in sigfiles:
        parse_sig(s.fullpath, ip_details)
    chanips = sorted([int(i) for i in ip_details.keys()])
    chan_range = range(1, chanips[-1])
    for i, j in zip(chanips, chan_range):
        assert i == j  # Asserts that all channels are picked up!
    for chan in chans:
        chan.add_sigfile_info(ip_details[chan.IP])
    return chans


def parse_files(scfile, qcdfile, sigfiles, MEfiles):
    allchans = parse_infile(scfile)
    parse_qcdnorm(qcdfile, allchans)
    allchans = parse_sigs(sigfiles, allchans)
    allchans = parse_MEs(MEfiles, allchans)
    return allchans
