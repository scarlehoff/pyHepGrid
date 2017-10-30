import channel

line = "c      ip(3)  = .true.   ! u g to d nu lp        B1g0W(1,2,3,4,5)      qgB1g0WSNLO(1,2,3,4,5)"


def mock_channel_setup():
    x = channel.Channel(line, "R")
    return x


def nf_nc_factor_setup(factor, nfnc):
    x = mock_channel_setup()
    if nfnc == "nf":
        factor = x.combine_powers(factor, nfnc)
    x.rmFAC = factor
    x._get_NC_NF_order()
    return x


def assert_equal(a, b, *args):
    try:
        assert a == b
    except AssertionError as e:
        print("Values not equal: ", a, b)
        if len(args) != 0:
            print("Other info", args)
        raise


def check_nf_factor(fac, output):
    chan = nf_nc_factor_setup(fac, "nf")
    assert_equal(chan.NF, output)


def check_nc_factor(fac, output):
    chan = nf_nc_factor_setup(fac, "nc")
    assert_equal(chan.NC, output)
