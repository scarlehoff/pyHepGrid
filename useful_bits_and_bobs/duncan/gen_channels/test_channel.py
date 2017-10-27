import test_helpers as th


def test_nf_sq_parsing():
    th.check_nf_factor("1*nf**2", ["2"])
    th.check_nf_factor("nf*nf", ["2"])


def test_nf_single_parsing():
    th.check_nf_factor("nf*gobbledegook", ["1"])
    th.check_nf_factor("nf*nc", ["1"])


def test_nf_cube_parsing():
    th.check_nf_factor("nf**3", ["3"])
    th.check_nf_factor("1*nf**3", ["3"])
    th.check_nf_factor("nf*nf*nf", ["3"])
    th.check_nf_factor("nf*nf**2", ["3"])
    th.check_nf_factor("nf**2*nf", ["3"])
    th.check_nf_factor("nf**3*gobbledegook", ["3"])


def test_nf_inverse_parsing():
    th.check_nf_factor("nf**-1*gobbledegook", ["-1"])
    th.check_nf_factor("nf**-3*gobbledegook", ["-3"])
    th.check_nf_factor("nf**-3*nf**-1*nf**2*gobbledegook", ["-2"])
    th.check_nf_factor("nf**-3*nf**-1*nf**2*nf*gobbledegook", ["-1"])


def test_no_nf_parsing():
    th.check_nf_factor("gobbledegook", ["0"])
    th.check_nf_factor("1", ["0"])
    th.check_nf_factor("", ["0"])


def test_nc_sq_parsing():
    th.check_nc_factor("1*1/nc**2", ["-2"])
    th.check_nc_factor("1/nc*1/nc", ["-2"])


def test_nc_cube_parsing():
    th.check_nc_factor("1/nc**3", ["-3"])
    th.check_nc_factor("1*1/nc**3", ["-3"])
    th.check_nc_factor("1/nc*1/nc*1/nc", ["-3"])
    th.check_nc_factor("1/nc*1/nc**2", ["-3"])
    th.check_nc_factor("1/nc**2*1/nc", ["-3"])
    th.check_nc_factor("1/nc**3*gobbledegook", ["-3"])


def test_nc_inverse_parsing():
    th.check_nc_factor("1/nc**-1*gobbledegook", ["1"])
    th.check_nc_factor("1/nc**-3*gobbledegook", ["3"])
    th.check_nc_factor("1/nc**-3*1/nc**-1*1/nc**2*gobbledegook", ["2"])
    th.check_nc_factor("1/nc**-3*1/nc**-1*1/nc**2*1/nc*gobbledegook", ["1"])


def test_no_nc_parsing():
    th.check_nc_factor("gobbledegook", ["0"])
    th.check_nc_factor("1", ["0"])
    th.check_nc_factor("", ["0"])


def test_bracketed_nc_factors():
    th.check_nc_factor("(nc**2-1)/nc**2", ["0", "-2"])
    th.check_nc_factor("(nc**2-1)/nc**4", ["-2", "-4"])
    th.check_nc_factor("(nc**2-1)/nc", ["1", "-1"])
    th.check_nc_factor("(nc**2-1)(nc**2-1)/nc", ["3", "1", "-1"])
    th.check_nc_factor("nc**2/nc", ["1"])
