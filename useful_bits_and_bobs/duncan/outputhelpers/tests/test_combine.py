import nose
import os
from .. import combine_output

###########################################
#                FIXTURES                 #
###########################################
dir_path = os.path.dirname(os.path.realpath(__file__))
cross_result = os.path.join(
    dir_path, 'fixtures/cross_section_addition_result.dat')
cross_input = os.path.join(dir_path, 'fixtures/ZJ.EXM.vRa.cross.s1.dat')
hist_result = os.path.join(dir_path, 'fixtures/hist_addition_result.dat')
hist_input = os.path.join(dir_path, 'fixtures/ZJ.EXM.vRa.etaj1.s1.dat')
hist_input_2 = os.path.join(dir_path, 'fixtures/ZJ.EXM.vRa.etaz.s1.dat')
tolerance = 0.000001


###########################################
#             HELPER FUNCTIONS            #
###########################################

def check_ratio(expectedval, inputval):
    if abs(expectedval - inputval) < tolerance:
        return True
    else:
        print "Expected ratio: ", expectedval
        print "Actual ratio: ", inputval
        return False


def assert_files_equal(filea, fileb):
    def file_len(fname):
        with open(fname) as f:
            for i, l in enumerate(f):
                pass
        return i + 1

    with open(filea) as infile:
        with open(fileb) as infile2:
            for idx, (linea, lineb) in enumerate(zip(infile, infile2)):
                try:
                    assert linea == lineb
                except AssertionError:
                    print("line " + str(idx) + " does not match")
                    print(linea)
                    print
                    print(lineb)
                    raise
    assert file_len(filea) == file_len(fileb)


def assert_vals_ratio(filea, fileb, ratio):
    """Values from file a are ratio multiplied by the values from line b
    Ignores errors.
    """
    def check_line(labels, linea, lineb, ratio):
        labels = labels.split()[1:]
        asplit = linea.split()
        bsplit = lineb.split()
        for x in zip(labels, asplit, bsplit):
            if "Err" not in x[0] and "lower" not in x[0] \
               and "center" not in x[0] and "upper" not in x[0]:
                x1 = float(x[1])
                x2 = float(x[2])
                if x1 != x1 and x2 != x2:  # Nan check
                    continue
                try:
                    val = x1 / x2
                except ZeroDivisionError:
                    if x1 == x2:
                        continue
                    else:
                        raise
                assert check_ratio(ratio, val)

    with open(filea) as infile:
        with open(fileb) as infile2:
            first = True
            for idx, (linea, lineb) in enumerate(zip(infile, infile2)):
                if "#" == linea[0]:
                    if linea == lineb:
                        if first:
                            first = False
                            labels = linea
                        continue
                    else:
                        print("File labels do not match")
                        raise AssertionError
                try:
                    check_line(labels, linea, lineb, ratio)
                except AssertionError:
                    print("line " + str(idx) +
                          " does not agree w/ factor of " + str(ratio))
                    print(linea)
                    print
                    print(lineb)
                    raise


###########################################
#                  TESTS                  #
###########################################

def test_cross_combine_full_output():
    """Tests overall result of combining a cross section file with itself"""
    test_out = "tmp_output"
    labels = combine_output.get_labels(cross_input)
    combine_output.add_all_files(
        test_out, [cross_input, cross_input], labels)
    assert_files_equal(cross_result, test_out)
    os.remove(test_out)


def test_hist_combine_full_output():
    """Tests overall result of combining a histogram file with itself"""
    test_out = "tmp_output"
    labels = combine_output.get_labels(hist_input)
    combine_output.add_all_files(
        test_out, [hist_input, hist_input], labels)
    assert_files_equal(hist_result, test_out)
    os.remove(test_out)


def test_cross_combine_double_output():
    """Tests combining a cross section file with itself. Should return double the result. Ignores Errors"""
    test_out = "tmp_output"
    labels = combine_output.get_labels(cross_input)
    combine_output.add_all_files(
        test_out, [cross_input, cross_input], labels)
    assert_vals_ratio(test_out, cross_input, 2)
    os.remove(test_out)


def test_cross_combine_quintuple_output():
    """Tests combining a cross section file with itself. Should return double the result. Ignores Errors"""
    test_out = "tmp_output"
    labels = combine_output.get_labels(cross_input)
    combine_output.add_all_files(test_out,
                                 [cross_input, cross_input, cross_input, cross_input, cross_input], labels)
    assert_vals_ratio(test_out, cross_input, 5)
    os.remove(test_out)


def test_hist_combine_double_output():
    """Tests combining a histogram file with itself. Should return double the result. Ignores Errors"""
    test_out = "tmp_output"
    labels = combine_output.get_labels(hist_input)
    combine_output.add_all_files(
        test_out, [hist_input, hist_input], labels)
    assert_vals_ratio(test_out, hist_input, 2)
    os.remove(test_out)


def test_hist_combine_quintuple_output():
    """Tests combining a histogram file with itself. Should return double the result. Ignores Errors"""
    test_out = "tmp_output"
    labels = combine_output.get_labels(hist_input)
    combine_output.add_all_files(test_out,
                                 [hist_input, hist_input, hist_input, hist_input, hist_input], labels)
    assert_vals_ratio(test_out, hist_input, 5)
    os.remove(test_out)


def test_hist_combine_double_output_2():
    """Tests combining a histogram file with itself. Should return double the result. Ignores Errors"""
    test_out = "tmp_output"
    labels = combine_output.get_labels(hist_input_2)
    combine_output.add_all_files(
        test_out, [hist_input_2, hist_input_2], labels)
    assert_vals_ratio(test_out, hist_input_2, 2)
    os.remove(test_out)


def test_hist_combine_quintuple_output_2():
    """Tests combining a histogram file with itself. Should return double the result. Ignores Errors"""
    test_out = "tmp_output"
    labels = combine_output.get_labels(hist_input_2)
    combine_output.add_all_files(test_out,
                                 [hist_input_2, hist_input_2, hist_input_2, hist_input_2, hist_input_2], labels)
    assert_vals_ratio(test_out, hist_input_2, 5)
    os.remove(test_out)
