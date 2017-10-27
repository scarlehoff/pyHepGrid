""" End to end tests.
"""
import subprocess as sp
import json
from nose.tools import nottest
import difflib
import testconfig as tc
import gzip
import os
import shutil

reference_file = "fixtures/data.txt"

flags = tc.print_tests
fileflags = tc.filegen_tests


def _unidiff_output(expected, actual):
    """
    Helper function. Returns a string containing the unified diff of two multiline strings.
    """

    expected = expected.splitlines(1)
    actual = actual.splitlines(1)

    diff = difflib.unified_diff(expected, actual)

    return ''.join(diff)


class Output():
    def run_gen_channel(self, flags):
        cmd = ['python', 'gen_channels.py']
        cmd += flags
        p = sp.Popen(cmd, stdin=sp.PIPE, stdout=sp.PIPE, stderr=sp.PIPE)
        output, err = p.communicate()
        rc = p.returncode
        return output, err, rc


class Gen_File_Output(Output):
    def run_gen_channel_to_file(self, testdict):
        output, err, rc = self.run_gen_channel(testdict["flags"])
        outfilename = testdict["filename"]
        outtarname = outfilename+".gz"
        with open(outfilename, 'rb') as f_in:
            with gzip.open(os.path.join(tc.fixturesdir, outtarname),
                           'wb') as f_out:
                shutil.copyfileobj(f_in, f_out)
        os.remove(outfilename)
        return outfilename, outtarname

    def gen_all(self):
        for name in fileflags:
            self.run_gen_channel_to_file(fileflags[name])


class Gen_Output(Output):
    def run_gen_channel_to_file(self, flags, fn_name):
        output, err, rc = self.run_gen_channel(flags)
        data = {"output": output, "err": err, "retcode": rc}
        return {fn_name: data}

    def output_gen(self, name):
        return self.run_gen_channel_to_file(flags[name], name)

    def gen_all(self):
        retval = {}
        for name in flags:
            retval.update(self.output_gen(name))
        return retval


class Test_Output(Output):
    def setUp(self):
        with open(reference_file) as data_file:
            self.data = json.load(data_file)

    def __init__(self):
        self.setUp()

    def do_assertion(self, testdata, name, val, testname):
        try:
            assert testdata[name] == val
        except AssertionError as e:
            print(name, testname)
            # print("CURRENT OUTPUT:")
            # print(val)
            # print("REFERENCE OUTPUT:")
            # print(testdata[name])
            print _unidiff_output(testdata[name], val)
            raise

    def file_assertion(self, indata, olddata, name):
        try:
            assert indata == olddata
        except AssertionError as e:
            print(name)
            print(name["flags"],
                  os.path.join(tc.fixturesdir,
                               name["filename"]+".gz"),
                  name["filename"])
            print _unidiff_output("\n".join(i for i in indata),
                                  "\n".join(i for i in olddata))
            os.remove(name["filename"])
            raise

    @nottest
    def test_file_check(self, name):  # Hide from nose
        output, err, rc = self.run_gen_channel(fileflags[name]["flags"])
        with open(fileflags[name]["filename"]) as newfile:
            new = newfile.readlines()
        with gzip.open(os.path.join(tc.fixturesdir,
                                    fileflags[name]["filename"]+".gz")) as reffile:
            old = reffile.readlines()
        self.file_assertion(new, old, fileflags[name])
        os.remove(fileflags[name]["filename"])

    @nottest
    def test_output_check(self, name):  # Hide from nose
        output, err, rc = self.run_gen_channel(flags[name])
        try:
            test_data = self.data[name]
        except KeyError as e:
            print("Test data not found for name: "+name)
            print("Need to rerun tests?")
            raise
        self.do_assertion(test_data, "output", output, name)
        self.do_assertion(test_data, "err", err, name)
        self.do_assertion(test_data, "retcode", rc, name)

    def test_all_flags(self):  # Tests that only look at print output
        for name in flags:
            yield self.test_output_check, name

    def test_all_file_flags(self):  # Tests that generate output files
        for name in fileflags:
            yield self.test_file_check, name


if __name__ == "__main__":
    # Regenerate all print output
    Output = Gen_Output()
    reference_output = {}
    reference_output.update(Output.gen_all())

    # Write JSON output to file
    with open(reference_file, 'w') as outfile:
        json.dump(reference_output, outfile, sort_keys=True,
                  indent=4, separators=(',', ': '))

    # Regenerate all file output to fixtures directory
    FileOutput = Gen_File_Output()
    FileOutput.gen_all()
