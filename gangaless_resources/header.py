import template_header as template
import sys
from types import ModuleType
import getpass
import importlib

 
header_mappings = {"jmartinez":"juan_header",
                   "dwalker":"duncan_header"}

grid_username = getpass.getuser()
head = importlib.import_module(header_mappings[grid_username])

print("Using header file {0}.py".format(head.__name__))

############ COPY NAMESPACE FROM MY_HEADER #############
# Only slightly abusive...
this_file = sys.modules[__name__]

# Check that the all of the attributes in template are present...
template_namespace = [i for i in dir(template) if not i.startswith("__")]
# remove functions from template namespace
template_attributes = [i for i in template_namespace if not 
                       callable(getattr(template, i))]
# remove modules from template namespace
template_attributes = [i for i in template_namespace if not 
                       isinstance(getattr(template, i),ModuleType)]

# Set attributes inside header
for i in dir(head):
    if not i.startswith("__"):
        attr = getattr(head,i)
        setattr(this_file,i,attr)
        # Give warnings if you've added any new attributes and not put them in the template.
        if i not in template_attributes and not isinstance(attr,ModuleType)\
                and not callable(attr):
            print("> WARNING: attribute {0} not present in {1}".format(i, template.__name__))
            print("> Please add it in before committing so you don't break compatibility(!)")


# Raise errors if you try to run without parameters specified in the template
for i in template_attributes:
    try:
        assert(hasattr(this_file, i))
    except AssertionError as e:
        print("> ERROR: Missing {0} attribute inside {1}.py file that is present in {2}.py.".format(
                i, head.__name__, template.__name__))
        print("> Check that {0}.py file is up to date as functionality may be broken otherwise.".format(head.__name__))
        sys.exit(1)

############ General header #################
# Grid config
arcbase  = "/mt/home/{}/.arc/jobs.dat".format(grid_username) # arc database
gsiftp   = "gsiftp://se01.dur.scotgrid.ac.uk/dpm/dur.scotgrid.ac.uk/home/pheno/generated/"
LFC_HOST = "lfc01.dur.scotgrid.ac.uk"
LFC_CATALOG_TYPE = "lfc"

# Database config
arctable   = "arcjobs"
diractable = "diracjobs"
dbfields   = ['jobid', 'date', 'runcard', 'runfolder', 'pathfolder', 'status', 'jobtype']

#
# Templates
# 

# # If a job is expected to run for long, use the following property (in minutes)
# "(wallTime  =    \"3 days\")" 
# it is also possible to specifiy the maximum cpu time instead (or 'as well')
# "(cpuTime = \"3 days\")"
# if nothing is used, the end system will decide what the maximum is
#

ARCSCRIPTDEFAULT = ["&",
        "(executable   = \"ARC.py\")",
        "(outputFiles  = (\"outfile.out\" \"\") )",
        "(stdout       = \"stdout\")",
        "(stderr       = \"stderr\")",
        "(gmlog        = \"testjob.log\")",
        "(memory       = \"100\")",
        ]

ARCSCRIPTDEFAULTPRODUCTION = ["&",
        "(executable   = \"DIRAC.py\")",
        "(outputFiles  = (\"outfile.out\" \"\") )",
        "(stdout       = \"stdout\")",
        "(stderr       = \"stderr\")",
        "(gmlog        = \"testjob.log\")",
        "(memory       = \"100\")",
        ]

DIRACSCRIPTDEFAULT = [
        "JobName    = \"gridjob1\";",
        "Executable = \"DIRAC.py\";",
        "StdOutput  = \"StdOut\";",
        "StdError   = \"StdErr\";",
        "InputSandbox  = {\"DIRAC.py\"};",
        "OutputSandbox = {\"StdOut\",\"StdErr\"};",
        ]

from argument_parser import runcard as runcard_file
print(runcard_file)
if runcard_file:
    runcard = importlib.import_module(runcard_file.replace(".py",""))
    # todo: some safety checks
    for attr_name in dir(runcard):
        if not attr_name.startswith("__") and attr_name != "dictCard":
            attr_value = getattr(runcard, attr_name)
            setattr(this_file, attr_name, attr_value)

print(runcardDir)

