import headers.template_header as template
import sys
from types import ModuleType
import getpass
import importlib
import get_site_info
 
header_mappings = {"jmartinez":"headers.juan_header",
                   "dwalker":"headers.duncan_header",
                   "jniehues":"headers.jan_header",
                   "jwhitehead":"headers.james_header"}

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
            print("  \033[93m WARNING:\033[0m attribute {0} not present in {1}".format(i, template.__name__))
            print("> Please add it in before committing so you don't break compatibility(!)")


# Raise errors if you try to run without parameters specified in the template
for i in template_attributes:
    try:
        assert(hasattr(this_file, i))
    except AssertionError as e:
        print("  \033[91m ERROR:\033[0m Missing attribute {0} inside {1}.py that is present in {2}.py.".format(
                i, head.__name__, template.__name__))
        print("> Check that {0}.py file is up to date as functionality may be broken otherwise.".format(head.__name__))
        sys.exit(1)

############ General header #################
# This should not be changed unless you really know what you are doing!
# Grid config
arcbase  = "/mt/home/{}/.arc/jobs.dat".format(grid_username) # arc database
gsiftp   = "gsiftp://se01.dur.scotgrid.ac.uk/dpm/dur.scotgrid.ac.uk/home/pheno/generated/"
LFC_HOST = "lfc01.dur.scotgrid.ac.uk"
LFC_CATALOG_TYPE = "lfc"
runfile = "nnlorun.py"
sandbox_dir = "test_sandbox"


# Database config
arctable   = "arcjobs"
diractable = "diracjobs"
dbfields   = ['jobid', 'date', 'runcard', 'runfolder', 'pathfolder', 'status', 'jobtype', 'iseed', 'sub_status']

#
# Templates
# 
sockets_active = 1 # 1 socket == no sockets

# # If a job is expected to run for long, use the following property (in minutes)
# "(wallTime  =    \"3 days\")" 
# it is also possible to specifiy the maximum cpu time instead (or 'as well')
# "(cpuTime = \"3 days\")"
# if nothing is used, the end system will decide what the maximum is
#

from src.argument_parser import runcard as runcard_file
if runcard_file:
    runcard = importlib.import_module(runcard_file.replace(".py","").replace("/","."))
    # todo: some safety checks
    for attr_name in dir(runcard):
#        print(attr_name, runcard)
        if not attr_name.startswith("__") and attr_name != "dictCard" and \
                not isinstance(getattr(runcard, attr_name), ModuleType):
            if not hasattr(this_file, attr_name):
                print(">\033[93m WARNING!\033[0m {0} defined in {1}.py but not {2}.py.".format(attr_name, runcard.__name__, template.__name__))
                print("> Be very careful if you're trying to override attributes that don't exist elsewhere.")
                print("> Or even if they do.")

            attr_value = getattr(runcard, attr_name)
            print("> Setting value of {0} to {1} in {2}.py".format(attr_name, attr_value, runcard.__name__))
            setattr(this_file, attr_name, attr_value)
try:
    from src.argument_parser import override_ce_base as use_best_ce
    if use_best_ce:
        setattr(this_file, "ce_base", get_site_info.get_most_free_cores())
        print("> Setting value of {0} to {1} due to most_free_cores override".format("ce_base", ce_base))
except ImportError as e:
    pass

### Moved to the bottom to allow runcard to override jobName

ARCSCRIPTDEFAULT = ["&",
        "(executable   = \"{0}\")".format(runfile),
        "(outputFiles  = (\"outfile.out\" \"\") )",
        "(stdout       = \"stdout\")",
        "(stderr       = \"stderr\")",
        "(gmlog        = \"testjob.log\")",
        "(memory       = \"100\")",
        ]

ARCSCRIPTDEFAULTPRODUCTION = ["&",
        "(executable   = \"{0}\")".format(runfile),
        "(outputFiles  = (\"outfile.out\" \"\") )",
        "(stdout       = \"stdout\")",
        "(stderr       = \"stderr\")",
        "(gmlog        = \"testjob.log\")",
        "(memory       = \"100\")",
        ]

DIRACSCRIPTDEFAULT = [
        "JobName    = \"{0}\";".format(jobName),
        "Executable = \"{0}\";".format(runfile),
        "StdOutput  = \"StdOut\";",
        "StdError   = \"StdErr\";",
        "InputSandbox  = {{\"{0}\"}};".format(runfile),
        "OutputSandbox = {\"StdOut\",\"StdErr\"};",
        ]
