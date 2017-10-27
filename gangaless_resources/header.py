import template_header as template
import sys
from types import ModuleType
import getpass
import importlib
 
header_mappings = {"jmartinez":"juan_header",
                   "dwalker":"duncan_header"}

head = importlib.import_module(header_mappings[getpass.getuser()])

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
