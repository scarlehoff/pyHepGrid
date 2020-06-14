# This is header is run specific

# Only mandatory option
import pyHepGrid.src.Database as dbapi
from getpass import getuser
dictCard = {  # first: -r second: -j
    # we use -r as the name and -j as the runcard
    # 'Wp2j_mw_13TeV-all': 'config_all',
    'Wp2j_HT2_13TeV-all': 'config_all'
}

# You can overwrite any value in your header by specifying the same attribute
# here. E.g to set the number of jobs 1234 for this runcard, you could include
# the line
# producRun = 1234

# You can even import and use other functions here, such as the following to
# auto pick the CE with most cores free
# import get_site_info
# ce_base = get_site_info.get_most_free_cores()
# or use the aliases defined at the top of get_site_info.py
# ce_base = get_site_info.manchester

# Automatically pick the next seed you haven't run (uses seeds stored in the
# database for this ;)

# Unfortunitly custom function definition do _not overwrite_ the definition in
# your header, if you use them here make sure you define them again
# def base_dir(folder):
#   return "/mt/home/{0}/tst_grid/{1}".format(getuser(), folder)


def scratch_dir(folder):
    return "/scratch/{0}/tst_grid/{1}".format(getuser(), folder)


baseSeed = dbapi.get_next_seed()
# If overwriting dbname in this runcard.py file, pass through the name here:
baseSeed = dbapi.get_next_seed(dbname=scratch_dir("hej_database"))
