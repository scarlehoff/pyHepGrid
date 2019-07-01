#runcardDir = "/custom/runcard/directory" # overwrites header
#NNLOJETdir = "/custom/nnlojet/directory"
print("Sourcing runcard")
dictCard = { # first: -r second: -j
  # we use -r as the name and -j as the runcard
    # 'Wp2j_mw_13TeV-all': 'config_all',
    'Wp2j_HT2_13TeV-all':'config_all'
}

# events = 10

## Optional values
# sockets_active = 5
# port = 8888

## You can overwrite any value in your header by specifying the same attribute
## here. E.g to set the number of jobs 99999 for this runcard, you could include
## the line
# producRun = 24

## You can even import and use other functions here, such as the following to
## auto pick the CE with most cores free
# import get_site_info
# ce_base = get_site_info.get_most_free_cores()
## or use the aliases defined at the top of get_site_info.py
# ce_base = get_site_info.manchester

## Automatically pick the next seed you haven't run (uses seeds stored in the
## database for this ;)
import src.dbapi as dbapi
baseSeed = dbapi.get_next_seed()
## If overwriting dbname in this runcard.py file, pass through the name here:
baseSeed = dbapi.get_next_seed(dbname = "hej_database")
