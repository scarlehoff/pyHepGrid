dictCard = { # first: -r second: -j
  ## we use -r as the name and -j as the runcard
    ## 7 Tev
    ## NLO (uses pyHepGrid for Sherpa, see later)
    # 'Wm2j_HT2_7TeV_NLO':'Run',
    # 'Wp2j_HT2_7TeV_NLO':'Run',
    ## W-
    'Wm2j_HT2_7TeV':'config_all',
    # 'Wm3j_HT2_7TeV':'config_all',
    # 'Wm4j_HT2_7TeV':'config_all',
    # 'Wm5j_HT2_7TeV':'config_all',
    # 'Wm2j_HT2_7TeV_lowpt':'config_all',
    # 'Wm3j_HT2_7TeV_lowpt':'config_all',
    # 'Wm4j_HT2_7TeV_lowpt':'config_all',
    # 'Wm5j_HT2_7TeV_lowpt':'config_all',
    # 'Wm3j_HT2_7TeV':'config_sublead12',
    # 'Wm4j_HT2_7TeV':'config_sublead12',
    # 'Wm5j_HT2_7TeV':'config_sublead12',
    ## W+
    # 'Wp2j_HT2_7TeV':'config_all',
    # 'Wp3j_HT2_7TeV':'config_all',
    # 'Wp4j_HT2_7TeV':'config_all',
    # 'Wp5j_HT2_7TeV':'config_all',
    # 'Wp2j_HT2_7TeV_lowpt':'config_all',
    # 'Wp3j_HT2_7TeV_lowpt':'config_all',
    # 'Wp4j_HT2_7TeV_lowpt':'config_all',
    # 'Wp5j_HT2_7TeV_lowpt':'config_all',
    # 'Wp3j_HT2_7TeV':'config_sublead12',
    # 'Wp4j_HT2_7TeV':'config_sublead12',
    # 'Wp5j_HT2_7TeV':'config_sublead12',
    ## 13 TeV
    ## HT2
    # 'Wp2j_HT2_13TeV':'config_all',
    # 'Wp3j_HT2_13TeV':'config_all',
    # 'Wp4j_HT2_13TeV':'config_all',
    # 'Wp5j_HT2_13TeV':'config_all',
    ## mw
    # 'Wp2j_mw_13TeV':'config_all',
    # 'Wp3j_mw_13TeV':'config_all',
    # 'Wp4j_mw_13TeV':'config_all',
}
## You can overwrite any value in your header by specifying the same attribute
## here. E.g to set the number of jobs 99999 for this runcard, you could include
## the line

## project specific setup
## everything using base_dir in hej_header.py has to be overwritten here
def base_dir(folder):
    return "/mt/home/mheil/projects/Wjets/{0}".format(folder) # user specific!

dirac_name = "marian.heil" # user specific!
DIRAC_BANNED_SITES = ["LCG.UKI-SCOTGRID-DURHAM.uk"]

grid_input_dir  = "Wjets/input"
# grid_output_dir = "Wjets/output_a882cacf"
# grid_warmup_dir = "HEJ/HEJ_a882cacf.tar.gz" # path of the HEJ tar on gfal (ignore the variable name)
grid_output_dir = "Wjets/output_test"
grid_warmup_dir = "HEJ/HEJ.tar.gz"

runcardDir = base_dir("Setup/")
local_run_directory = base_dir("Wjets/")
provided_warmup_dir = runcardDir
finalisation_script = base_dir("results/CombineRuns_main")

arcbase = base_dir(".arc/jobs.dat") # arc database
dbname  = base_dir("hej_database") # sql database

events     = -1 # => number of events from runcard

## You can even import and use other functions here, such as the following to
## auto pick the CE with most cores free
# import get_site_info
# ce_base = get_site_info.get_most_free_cores()
## or use the aliases defined at the top of get_site_info.py
# ce_base = get_site_info.manchester

## Automatically pick the next seed you haven't run (uses seeds stored in the
## database for this ;)
import pyHepGrid.src.dbapi as dbapi
## If overwriting dbname in this runcard.py file, pass through the name here:
baseSeed = dbapi.get_next_seed(dbname = base_dir("hej_database"))
# baseSeed = 1 # setting seed manually

## miscellaneous overwrites
# jobName    = "HEJ_W_7TeV_lowpt"
# producRun = 5000
events    = 10

## to run Sherpa (since pyHepGrid 3b1bf9690d30ad9608c68e3c01413bb6c955b448)
# jobName = "Wjets_NLO"
# runmode = "sHeRpa"
# runfile = "sherparun.py"

# dbname  = base_dir("test_database") # sql database
