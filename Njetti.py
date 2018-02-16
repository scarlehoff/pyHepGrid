# runcardDir = "/custom/runcard/directory" # overwrites header
# NNLOJETdir = "/custom/nnlojet/directory" 
print("Sourcing runcard")
dictCard = {
    # 'LO_Njetti_njetti.run':"NJ", 
    # 'R_Njetti_njetti.run':"NJ", 
    # 'V_Njetti_njetti.run':"NJ", 
    # 'RV_Njetti_njetti.run':"NJ", 
    # 'VV_Njetti_njetti.run':"NJ", 
    'RRa_Njetti_njetti.run':"NJ", 
    'RRb_Njetti_njetti.run':"NJ", 
}
# Optional values
# sockets_active = 5
# port = 8888

# You can overwrite any value in your header by specifying the same attribute here. 
# E.g to set the number of jobs 99999 for this runcard, you could include the line
producRun = 10000
baseSeed = 12001
jobName = "WJNJ"
# You can even import and use other functions here, such as the following to auto pick the
# CE with most cores free
# import get_site_info
# ce_base = get_site_info.get_most_free_cores()
