#runcardDir = "/custom/runcard/directory" # overwrites header
#NNLOJETdir = "/custom/nnlojet/directory" 
print("Sourcing runcard")
dictCard = {
    # 'LO.run':'DM_LO',
    # 'RRa.run':'DM_RRa'
    'Ra_Wm.run':'WM_VAL_Ra',
    'Rb_Wm.run':'WM_VAL_Rb',
    'V_Wm.run':'WM_VAL_V',
    'Ra_Wp.run':'WP_VAL_Ra',
    'Rb_Wp.run':'WP_VAL_Rb',
    'V_Wp.run':'WP_VAL_V',
}

# Optional values
# sockets_active = 5
# port = 8888

# You can overwrite any value in your header by specifying the same attribute here. 
# E.g to set the number of jobs 99999 for this runcard, you could include the line
# producRun = 999999

# You can even import and use other functions here, such as the following to auto pick the
# CE with most cores free
# import get_site_info
# ce_base = get_site_info.get_most_free_cores()
