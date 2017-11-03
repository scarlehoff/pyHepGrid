import header
runcardDir = header.runcardDir
NNLOJETdir = header.NNLOJETdir
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
