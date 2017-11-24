fixturesdir = "fixtures/"
filegen_tests = {
    "WpJME_output": {"filename": "WpJLOME", "flags": ["WpJ", "-LO", "WpJLOME"]},
    "WmJME_output": {"filename": "WmJLOME", "flags": ["WpJ", "-LO", "WmJLOME"]},
    "ZME_output": {"filename": "ZLOME", "flags": ["Z", "-LO", "ZLOME"]},
    "ZJME_output": {"filename": "ZJLOME", "flags": ["ZJ", "-LO", "ZJLOME"]},
    "VFHME_output": {"filename": "VFHLOME", "flags": ["VFH", "-LO", "VFHLOME"]},
    "jetME_output": {"filename": "jetLOME", "flags": ["jet", "-LO", "jetLOME"]},
    "WpME_output": {"filename": "WpLOME", "flags": ["Wp", "-LO", "WpLOME"]},
    "WpJJJME_output": {"filename": "WpJJJLOME", "flags": ["WpJJJ", "-LO", "WpJJJLOME"]},
}
print_tests = {
    "no_input": [],
    "ggWp": ["Wp", "-ip1", "g", "-ip2", "g"],
    "qqbWp": ["Wp", "-ip1", "u", "-ip2", "db"],
    "ggH": ["H", "-ip1", "g", "-ip2", "g", "--PDFS"],
    "qqbZ": ["Z", "-ip1", "u", "-ip2", "ub", "--FLAV"],
    "BtgZ": ["Z", "-me", "*Bt*", "-ip2", "g"],
    "jet": ["jet"],
    "fac": ["Wp", "-ip1", "g", "-ip2", "g", "-f"],
    "fullfac": ["Wp", "-ip1", "u", "-ip2", "db", "-ff"],
    "table": ["Z", "-ip1", "u", "-ip2", "ub", "--FLAV", "-t"],
    "linkings": ["Z", "-me", "*Bt*", "-ip2", "g", "-t", "-lk"],
    "msub": ["Z", "-msub", "*B*", "*D*"],
    "sub": ["ZJ", "-sub", "*B*", "*D*"],
    "msub2": ["ZJ", "-msub", "*C*", "*D*"],
    "rc": ["Wm", "-ip1", "u Q d", "-ip2", "u Q d", "-rc"],
    "O1": ["Wm", "-ip1", "u Q d", "-ip2", "u Q d", "-o", "R", "V"],
    "O2": ["Wm", "-ip1", "u Q d", "-ip2", "u Q d", "-o", "R", "LO"],
    "O3": ["Wm", "-ip1", "u Q d", "-ip2", "u Q d", "-o", "RR", "V"],
    "O4": ["Wm", "-ip1", "u Q d", "-ip2", "u Q d", "-o", "R", "V"],
    "O5": ["Wm", "-ip1", "u Q d", "-ip2", "u Q d", "-o", "RV", "V"],
    "O6": ["ZJ", "-ip1", "u Q d", "-ip2", "u Q d", "-o", "LO"],
    "O7": ["HJ", "-ip1", "u Q d", "-ip2", "u Q d", "-o", "R"],
    "O8": ["Wp", "-ip1", "u Q d", "-ip2", "u Q d", "-o", "V"],
    "O9": ["Wm", "-ip1", "u Q d", "-ip2", "u Q d", "-o", "RR"],
    "O10": ["Z", "-ip1", "u Q d", "-ip2", "u Q d", "-o", "RV"],
    "O11": ["H", "-ip1", "u Q d", "-ip2", "u Q d", "-o", "VV"],
    "PO1": ["Wm", "-ip1", "u Q d", "-ip2", "u Q d", "-po", "NLO"],
    "PO2": ["Wm", "-ip1", "u Q d", "-ip2", "u Q d", "-po", "LO"],
    "PO3": ["Wm", "-ip1", "u Q d", "-ip2", "u Q d", "-po", "NNLO"],
    "debug": ["Wm", "-d"],
    "Multichannel": ["Wm", "Z"],
    "Multichannel_with_selectors": ["ZJ", "HJ", "-ip1", "g"],
    "ZJJC1": ["ZJJ", "-C1", "1"],
    "ZJJC2": ["ZJJ", "-C2", "2"],
    "VFHB1": ["VFH", "-B1", "1"],
    "VFHC12": ["VFHJ", "-C1", "1", "2"],
    "ZJJC1PDF": ["ZJJ", "-PDF1", "-20", "--FLAV"],
    "ZJJC2PDF": ["ZJJ", "-PDF2", "10", "--FLAV"],
    "VFHB1PDF": ["VFH", "-PDF1", "0", "--FLAV"],
    "ZJJC1print": ["ZJJ", "-C1", "1", "--FLAV"],
    "ZJJC2print": ["ZJJ", "-C2", "2", "--FLAV"],
    "VFHB1print": ["VFH", "-B1", "1", "--FLAV"],
    "VFHC12print": ["VFHJ", "-C1", "1", "2", "--FLAV"],
    "ZJJC1_unique": ["ZJJ", "-C1", "1", "--unique_PDFs"],
    "ZJJC2_unique": ["ZJJ", "-C2", "2", "--unique_initial_states", "--unique_PDFs"],
    "VFHB1_unique": ["VFH", "-B1", "1", "--unique_initial_states", "--unique_PDFs"],
    "VFHC12_unique": ["VFHJ", "-C1", "1", "2", "--unique_PDFs"],
    "ZJJC1print_unique": ["ZJJ", "-C1", "1", "--FLAV", "--unique_PDFs"],
    "ZJJC2print_unique": ["ZJJ", "-C2", "2", "--FLAV", "--unique_initial_states",
                          "--unique_PDFs"],
    "VFHB1print_unique": ["VFH", "-B1", "1", "--FLAV", "--unique_initial_states",
                          "--unique_PDFs"],
    "VFHC12print_unique": ["VFHJ", "-C1", "1", "2", "--FLAV",
                           "--unique_initial_states", "--unique_PDFs"],
    "ZJJC1F": ["ZJJ", "-C1", "1", "-f", "-rmfac"],
    "ZJJC2F": ["ZJJ", "-C2", "2", "-f", "-rmfac"],
    "VFHB1F": ["VFH", "-B1", "1", "-f", "-rmfac"],
    "VFHC12F": ["VFHJ", "-C1", "1", "2", "-f", "-rmfac"],
    "ZJJC1printsort": ["ZJJ", "-C1", "1", "--FLAV", "-s", "IP1"],
    "ZJJC2printsort": ["ZJJ", "-C2", "2", "--FLAV", "-s", "IP2"],
    "VFHB1printsort": ["VFH", "-B1", "1", "--FLAV", "-s", "ME"],
    "VFHC12printsort": ["VFHJ", "-C1", "1", "2", "--FLAV", "-s", "MSUB"],
    "ZJJC1_uniquesort": ["ZJJ", "-C1", "1", "--unique_PDFs", "-s", "PDF1"],
    "ZJNC_select": ["ZJ", "-NC", "-1"],
    "HJJNC_select": ["HJJ", "-NC", "-1", "-3", "-4"],
    "ZJJNF_select": ["ZJJ", "-NF", "1"],
    "HJNF_select": ["HJ", "-NF", "2", ],
    "jetNC_select": ["jet", "-NC", "0", "-2", "-4"],
    "jetNF_select": ["jet", "-NF", "1", "2"],
    "ZJNC_reject": ["ZJ", "-rNC", "-1"],
    "HJJNC_reject": ["HJJ", "-rNC", "-1", "-3", "-4"],
    "ZJJNF_reject": ["ZJJ", "-rNF", "1"],
    "HJNF_reject": ["HJ", "-rNF", "2", ],
    "jetNC_reject": ["jet", "-rNC", "0", "-2", "-4"],
    "jetNF_reject": ["jet", "-rNF", "1", "2"],
    "ggWppfc": ["Wp", "-ip1", "g", "-ip2", "g", "-pfc"],
    "qqbWppfc": ["Wp", "-ip1", "u", "-ip2", "db", "-pfc"],
    "ggHpfc": ["H", "-ip1", "g", "-ip2", "g", "--PDFS", "-pfc"],
    "qqbZpfc": ["Z", "-ip1", "u", "-ip2", "ub", "--FLAV", "-pfc"],
    "BtgZpfc": ["Z", "-me", "*Bt*", "-ip2", "g", "-pfc"],
    "jetpfc": ["jet", "-pfc"],
    "facpfc": ["Wp", "-ip1", "g", "-ip2", "g", "-f", "-pfc"],
    "fullfacpfc": ["Wp", "-ip1", "u", "-ip2", "db", "-ff", "-pfc"],
    "jetNC_reject": ["jet", "-rNC", "0", "-2", "-4", "-pnc", "-pnf"],
    "jetNF_reject": ["jet", "-rNF", "1", "2", "-pnc", "-pnf"],
    "ggWppfc": ["Wp", "-ip1", "g", "-ip2", "g", "-pfc", "-pnf"],
    "qqbWppfc": ["Wp", "-ip1", "u", "-ip2", "db", "-pfc", "-pnc", "-pnf"],
    "ggHpfc": ["H", "-ip1", "g", "-ip2", "g", "--PDFS", "-pfc", "-pnc", "-pnf"],
    "qqbZpfc": ["Z", "-ip1", "u", "-ip2", "ub", "--FLAV", "-pfc", "-pnc"]
}