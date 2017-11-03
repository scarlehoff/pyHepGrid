# ONLY works for W+ for now!
MCFM_label_lookup = {'W_rapidity':'yw',
                     'W_ps-rap':'etaw',
                     'W_pt':'ptw',
                     'ylep':'ylp',
                     'ptlep':'ptlp',
                     'ptlepmagnified':'ptlpmag',
                     'pt4':'ptlp',
                     'pt3':'etmiss',
                     'pt5':'ptj1',
                     'pt6':'ptj2',
                     'pt7':'ptj3',
                     'ptnu':'etmiss',
                     'ptnumagnified':'etmissmag',
                     'y4':'ylp',
                     'y3':'ynu',
                     'y5':'yj1',
                     'y6':'yj2',
                     'y7':'yj3',
                     'm34':'mln',
                     'pt34':'ptw',
                     'y34':'yw',
                     '|y34|':'abs_yw',
                     'ymiss':'ynu',
                     'ptmiss':'etmiss',
                     'Jet_1_pt':'ptj1',
                     'Jet_2_pt':'ptj2',
                     'Jet_3_pt':'ptj3',
                     'Jet_1_pt_log':'ptj1ln',
                     'Jet_1_pt_lin':'ptj1lin',
                     'Jet_2_pt_log':'ptj2ln',
                     'Jet_2_pt_lin':'ptj2lin',
                     'Jet_3_pt_log':'ptj3ln',
                     'Jet_3_pt_lin':'ptj3lin',
                     'Jet_1_eta':'etaj1',
                     'Jet_2_eta':'etaj2',
                     'Jet_3_eta':'etaj3',
                     'jet_1,jet_2_invariant_mass':'m12',
                     'delr':'min_dr_lj',
                     'DeltaRe5':'min_dr_lj',
                     'deleta':'deleta',
                     'etastar':'unknown'}

# for Z prodn:
# MCFM_label_lookup = {'W_rapidity': 'yw',
#                      'W_ps-rap': 'etaw',
#                      'W_pt': 'ptw',
#                      'ylep': 'ylp',
#                      'ptlep': 'ptlp',
#                      'ptlepmagnified': 'ptlpmag',
#                      'pt4': 'ptlp',
#                      'pt3': 'etmiss',
#                      'pt5': 'ptj1',
#                      'pt6': 'ptj2',
#                      'pt7': 'ptj3',
#                      'ptnu': 'etmiss',
#                      'ptnumagnified': 'etmissmag',
#                      'y4': 'ylp',
#                      'y3': 'ynu',
#                      'y5': 'yj1',
#                      'y6': 'etaj2',
#                      'y7': 'etaj3',
#                      'm34': 'mll',
#                      'pt34': 'ptz',
#                      'y34': 'yz',
#                      '|y34|': 'abs_yz',
#                      'ymiss': 'ymiss',
#                      'ptmiss': 'etmiss',
#                      'Jet_1_pt': 'ptj1',
#                      'Jet_2_pt': 'ptj2',
#                      'Jet_3_pt': 'ptj3',
#                      'Jet_1_pt_log': 'ptj1ln',
#                      'Jet_1_pt_lin': 'ptj1lin',
#                      'Jet_2_pt_log': 'ptj2ln',
#                      'Jet_2_pt_lin': 'ptj2lin',
#                      'Jet_3_pt_log': 'ptj3ln',
#                      'Jet_3_pt_lin': 'ptj3lin',
#                      'Jet_1_eta': 'etaj1',
#                      'Jet_2_eta': 'etaj2',
#                      'Jet_3_eta': 'etaj3',
#                      'jet_1,jet_2_invariant_mass': 'm12',
#                      'delr': 'min_dr_lj',
#                      'DeltaRe5': 'min_dr_lj',
#                      'deleta': 'deleta',
#                      'etastar': 'unknown'}

# ONLY works for W+ for now!
FEWZ_label_lookup = {'WpT': 'ptw',
                     'Wrapidity': 'yw',
                     'Q_llInvaria': 'mln',
                     'l-lep.pT': 'ptlp',
                     'l-lep.eta': 'ylp',
                     'l+neu.pT': 'etmiss',
                     'l+neu.eta': 'ynu',
                     'jet1pT': 'ptj1',
                     'jet1eta': 'yj1',
                     'jet2pT': 'ptj2',
                     'jet2eta': 'yj2',
                     'M_T': 'mt',
                     'phot.pT': 'ptg',
                     'phot.eta': 'yg',
                     'beamthrust': 'bt',
                     'dR_lep,lep': 'dr_ll',
                     'dR_jet1,l-l': 'dr_jll',
                     'dR_jet1,l+n': 'dr_jz',
                     'dR_jet2,l-l': 'dr_j2ll',
                     'dR_jet2,l+n': 'dr_j2z',
                     'dR_jet,jet': 'dr_jj',
                     'dR_phot,lep': 'dr_gl',
                     'H_T': 'ht',
                     'Delta_Phi': 'delphi',
                     'A_FBvsQll': 'unknown',
                     'A0vspT': 'unknown',
                     'A1vspT': 'unknown',
                     'A2vspT': 'unknown',
                     'A3vspT': 'unknown',
                     'A4vspT': 'unknown',
                     'phiCS': 'unknown',
                     'costhCS': 'unknown'}

# MAY NEED TO SWAP LEP AND NU
DYNNLO_label_lookup = {'m34 distribution': 'mln',
                                           'eta3 distribution': 'ynu',
                                           'eta4 distribution': 'ylp',
                                           'pt3 distribution': 'etmiss',
                                           'pt4 distribution': 'ptlp',
                                           'pt5 distribution': 'ptj1',
                                           'pt6 distribution': 'ptj2',
                                           'y34 distribution': 'yw',
                                           'pt34 distribution': 'ptw',
                                           'mt distribution': 'mt'}

VBFNLO_label_lookup = {
    "pT_j": "ptj2",
    "pTmax_j": "ptj1",
    "pTmin_j": "ptj3",
    "y_j": "yj2",
    "y_j1": "yj1",
    "y_j2": "yj2",
    "pTmax_l": "ptlp",
    "pTmin_l": "ptlp",
    "|eta|max_l": "ylp",
    "|eta|min_l": "ylm",
    "Phi_jj": "unknown",
    "pT_j(NLO)": "ptj2-NLO",
    "pTmax_j(NLO)": "ptj1-NLO",
    "pTmin_j(NLO)": "ptj3-NLO",
    "y_j(NLO)": "yj2-NLO",
    "y_j1(NLO)": "yj1-NLO",
    "y_j2(NLO)": "yj2-NLO",
    "pTmax_l(NLO)": "ptlp-NLO",
    "pTmin_l(NLO)": "ptlp-NLO",
    "|eta|max_l(NLO)": "ylp-NLO",
    "|eta|min_l(NLO)": "ylm-NLO",
    "Phi_jj(NLO)": "unknown-NLO",
    "pT_j(K)": "ptj2-K",
    "pTmax_j(K)": "ptj1-K",
    "pTmin_j(K)": "ptj3-K",
    "y_j(K)": "yj2-K",
    "y_j1(K)": "yj1-K",
    "y_j2(K)": "yj2-K",
    "pTmax_l(K)": "ptlp-K",
    "pTmin_l(K)": "ptlp-K",
    "|eta|max_l(K)": "ylp-K",
    "|eta|min_l(K)": "ylm-K",
    "Phi_jj(K)": "unknown"}