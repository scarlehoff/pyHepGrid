from __future__ import division
import config as cf
import selectors_rejectors as selrej
import re
import itertools


class Channel():
    """Class containing the information for each partonic channel.

    Generated from lines in the selectchannel*.f input files."""
    pad = cf.PRINT_PADDING
    pertorder = {"LO": "LO", "R": "NLO", "V": "NLO",
                 "RR": "NNLO", "RV": "NNLO",
                 "VV": "NNLO"}

    def __init__(self, line, chantype):
        self.line = line
        self.__set_chantype_attributes(chantype)
        self.__set_components(line)
        self.__set_component_attributes()
        self.__add_temp_attributes()
        self.__get_ME_sub()
        for i in selrej.get_selector_IDs():  # generate rejection attributes.
            setattr(self, "r" + i, getattr(self, i))
        self.__get_partons()

    def __set_chantype_attributes(self, chantype):
        self.O = chantype
        self.PO = Channel.pertorder[chantype]

    def __set_components(self, line):
        self.components = line.split()
        if "=" in self.components[1]:
            twopart = self.components[1].split("=")
            self.components = [self.components[0]] + \
                twopart + self.components[2:]

    def __set_component_attributes(self):
        self.args = ["(" + self.components[-1].split("(")[1]]
        self.IP = self.components[1].split("(")[1][:-1]
        self.IP1 = self.components[5]
        self.IP2 = self.components[6]
        self.MSUB = self.components[-1].split("(")[0]

    def __add_temp_attributes(self):
        self.PDF1 = ""  # Temp as added later by parsing the sig files
        self.PDF2 = ""
        self.B1 = "-"
        self.C1 = "-"
        self.C2 = "-"
        self.NC = ""
        self.NF = ""

    def __get_ME_sub(self):
        x2 = self.components[-2]
        if x2 == "subtraction":  # subtraction only
            self.ME = ""
            self.SUB = self.components[-1].split("(")[0]
            return
        elif "(" in x2:  # subtraction and ME present
            self.ME = self.components[-2].split("(")[0]
            self.SUB = self.components[-1].split("(")[0]
            return
        else:  # LO
            self.ME = self.components[-1].split("(")[0]
            self.SUB = ""
            return

    def combine_powers(self, val, string):
        total_power, powers, single = self.__calc_total_power(string, val)
        val = powers.sub("@", val)
        val = single.sub("@", val)
        val = val.replace("@*", "")
        val = val.replace("*@", "")
        val = val.replace("@", "")
        val = self.__add_power_of_string(val, string, total_power)
        return val

    def __calc_total_power(self, string, val):
        powers = re.compile(string+"\*\*[+-]?[0-9]*")
        single = re.compile(string)
        matches = powers.findall(val)
        singles = single.findall(val)
        nf_powers = [x.split("**")[-1] for x in matches]
        combined_power = sum(int(power) for power in nf_powers)
        total_power = combined_power - len(nf_powers)+len(singles)
        return total_power, powers, single

    def __add_power_of_string(self, val, string, total_power):
        """ Adds a term to a given string of string**total_power output dependent on
        circumstance, e.g total_power = 0, return 1 etc."""
        if val != "":
            if total_power == 1:
                val = val+"*"+string
            elif total_power == 0:
                pass
            else:
                val = val+"*"+string+"**"+str(total_power)
        else:
            if total_power == 1:
                val = string
            elif total_power == 0:
                val = "1"
            else:
                val = string+"**"+str(total_power)
        return val

    def __get_subterms(self, term):
        pm = re.compile("([+-])")
        xterm = term[1:-1]  # strip brackets
        subterms = pm.split(xterm)
        newsubterms = []
        buff = False
        for subterm in subterms:
            if not buff and "+" not in subterm and "-" not in subterm:
                newsubterms.append(subterm)
            elif "+" in subterm or "-" in subterm:
                buff = True
                buffstring = subterm
            elif buff:
                newsubterms.append(buffstring+subterm)
                buff = False
        return newsubterms

    def __gen_power_list(self, searchterm, all_poss):
        outlist = []
        for x in all_poss:
            neg_powers, a, b = self.__calc_total_power("/"+searchterm, x)
            pos_powers, a, b = self.__calc_total_power(searchterm, x)
            outlist.append(str(pos_powers-2*neg_powers))
        return outlist

    def __get_order(self, instring, searchterm):
        brackets = re.compile("\(.*?\)")
        bracketed_terms, x = brackets.findall(instring), instring
        all_subterms = []
        if len(bracketed_terms) > 0:
            for term in bracketed_terms:
                all_subterms.append(self.__get_subterms(term))
            no_brackets = brackets.sub("1", instring)
            all_poss = ['*'.join(s) for s
                        in itertools.product(*all_subterms)]
            all_poss = [x + "*"+no_brackets for x in all_poss]
        else:
            all_poss = [instring]
        outlist = self.__gen_power_list(searchterm, all_poss)
        return sorted(list(set(outlist)), key=lambda x: int(x), reverse=True)

    def _get_NC_NF_order(self):
        self.NC = [str(i) for i in self.__get_order(self.rmFAC, "nc")]
        self.NF = [str(i) for i in self.__get_order(self.rmFAC, "nf")]

    def __get_partons(self):
        self.start_idx = 0
        self.end_idx = 1
        for idx, i in enumerate(self.components):
            if i == "!":
                self.start_idx = idx + 1
                break
        for idx, i in enumerate(self.components[self.start_idx:]):
            if "(" in i:
                self.end_idx = idx + self.start_idx
                break
        self.partons = " ".join(i for i in
                                self.components[self.start_idx:self.end_idx])

    def __get_NF_NC_str(self):
        retval = ""
        if Channel.print_NF:
            retval = self.__justified_val("NF: " + " ".join(
                i for i in self.NF)+" ", "NF")
        if Channel.print_NC:
            retval += self.__justified_val("NC: " + " ".join(
                i for i in self.NC)+" ", "NC")
        return retval

    def __get_flavour_str(self):
        retval = ""
        if Channel.print_flavour_info:
            retval += self.__justified_val("B1:", "B1")
            retval += self.__justified_val(self.B1, "FLAVOUR")
            retval += self.__justified_val("C1:", "C1")
            retval += self.__justified_val(self.C1, "FLAVOUR")
            retval += self.__justified_val("C2:", "C2")
            retval += self.__justified_val(self.C2, "FLAVOUR")
        return retval

    def __get_PDFS_str(self):
        retval = ""
        if Channel.print_PDFS:
            retval += self.__justified_val(self.PDF1, "PDF1")
            retval += self.__justified_val(self.PDF2, "PDF2")
        return retval

    def __get_factor_str(self):
        retval = ""
        if Channel.print_fac:
            retval += self.FAC.ljust(len(self.FAC) + Channel.pad["FAC"])
        if Channel.print_fullfac:
            retval += self.FULLFAC.ljust(len(self.FULLFAC) +
                                         Channel.pad["FULLFAC"])
        return retval

    def __get_args_str(self):
        retval = ""
        if Channel.print_all_ME_calls:
            retval += self.__justified_val(" ".join(i for i in self.args),
                                           "allargs")
        else:
            retval += self.__justified_val(self.args[0], "args")
        return retval

    def __get_IP_data_str(self):
        retval = self.__justified_val(self.IP.strip()+": ", "IP")
        retval += self.__justified_val(self.IP1, "IP1")
        retval += self.__justified_val(self.IP2, "IP2")
        return retval

    def __justified_val(self, val, padtxt):
        return val.ljust(Channel.pad[padtxt])

    def __repr__(self):
        retval = self.__get_IP_data_str()
        retval += self.__get_NF_NC_str()
        retval += self.__get_PDFS_str()
        retval += self.__get_flavour_str()
        retval += self.__justified_val(self.O, "O") + " "
        retval += self.__justified_val(self.ME, "ME")
        retval += self.__justified_val(self.SUB, "SUB")
        retval += self.__get_args_str()
        retval += self.__justified_val(self.partons, "partons")
        retval += self.__get_factor_str()
        retval.replace("\n", "")
        return retval.strip()

    def print_table_line(self, args):
        div = " | "
        print_string = div + self.__justified_val(self.IP, "IP")
        print_string += div + self.__justified_val(self.IP1, "IP1")
        print_string += self.__justified_val(self.IP2, "IP2")
        print_string += div + self.__justified_val(self.SUB, "SUB")
        print_string += div + self.__justified_val(self.args[0], "args")
        print_string += div + self.__justified_val(self.partons, "partons")
        print_string += div
        print_string.replace("\n", "")
        if args.links:
            print_string += "".ljust(6)+div
        if args.tested:
            print_string += "".ljust(6)+div
        print(print_string)

    @classmethod
    def print_table_header(cls, args):
        div = " | "
        print_string = div + "IP".ljust(Channel.pad["IP"])
        print_string += div + \
            "Incoming Partons".ljust(Channel.pad["Incoming Partons"])
        print_string += div + "Subtraction Term".ljust(Channel.pad["SUB"])
        print_string += div + "Crossing".ljust(Channel.pad["Crossing"])
        print_string += div + "All Partons".ljust(Channel.pad["partons"]) + div
        print_string.replace("\n", "")
        if args.links:
            print_string += "Linked" + div
        if args.tested:
            print_string += "Tested" + div
        print(print_string)
        print("|-")

    def __add_flavours(self):
        info = self.FLAVOUR.split("nf")[1:]
        for i in info:
            x = i.split(": ")
            line = x[0]
            val = x[1].strip()
            setattr(self, line, val)

    def add_sigfile_info(self, sig_obj):
        self.PDF1 = sig_obj.PDF1
        self.PDF2 = sig_obj.PDF2
        self.FLAVOUR = sig_obj.FLAVOUR
        self.__add_flavours()

    def add_MEfile_info(self, sig_obj):
        self.args += sig_obj.ME_args
        self.args = list(set(self.args))
        arg_len = len(" ".join(i for i in self.args))+3
        if arg_len > Channel.pad["allargs"]:
            Channel.pad["allargs"] = arg_len
