#/usr/bin/env python3

import re

r1 = re.compile("(?<=`).+(?=\_')")

errors = "errors.txt"

functions = []
with open(errors, 'r') as f:
    for line in f:
        if "undefined" in line:
            functions.append(r1.search(line).group())

functions = list(set(functions))

def print_function_fortran(f):
    template = """
      real function {0}(a,b,c,d,e,f,h,i,j,k)
      end function"""
    return template.format(f)

for f in functions:
    print(print_function_fortran(f))

