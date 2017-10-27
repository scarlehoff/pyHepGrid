import juan_header as head
import sys

############ COPY NAMESPACE FROM MY_HEADER #############
# Only slightly abusive...
this_file = sys.modules[__name__]
for i in dir(head):
    if not i.startswith("__"):
        setattr(this_file,i,getattr(head,i))
