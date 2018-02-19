#!/bin/bash
cd /mt/home/<USERNAME>/gridscripts/gangaless_resources/proxy_renewal/
source /mt/home/USERNAME/.bashrc
date
./.script.exp > syncjobs.log
date
./.script2.exp >> syncjobs.log
echo Proxy initialisation finished
date
