#!/bin/bash

echo "Sending $1 to warmup ARC run"
yes | ./main.py ini $1 -An
yes | ./main.py run $1 -An
