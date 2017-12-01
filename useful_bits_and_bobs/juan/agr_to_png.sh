#!/bin/bash

#
# https://mail.gna.org/public/relax-users/2013-06/msg00022.html
# Troels Emtekær Linnet


for gracefile in *.agr; do
filename=$(basename "$gracefile")
extension="${filename##*.}"
filename="${filename%.*}"

TMPPNG=${filename}_png.tmp
cat $gracefile > $TMPPNG
echo "#Print out to" >> $TMPPNG
echo '@PRINT TO "'"${PWD}/${filename}.png"'"' >> $TMPPNG
echo '@HARDCOPY DEVICE "PNG"' >> $TMPPNG
echo '@DEVICE "PNG" FONT ANTIALIASING on' >> $TMPPNG
echo '# Make white background transparent' >> $TMPPNG
echo '#@DEVICE "PNG" OP "transparent:on"' >> $TMPPNG
echo '@DEVICE "PNG" OP "compression:9"' >> $TMPPNG
echo '@PRINT' >> $TMPPNG
xmgrace -hardcopy $TMPPNG

TMPEPS=${filename}_eps.tmp
cat $gracefile > $TMPEPS
echo "#Print out to" >> $TMPEPS
echo '@PRINT TO "'"${PWD}/${filename}.eps"'"' >> $TMPEPS
echo '@HARDCOPY DEVICE "EPS"' >> $TMPEPS
echo '@DEVICE "EPS" OP "level2"' >> $TMPEPS
echo '@PRINT' >> $TMPEPS
xmgrace -hardcopy $TMPEPS

echo "$filename $extension"
#eps2png -resolution 200 $TMPEPS
#epstopdf $TMPEPS
done
