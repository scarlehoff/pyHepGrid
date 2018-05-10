#!/bin/bash


# HELP TEXT function

function display_help(){
  echo -n "change_runcard.sh [OPTIONS] ... [FILE] ... [-o OUTFILE]

  A simple script to automate the changing of NNLOJET runcards.
  Without any of the below arguments, it will print the runcard's properties and exit.

  Where both -w and -p are present, both warmup and production mode will be enabled in the runcard.


  Options:
    -E, --energy        Changes energy (in TeV)
    -I, --Iterations    Changes number of integration iterations (e.g. 5 for warm-ups)
    -N, --events        Changes number of events per iteration
    -p, --prod          Switches production ON (and warmup off)
    -w, --warm          Switches warmup ON (and production off)
    -o, --output        Sets output file, to which to write changed runcard.

    -h, --help          Display this help and exit.
  "
}


getopt --test > /dev/null
if [[ $? -ne 4 ]]; then
    echo "I’m sorry, `getopt --test` failed in this environment."
    exit 1
fi

OPTIONS=E:I:N:o:pwh
LONGOPTIONS=energy:,iterations:,events:,output:,prod,warmup,help

# -temporarily store output to be able to check for errors
# -e.g. use “--options” parameter by name to activate quoting/enhanced mode
# -pass arguments only via   -- "$@"   to separate them correctly
PARSED=$(getopt --options=$OPTIONS --longoptions=$LONGOPTIONS --name "$0" -- "$@")
if [[ $? -ne 0 ]]; then
    # e.g. $? == 1
    #  then getopt has complained about wrong arguments to stdout
    exit 2
fi
# read getopt’s output this way to handle the quoting right:
eval set -- "$PARSED"

# now enjoy the options in order and nicely split until we see --
while true; do
    case "$1" in
        -E|--energy)
            energy="$2"
            shift 2
            ;;
        -I|--iterations)
            iter="$2"
            shift 2
            ;;
        -N|--events)
            events="$2"
            shift 2
            ;;
        -o|--output)
            outFile="$2"
            shift 2
            ;;
        -p|--prod)
            prod=y
            shift
            ;;
        -w|--warm)
            warm=y
            shift
            ;;
        -h|--help)
            display_help
            exit 0
            ;;
        --)
            shift
            break
            ;;
        *)
            echo "Programming error"
            exit 3
            ;;
    esac
done

# handle non-option arguments
if [[ $# -ne 1 ]]; then
    echo "$0: A single input file is required."
    exit 4
fi

# echo "write: $write, energy: $sqrts, iterations: $iter, prod: $prod, warm: $warm, verbose: $v, in: $1, out: $outFile"
# echo " "

if [[ -z $energy && -z $iter && -z $events && -z $prod && -z $warm ]]; then
  print=y
fi

oldeventsline=$(head -4 $1 | tail -1)
oldevents=${oldeventsline%% *}

olditerline=$(head -5 $1 | tail -1)
olditer=${olditerline%% *}

oldwarmline=$(head -7 $1 | tail -1)
oldwarm=${oldwarmline%. *}

oldprodline=$(head -8 $1 | tail -1)
oldprod=${oldprodline%. *}

oldenergyline=$(head -26 $1 | tail -1)
oldenergy=${oldenergyline%000d0*}

if [[ $print == y ]]; then
    echo "print enabled: reading attributes from file"
    echo "=== PARAMETERS OF EXISTING FILE ==="
    echo "Events: $oldevents"
    echo "Iterations: $olditer"
    echo "Warmup: $oldwarm"
    echo "Production: $oldprod"
    echo "Energy: $oldenergy TeV"

else
    if [[ -n "$outFile" ]]; then
      echo "Writing file to $outFile"
      cp $1 $outFile
    else
      echo "Rewriting input file $1"
      outFile=$1
    fi

    echo " "

    echo "=== OLD PARAMETERS ==="
    echo "Events: $oldevents"
    echo "Iterations: $olditer"
    echo "Warmup: $oldwarm"
    echo "Production: $oldprod"
    echo "Energy: $oldenergy TeV"

    echo " "

    if [[ -n "$events" ]]; then
      sed -i "4 s/$oldevents/$events/g" $outFile
    fi
    if [[ -n "$iter" ]]; then
      sed -i "5 s/$olditer/$iter/g" $outFile
    fi
    if [[ $warm == y ]] && [[ $prod == y ]]; then
      sed -i "7 s/\..*\./\.TRUE\./g" $outFile
      sed -i "8 s/\..*\./\.TRUE\./g" $outFile
    elif [[ $warm == y ]]; then
      sed -i "7 s/\..*\./\.TRUE\./g" $outFile
      sed -i "8 s/\..*\./\.FALSE\./g" $outFile
    elif [[ $prod == y ]]; then
      sed -i "7 s/\..*\./\.FALSE\./g" $outFile
      sed -i "8 s/\..*\./\.TRUE\./g" $outFile
    fi
    if [[ -n "$energy" ]]; then
      sed -i "26 s/$oldenergy/$energy/g" $outFile
    fi


    neweventsline=$(head -4 $1 | tail -1)
    newevents=${neweventsline%% *}

    newiterline=$(head -5 $1 | tail -1)
    newiter=${newiterline%% *}

    newwarmline=$(head -7 $1 | tail -1)
    newwarm=${newwarmline%. *}

    newprodline=$(head -8 $1 | tail -1)
    newprod=${newprodline%. *}

    newenergyline=$(head -26 $1 | tail -1)
    newenergy=${newenergyline%000d0*}

    echo "=== NEW PARAMETERS ==="
    echo "Events: $newevents"
    echo "Iterations: $newiter"
    echo "Warmup: $newwarm"
    echo "Production: $newprod"
    echo "Energy: $newenergy TeV"

fi


#
# for file in $1*.run
# do
#   wfile="${file%.run}_w.run"
#   pfile="${file%.run}_p.run"
#   cp ${file} ${wfile}
#   cp ${wfile} ${pfile}
#   sed -i 's/00    /      /g' ${pfile}
#   sed -i 's/5                      ! Number/1                      ! Number/g' ${pfile}
#   sed -i 's/.true.                 ! Warmup/.false.                 ! Warmup/g' ${pfile}
#   sed -i 's/.false.                 ! Production/.true.                 ! Production/g' ${pfile}
#   cp ${pfile} "${wfile%_w.run}.run"
# done

# vars=$(echo $1 | tr "!" "\n")
#
# for line in $vars
# do
#   echo "$line"
# done
#
# title=$1
# ID=$2
# proc=$3
# noevents=$4
# noiter=$5
# seedno=$6
# wu=$7
# prod=$8
#
#
# mails=$(echo $IN | tr ";" "\n")
#
# for addr in $mails
# do
#     echo "> [$addr]"
# done

#
# count = 1
#
# while IFS='' read -r line || [[ -n "$line" ]]; do
#     echo "Text read from file: $line"
#     if [$count -eq 4]
#     then
#
#     count++
# done < "$1"
