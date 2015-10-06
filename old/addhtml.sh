#!/bin/bash

#screen tcsh ~/bin/doWebTunnel &

if [[ ! -d run$1 ]]
    then
    echo No such folder run$1 , please first submit the noise tool for it ... 
    exit -1
fi

export run=`echo run$1`
export tomask=`cat $run/ToMask* | grep -v No | wc -l`
export tounmask=`cat $run/ToUnmask* | grep -v No | wc -l`
export masked=`cat $run/Masked* | grep -v No | wc -l`
export deadstrips=`cat $run/DeadChannels* | grep -v No | wc -l`
export inactive=$((masked+deadstrips))
export lowrate=`cat $run/LowRate* | grep -v No | wc -l`
export inactiveB=`cat $run/Masked_* $run/DeadChannels_* | awk '{print $7}' | grep W | wc -l`
export inactiveE=`cat $run/Masked_* $run/DeadChannels_* | awk '{print $7}' | grep RE | wc -l`

export wrong_syntax=`echo $1 | grep run | wc -l`

echo $wrong_syntax

if [[ $wrong_syntax -ne 0 ]]
then
 echo wrong syntax - please use ./addhtml.sh xxxxxx - without the word run
 exit -1
fi

export runexist=`cat index.html | grep run$1 | wc -l`

echo $runexist

if [[ $runexist -ge 1 && $2 -ne 1 ]]
then
 echo The run was already submitted $runexist times!
 echo If you are sure you want to submit it again, resubmit with 1 as second argument of this script
 echo But first check the bottom of http://cmsrpc402b20.cern.ch/noise/ 
 exit -1
fi

#make the index with the ToMask* links here

python RRApi/getLumiSectionsForRun.py $1 /rpctdata/CAF/run$1/LSnumber_$1.txt

if [[ -e /rpctdata/CAF/run$1/LSnumber_$1.txt ]]
then
    export LSnum=`cat /rpctdata/CAF/run$1/LSnumber_$1.txt`
else
    export LSnum=""
fi

echo "$run <a href="./$run/masked.html">Disabled(Masked) $masked</a>  <a href="./$run/deadstrips.html">Dead Strips $deadstrips</a>  <a href="./$run/tounmask.html">To Enable(To Unmask) $tounmask</a>  <a href="./$run/tomask.html">To Disable(To Mask) $tomask</a>  <a href="./$run/summary.txt">Summary </a>  <a href="./$run/database.txt">Database </a>  <a href="./$run/lowrate.html">LowRate </a> <a href="./run$1/fractions_files/pictures/fractions_plot_run$1.png"> Fraction plots</a> <a href="./run$1/strips_pics/Rate_distr.png"> Strips Distr</a> <a href="./$run/average_rates.html">Average Rates</a> $LSnum LS <br>" >> index.html

cd $run 
wc -l ToMask_* > tomask.txt
wc -l ToUnmask_* > tounmask.txt
wc -l Masked_* > masked.txt
wc -l DeadChannels_* > deadstrips.txt
wc -l LowRate_* > lowrate.txt

cd /rpctdata/CAF
if [[ 0 < `ls $run | grep tomask.html | wc -l` ]]
then
rm $run/tomask.html
fi
if [[ 0 < `ls $run | grep tounmask.html | wc -l` ]]
then 
rm $run/tounmask.html
fi
if [[ 0 < `ls $run | grep deadstrips.html | wc -l` ]]
then 
rm $run/deadstrips.html
fi
if [[ 0 < `ls $run | grep masked.html | wc -l` ]]
then 
rm $run/masked.html
fi
if [[ 0 < `ls $run | grep lowrate.html | wc -l` ]]
then 
rm $run/lowrate.html
fi

while read LINE
do
echo $LINE | awk {'print $1" <a href=./"$2">"$2"</a><br>"'} >> $run/tomask.html
done < "$run/tomask.txt"
while read LINE
do
echo $LINE | awk {'print $1" <a href=./"$2">"$2"</a><br>"'} >> $run/tounmask.html
done < "$run/tounmask.txt"
while read LINE
do
echo $LINE | awk {'print $1" <a href=./"$2">"$2"</a><br>"'} >> $run/masked.html
done < "$run/masked.txt"

while read LINE
do
echo $LINE | awk {'print $1" <a href=./"$2">"$2"</a><br>"'} >> $run/lowrate.html
done < "$run/lowrate.txt"
while read LINE
do
echo $LINE | awk {'print $1" <a href=./"$2">"$2"</a><br>"'} >> $run/deadstrips.html
done < "$run/deadstrips.txt"

wc -l $run/ToUnmask_* > $run/tounmask.txt
cat $run/Summary_* > $run/summary.txt
cat $run/Database_* > $run/database.txt

sed -e "s|-RUN-|$run|g" -e "s|-masked-|$masked|g" -e "s|-deadstrips-|$deadstrips|g"  -e "s|-inactive-|$inactive|g" -e "s|-inactiveB-|$inactiveB|g" -e "s|-inactiveE-|$inactiveE|g" -e "s|-tomask-|$tomask|g" -e "s|-tounmask-|$tounmask|g" -e "s|-summary-|$summary|g" -e "s|-database-|$database|g" -e "s|-lowrate-|$lowrate|g" htmltemplates/index.html > $run/index.html 

echo creating summary root files, please wait a moment...
cd $run
hadd -f barrel.root Noise*_RB+1_far*  Noise*RB+1_near*  Noise*RB+2_far*  Noise*RB+2_near*  Noise*RB0_far*  Noise*RB0_near*  Noise*RB-1_far*  Noise*RB-1_near*  Noise*RB-2_far*  Noise*RB-2_near* 

hadd -f eplus.root Noise*YEP1_far* Noise*YEP1_near* Noise*YEP3_far* Noise*YEP3_near* 
hadd -f eminus.root Noise*YEN1_far* Noise*YEN1_near* Noise*YEN3_far* Noise*YEN3_near* 

hadd -f endcap.root eplus.root eminus.root
hadd -f total.root barrel.root endcap.root

cd ..
echo done!

echo "Prepare the temporary index file ... "
cat index.html | sort -u -d > myTESTS/new_index.html #nope

### Recompile !
##echo "Preparing general history plots for the main page ..."
##cd myTESTS
##if [[ -d cont ]]
##    then
##    rm -rf cont
##fi
##./shell.sh
##cd ..
##echo "General history plots done"

#add new folder for the fractions for that run 

##cd myTESTS/fractions 
##./shell_fractions.sh $1

#fractions history plots
##cd HistoryProgram
##./plot_fract_history.sh
##cd /rpctdata/CAF
#add new folder for the distributions of the rate by strips

cd PlotsPerEachRun/RateDistrByStrips
./StripsDistrPlot.sh $1
cd /rpctdata/CAF

#echo Preparing database files ..........

./DBFileTranslationApp/translate_efficiency_and_DB_files.sh $1

# preparing the files with strip conditions with CMSSW IDs
./DBFileTranslationApp/translate_strip_conditions.sh $1
#get the files with the rates
./DBFileTranslationApp/get_average_rates_11parts.sh $1
#get the pictures for special lbs
./DBFileTranslationApp/get_pictures_for_special_LBs.sh $1

echo test 3
#cat myTESTS/header.html myTESTS/new_index.html myTESTS/tail.html > myTESTS/index.html
cat myTESTS/header_new.html myTESTS/new_index.html myTESTS/tail_new.html> myTESTS/index.html
cp myTESTS/index.html /rpctdata/CAF/DBFileTranslationApp/resources/index_special_lbs.html ~pugliese/toTransfer/TEST/
echo test 4
cp $run -rf ~pugliese/toTransfer/
cp -rf myTESTS/pics ~pugliese/toTransfer/TEST/
cp -rf myTESTS/histosOfPics  myTESTS/fractions/HistoryProgram/pictures ~pugliese/toTransfer/TEST/

echo test 5
echo moving files 

#scp -P 22222 -o NoHostAuthenticationForLocalhost=yes -r ~/toTransfer/TEST/index.html ~/toTransfer/TEST/index_special_lbs.html ~/toTransfer/TEST/pics ~/toTransfer/TEST/pictures ~/toTransfer/$run ~/toTransfer/TEST/histosOfPics pigi@localhost:/var/www/noise/

#ssh srv-C2C03-22.cms "scp -r ~/toTransfer/TEST/index.html ~/toTransfer/TEST/pics ~/toTransfer/TEST/pictures ~/toTransfer/$run ~/toTransfer/TEST/histosOfPics  pigi@cmsrpc402b20.cern.ch:/var/www/noise/"

#ssh srv-C2C03-22.cms "scp -r ~/toTransfer/TEST/index.html ~/toTransfer/TEST/pics ~/toTransfer/TEST/pictures ~/toTransfer/run1* ~/toTransfer/TEST/histosOfPics  pigi@cmsrpc402b20.cern.ch:/var/www/noise/"



# rm after scp, otherwise we will run out of disk space
rm -rf ~/toTransfer/$run

echo test 6

exit 0
