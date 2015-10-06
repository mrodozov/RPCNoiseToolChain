#!/bin/bash

export CMS_PATH=/opt/offline
export SCRAM_ARCH=slc6_amd64_gcc491
source $CMS_PATH/cmsset_default.sh

cd /opt/offline/slc6_amd64_gcc491/cms/cmssw/CMSSW_7_3_6
eval `scramv1 runtime -sh`

export ROOTSYS=/opt/offline/slc6_amd64_gcc491/lcg/root/5.34.18-jlbgio
export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:$ROOTSYS/lib
export MANPATH=$MANPATH:$ROOTSYS/man
export PATH=$PATH:$ROOTSYS/bin:~/bin

export TNS_ADMIN=/nfshome0/popcondev/conddb
export PYTHONPATH=/nfshome0/mmaggi/PYTHON
export JAVA_HOME=/nfshome0/pugliese/miheejo/jdk1.6.0_24

########## Working directory
cd /rpctdata/CAF/
#cd ~

############# Check ssh tunnels
# add tunnel for git

python checkTunnels.py

############# Get year in variable

export curryear=`date | awk '{print $6}'`
export year=`echo ${curryear:2:2}`

########## Get run range for collision runs

last_=$(tail -n1 ./next_noiseDataChain_Coll.txt)
export range=`python getNextRunRange.py Collisions$year $last_ 1 | grep 'Next range' | awk '{print $4" "$5}'`
export startRun=`echo $range | awk {'print $1'}`
export endRun=`echo $range | awk {'print $2'}`
echo "From run"$startRun "to run"$endRun "for Collisions"

########## Run noiseDataChain.py for collision runs

if [ -f noiseAuto_Collisions$year.log ]
then
  echo "DATE" >> noiseAuto_Collisions$year.log
else
  echo "DATE" > noiseAuto_Collisions$year.log
fi
date >> noiseAuto_Collisions$year.log

if [[ ! $last_ == $endRun ]]
then
    python noiseDataChain.py 1 Collisions$year $startRun $endRun 1 >> noiseAuto_Collisions$year.log 2>&1
    mv result_noiseDataChain.txt LOGS/Collisions$year\_$startRun\_$$endRun.log
fi

########## Get the latest run number                                                                                                                                    
echo $endRun > next_noiseDataChain_Coll.txt

########## Get run range for cosmic runs

last_=$(tail -n1 ./next_noiseDataChain_Cosm.txt)
export range=`python getNextRunRange.py Cosmics$year $last_ 1 | grep 'Next range' | awk '{print $4" "$5}'`
export startRun=`echo $range | awk {'print $1'}`
export endRun=`echo $range | awk {'print $2'}`
echo "From run"$startRun "to run"$endRun "for Cosmics"

########## Run noiseDataChain.py for cosmic runs

if [ -f noiseAuto_Cosmics$year.log ]
then
  echo "DATE" >> noiseAuto_Cosmics$year.log
else
  echo "DATE" > noiseAuto_Cosmics$year.log
fi
date >> noiseAuto_Cosmics$year.log

if [[ ! $last_ == $endRun ]]
then
    python noiseDataChain.py 1 Cosmics$year $startRun $endRun 1 >> noiseAuto_Cosmics$year.log 2>&1
    mv result_noiseDataChain.txt LOGS/Cosmics$year\_$startRun\_$$endRun.log
fi

########## Get the latest run number                                                                                                                                      
echo $endRun > next_noiseDataChain_Cosm.txt

########## Get run range for commissioning runs

last_=$(tail -n1 ./next_noiseDataChain_Comm.txt)
export range=`python getNextRunRange.py Commissioning$year $last_ 1 | grep 'Next range' | awk '{print $4" "$5}'`
export startRun=`echo $range | awk {'print $1'}`
export endRun=`echo $range | awk {'print $2'}`
echo "From run"$startRun "to run"$endRun "for Commisioning"

########## Run noiseDataChain.py for comm runs

if [ -f noiseAuto_Comm$year.log ]
then
  echo "DATE" >> noiseAuto_Comm$year.log
else
  echo "DATE" > noiseAuto_Comm$year.log
fi
date >> noiseAuto_Comm$year.log

if [[ ! $last_ == $endRun ]]
then
    python noiseDataChain.py 1 Commissioning$year $startRun $endRun 1 >> noiseAuto_Comm$year.log 2>&1
    mv result_noiseDataChain.txt LOGS/Comm$year\_$startRun\_$$endRun.log
fi

########## Get the latest run number

echo $endRun > next_noiseDataChain_Comm.txt

########## Sort run numbers in noise tool web page

if [[ -e index_tmp.html ]]
then
    rm index_tmp.html
fi
sort index.html > index_tmp.html
mv index_tmp.html index.html

