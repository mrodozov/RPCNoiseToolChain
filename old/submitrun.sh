#!/bin/bash

#Declare an array of the tower names , in order to check if each tower exists - mrodozov@cern.ch

array_of_towers=("RB+2_far" "RB+2_near" "RB+1_near" "RB+1_far" "RB-1_far" "RB-1_near" "RB-2_far" "RB-2_near" "RB0_far" "RB0_near" "YEN1_far" "YEN1_near" "YEN3_far" "YEN3_near" "YEP1_far"  "YEP1_near" "YEP3_far" "YEP3_near");

export project=`echo $LOCALRT | grep CMSSW | wc -l`

echo $project

if [[ $project -ne 1 ]]
then 
 echo please cmsenv before executing the macro
 exit -1
fi

export wrong_syntax=`echo $1 | grep run | wc -l`

echo $wrong_syntax

if [[ $wrong_syntax -ne 0 ]]
    then 
    echo wrong syntax - please use ./submitrun.sh xxxxxx - without the word run
    exit -1
fi

export runexist=`cat index.html | grep run$1 | wc -l`

echo $runexist

if [[ $runexist -ge 1 && $2 -ne 1 ]]
    then 
    echo The run was already analized $runexist times!
    echo if you want to continue, resubmit with 1 as second argument of the script
    exit -2
fi

#new part starts here # Please do not remove the debug lines ! they may be usefull , thank you :)

file_counter=0;

for i in `ls /rpctdata/LBMonitorHistos/ | grep _${1}_`
  do
  
  array_of_files[$file_counter]=$i;
  let file_counter++;
  
done

#echo $file_counter # its a debug line
EXIT_CODE=0; # the normal exit code
if [[ $file_counter == 0 ]]
then
    echo "No single file found for run$1 , check the runnumber"
    EXIT_CODE=-2;
    echo $EXIT_CODE;
    exit $EXIT_CODE # cause error because none single file found ,write a entry in the log
fi

counter=0;

for i in ${array_of_towers[@]}
  do
  
  #echo $i #debug line
  
  for j in ${array_of_files[@]}
    do
    if [[ `echo ${j} | grep ${i} | wc -l` > 0 ]]
	then
        let counter++; # echo "   "$i" "$j" " $counter; #debug line
    fi
    # decide what to do if the  counter is =0 or >1
    
  done
  
  # three cases 1. No file for that tower 2. More then one 3. Exactly one file
  # in 1. skip the execution , no problem , nothing happens
  # in 2. find the most recent file to be analyzed by the Noise tool
  # in 3. execute normally while loop and grep for the file corresponding to that tower (instead of grep in the dir which will take too much time)

  if [[ $counter < 1 ]]
      then
      #debug check
      echo "No file for tower $i" #debug line
      #change the exit code to -3 , at least 1 file is missing for a tower , do not upload on DB
      EXIT_CODE=-3;
      
  elif [[ $counter > 1 ]]
      then

      EXIT_CODE=-4; # exit code -4 -> more than one file for at least 1 tower , writes a log
      echo "More than one file for tower $i" #debug line
      #get with grep the most recent file to analyze with the noise tool
      # change the exit code to say to Mihee's script that there are more than 1 file per tower and not to upload on the DB - should be -4
      export most_biggest_file=`ls -S /rpctdata/LBMonitorHistos/ | grep _${1}_ | grep -m 1 $i`
      echo "The biggest is $most_biggest_file" # debug line
      export ex_code=`./CheckIfFileIsCorrupted.app /rpctdata/LBMonitorHistos/$most_biggest_file`
      if [[ $ex_code == 0 ]]
	  
	  then
	  cd code
	  ./LBNoise_RE4updated /rpctdata/LBMonitorHistos/$most_biggest_file 0
	  #./LBnoise_avg_0_005_13 /rpctdata/LBMonitorHistos/$most_biggest_file
	  cd ..
	  
      else
	  
	  EXIT_CODE=-5;
	  echo "The file $most_biggest_file is corrupted !"
	  
      fi
      
  else
      echo "Exactly 1 file for $i" #debug line
      for k in ${array_of_files[@]}
      do
        if [[ `echo ${k} | grep ${i} | wc -l` > 0 ]] #use the array instead of grep again , which will consume more time
            then
            echo "Using file $k"
	    export ex_code=`./CheckIfFileIsCorrupted.app /rpctdata/LBMonitorHistos/$k`
	    if [[ $ex_code == 0 ]]
		then
		cd code
		./LBNoise_RE4updated /rpctdata/LBMonitorHistos/$k 0
		#./LBnoise_avg_0_005_13 /rpctdata/LBMonitorHistos/$k
		cd ..
	    else
		EXIT_CODE=-5;
		echo "The file $k is corrupted !"
	    fi
        fi
      done
  fi
  #reset the counter
  counter=0;
  
done

#end of the new logic

if [[ $EXIT_CODE != -5 ]]
then

    if [[ ! -d run$1 ]]
	then
	mkdir run$1
	mv code/*${1}*.* run$1
    else
	mv code/*${1}*.* run$1
    fi
    echo $1 >> run_list_with_resubmited_runs.list
    
fi

if [[ $EXIT_CODE == -5 ]]
    then
    python SendErrorNotification.py ListOfEmailsNT.txt "NT automatic notification - corrupted files" "At least one file is corrupted for run$1, please check "
elif [[ $EXIT_CODE == -3 ]]
    then
    python SendErrorNotification.py ListOfEmailsNT.txt "NT automatic notification - missing tower files" "At least one LBHistogram tower file is missing for run$1, please check"
elif [[ $EXIT_CODE = -4 ]]
    then python SendErrorNotification.py ListOfEmailsNT.txt "NT automatic notification - more than one file for tower" "The program detected that for at least one tower there is more than 1 file for run$1" 
fi

echo "Exit code is" $EXIT_CODE
exit $EXIT_CODE
