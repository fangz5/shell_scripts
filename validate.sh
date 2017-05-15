#!/bin/bash

RUN_FLAG="yes"

LOG=log.txt
rm -f $LOG

# These arguments need to be set.
HOME="/home/fang/Work/LevyxSpark/"
SPARK_SUBMIT=$HOME"spark-1.6.1-bin-hadoop2.6/bin/spark-submit"
SPARK_XENON_JAR=$HOME"spark-xenon/target/scala-2.10/spark-xenon_2.10-1.1.0.jar"
XENON_FLOW_ROOT=$HOME"stac-a3/"
VALIDATION_FLOW_ROOT=$HOME"stac-a3-pack-for-cloudera-intel/trunk/mean_revert/"
LOOPS=10


# Extract values from default.properties for validation program to use.
XE_CONFIG=$XENON_FLOW_ROOT"default.properties"
# ---------------------------------------
A3="stac.a3.meanrevert"
OPT="cut -d= -f2"
BOOKS_PATH=`grep $A3.input.dir $XE_CONFIG | $OPT`
EXT=`grep $A3.input.extension $XE_CONFIG | $OPT`
JOBS_PATH=`grep $A3.job.desc.dir $XE_CONFIG | $OPT`
JOB_FILE=`grep $A3.job.desc.file $XE_CONFIG | $OPT`
BASKETS_PATH=`grep $A3.basket.dir $XE_CONFIG | $OPT`
DATES_PATH=`grep $A3.date.dir $XE_CONFIG | $OPT`
DATES_FILE=`grep $A3.date.file $XE_CONFIG | $OPT`
XE_RESULT_PATH=`grep $A3.output.dir $XE_CONFIG | $OPT`


# Other Xenon flow arguments.
XENON_FLOW_JAR=$XENON_FLOW_ROOT"target/scala-2.10/stac-a3-dataframe_2.10-1.0.jar"
XENON_RESULT=$XENON_FLOW_ROOT$XE_RESULT_PATH"/result.XenonCluster"
JOB=$JOBS_PATH$JOB_FILE                                                                             
DATES=$DATES_PATH$DATES_FILE                                                                        


# Other validation flow arguments.
CLASS="com.intel.a3.MeanRevertMainPnL"
VALIDATION_JAR=$VALIDATION_FLOW_ROOT"target/scala-2.10/stac-a3_2.10-1.0.jar"
LVOLDIR=$BOOKS_PATH
HVOLDIR=$BOOKS_PATH
LVEXECS=8
HVEXECS=8
VALIDATION_RESULT=$VALIDATION_FLOW_ROOT"result"


echo
echo "------------------------------------------------------------------------"
echo "CONFIGURATION:"
echo " - BOOKS_PATH        :  \"$BOOKS_PATH\""
echo " - JOBS_PATH         :  \"$JOBS_PATH\""
echo " - JOB_FILE          :  \"$JOB_FILE\""
echo " - BASKETS_PATH      :  \"$BASKETS_PATH\""
echo " - DATES_PATH        :  \"$DATES_PATH\""
echo " - DATES_FILE        :  \"$DATES_FILE\""
echo " - XENON_RESULT      :  \"$XENON_RESULT\""
echo " - VALIDATION_RESULT :  \"$VALIDATION_RESULT\""
echo
echo >> $LOG
echo "------------------------------------------------------------------------" >> $LOG
echo "CONFIGURATION:" >> $LOG
echo " - BOOKS_PATH        :  \"$BOOKS_PATH\"" >> $LOG
echo " - JOBS_PATH         :  \"$JOBS_PATH\"" >> $LOG
echo " - JOB_FILE          :  \"$JOB_FILE\"" >> $LOG
echo " - BASKETS_PATH      :  \"$BASKETS_PATH\"" >> $LOG
echo " - DATES_PATH        :  \"$DATES_PATH\"" >> $LOG
echo " - DATES_FILE        :  \"$DATES_FILE\"" >> $LOG
echo " - XENON_RESULT      :  \"$XENON_RESULT\"" >> $LOG
echo " - VALIDATION_RESULT :  \"$VALIDATION_RESULT\"" >> $LOG
echo >> $LOG


# Prepare execution commands.
XENON_CMD="$SPARK_SUBMIT
    --master local[*]
    --jars $SPARK_XENON_JAR
    $XENON_FLOW_JAR"
echo $XENON_CMD

VALIDATION_CMD="$SPARK_SUBMIT
    --master local[*]
    --class $CLASS
    $VALIDATION_JAR
    $JOB
    $BASKETS_PATH
    $EXT
    $LVOLDIR
    $HVOLDIR
    $LVEXECS
    $HVEXECS
    $VALIDATION_RESULT"
echo
echo $VALIDATION_CMD

    
# Back up original job description, which will be replaced a random file.
cp $JOB job.bk


# Main loop.
for i in `seq 1 $LOOPS`; do
  echo
  echo "LOOP $i"
  echo >> $LOG
  echo "LOOP $i" >> $LOG

  # Build random job file.
  NUM_DAYS=$((1 + (RANDOM%99)))                                                                     
  FIRST_DAY_IDX=$(((RANDOM%150) + 1))                                                               
  LAST_DAY_IDX=$((FIRST_DAY_IDX + NUM_DAYS))                                                        
  NUM_SIMS=$((50 + (RANDOM%50)))                                                                    

  FIRST_DAY=`sed "${FIRST_DAY_IDX}q;d" $DATES | sed "s/-/\//g"`                                     
  LAST_DAY=`sed "${LAST_DAY_IDX}q;d" $DATES | sed "s/-/\//g"`                                       

  BASKET=`head -1 $JOB | cut -d"," -f3`                                                             

  echo "-------------------------------------"
  echo "Random job configuration:"
  echo " - FIRST_DAY :  \"${FIRST_DAY:: -1}\""
  echo " - LAST_DAY  :  \"${LAST_DAY:: -1}\""
  echo " - NUM_SIMS  :  \"$NUM_SIMS\""
  echo "-------------------------------------" >> $LOG
  echo "Random job configuration:" >> $LOG
  echo " - FIRST_DAY :  \"${FIRST_DAY:: -1}\"" >> $LOG
  echo " - LAST_DAY  :  \"${LAST_DAY:: -1}\"" >> $LOG
  echo " - NUM_SIMS  :  \"$NUM_SIMS\"" >> $LOG

  rm -rf job.random                                                                                
  for j in `seq 1 $NUM_SIMS`; do                                                                    
    echo "\"${FIRST_DAY:: -1}\",\"${LAST_DAY:: -1}\",$BASKET,$((60 + $j)),600" >> job.random 
  done                                                                                              

  sed "s/\"0/\"/g" job.random -i                                                                    
  sed "s/\/0/\//g" job.random -i

  mv job.random $JOB


  # Run tests.
  echo "Running xenon flow... "
  if [ "$RUN_FLAG" = "yes" ]; then
    eval $XENON_CMD
  fi

  echo "Running validation flow... "
  if [ "$RUN_FLAG" = "yes" ]; then
    eval $VALIDATION_CMD
  fi

  # Compare results.
  less $XENON_RESULT | cut -d',' -f2,3 | sort > xenon.tmp 
  less $VALIDATION_RESULT | cut -d',' -f2,3 | sort > validation.tmp 

  DIFF=`diff xenon.tmp validation.tmp | wc -l`

  if [ "$DIFF" = "0" ]; then
    echo "Results are identical."
    echo "Results are identical." >> $LOG

    rm xenon.tmp
    rm validation.tmp

  else
    echo "Inconsistency found!!!"
    echo $DIFF
    echo "------------------------------------------------------------------------"                     
    echo "Inconsistency found!!!" >> $LOG
    echo $DIFF >> $LOG
    echo "------------------------------------------------------------------------" >> $LOG

    exit 1
  fi

done


mv job.bk $JOB
echo "------------------------------------------------------------------------"
echo "------------------------------------------------------------------------" >> $LOG

exit 0

~
