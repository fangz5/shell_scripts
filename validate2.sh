#!/bin/bash

RUN_FLAG=no
GENERATE_DATA_FLAG=yes

LOG_ID=`ls *.txt -l |wc -l`
LOG=log_$LOG_ID.txt


# These arguments need to be set.
HOME="/home/fang/Work/LevyxSpark/"
SPARK_SUBMIT=$HOME"spark-1.6.1-bin-hadoop2.6/bin/spark-submit"
SPARK_XENON_JAR=$HOME"spark-xenon/target/scala-2.10/spark-xenon_2.10-1.1.0.jar"
XENON_FLOW_ROOT=$HOME"stac-a3/"
VALIDATION_FLOW_ROOT=$HOME"stac-a3-pack-for-cloudera-intel/trunk/mean_revert/"
LOOPS=1

XENON_MASTER="local[*]"
#XENON_MASTER="spark://fang-m:7077"
#VALIDATION_MASTER="spark://fang-m:7077"
VALIDATION_MASTER="local[*]"

BOOKS_PATH=/mnt/nvme/order-books/
EXT=.csv
JOBS_PATH=/home/fang/Work/LevyxSpark/SWEEP.FIXED/
JOB_FILE=job.csv
BASKETS_PATH=/home/fang/Work/LevyxSpark/SWEEP.FIXED/
DATES_PATH=/home/fang/
DATES_YEAR=$DATES_PATH"dates_year.csv"
DATES_FILE=dates.csv
XE_RESULT_PATH=$XENON_FLOW_ROOT"result"
MEDIUM=Xenon
SYMBOLS_DIR=/home/fang/
SYMBOLS_FILE=symbols.csv

GENERATOR_DIR=$HOME/stac-a3-test-harness/trunk/input_datagen/orderGenerator
GENERATOR_SCRIPT=gen_dates_and_symbols.py
SEED=$((RANDOM))
START_PRICE=$((1000+(RANDOM%9000)))
FORMAT=v.2
TARGET=/mnt/nvme/order-books/

# Other Xenon flow arguments.
XENON_FLOW_JAR=$XENON_FLOW_ROOT"target/scala-2.10/stac-a3-dataframe_2.10-1.0.jar"
XENON_RESULT=$XE_RESULT_PATH"/result.XenonCluster"
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

# Xenon info. Needed when data is to be re-generated.
XENON=$HOME"xenon/src/xenon"
XENON_ADDRESS=localhost:41000


# Create initial properties file.
function build_local() {
  PROPERTIES_FILE="local.properties"
  PROPERTIES_FILE_BK=$XENON_FLOW_ROOT"local.properties_bk"
  if [ -d $PROPERTIES_FILE ]; then
    echo "mv $PROPERTIES_FILE $PROPERTIES_FILE_BK"
    mv $PROPERTIES_FILE $PROPERTIES_FILE_BK
  fi

  REVERT="stac.a3.meanrevert."

  echo $REVERT"input.dir="$BOOKS_PATH
  echo $REVERT"input.extension="$EXT
  echo $REVERT"job.desc.dir="$JOBS_PATH
  echo $REVERT"job.desc.file="$JOB_FILE
  echo $REVERT"basket.dir="$BASKETS_PATH
  echo $REVERT"date.dir="$DATES_PATH
  echo $REVERT"date.file="$DATES_FILE
  echo $REVERT"output.dir="$XE_RESULT_PATH
  echo $REVERT"shared.media="$MEDIUM
  echo $REVERT"symbols.dir="$SYMBOLS_DIR
  echo $REVERT"symbols.file="$SYMBOLS_FILE
  echo $REVERT"order.gen.dir="$GENERATOR_DIR
  echo $REVERT"order.gen.script="$GENERATOR_SCRIPT
  echo $REVERT"order.gen.seed="$SEED
  echo $REVERT"order.gen.start.price="$START_PRICE
  echo $REVERT"order.gen.format="$FORMAT
  echo $REVERT"order.target.local="$TARGET

  rm -f $PROPERTIES_FILE
  echo $REVERT"input.dir="$BOOKS_PATH >> $PROPERTIES_FILE
  echo $REVERT"input.extension="$EXT >> $PROPERTIES_FILE
  echo $REVERT"job.desc.dir="$JOBS_PATH >> $PROPERTIES_FILE
  echo $REVERT"job.desc.file="$JOB_FILE >> $PROPERTIES_FILE
  echo $REVERT"basket.dir="$BASKETS_PATH >> $PROPERTIES_FILE
  echo $REVERT"date.dir="$DATES_PATH >> $PROPERTIES_FILE
  echo $REVERT"date.file="$DATES_FILE >> $PROPERTIES_FILE
  echo $REVERT"output.dir="$XE_RESULT_PATH >> $PROPERTIES_FILE
  echo $REVERT"shared.media="$MEDIUM >> $PROPERTIES_FILE
  echo $REVERT"symbols.dir="$SYMBOLS_DIR  >> $PROPERTIES_FILE
  echo $REVERT"symbols.file="$SYMBOLS_FILE >> $PROPERTIES_FILE
  echo $REVERT"order.gen.dir="$GENERATOR_DIR >> $PROPERTIES_FILE
  echo $REVERT"order.gen.script="$GENERATOR_SCRIPT >> $PROPERTIES_FILE
  echo $REVERT"order.gen.seed="$SEED >> $PROPERTIES_FILE
  echo $REVERT"order.gen.start.price="$START_PRICE >> $PROPERTIES_FILE
  echo $REVERT"order.gen.format="$FORMAT >> $PROPERTIES_FILE
  echo $REVERT"order.target.local="$TARGET >> $PROPERTIES_FILE
}

echo
echo Initial properties file:
build_local
echo


SYMBOLS=$SYMBOLS_DIR$SYMBOLS_FILE

if [ "$GENERATE_DATA_FLAG" = "yes" ]; then
  # Create symbols
  SYMBOL_PAIRS=20
  #SYMBOL_PAIRS=$((2 + (RANDOM%3)))
  rm $SYMBOLS

  function random_letters(){
    echo `cat /dev/urandom | tr -dc 'A-Z' | fold -w 3 | head -n 1`
  }

  for i in `seq 1 $SYMBOL_PAIRS`; do
    H_SYMBOL=H$(random_letters)
    L_SYMBOL=L$(random_letters)

    echo $H_SYMBOL >> $SYMBOLS
    echo $L_SYMBOL >> $SYMBOLS
  done

  echo "Symbols:"
  cat $SYMBOLS
  echo

  echo >> $LOG
  cat $SYMBOLS >> $LOG


  # Build dates file (a subset of dates_year).
  BOOK_LENGTH=251
  # BOOK_LENGTH=$((20 + (RANDOM % 21)))
  TOTAL_NUM_DAYS=`less $DATES_YEAR |wc -l`
  FIRST_DAY_IDX=$((1 + (RANDOM % (TOTAL_NUM_DAYS - BOOK_LENGTH + 1))))

  rm $DATES
  for i in `seq $FIRST_DAY_IDX $((FIRST_DAY_IDX + BOOK_LENGTH - 1))`; do
    echo `sed "${i}q;d" $DATES_YEAR` >> $DATES
  done


  # Clean data
  rm -rf $BOOKS_PATH/*.csv
  $XENON --client $XENON_ADDRESS --wipe

  # Generate data
  GENERATE_CMD="$SPARK_SUBMIT
  --master local[*]
  --class com.levyx.stac.a3.meanrevert.GenerateOrderBook
  --jars $SPARK_XENON_JAR
  $XENON_FLOW_JAR"

  echo "Generating books..."
  echo $GENERATE_CMD

  eval $GENERATE_CMD

  # Load data
  LOAD_CMD="$SPARK_SUBMIT
  --master local[*]
  --class com.levyx.stac.a3.meanrevert.LoadDataSets
  --jars $SPARK_XENON_JAR
  $XENON_FLOW_JAR"

  echo "Loading datasets..."
  echo $LOAD_CMD

  eval $LOAD_CMD
fi


# Update basket
BASKET=$BASKETS_PATH"basket.csv"
cp -f $SYMBOLS $BASKET


cat $SYMBOLS > $LOG

# Main loop.
for i in `seq 1 $LOOPS`; do
  echo
  echo "LOOP $i"
  echo >> $LOG
  echo "LOOP $i" >> $LOG


  # Generate random parameters
  BOOK_LENGTH=`less $DATES |wc -l`
  NUM_DAYS=$((1 + (RANDOM%BOOK_LENGTH)))

  NUM_SIMS=10
  # NUM_SIMS=$((10 + (RANDOM%10)))


  # Build job and basket.
  PY=python
  NUM_SYMS=`less $SYMBOLS |wc -l`

  CMD="$PY ./createMeanRevertIntensiveJobs.py
    --symbol-file-path $SYMBOLS
    --dates-file-path $DATES
    --output-directory $JOBS_PATH
    --number-of-dates-per-simulation $NUM_DAYS
    --number-of-basket-symbols $NUM_SYMS
    --percentage-of-high-symbols .5
    --number-of-simulations $NUM_SIMS
    -v 0"

  eval $CMD
  JOB_FILE="job.sweep."$NUM_SIMS"sims"$NUM_SYMS"symb.csv"
  JOB=$JOBS_PATH$JOB_FILE 


  # Rebuild local properties.
  build_local


  # Prepare execution commands.
  XENON_CMD="$SPARK_SUBMIT
    --master $XENON_MASTER
    --class com.levyx.stac.a3.meanrevert.MeanRevertPnLMain
    --jars $SPARK_XENON_JAR
    $XENON_FLOW_JAR"
  echo $XENON_CMD

  VALIDATION_CMD="$SPARK_SUBMIT
    --master $VALIDATION_MASTER
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

  echo SEED=$SEED SIMS=$NUM_SIMS  DAYS=$NUM_DAYS >> $LOG

  # Run tests.
  if [ "$RUN_FLAG" = "yes" ]; then
    echo "Running xenon flow... "
    eval $XENON_CMD

    echo "Running validation flow... "
    #eval $VALIDATION_CMD
  fi

  # Compare results.
  less $XENON_RESULT | cut -d',' -f2,3 | sort > xenon.tmp 
  #less $VALIDATION_RESULT |grep -v NOSYM| cut -d',' -f2,3 | sort > validation.tmp 

  #DIFF=`diff xenon.tmp validation.tmp | wc -l`
  DIFF=0

  if [ "$DIFF" = "0" ]; then
    echo "Results are identical."
    echo "Results are identical." >> $LOG

  else
    echo "Inconsistency found!!!"
    echo $DIFF
    echo "------------------------------------------------------------------------"                     
    echo "Inconsistency found!!!" >> $LOG
    echo "Xe:"`less xenon.tmp |wc -l` >> $LOG
    echo "Va:"`less validation.tmp |wc -l` >> $LOG
    echo $DIFF >> $LOG
    echo "------------------------------------------------------------------------" >> $LOG

    exit 1

  fi

done


echo "------------------------------------------------------------------------"
echo "------------------------------------------------------------------------" >> $LOG

exit 0

~
