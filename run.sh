OPTION=$1

if [ "$OPTION" = "" ]; then
  echo "Provide parameter for rush.sh to run!!"
  echo "1 : run spark-test only"
  echo "2 : compile and run spark-test"
  echo "3 : compile spark-xenon and run spark-test without compiling spark-test"
  echo "4 : compile both spark-xenon and spark-test and run"
  exit 1
fi

HOME="/home/fang/Work/LevyxSpark/"
SUBMIT="$HOME/spark-1.6.1-bin-hadoop2.6/bin/spark-submit"
SPARK_XENON="$HOME/spark-xenon/target/scala-2.10/spark-xenon_2.10-1.1.0.jar"
TEST="$HOME/spark-test/target/scala-2.10/sparktest_2.10-1.0.jar"
RUN="sudo $SUBMIT --jars $SPARK_XENON $TEST" 
SBT="/home/fang/sbt/bin/sbt"

cd $HOME/spark-test

if [ "$OPTION" = "1" ]; then
  eval $RUN
  exit 0
fi

if [ "$OPTION" = "2" ]; then
  eval "$SBT package"
  eval $RUN
  exit 0
fi

if [ "$OPTION" = "3" ]; then
  cd $HOME/spark-xenon
  eval "$SBT publish-local"
  cd $HOME/spark-test
  eval $RUN
  exit 0
fi

if [ "$OPTION" = "4" ]; then
  cd $HOME/spark-xenon
  eval "$SBT publish-local"
  cd $HOME/spark-test
  eval "$SBT package"
  eval $RUN
  exit 0
fi
