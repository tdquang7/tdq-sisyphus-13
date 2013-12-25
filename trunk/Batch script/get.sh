rm -r ../$1
bin/hadoop fs -get $1 ../
bin/hadoop fs -rmr $1
