# RELATIONAL
nohup spark-submit --master yarn --deploy-mode cluster --num-executors 20 --executor-cores 3 --executor-memory 10g --class com.pragsis.silkroad.spark.relational.raw.RelationalLoaderDriver silkroad-spark-0.0.1-SNAPSHOT-jar-with-dependencies.jar -i raw/relational-2/ -d silkroad_master -t silkroad_master.relational_conf_transformations --partitions 60 --file-format gz --with-header &

nohup spark-submit --master yarn --deploy-mode cluster --num-executors 8 --executor-cores 4 --executor-memory 8g --class com.pragsis.silkroad.spark.relational.raw.RelationalLoaderDriver silkroad-spark-0.0.1-SNAPSHOT-jar-with-dependencies.jar -i raw/relational/ -d silkroad_master -t silkroad_master.relational_conf_transformations --file-format gz --with-header &
