#!/usr/bin/env bash
# RELATIONAL
nohup spark-submit --master yarn --deploy-mode cluster --num-executors 20 --executor-cores 3 --executor-memory 10g --class com.pragsis.silkroad.spark.relational.raw.RelationalLoaderDriver silkroad-spark-0.0.1-SNAPSHOT-jar-with-dependencies.jar -i raw/relational-2/ -d silkroad_master -t silkroad_master.relational_conf_transformations --partitions 60 --file-format gz --with-header &

nohup spark-submit --master yarn --deploy-mode cluster --num-executors 8 --executor-cores 4 --executor-memory 8g --class com.pragsis.silkroad.spark.relational.raw.RelationalLoaderDriver silkroad-spark-0.0.1-SNAPSHOT-jar-with-dependencies.jar -i raw/relational/ -d silkroad_master -t silkroad_master.relational_conf_transformations --file-format gz --with-header &

# TICKETS
nohup spark-submit --master yarn --deploy-mode cluster --num-executors 35 --executor-cores 4 --executor-memory 8g  --class com.pragsis.silkroad.spark.t.tickets.TicketsLoaderDriver silkroad-spark-0.0.1-SNAPSHOT-jar-with-dependencies.jar -i raw/t-sample/ -d silkroad_master -b silkroad_master.t_conf_rowbuilders -t silkroad_master.t_conf_transformations --file-format tar.gz &

nohup spark-submit --master yarn --deploy-mode cluster --num-executors 22 --executor-cores 4 --executor-memory 15g  --class com.pragsis.silkroad.spark.t.tickets.TicketsLoaderDriver silkroad-spark-0.0.1-SNAPSHOT-jar-with-dependencies.jar -i raw/t/ -d silkroad_master -b silkroad_master.t_conf_rowbuilders -t silkroad_master.t_conf_transformations --file-format tar.gz &

# MOVI
nohup spark-submit --master yarn --deploy-mode cluster --num-executors 30 --executor-cores 4 --executor-memory 8g --class com.pragsis.silkroad.spark.t.movi.MoviLoaderDriver silkroad-spark-0.0.1-SNAPSHOT-jar-with-dependencies.jar -i raw/t-sample/ -d silkroad_master -f silkroad_master.movi_conf_fields -t silkroad_master.movi_conf_transformations --file-format tar.gz &

nohup spark-submit --master yarn --deploy-mode cluster --num-executors 22 --executor-cores 4 --executor-memory 15g --class com.pragsis.silkroad.spark.t.movi.MoviLoaderDriver silkroad-spark-0.0.1-SNAPSHOT-jar-with-dependencies.jar -i raw/t/ -d silkroad_master -f silkroad_master.movi_conf_fields -t silkroad_master.movi_conf_transformations --file-format tar.gz &

# SEGMENTACION CLIENTES
nohup spark-submit --master yarn --deploy-mode cluster --num-executors 2 --executor-cores 1 --executor-memory 2g --class com.pragsis.silkroad.spark.relational.raw.RelationalLoaderDriver silkroad-spark-0.0.1-SNAPSHOT-jar-with-dependencies.jar -i raw/segmentacion_clientes -d silkroad_master -t silkroad_master.fichaneg_conf_transformations --file-format text --with-header &

# FICHA NEGOCIACIÓN
nohup spark-submit --master yarn --deploy-mode cluster --num-executors 5 --executor-cores 4 --executor-memory 8g --class com.pragsis.silkroad.spark.relational.raw.RelationalLoaderDriver silkroad-spark-0.0.1-SNAPSHOT-jar-with-dependencies.jar -i raw/ficha_negociacion -d silkroad_master -t silkroad_master.fichaneg_conf_transformations --file-format gz --with-header &
