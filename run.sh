#!/bin/bash
###### TEMPLATE run.sh ######
###### YOU NEED TO UNCOMMENT THE FOLLOWING LINE AND INSERT YOUR OWN PARAMETERS ######

spark-submit --conf spark.driver.memory=12g --conf spark.default.parallelism=40 --conf spark.executor.cores=4 --conf spark.executor.instances=10 --class PageRank target/project_spark.jar wasb://datasets@clouddeveloper.blob.core.windows.net/iterative-processing/Graph wasbs:///pagerank-output

# For more information about tuning the Spark configurations, refer: https://spark.apache.org/docs/2.3.0/configuration.html
