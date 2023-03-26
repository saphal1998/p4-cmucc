#!/bin/bash
###### TEMPLATE run.sh ######
###### YOU NEED TO UNCOMMENT THE FOLLOWING LINE AND INSERT YOUR OWN PARAMETERS ######

spark-submit --conf spark.driver.memory=5g spark.executor.instances=5 spark.executor.cores=4 spark.default.parallelism=20 --class PageRank target/project_spark.jar target/project_spark.jar wasb://datasets@clouddeveloper.blob.core.windows.net/iterative-processing/Graph wasbs:///pagerank-output

# For more information about tuning the Spark configurations, refer: https://spark.apache.org/docs/2.3.0/configuration.html
