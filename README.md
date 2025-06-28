
Spark local mode
python spark_job/generate_report_local.py \
 --input_green=data/pq/green/2024/*/ \
 --input_yellow=data/pq/yellow/2024/*/* \
 --output=data/report/2024



Standalone Spark Cluster
Use spark-submit for running the script on the cluster

URL="spark://de-vm.asia-south1-c.c.velvety-tangent-463717-h8.internal:7077"

spark-submit \
    --master="${URL}" \
    spark_job/generate_report_standalone_cluster.py \
    --input_green=data/pq/green/2024/*/ \
    --input_yellow=data/pq/yellow/2024/*/* \
    --output=data/report/2024




submitting job through dataproc cluster
here we are getting data from GCS, processing it and storing in GCS

using web UI
spark-submit \
    --master="${URL}" \
    spark_job_generate_report.py \
    --input_green=gs://data_lake_de_bucket/pq/green/2024/*/ \
    --input_yellow=gs://data_lake_de_bucket/pq/yellow/2024/*/* \
    --output=gs://data_lake_de_bucket/report/2024

using rest api


Using google cloud sdk 
gcloud dataproc jobs submit pyspark \
    --cluster=de-cluster \
    --region=asia-south1 \
    gs://data_lake_de_bucket/spark_job_generate_report.py \
    -- \
    --input_green=gs://data_lake_de_bucket/pq/green/2024/* \
    --input_yellow=gs://data_lake_de_bucket/pq/yellow/2024/* \
    --output=gs://data_lake_de_bucket/report-2024


writing to bigquery - we just need to change the output
Using google cloud sdk 
gcloud dataproc jobs submit pyspark \
     --cluster=de-cluster \
     --region=asia-south1 \
     gs://data_lake_de_bucket/spark_job_generate_report_big_query.py \
     -- \
     --input_green=gs://data_lake_de_bucket/pq/green/2024/* \
     --input_yellow=gs://data_lake_de_bucket/pq/yellow/2024/* \
     --output=trips_data_all.report-2024