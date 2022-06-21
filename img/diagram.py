from diagrams import Cluster, Diagram
from diagrams.aws.database import RDSPostgresqlInstance
from diagrams.aws.storage import S3
from diagrams.onprem.workflow import Airflow
from diagrams.aws.analytics import EMRCluster, Analytics
from diagrams.onprem.analytics import Spark
from diagrams.aws.engagement import SimpleEmailServiceSesEmail


with Diagram("ETL Processing on AWS", show=False):
    source = S3("Landing Bucket")

    with Cluster("Data Pipeline Workflow"):
        with Cluster("EMR"):
            spark_cluster = EMRCluster("Spark Cluster")
            spark = Spark()

        dw = RDSPostgresqlInstance("Postgis DB")

        airflow = Airflow("Apache Airflow")

    notification = SimpleEmailServiceSesEmail("Notification")

    reports = Analytics("analytics")

    source >> airflow
    source << airflow
    spark_cluster >> dw
    dw >> reports
    dw >> notification
    airflow >> spark_cluster
    airflow << spark_cluster
    airflow >> notification
