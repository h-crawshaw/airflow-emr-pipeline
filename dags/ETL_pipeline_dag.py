from datetime import datetime, timedelta
import logging
from tracemalloc import start
from airflow.operators.dummy_operator import DummyOperator
from airflow import DAG 
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.operators.emr_create_job_flow import EmrCreateJobFlowOperator
from airflow.providers.amazon.aws.operators.emr_add_steps import EmrAddStepsOperator
from airflow.providers.amazon.aws.operators.redshift import RedshiftResumeClusterOperator
from airflow.providers.amazon.aws.sensors.redshift_cluster import RedshiftClusterSensor
from airflow.providers.amazon.aws.operators.redshift import RedshiftSQLOperator
from airflow.providers.amazon.aws.operators.s3_delete_objects import S3DeleteObjectsOperator
from airflow.providers.amazon.aws.sensors.s3_key import S3KeySensor
from airflow.providers.amazon.aws.sensors.emr_step import EmrStepSensor


from airflow.models.baseoperator import chain

from airflow.providers.amazon.aws.hooks.s3 import S3Hook
source_s3 = S3Hook(aws_conn_id='aws_default')
bucket_keys = source_s3.list_keys(
  bucket_name='2022-raw-stats',
  prefix='PUBGMatchDeathsAndStatistics/'
)

def get_key():
  latestKey = bucket_keys[-1]
  latestObject = latestKey.split("/")[-1]
  latestName = latestObject.strip(".csv")
  logging.info('Checking bucket for new files...')
  print(f"The latest file is: {latestObject}")
  return latestName

SPARK_STEPS = [
  {
    'Name': 'import_raw',
    'ActionOnFailure': 'TERMINATE_CLUSTER',
    'HadoopJarStep': {
      'Jar': 'command-runner.jar',
      'Args': [
        's3-dist-cp',
        f"--src=s3://2022-raw-stats/PUBGMatchDeathsAndStatistics/{get_key()}.csv",
        '--dest=/raw'
      ] 
    }
  },
  {
    'Name': 'format_processing',
    'ActionOnFailure': 'TERMINATE_CLUSTER',
    'HadoopJarStep': {
      'Jar': 'command-runner.jar', 
      'Args': [
        'spark-submit',
        '--deploy-mode',
        'cluster',
        '--class',
        'mySparkJob',
        's3://2022sparkjars/processing-runner.jar'  
      ]                                             
    }
  },
  {
    'Name': 'export_processed',
    'ActionOnFailure': 'TERMINATE_CLUSTER',
    'HadoopJarStep': {
      'Jar': 'command-runner.jar',
      'Args': [
        's3-dist-cp',
        "--src=/processed",
        f'--dest=s3a://aggrjobout/{get_key()}'
      ]
    }
  }
]

JOB_FLOW_OVERRIDES = {
  'Name': 'Processing_Cluster',
  'ReleaseLabel': 'emr-5.36.0',
  "Applications": [{"Name": "Hadoop"}, {"Name": "Spark"}],
  'Instances': {
    'InstanceGroups': [
      {
        'Name': 'Master node',
        'Market': 'ON_DEMAND',
        'InstanceRole': 'MASTER',
        'InstanceType': 'm5.xlarge',
        'InstanceCount': 1
      },
      {
        'Name': 'Core node',
        'Market': 'ON_DEMAND',
        'InstanceRole': 'CORE',
        'InstanceType': 'm5.xlarge',
        'InstanceCount': 2
      }
    ],
    'KeepJobFlowAliveWhenNoSteps': False,
    'TerminationProtected': False
  },
  'JobFlowRole': 'EMR_EC2_DefaultRole',
  'ServiceRole': 'EMR_DefaultRole',
  'LogUri': 's3://2022-sparkjoblogs'
}

default_args = {
  'owner': 'Harry',
  'retries': 3,
  'retry_delay': timedelta(minutes=1)
}

with DAG(
  dag_id='ETL_pipeline_dag',
  default_args=default_args,
  schedule_interval='@once',
  start_date=datetime(2022, 7, 19),
  catchup=False
) as dag:

  starting_dummy = DummyOperator(task_id='Start')
  ending_dummy = DummyOperator(task_id='Finished')

  get_keys = PythonOperator(
    task_id='get_object_key',
    python_callable=get_key
  )
  
  create_emr = EmrCreateJobFlowOperator(
    task_id='create_emr_cluster',
    job_flow_overrides=JOB_FLOW_OVERRIDES,
    aws_conn_id='aws_default',
    emr_conn_id='emr_default'  
  )

  add_steps = EmrAddStepsOperator(
    task_id='add_steps',
    job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster', key='return_value') }}",
    aws_conn_id='aws_default',
    steps=SPARK_STEPS,
  )

  final_step = len(SPARK_STEPS) - 1
  step_checker = EmrStepSensor(
    task_id='check_step',
    job_flow_id="{{ task_instance.xcom_pull('create_emr_cluster', key='return_value') }}",
    step_id="{{ task_instance.xcom_pull(task_ids='add_steps', key='return_value')[" + str(final_step) + "] }}",
    aws_conn_id="aws_default",
    poke_interval=10,
    timeout=60 * 30,
  )

  resume_rs = RedshiftResumeClusterOperator(
      task_id='resume_redshift',
      cluster_identifier='rs-dev-1',
      aws_conn_id='aws_default'
  )

  check_rs = RedshiftClusterSensor(
    task_id='check_redshift',
    cluster_identifier='rs-dev-1',
    target_status='available',
    poke_interval=10,
    timeout=60 * 30,
  )

  check_s3 = S3KeySensor(
    task_id='check_S3_out',
    bucket_name='aggrjobout',
    bucket_key=f'{get_key()}/_SUCCESS'
  )


  crc_delete = S3DeleteObjectsOperator(
        task_id='delete_crc_file',
        bucket='aggrjobout',
        keys=f'{get_key()}/_SUCCESS',
        aws_conn_id='aws_default'
  )

  rs_copy = RedshiftSQLOperator(
    task_id="copy_to_redshift",
    sql=f"""
        COPY agg_stats
        FROM 's3://aggrjobout/{get_key()}/'
        IAM_ROLE 'arn:aws:iam::468540957817:role/RedshiftCopyUnload'
        ACCEPTINVCHARS
        FORMAT AS PARQUET;
        """
  )

chain(starting_dummy, get_keys, create_emr, [add_steps, resume_rs], [step_checker, check_rs], 
check_s3, crc_delete, rs_copy, ending_dummy)

