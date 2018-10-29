# -*- coding: utf-8 -*-
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

from datetime import timedelta

import airflow
from airflow import DAG
from airflow.contrib.operators.emr_create_job_flow_operator \
    import EmrCreateJobFlowOperator
from airflow.contrib.operators.emr_add_steps_operator \
    import EmrAddStepsOperator
from airflow.contrib.sensors.emr_step_sensor import EmrStepSensor
from airflow.contrib.operators.emr_terminate_job_flow_operator \
    import EmrTerminateJobFlowOperator

DEFAULT_ARGS = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': airflow.utils.dates.days_ago(2),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False
}

SPARK_TEST_STEPS = [
    {
        'Name': 'calculate_pi',
        'ActionOnFailure': 'CONTINUE',
        'HadoopJarStep': {
            'Jar': 'command-runner.jar',
            'Args': [
                '/usr/lib/spark/bin/run-example',
                'SparkPi',
                '10'
            ]
        }
    }
]

def generateInstances():
    return [
        {
            'Name': 'Master',
            'Market': 'ON_DEMAND',
            'InstanceRole': 'MASTER',
            'InstanceType': 'm4.large',
            'InstanceCount': 1,
            'EbsConfiguration': {
                'EbsBlockDeviceConfigs': [
                    {
                        'VolumeSpecification': {
                            'VolumeType': 'gp2',
                            'SizeInGB': 100
                        },
                        'VolumesPerInstance': 1
                    },
                ],
                'EbsOptimized': True
            }
        },
        {
            'Name': 'Core',
            'Market': 'ON_DEMAND',
            'InstanceRole': 'CORE',
            'InstanceType': 'i3.4xlarge',
            'InstanceCount': 1
        }
    ]

JOB_FLOW_OVERRIDES = {
    'Name': 'PiCalc',
    'KeepJobFlowAliveWhenNoSteps': True,
    'LogUri': "s3://xl8sync-assets-alpha/emr/logs/",
    'ReleaseLabel': 'emr-5.12.1',
    'Applications':[],
    'JobFlowRole': "nsync-parser-emr",
    'ServiceRole': "nsync-parser-emr",
    'Instances': {
        'InstanceGroups': generateInstances(),
        'Ec2KeyName': 'bdi-alpha',
        'KeepJobFlowAliveWhenNoSteps': True,
        'TerminationProtected': False,
        'Ec2SubnetId': 'subnet-e60320da',
        'EmrManagedMasterSecurityGroup': 'sg-4d497b05',
        'EmrManagedSlaveSecurityGroup': 'sg-4d497b05',
        'AdditionalMasterSecurityGroups': [
            'sg-db873aa5','sg-e8cdde9d'
        ],
        'AdditionalSlaveSecurityGroups': [
            'sg-db873aa5','sg-e8cdde9d'
        ]
    },
    'VisibleToAllUsers':True,
    'Tags': [{
        'Key': 'Environment',
               'Value': 'Development'
        },
        {
            'Key': 'Owner',
            'Value': 'BDI'
        }
    ]
}

dag = DAG(
    'emr_job_flow_manual_steps_dag',
    default_args=DEFAULT_ARGS,
    dagrun_timeout=timedelta(hours=2),
    schedule_interval=None
)

cluster_creator = EmrCreateJobFlowOperator(
    task_id='create_job_flow',
    job_flow_overrides=JOB_FLOW_OVERRIDES,
    aws_conn_id='aws_default',
    emr_conn_id='emr_default',
    dag=dag
)

step_adder = EmrAddStepsOperator(
    task_id='add_steps',
    job_flow_id="{{ task_instance.xcom_pull('create_job_flow', key='return_value') }}",
    aws_conn_id='aws_default',
    steps=SPARK_TEST_STEPS,
    dag=dag
)

step_checker = EmrStepSensor(
    task_id='watch_step',
    job_flow_id="{{ task_instance.xcom_pull('create_job_flow', key='return_value') }}",
    step_id="{{ task_instance.xcom_pull('add_steps', key='return_value')[0] }}",
    aws_conn_id='aws_default',
    dag=dag
)

cluster_remover = EmrTerminateJobFlowOperator(
    task_id='remove_cluster',
    job_flow_id="{{ task_instance.xcom_pull('create_job_flow', key='return_value') }}",
    aws_conn_id='aws_default',
    dag=dag
)

cluster_creator.set_downstream(step_adder)
step_adder.set_downstream(step_checker)
step_checker.set_downstream(cluster_remover)
