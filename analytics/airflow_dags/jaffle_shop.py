import os
from airflow import DAG
from airflow.contrib.operators.ecs_operator import ECSOperator
from airflow.operators.bash_operator import BashOperator
from airflow.utils.dates import days_ago
from datetime import timedelta

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": days_ago(2),
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    "Jaffle_shop",
    default_args=default_args,
    description="Runs dbt container",
    schedule_interval=None,
    is_paused_upon_creation=False,
)



dbt_seed = ECSOperator(
    task_id="dbt_seed",
    dag=dag,
    aws_conn_id="aws_ecs",
    cluster="MyCluster",
    task_definition="jaffle_shop",
    launch_type="FARGATE",
    overrides={
        "containerOverrides": [
            {
                "name": "jaffle_shop-container",
                "command": ["dbt", "seed"],
            },
        ],
    },
    network_configuration={
        "awsvpcConfiguration": {
            "securityGroups": ["sg-06b8b78f18eb450f1"],
            "subnets": ["subnet-04c3e82ee51a5d470", "subnet-0bc5f92fe1dc3bf09"],
        },
    },
    awslogs_group="/ecs/jaffle_shop",
    awslogs_stream_prefix="ecs/jaffle_shop-container",
)


dbt_test1 = ECSOperator(
    task_id="dbt_test1",
    dag=dag,
    aws_conn_id="aws_ecs",
    cluster="MyCluster",
    task_definition="jaffle_shop",
    launch_type="FARGATE",
    overrides={
        "containerOverrides": [
            {
                "name": "jaffle_shop-container",
                "command": ["dbt", "test", "-m", "not_null_customers_customer_id"],
            },
        ],
    },
    network_configuration={
        "awsvpcConfiguration": {
            "securityGroups": ["sg-06b8b78f18eb450f1"],
            "subnets": ["subnet-04c3e82ee51a5d470", "subnet-0bc5f92fe1dc3bf09"],
        },
    },
    awslogs_group="/ecs/jaffle_shop",
    awslogs_stream_prefix="ecs/jaffle_shop-container",
)


dbt_test2 = ECSOperator(
    task_id="dbt_test2",
    dag=dag,
    aws_conn_id="aws_ecs",
    cluster="MyCluster",
    task_definition="jaffle_shop",
    launch_type="FARGATE",
    overrides={
        "containerOverrides": [
            {
                "name": "jaffle_shop-container",
                "command": ["dbt", "test", "-m", "not_null_orders_amount"],
            },
        ],
    },
    network_configuration={
        "awsvpcConfiguration": {
            "securityGroups": ["sg-06b8b78f18eb450f1"],
            "subnets": ["subnet-04c3e82ee51a5d470", "subnet-0bc5f92fe1dc3bf09"],
        },
    },
    awslogs_group="/ecs/jaffle_shop",
    awslogs_stream_prefix="ecs/jaffle_shop-container",
)


dbt_test3 = ECSOperator(
    task_id="dbt_test3",
    dag=dag,
    aws_conn_id="aws_ecs",
    cluster="MyCluster",
    task_definition="jaffle_shop",
    launch_type="FARGATE",
    overrides={
        "containerOverrides": [
            {
                "name": "jaffle_shop-container",
                "command": ["dbt", "test"],
            },
        ],
    },
    network_configuration={
        "awsvpcConfiguration": {
            "securityGroups": ["sg-06b8b78f18eb450f1"],
            "subnets": ["subnet-04c3e82ee51a5d470", "subnet-0bc5f92fe1dc3bf09"],
        },
    },
    awslogs_group="/ecs/jaffle_shop",
    awslogs_stream_prefix="ecs/jaffle_shop-container",
)



dbt_run = ECSOperator(
    task_id="dbt_run",
    dag=dag,
    aws_conn_id="aws_ecs",
    cluster="MyCluster",
    task_definition="jaffle_shop",
    launch_type="FARGATE",
    overrides={
        "containerOverrides": [
            {
                "name": "jaffle_shop-container",
                "command": ["dbt", "run"],
            },
        ],
    },
    network_configuration={
        "awsvpcConfiguration": {
            "securityGroups": ["sg-06b8b78f18eb450f1"],
            "subnets": ["subnet-04c3e82ee51a5d470", "subnet-0bc5f92fe1dc3bf09"],
        },
    },
    awslogs_group="/ecs/jaffle_shop",
    awslogs_stream_prefix="ecs/jaffle_shop-container",
)

dbt_seed >> dbt_run >> dbt_test1 >> dbt_test3
dbt_seed >> dbt_run >> dbt_test2 >> dbt_test3

