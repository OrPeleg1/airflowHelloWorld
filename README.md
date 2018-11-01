1)navigate to https://github.com/puckel/docker-airflow and clone the repositiry
2)run docker build --rm --build-arg AIRFLOW_DEPS="datadog,dask" --build-arg PYTHON_DEPS="flask_oauthlib>=0.9" -t puckel/docker-airflow .
3)run docker-compose -f docker-compose-LocalExecutor.yml up -d
4)copy hello_world.py to your dags folder on the repositiry.
5)navigate to http://localhost:8080/admin/ and run your dag

-----------------------------------
1)change your dockerfile and add 
  && pip install boto3 \
    && pip install boto \
2)add aws_default and emr_default connections like shown in the files attached(leave the extra empty on emr_default,we are going to overwrite it).
3)fill the secret key and access key for aws dev account
4)copy hello_world.py to your dags folder on the repositiry.
5) change the cluster name under JOB_FLOW_OVERRIDES(change the name 'PiCalc' to your name)
45)navigate to http://localhost:8080/admin/ and run your dag

