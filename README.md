navigate to https://github.com/puckel/docker-airflow and clone the repositiry
run docker build --rm --build-arg AIRFLOW_DEPS="datadog,dask" --build-arg PYTHON_DEPS="flask_oauthlib>=0.9" -t puckel/docker-airflow .
run docker-compose -f docker-compose-LocalExecutor.yml up -d
copy hello_world.py to your dags folder on the repositiry.
navigate to http://localhost:8080/admin/ and run your dag

-----------------------------------
change your dockerfile and add 
  && pip install boto3 \
    && pip install boto \
add aws_default and emr_default connections.

