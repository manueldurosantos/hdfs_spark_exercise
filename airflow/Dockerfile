FROM apache/airflow
USER root
RUN apt-get update
RUN apt-get upgrade
#RUN apt-get install -y libkrb5-dev
#RUN apt-get install -y build-essential libkrb5-dev
RUN apt-get install -y nano
#RUN apt-get install -y docker
RUN usermod -aG sudo airflow
USER airflow
#CMD ["dockerd"]
#RUN pip install hdfs
#RUN pip install apache-airflow-providers-apache-hdfs