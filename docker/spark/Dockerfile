FROM apache/spark:4.0.3-python3

# benchmark.py needs to talk to ClickHouse from inside a Spark executor/driver
# to do a fair, same-process timing comparison; the base image only ships
# bare PySpark. Installed as root since the image's `spark` user has no
# writable home directory for pip's cache.
USER root
RUN pip install --no-cache-dir clickhouse-connect==0.7.19
USER spark
