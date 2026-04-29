FROM nedbank-de-challenge/base:1.0

# Fix Spark paths
ENV SPARK_HOME=/usr/local/lib/python3.11/site-packages/pyspark
ENV PATH="${SPARK_HOME}/bin:${PATH}"

# Fix for network=none - Spark needs this to avoid hostname resolution  
ENV SPARK_LOCAL_HOSTNAME=localhost
ENV SPARK_LOCAL_IP=127.0.0.1

# Add Delta Lake JARs to Spark classpath
ENV SPARK_EXTRA_CLASSPATH=/usr/local/lib/python3.11/site-packages/delta/jars/*

WORKDIR /app

# Copy pipeline code
COPY pipeline/ /app/pipeline/
COPY config/ /app/config/

# Set Python path
ENV PYTHONPATH=/app:${PYTHONPATH}

# Run the pipeline
CMD ["python", "-m", "pipeline.run_all"]