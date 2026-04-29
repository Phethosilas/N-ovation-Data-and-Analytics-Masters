# Delta Lake Configuration Issue - Nedbank DE Challenge Base Image

## Issue Summary

The provided base image `nedbank-de-challenge/base:1.0` has Delta Lake Python package installed but is missing the required JAR files in Spark's classpath, causing pipeline failures.

## Error Details

**Error Message:**
```
java.lang.ClassNotFoundException: org.apache.spark.sql.delta.catalog.DeltaCatalog
	at java.base/java.net.URLClassLoader.findClass(URLClassLoader.java:445)
	at java.base/java.lang.ClassLoader.loadClass(ClassLoader.java:592)
```

**When it occurs:** During Spark session initialization when Delta Lake catalog configuration is applied

**Pipeline Configuration (from challenge specs):**
```yaml
spark:
  config:
    spark.sql.extensions: "io.delta.sql.DeltaSparkSessionExtension"
    spark.sql.catalog.spark_catalog: "org.apache.spark.sql.delta.catalog.DeltaCatalog"
```

## Root Cause

The base image has:
- ✅ `delta-spark==3.1.0` Python package installed (verified at `/usr/local/lib/python3.11/site-packages/delta/`)
- ❌ **Missing**: Delta Lake JAR files in Spark's classpath
- ❌ **Missing**: No JARs found in `/usr/local/lib/python3.11/site-packages/delta/jars/` directory

When Spark tries to load `org.apache.spark.sql.delta.catalog.DeltaCatalog`, it cannot find the class because the JAR containing it is not available.

## Reproduction Steps

1. Pull base image: `docker pull nedbank-de-challenge/base:1.0`
2. Run: `docker run --rm nedbank-de-challenge/base:1.0 find / -name "*delta*.jar" 2>/dev/null`
3. Result: No JAR files found
4. Try to create Spark session with Delta configs → ClassNotFoundException

## Required Fix (for Challenge Organizers)

The base image Dockerfile needs to be updated to include Delta Lake JARs. Here are two solutions:

### Solution 1: Use delta-spark's built-in configuration helper

```dockerfile
# In Dockerfile.base (before exposing to participants)
FROM python:3.11-slim-bookworm

# ... existing setup ...

RUN pip install delta-spark==3.1.0

# Configure Delta Lake JARs using delta-spark's pip_utils
RUN python3 -c "from delta import configure_spark_with_delta_pip; \
    from pyspark.sql import SparkSession; \
    builder = SparkSession.builder; \
    builder = configure_spark_with_delta_pip(builder); \
    # This downloads and configures Delta JARs"

# OR explicitly set environment variable
ENV PYSPARK_SUBMIT_ARGS='--packages io.delta:delta-core_2.12:2.4.0 pyspark-shell'
```

### Solution 2: Pre-download Delta JARs to known location

```dockerfile
# In Dockerfile.base
RUN mkdir -p /opt/delta-jars && \
    wget -P /opt/delta-jars \
    https://repo1.maven.org/maven2/io/delta/delta-core_2.12/2.4.0/delta-core_2.12-2.4.0.jar && \
    wget -P /opt/delta-jars \
    https://repo1.maven.org/maven2/io/delta/delta-storage/2.4.0/delta-storage-2.4.0.jar

ENV SPARK_EXTRA_CLASSPATH=/opt/delta-jars/*
```

### Solution 3: Modify Spark defaults

```dockerfile
# In Dockerfile.base - set Spark defaults to include Delta
RUN echo "spark.jars.packages io.delta:delta-core_2.12:2.4.0" >> \
    /usr/local/lib/python3.11/site-packages/pyspark/conf/spark-defaults.conf
```

## Workaround for Participants (Temporary)

Until the base image is fixed, participants can:

1. Change output format from `delta` to `parquet`:
   ```python
   # Instead of: df.write.format("delta").save(path)
   df.write.format("parquet").save(path)
   ```

2. Comment out Delta configs in `pipeline_config.yaml`:
   ```yaml
   # spark.sql.extensions: "io.delta.sql.DeltaSparkSessionExtension"
   # spark.sql.catalog.spark_catalog: "org.apache.spark.sql.delta.catalog.DeltaCatalog"
   ```

**Note**: This workaround allows testing pipeline logic but outputs won't be in Delta format as required by challenge specifications.

## Testing the Fix

After updating the base image, verify with:

```bash
docker run --rm nedbank-de-challenge/base:1.0 python3 -c "
from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip

builder = SparkSession.builder.master('local[1]')
builder = configure_spark_with_delta_pip(builder)
spark = builder.getOrCreate()
print('Delta Lake configured successfully')
spark.stop()
"
```

Should output: `Delta Lake configured successfully` (not ClassNotFoundException)

## Impact

- **Severity**: High - Blocks all participants from completing Stage 1 with Delta format
- **Workaround Available**: Yes (use Parquet temporarily)
- **Affects**: All participants using the base image for Delta Lake output

## Environment Details

- Base Image: `nedbank-de-challenge/base:1.0`
- Python: 3.11
- PySpark: 3.5.0
- delta-spark: 3.1.0 (installed but JARs missing)
- OS: Debian Bookworm (python:3.11-slim-bookworm base)

---

**Submitted by**: Challenge Participant
**Date**: April 29, 2026
**Contact**: [Your email]
