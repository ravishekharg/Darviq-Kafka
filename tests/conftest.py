import os

# Must happen before pyspark ever launches the JVM gateway subprocess --
# `spark.sql.session.timeZone` alone controls how Spark FORMATS/PARSES
# timestamps at the SQL-execution layer, but the JVM's own default
# timezone (java.util.TimeZone.getDefault(), inherited from the host OS
# locale) can still leak into timestamp<->string conversions depending on
# code path. Without pinning both, these tests would assert different
# window boundaries on a machine set to IST than on a UTC CI runner --
# forcing UTC everywhere makes window/watermark math deterministic
# regardless of where the suite runs.
os.environ.setdefault("TZ", "UTC")
os.environ["JAVA_TOOL_OPTIONS"] = (os.environ.get("JAVA_TOOL_OPTIONS", "") + " -Duser.timezone=UTC").strip()

import pytest  # noqa: E402
from pyspark.sql import SparkSession  # noqa: E402


@pytest.fixture(scope="session")
def spark():
    session = (
        SparkSession.builder.master("local[2]")
        .appName("iot-telemetry-tests")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()
