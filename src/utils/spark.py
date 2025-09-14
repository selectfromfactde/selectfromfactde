
from pyspark.sql import SparkSession


def get_spark(
    app_name: str = "olist-pipeline-local",
    master: str = "local[*]",
    timezone: str = "UTC",
    shuffle_partitions: int = 8,
):
    """Create a SparkSession, allowing overrides from config/paths.yaml if present.

    This lets you dial down local resources (e.g., master=local[2], shuffle=2)
    without changing code.
    """
    # Try to pick up settings from config/paths.yaml
    try:
        from src.utils.io import load_paths

        cfg = load_paths()
        app_cfg = cfg.get("app", {}) or {}
        spark_cfg = cfg.get("spark", {}) or {}
        app_name = app_cfg.get("app_name", app_name)
        master = app_cfg.get("master", master)
        timezone = spark_cfg.get("timezone", timezone)
        shuffle_partitions = int(spark_cfg.get("shuffle_partitions", shuffle_partitions))
    except Exception:
        # Fall back to defaults if config is missing
        pass

    builder = (
        SparkSession.builder.appName(app_name)
        .master(master)
        .config("spark.sql.session.timeZone", timezone)
        .config("spark.sql.shuffle.partitions", shuffle_partitions)
    )

    spark = builder.getOrCreate()
    return spark
