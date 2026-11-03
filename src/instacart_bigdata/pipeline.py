"""Run ingestion, quality checks, analytics, training and scoring with Spark 3.5."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from pyspark.ml import Pipeline, PipelineModel
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.functions import vector_to_array
from pyspark.sql import DataFrame, SparkSession, Window, functions as F, types as T


SCHEMAS = {
    "orders": T.StructType([
        T.StructField("order_id", T.IntegerType()),
        T.StructField("user_id", T.IntegerType()),
        T.StructField("eval_set", T.StringType()),
        T.StructField("order_number", T.IntegerType()),
        T.StructField("order_dow", T.IntegerType()),
        T.StructField("order_hour_of_day", T.IntegerType()),
        T.StructField("days_since_prior_order", T.DoubleType()),
    ]),
    "order_products__prior": T.StructType([
        T.StructField("order_id", T.IntegerType()),
        T.StructField("product_id", T.IntegerType()),
        T.StructField("add_to_cart_order", T.IntegerType()),
        T.StructField("reordered", T.IntegerType()),
    ]),
    "order_products__train": T.StructType([
        T.StructField("order_id", T.IntegerType()),
        T.StructField("product_id", T.IntegerType()),
        T.StructField("add_to_cart_order", T.IntegerType()),
        T.StructField("reordered", T.IntegerType()),
    ]),
    "products": T.StructType([
        T.StructField("product_id", T.IntegerType()),
        T.StructField("product_name", T.StringType()),
        T.StructField("aisle_id", T.IntegerType()),
        T.StructField("department_id", T.IntegerType()),
    ]),
    "aisles": T.StructType([
        T.StructField("aisle_id", T.IntegerType()),
        T.StructField("aisle", T.StringType()),
    ]),
    "departments": T.StructType([
        T.StructField("department_id", T.IntegerType()),
        T.StructField("department", T.StringType()),
    ]),
}

FEATURES = [
    "up_orders", "up_reorders", "up_mean_cart_position", "up_first_order",
    "up_last_order", "up_order_rate", "up_orders_since_last",
    "user_prior_orders", "user_items", "user_distinct_products",
    "user_reorder_rate", "product_orders", "product_reorder_rate",
    "order_dow", "order_hour_of_day", "days_since_prior_order",
]


def spark_session() -> SparkSession:
    spark = (
        SparkSession.builder.appName("InstacartBigData")
        .config("spark.sql.adaptive.enabled", "true")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def read_raw(spark: SparkSession, root: str, name: str) -> DataFrame:
    return (spark.read.option("header", "true").option("mode", "FAILFAST")
            .option("escape", '"')
            .schema(SCHEMAS[name]).csv(f"{root.rstrip('/')}/{name}.csv"))


def read_stage(spark: SparkSession, root: str, name: str) -> DataFrame:
    return spark.read.parquet(f"{root.rstrip('/')}/{name}")


def write_csv(frame: DataFrame, root: str, name: str) -> None:
    (frame.coalesce(1).write.mode("overwrite").option("header", "true")
     .csv(f"{root.rstrip('/')}/{name}"))


def prepare(spark: SparkSession, raw: str, stage: str, sample_mod: int) -> None:
    if sample_mod < 1:
        raise ValueError("--sample-mod must be >= 1")
    orders = read_raw(spark, raw, "orders")
    if sample_mod > 1:
        orders = orders.where(F.pmod("user_id", F.lit(sample_mod)) == 0)
    prior_ids = orders.where(F.col("eval_set") == "prior").select("order_id")
    train_ids = orders.where(F.col("eval_set") == "train").select("order_id")
    frames = {
        "orders": orders,
        "prior": read_raw(spark, raw, "order_products__prior").join(prior_ids, "order_id"),
        "train": read_raw(spark, raw, "order_products__train").join(train_ids, "order_id"),
        "products": read_raw(spark, raw, "products"),
        "aisles": read_raw(spark, raw, "aisles"),
        "departments": read_raw(spark, raw, "departments"),
    }
    for name, frame in frames.items():
        path = f"{stage.rstrip('/')}/{name}"
        frame.write.mode("overwrite").parquet(path)
        print(f"Wrote {name}: {path}", flush=True)


def quality(spark: SparkSession, stage: str, output: str) -> None:
    orders = read_stage(spark, stage, "orders")
    prior = read_stage(spark, stage, "prior")
    train = read_stage(spark, stage, "train")
    products = read_stage(spark, stage, "products")
    report = [
        ("orders", "rows", orders.count()),
        ("prior_items", "rows", prior.count()),
        ("train_items", "rows", train.count()),
        ("products", "rows", products.count()),
        ("orders", "duplicate_order_id", orders.count() - orders.select("order_id").distinct().count()),
        ("products", "duplicate_product_id", products.count() - products.select("product_id").distinct().count()),
        ("prior_items", "orphan_order_id", prior.join(orders, "order_id", "left_anti").count()),
        ("train_items", "orphan_order_id", train.join(orders, "order_id", "left_anti").count()),
        ("prior_items", "orphan_product_id", prior.join(products, "product_id", "left_anti").count()),
        ("orders", "invalid_hour", orders.where(~F.col("order_hour_of_day").between(0, 23)).count()),
        ("orders", "invalid_dow", orders.where(~F.col("order_dow").between(0, 6)).count()),
        ("prior_items", "invalid_reordered", prior.where(~F.col("reordered").isin(0, 1)).count()),
    ]
    schema = "table string, check string, value long"
    write_csv(spark.createDataFrame(report, schema), output, "quality")
    print(json.dumps({f"{table}.{check}": value for table, check, value in report}), flush=True)


def analytics(spark: SparkSession, stage: str, output: str) -> None:
    orders = read_stage(spark, stage, "orders")
    prior = read_stage(spark, stage, "prior")
    products = read_stage(spark, stage, "products")
    departments = read_stage(spark, stage, "departments")
    prior_orders = orders.where(F.col("eval_set") == "prior")
    enriched = prior.join(products, "product_id")

    overview = spark.createDataFrame([
        ("users", orders.select("user_id").distinct().count()),
        ("orders", orders.count()),
        ("prior_order_items", prior.count()),
        ("products", products.count()),
    ], "metric string, value long")
    write_csv(overview, output, "overview")

    top_products = (enriched.groupBy("product_id", "product_name")
                    .agg(F.count("*").alias("purchase_count"),
                         F.avg("reordered").alias("reorder_rate"))
                    .orderBy(F.desc("purchase_count")).limit(20))
    write_csv(top_products, output, "top_products")

    by_hour = (prior_orders.groupBy("order_hour_of_day")
               .agg(F.count("*").alias("orders")).orderBy("order_hour_of_day"))
    write_csv(by_hour, output, "orders_by_hour")

    by_dow = (prior_orders.groupBy("order_dow")
              .agg(F.count("*").alias("orders")).orderBy("order_dow"))
    write_csv(by_dow, output, "orders_by_dow")

    by_department = (enriched.join(departments, "department_id")
                     .groupBy("department_id", "department")
                     .agg(F.count("*").alias("purchase_count"),
                          F.avg("reordered").alias("reorder_rate"))
                     .orderBy(F.desc("purchase_count")))
    write_csv(by_department, output, "departments")

    basket_size = prior.groupBy("order_id").agg(F.count("*").alias("basket_size"))
    basket_distribution = (basket_size.groupBy("basket_size")
                           .agg(F.count("*").alias("orders"))
                           .orderBy("basket_size"))
    write_csv(basket_distribution, output, "basket_size")


def candidate_features(spark: SparkSession, stage: str, target_set: str) -> DataFrame:
    """Features use prior orders only; train/test order metadata is known at scoring time."""
    orders = read_stage(spark, stage, "orders")
    prior = read_stage(spark, stage, "prior")
    prior_orders = orders.where(F.col("eval_set") == "prior")
    target = (orders.where(F.col("eval_set") == target_set)
              .select("user_id", F.col("order_id").alias("target_order_id"),
                      F.col("order_number").alias("target_order_number"),
                      "order_dow", "order_hour_of_day", "days_since_prior_order"))
    all_history = prior.join(prior_orders.select("order_id", "user_id", "order_number"), "order_id")
    history = all_history.join(target.select("user_id"), "user_id")
    up = (history.groupBy("user_id", "product_id")
          .agg(F.count("*").alias("up_orders"),
               F.sum("reordered").alias("up_reorders"),
               F.avg("add_to_cart_order").alias("up_mean_cart_position"),
               F.min("order_number").alias("up_first_order"),
               F.max("order_number").alias("up_last_order")))
    user = (history.groupBy("user_id")
            .agg(F.max("order_number").alias("user_prior_orders"),
                 F.count("*").alias("user_items"),
                 F.countDistinct("product_id").alias("user_distinct_products"),
                 F.avg("reordered").alias("user_reorder_rate")))
    product = (all_history.groupBy("product_id")
               .agg(F.count("*").alias("product_orders"),
                    F.avg("reordered").alias("product_reorder_rate")))
    result = (up.join(user, "user_id").join(product, "product_id")
              .join(target, "user_id")
              .withColumn("up_order_rate", F.col("up_orders") / F.col("user_prior_orders"))
              .withColumn("up_orders_since_last", F.col("target_order_number") - F.col("up_last_order"))
              .fillna({"days_since_prior_order": 0.0}))
    return result


def labelled_candidates(spark: SparkSession, stage: str) -> DataFrame:
    candidates = candidate_features(spark, stage, "train")
    positives = (read_stage(spark, stage, "train")
                 .select(F.col("order_id").alias("target_order_id"), "product_id")
                 .withColumn("label", F.lit(1.0)))
    return (candidates.join(positives, ["target_order_id", "product_id"], "left")
            .fillna({"label": 0.0}))


def ranking_metrics(frame: DataFrame, score_column: str, k: int = 10) -> dict[str, float]:
    order = Window.partitionBy("user_id").orderBy(F.desc(score_column), F.asc("product_id"))
    ranked = frame.withColumn("rank", F.row_number().over(order))
    per_user = (ranked.groupBy("user_id")
                .agg(F.sum("label").alias("positives"),
                     F.count("*").alias("candidates"),
                     F.sum(F.when(F.col("rank") <= k, F.col("label")).otherwise(0)).alias("hits")))
    metrics = (per_user.select(
        F.avg(F.col("hits") / F.least(F.col("candidates"), F.lit(k))).alias("precision_at_10"),
        F.avg(F.when(F.col("positives") > 0, F.col("hits") / F.col("positives"))).alias("recall_at_10"),
        F.avg(F.when(F.col("hits") > 0, 1.0).otherwise(0.0)).alias("hit_rate_at_10"),
        F.count("*").alias("users"),
    ).first())
    return {name: float(metrics[name] or 0) for name in metrics.__fields__}


def train_model(spark: SparkSession, stage: str, model_path: str, metrics_path: Path) -> None:
    candidates = labelled_candidates(spark, stage).persist()
    user_fold = F.pmod(F.xxhash64("user_id"), F.lit(5))
    train = candidates.where(user_fold != 0)
    valid = candidates.where(user_fold == 0)
    if not train.take(1) or not valid.take(1):
        raise RuntimeError("Need both train and validation users; choose a larger sample")
    pipeline = Pipeline(stages=[
        VectorAssembler(inputCols=FEATURES, outputCol="features", handleInvalid="error"),
        LogisticRegression(maxIter=30, regParam=0.01, elasticNetParam=0.0,
                           standardization=True, featuresCol="features", labelCol="label"),
    ])
    model = pipeline.fit(train)
    model.write().overwrite().save(model_path)
    predictions = (model.transform(valid)
                   .withColumn("score", vector_to_array("probability")[1]).persist())
    evaluator = BinaryClassificationEvaluator(labelCol="label", rawPredictionCol="rawPrediction")
    valid_order_ids = (read_stage(spark, stage, "orders")
                       .where(F.col("eval_set") == "train")
                       .where(F.pmod(F.xxhash64("user_id"), F.lit(5)) == 0)
                       .select("order_id"))
    target_items = (read_stage(spark, stage, "train").join(valid_order_ids, "order_id")
                    .agg(F.count("*").alias("items"),
                         F.sum("reordered").alias("repeat_items")).first())
    matched_repeats = valid.where(F.col("label") == 1.0).count()
    if matched_repeats != target_items["repeat_items"]:
        raise RuntimeError("Positive candidate count differs from repeat items in target orders")
    result = {
        "execution_master": spark.sparkContext.master,
        "stage": stage,
        "validation": "pmod(xxhash64(user_id), 5) == 0 (disjoint users)",
        "model": "Spark ML logistic regression",
        "features": FEATURES,
        "candidate_definition": "products purchased by user in prior orders",
        "positive_rate": float(valid.agg(F.avg("label")).first()[0]),
        "candidate_rows": valid.count(),
        "validation_target_items": int(target_items["items"]),
        "repeat_item_coverage": float(target_items["repeat_items"] / target_items["items"]),
        "area_under_pr": evaluator.setMetricName("areaUnderPR").evaluate(predictions),
        "area_under_roc": evaluator.setMetricName("areaUnderROC").evaluate(predictions),
        "model_top10": ranking_metrics(predictions, "score"),
        "frequency_baseline_top10": ranking_metrics(predictions, "up_orders"),
    }
    metrics_path.parent.mkdir(parents=True, exist_ok=True)
    metrics_path.write_text(json.dumps(result, indent=2), encoding="utf-8")
    print(json.dumps(result, indent=2), flush=True)
    predictions.unpersist()
    candidates.unpersist()


def score_test(spark: SparkSession, stage: str, model_path: str, output: str) -> None:
    candidates = candidate_features(spark, stage, "test")
    model = PipelineModel.load(model_path)
    scored = (model.transform(candidates)
              .withColumn("score", vector_to_array("probability")[1]))
    rank = Window.partitionBy("user_id").orderBy(F.desc("score"), F.asc("product_id"))
    top = scored.withColumn("rank", F.row_number().over(rank)).where(F.col("rank") <= 10)
    names = read_stage(spark, stage, "products").select("product_id", "product_name")
    write_csv(top.join(names, "product_id").select(
        "user_id", "target_order_id", "rank", "product_id", "product_name", "score"
    ).orderBy("user_id", "rank"), output, "recommendations")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    p = sub.add_parser("prepare")
    p.add_argument("--raw", required=True)
    p.add_argument("--stage", required=True)
    p.add_argument("--sample-mod", type=int, default=1,
                   help="Keep users with user_id %% sample_mod == 0; 1 means full dataset")
    for name in ("quality", "analytics"):
        p = sub.add_parser(name)
        p.add_argument("--stage", required=True)
        p.add_argument("--output", required=True)
    p = sub.add_parser("train")
    p.add_argument("--stage", required=True)
    p.add_argument("--model", required=True)
    p.add_argument("--metrics", type=Path, required=True)
    p = sub.add_parser("score")
    p.add_argument("--stage", required=True)
    p.add_argument("--model", required=True)
    p.add_argument("--output", required=True)
    args = parser.parse_args()
    spark = spark_session()
    try:
        if args.command == "prepare":
            prepare(spark, args.raw, args.stage, args.sample_mod)
        elif args.command == "quality":
            quality(spark, args.stage, args.output)
        elif args.command == "analytics":
            analytics(spark, args.stage, args.output)
        elif args.command == "train":
            train_model(spark, args.stage, args.model, args.metrics)
        elif args.command == "score":
            score_test(spark, args.stage, args.model, args.output)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
