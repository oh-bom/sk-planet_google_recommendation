# Databricks notebook source
# MAGIC %md
# MAGIC 100만개 데이터로 성능 확인

# COMMAND ----------

train_df = spark.table("hive_metastore.item2item.train_100")

# COMMAND ----------

# 결측치 있는지 확인
from pyspark.sql.functions import isnan, when, count, col, isnull

display(train_df.select([count(when(isnull(c), c)).alias(c) for c in train_df.columns]))

# COMMAND ----------

# AUC, Precision, Recall, F1-score 확인하는 ver.

from pyspark.sql.functions import col, when
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.evaluation import BinaryClassificationEvaluator, MulticlassClassificationEvaluator

def train_random_forest_with_undersampling4(train_df, label_threshold, undersampling_ratio):
    # 라벨링 기준 설정
    train_df = train_df.withColumn("label", when(col("co_count") >= label_threshold, 1).otherwise(0))

    # 언더샘플링 수행
    majority_class_df = train_df.filter(col("label") == 0)
    minority_class_df = train_df.filter(col("label") == 1)
    minority_count = minority_class_df.count()
    majority_count = int(minority_count * undersampling_ratio)

    undersampled_majority_df = majority_class_df.sample(withReplacement=False, fraction=majority_count / majority_class_df.count(), seed=1234)
    train_df = undersampled_majority_df.union(minority_class_df)

    # 피처 벡터화
    columns_to_exclude = ["label", "co_count", "gmap_id1", "gmap_id2"]
    feature_columns = [c for c in train_df.columns if c not in columns_to_exclude]
    assembler = VectorAssembler(inputCols=feature_columns, outputCol="features")
    train_df = assembler.transform(train_df)

    # 전체 데이터를 train, validation 데이터로 분리 (8:2 비율)
    train_data, validation_data = train_df.randomSplit([0.8, 0.2], seed=1234)

    # RandomForest 모델 생성 (고정된 하이퍼파라미터 사용)
    rf = RandomForestClassifier(labelCol="label", featuresCol="features", numTrees=100, maxDepth=10)

    # 모델 훈련
    model = rf.fit(train_data)

    # Validation 데이터로 평가
    val_predictions = model.transform(validation_data)
    
    # AUC 평가
    evaluator = BinaryClassificationEvaluator(labelCol="label", metricName="areaUnderROC")
    val_auc = evaluator.evaluate(val_predictions)

    # Precision, Recall, F1-score 평가
    evaluator_precision = MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction", metricName="precisionByLabel")
    evaluator_recall = MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction", metricName="recallByLabel")
    evaluator_f1 = MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction", metricName="f1")
    
    precision = evaluator_precision.evaluate(val_predictions, {evaluator_precision.metricLabel: 1.0})
    recall = evaluator_recall.evaluate(val_predictions, {evaluator_recall.metricLabel: 1.0})
    f1_score = evaluator_f1.evaluate(val_predictions)

    print(f"라벨링: {label_threshold}, 샘플링: {undersampling_ratio}, Validation AUC: {val_auc}, Precision: {precision}, Recall: {recall}, F1 Score: {f1_score}")

    return model, val_auc, precision, recall, f1_score


# COMMAND ----------

# 전체 실험 결과를 표로 확인하는 ver.

import pandas as pd

# label_threshold와 undersampling_ratio의 조합을 테스트하는 함수
def experiment_sampling_labeling2(sampled_df, label_thresholds, undersampling_ratios):
    results = []
    
    for label_threshold in label_thresholds:
        for undersampling_ratio in undersampling_ratios:
            model, val_auc, precision, recall, f1_score = train_random_forest_with_undersampling4(sampled_df, label_threshold, undersampling_ratio)
            results.append({
                'label_threshold': label_threshold,
                'undersampling_ratio': undersampling_ratio,
                'Validation AUC': val_auc,
                'Precision': precision,
                'Recall': recall,
                'F1 Score': f1_score
            })
    
    results_df = pd.DataFrame(results)
    return results_df

# COMMAND ----------

# 테스트할 label_threshold와 undersampling_ratio 리스트 설정
label_thresholds = [3, 4]
undersampling_ratios = [1, 3, 5, 10]

# 샘플 데이터를 사용한 실험 수행 및 결과 출력
results_df = experiment_sampling_labeling2(train_df, label_thresholds, undersampling_ratios)
results_df
