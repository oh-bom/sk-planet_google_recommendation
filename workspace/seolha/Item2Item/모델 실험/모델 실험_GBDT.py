# Databricks notebook source
# MAGIC %pip install mlflow 

# COMMAND ----------

import mlflow

mlflow.autolog(
log_input_examples=False,
log_model_signatures=True,
log_models=True,
disable=False,
exclusive=False,
disable_for_unsupported_versions=True,
silent=False
)

# COMMAND ----------

train_df = spark.table("hive_metastore.item2item.train_under_v3")

# COMMAND ----------

train_df.printSchema()

# COMMAND ----------

from pyspark.sql.functions import col

# 남길 컬럼 리스트
columns_to_select = [
    "gmap_id1", "gmap_id2", "co_count", "year", "quarter", "distance_in_kms",
    "before_review1", "before_review2", "before_user1", "before_user2",
    "before_co_count", "price1", "price2", "description1", "description2",
    "desc_word_cnt1", "desc_word_cnt2", "prev_year_avg_rating1", "prev_year_avg_rating2"
]

# 해당 컬럼들만 선택
selected_df = train_df.select([col(column) for column in columns_to_select])

# COMMAND ----------

from pyspark.sql.functions import col, when
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.classification import GBTClassifier
from pyspark.ml.evaluation import BinaryClassificationEvaluator, MulticlassClassificationEvaluator

def train_gbdt_with_undersampling(train_df):
    # 라벨링 기준 설정
    train_df = train_df.withColumn("label", when(col("co_count") >= 4, 1).otherwise(0))

    # 피처 벡터화
    columns_to_exclude = ["label", "co_count", "gmap_id1", "gmap_id2"]
    feature_columns = [c for c in train_df.columns if c not in columns_to_exclude]
    assembler = VectorAssembler(inputCols=feature_columns, outputCol="features")
    train_df = assembler.transform(train_df)

    # 전체 데이터를 train, validation 데이터로 분리 (8:2 비율)
    train_data, validation_data = train_df.randomSplit([0.8, 0.2], seed=1234)

    # GBDT 모델 생성 (고정된 하이퍼파라미터 사용)
    gbdt = GBTClassifier(labelCol="label", featuresCol="features", maxIter=100, maxDepth=5, stepSize=0.1)

    # 모델 훈련
    model = gbdt.fit(train_data)

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

    print(f"Validation AUC: {val_auc}, Precision: {precision}, Recall: {recall}, F1 Score: {f1_score}")

    return model, feature_columns, val_auc, precision, recall, f1_score

# COMMAND ----------

# 모델 학습 및 평가
model, feature_columns, val_auc, precision, recall, f1_score = train_gbdt_with_undersampling(selected_df)

# COMMAND ----------

import pandas as pd

def get_feature_importances(model, feature_columns):
    # 피처 중요도 추출
    feature_importances = model.featureImportances.toArray()
    
    # 중요도 데이터프레임 생성
    importance_df = pd.DataFrame({
        'Feature': feature_columns,
        'Importance': feature_importances
    }).sort_values(by='Importance', ascending=False)
    
    return importance_df

# COMMAND ----------

import matplotlib.pyplot as plt

# 피처 중요도 데이터프레임 생성
importance_df = get_feature_importances(model, feature_columns)

# 시각화
plt.figure(figsize=(10, 6))
plt.barh(importance_df['Feature'], importance_df['Importance'])
plt.xlabel('Importance')
plt.ylabel('Feature')
plt.title('Feature Importances_GBDT')
plt.gca().invert_yaxis()
plt.show()
