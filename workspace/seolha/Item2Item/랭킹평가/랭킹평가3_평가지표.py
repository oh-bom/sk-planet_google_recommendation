# Databricks notebook source
# gmap_id1, gmap_id2, prob값이 있는 데이터 불러오기
prediction_df = spark.table("hive_metastore.item2item.gbdt_prediction_rank")

# COMMAND ----------

# 정답지 불러오기
label_df = spark.table("hive_metastore.item2item.test_ground_truth_rank")

# COMMAND ----------

# 랭킹평가 함수
def evaluation_function(prediction, label_df, col_1, col_2, col_rt, col_pre, reco_k, modelname, relevancy_method,  threshold):
    start = dt.datetime.now()
    
    prediction.createOrReplaceTempView("prediction")  
    
    spark.sql("""
        select count(distinct {0}) col_1_cnts, count(distinct {1}) col_2_cnts, count(*) cnts
        from prediction
    """.format(col_1, col_2)).show()
    
    label_df.createOrReplaceTempView("label_df")  
    
    spark.sql("""
        select count(distinct {0}) col_1_cnts, count(distinct {1}) col_2_cnts, count(*) cnts
        from label_df
    """.format(col_1, col_2)).show()

    if relevancy_method == 'top_k':
        spark.sql("""
            select count(distinct a.{0}) col_1_cnts, count(distinct a.{1}) col_2_cnts, count(*) cnts
            from prediction as a
            inner join label_df as b
            on 1=1
            and a.{0} = b.{0}
            and a.{1} = b.{1}
        """.format(col_1, col_2)).show()   
    elif relevancy_method == 'by_threshold':
         spark.sql("""
            select count(distinct a.{0}) col_1_cnts, count(distinct a.{1}) col_2_cnts, count(*) cnts
            from prediction as a
            inner join label_df as b
            on 1=1
            and a.{0} = b.{0}
            and a.{1} = b.{1}
            where 1=1
            and a.{2} >= {3}
        """.format(col_1, col_2, col_pre, threshold)).show()         
    else:
        spark.sql("""
            select count(distinct a.{0}) col_1_cnts, count(distinct a.{1}) col_2_cnts, count(*) cnts
            from prediction as a
            inner join label_df as b
            on 1=1
            and a.{0} = b.{0}
            and a.{1} = b.{1}
        """.format(col_1, col_2)).show()   

    ### Recommend Model Performance     
    rank_eval = SparkRankingEvaluation(label_df, prediction, k = reco_k, col_user=col_1, col_item=col_2, 
                                    col_rating=col_rt, col_prediction=col_pre, 
                                    relevancy_method=relevancy_method, threshold=threshold)
                                    
    print("Model: {}".format(modelname), "Top K:%d" % rank_eval.k, "MAP:%f" % rank_eval.map_at_k(), "NDCG:%f" % rank_eval.ndcg_at_k(), "Precision@K:%f" % rank_eval.precision_at_k(), "Recall@K:%f" % rank_eval.recall_at_k())
    
    end = dt.datetime.now() - start
    print (end)
    
    return(rank_eval)


# COMMAND ----------

# MAGIC %pip install recommenders

# COMMAND ----------

import datetime as dt
from recommenders.evaluation.spark_evaluation import SparkRankingEvaluation

rank_eval_rf = evaluation_function(
                       prediction = prediction_df, # 모델 예측값 df
                       label_df = label_df, # 정답지 df
                       col_1 = "gmap_id1", # 아이템1 컬럼
                       col_2 = "gmap_id2", # 아이템2 컬럼
                       col_rt = "co_count", # 실제값 컬럼
                       col_pre = "prob", # 예측 확률값 컬럼
                       reco_k = 5, # 상위 k개
                       modelname = "Random_Forest", # 모델명 
                       relevancy_method = 'top_k',  # 모델 성능 평가 방법
                       threshold = 0.5) # top_k 방법 사용시 threshold는 사용x
