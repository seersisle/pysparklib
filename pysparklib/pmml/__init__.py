import json
import time
import uuid
from typing import List

from py4j.java_collections import JavaArray
from pyspark import SparkContext
from pyspark.ml import PipelineModel
from pyspark.sql import SparkSession, DataFrame
from pyspark2pmml import PMMLBuilder

from pysparklib.exceptions import PySparkLibError


class PMMLUtil:

    def __init__(self, spark: SparkSession, df: DataFrame, pipeline_dir: str):
        self.spark = spark
        self.sc: SparkContext = spark.sparkContext
        self.df = df
        self.pipeline_dir = pipeline_dir
        self.stage_dir = f"{self.pipeline_dir}/stages"
        self.fs = self.sc._jvm.org.apache.hadoop.fs.FileSystem.get(self.sc._jsc.hadoopConfiguration())
        self.path = self.sc._jvm.org.apache.hadoop.fs.Path

    def _metadata(self, stage_uids: List):
        timestamp = int(str(time.time_ns())[0:13])
        pipeline_uid = str(uuid.uuid1()).split('-')[-1]
        return json.dumps({"class": "org.apache.spark.ml.PipelineModel", "paramMap": {"stageUids": stage_uids},
                           "sparkVersion": self.spark.version, "timestamp": timestamp,
                           "uid": f"PipelineModel_{pipeline_uid}"})

    @property
    def stage_uids(self) -> list:
        if not self.fs.exists(self.path(self.stage_dir)):
            raise RuntimeError(f"{self.stage_dir} does not exist")
        status = self.fs.listStatus(self.path(self.stage_dir))
        return ['_'.join(file_status.getPath().toString().split("/")[-1].split('_')[1:]) for file_status in status] \
            if status else []

    def save_stage(self, processor, stage_order: int) -> str:
        """
        Save sparkml pipeline stage metadata to HDFS.
        :param processor:  Transformer, Estimator, Evaluator.
        :param stage_order: Stage order in pipeline.
        :return:
        """
        stage_path = f"{self.stage_dir}/{stage_order}_{processor.uid}"
        stage_path_history = f"{self.stage_dir}/{stage_order}_{processor.uid.split('_')[0]}_*"
        status: JavaArray = self.fs.globStatus(self.path(stage_path_history))
        # fileStatus = self.sc._jvm.org.apache.hadoop.fs.FileStatus
        # file_status: fileStatus
        try:
            if status:
                for file_status in status:
                    if self.fs.exists(file_status.getPath()):
                        self.fs.delete(file_status.getPath())
                        print(f"Deleted history stage {file_status.getPath()}")
            processor.write().overwrite().save(stage_path)
        except Exception as e:
            raise PySparkLibError(e)
        return stage_path

    def save_pmml(self, pmml_dir: str) -> str:
        try:
            metadata_dir = f"{self.pipeline_dir}/metadata"
            metadata_path = self.path(metadata_dir)
            if self.fs.exists(metadata_path):
                self.fs.delete(metadata_path)
            if self.fs.exists(self.path(pmml_dir)):
                self.fs.delete(self.path(pmml_dir))

            self.sc.parallelize([self._metadata(self.stage_uids)], 1).saveAsTextFile(metadata_dir)
            pipeline_model: PipelineModel = PipelineModel.load(self.pipeline_dir)
            pmml_builder = PMMLBuilder(self.spark, self.df, pipeline_model)
            self.sc.parallelize([pmml_builder.buildByteArray()], 1).saveAsTextFile(pmml_dir)
        except Exception as e:
            raise PySparkLibError(e)
        return pmml_dir
