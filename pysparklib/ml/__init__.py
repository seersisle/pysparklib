from typing import Dict, Set

from pyspark.ml.param import Param
from pyspark.sql import SparkSession
from pyspark.sql.types import Row

from pysparklib.exceptions import PySparkLibError


class Processor:
    def __init__(self, spark: SparkSession):
        self.spark = spark

    def save_param(self, processor, table: str, exclude: Set = None) -> str:
        """
        Save parameter map as table.
        :param processor: Transformer, Estimator, Evaluator.
        :param table: Hive table.
        :param exclude:
        :return:
        """
        if exclude is None:
            exclude = set()

        param: Param
        param_map: Dict = {}

        try:
            for param, value in processor.extractParamMap().items():
                name = param.name
                if name not in exclude:
                    param_map[param.name] = value
            rdd = self.spark.sparkContext.parallelize([tuple(param_map.values())])
            row = Row(*param_map.keys())
            self.spark.createDataFrame(rdd.map(lambda field: row(*field))).write.saveAsTable(table, mode="overwrite")
        except Exception as e:
            raise PySparkLibError(e)
        return table
