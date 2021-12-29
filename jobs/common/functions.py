import hashlib
import random

from pyspark.sql.functions import udf
from pyspark.sql.types import StringType, IntegerType

get_partition_udf = udf(lambda k, p: str(int(hashlib.md5(str(k).encode()).hexdigest(), 16) % p), StringType())

random_range_udf = udf(lambda s, e: random.randint(int(s), int(e)), IntegerType())
