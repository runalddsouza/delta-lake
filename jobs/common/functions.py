import hashlib

from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

get_partition_udf = udf(lambda k, p: str(int(hashlib.md5(str(k).encode()).hexdigest(), 16) % p), StringType())
