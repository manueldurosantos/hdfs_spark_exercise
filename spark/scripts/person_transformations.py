from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp

def get_validation_expr(field, validation):
    if validation == "notEmpty":
        return field.isNotNull() & (field != "")
    elif validation == "notNull":
        return field.isNotNull()
    else:
        raise ValueError(f"Validation {validation} not supported")


spark = SparkSession.builder.appName("MetadataTransformations").getOrCreate()

# Leer los metadatos
# HDFS_PATH = "https://localhost:50070"
HDFS_PATH = "/opt/bitnami/spark/volume/"
metadata = spark.read.json(f"{HDFS_PATH}/input/metadata.json", multiLine=True)

# Leer los datos de entrada
input_name = metadata.selectExpr("dataflows[0].sources[0].name").collect()[0][0]
input_path = metadata.selectExpr("dataflows[0].sources[0].path").collect()[0][0]
input_format = metadata.selectExpr("dataflows[0].sources[0].format").collect()[0][0]
input_data = spark.read.format(input_format).load(f"{input_path}{input_name}.{input_format.lower()}")

# Validaciones
validations = metadata.selectExpr("dataflows[0].transformations['params']['validations']").collect()[0][0][0]
filter_expr = None
non_validation_reasons = []
for validation in validations:
    field = validation.field
    checks = validation.validations
    for check in checks:
        if filter_expr is None:
            filter_expr = get_validation_expr(input_data[field], check)
        else:
            filter_expr &= get_validation_expr(input_data[field], check)

# Filtrado
validated_data = input_data.filter(filter_expr)
non_validated_data = input_data.filter(~filter_expr)

# Transformaciones
transformation = metadata.select("dataflows.transformations").collect()[0][0][0]
for tr in transformation:
    if tr.type == "add_fields":
        add_fields = tr.params["addFields"]
        for field in add_fields:
            name = field["name"]
            function = field["function"]
            if function == "current_timestamp":
                validated_data = validated_data.withColumn(name, current_timestamp())

# Escribir datos resultantes
sinks = metadata.select("dataflows.sinks").collect()[0][0][0]
for sink in sinks:
    name = sink.name
    input = sink.input
    output_path = sink.paths[0]
    format = sink.format
    save_mode = sink.saveMode
    if name == "raw-ok":
        validated_data.write.format(format).mode(save_mode).save(f"{output_path}{input}")
    elif name == "raw-ko":
        non_validated_data.write.format(format).mode(save_mode).save(f"{output_path}{input}")


spark.stop()
