# ConnectSchemaRegistryConverter

This custom Kafka Connect converter maps Connect Struct fields (presumed to be of String type) to the datatypes defined in a pre-registered schema in Confluent Schema Registry.

The objective is to allow the Schema in the Schema Registry to drive data quality, avoiding the situation where Connect registers schemas with incorrect (or inaccurate) data types.

Why do this? Some sources of data (e.g. delimited text files) provide no mechanism for determining the correct data type for each field. Rather than configuring each connector individually with the correct data types through a Single Message Transform (SMT), we dynamically do that mapping by referring to the pre-registgered Schema in Confluent Schema Registry.


## Usage example
In this example we use Justin Custenborder's Spooldir connector to read CSV files and then convert from the (string) field types to the datatypes defined in the associated subject in Schema Registry.

We let the Connector inspect the CSV file header for field names (`csv.first.row.as.header`) and generate a (temporary) schema (`schema.generation.enabled`).

```
{
  "connector.class": "com.github.jcustenborder.kafka.connect.spooldir.SpoolDirCsvSourceConnector",
  "csv.first.row.as.header": "true",
  "csv.null.field.indicator": "BOTH",
  "empty.poll.wait.ms": "5000",
  "error.path": "/opt/data/error",
  "finished.path": "/opt/data/finished",
  "cleanup.policy": "MOVE",
  "halt.on.error": "true",
  "input.file.pattern": "file.csv",
  "input.path": "/opt/data/input",
  "max.tasks": "1",
  "name": "csv-loader",
  "schema.generation.enabled": "true",
  "topic": "csv.ingestion.topic",
  "value.converter": "name.kel.code.ConnectSchemaRegistryConverter",
  "value.converter.schema.registry.url": "schema-Registry-Hostname",
  "value.converter.basic.auth.credentials.source": "USER_INFO",
  "value.converter.basic.auth.user.info": "schema-Registry-Auth"
}
```
