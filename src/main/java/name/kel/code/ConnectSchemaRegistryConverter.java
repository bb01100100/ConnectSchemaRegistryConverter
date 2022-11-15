package name.kel.code;

import io.confluent.connect.avro.AvroConverterConfig;
import io.confluent.connect.avro.AvroData;
import io.confluent.connect.avro.AvroDataConfig;

import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.avro.AvroSchemaProvider;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaMetadata;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;

import org.apache.avro.generic.IndexedRecord;

import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.errors.InvalidConfigurationException;
import org.apache.kafka.common.errors.SerializationException;

import java.math.BigDecimal;
import java.time.*;

import java.time.temporal.ChronoField;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.Date;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import io.confluent.kafka.serializers.AbstractKafkaAvroDeserializer;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerializer;
import io.confluent.kafka.serializers.GenericContainerWithVersion;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;




import org.apache.kafka.connect.data.*;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.storage.Converter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConnectSchemaRegistryConverter implements Converter {
    public SchemaRegistryClient schemaRegistry;
    private Serializer serializer;
    private Deserializer deserializer;
    private boolean isKey;
    private AvroData avroData;

    private HashMap<String, Schema> subjectCache;

    private static final Logger log = LoggerFactory.getLogger(ConnectSchemaRegistryConverter.class);

    final String DATETIME_REGEX = "(?<datetime>(?<date>(?:(?<year>\\d{4})-(?<month>\\d{1,2})-(?<day>\\d{1,2}))|" +
            "(?:(?<day2>\\d{1,2})\\/(?<month2>\\d{1,2})\\/(?<year2>\\d{4}))|" +
            "(?:(?<day3>\\d{1,2})-(?<month3>\\w{3})-(?<year3>\\d{4})))?(?:\\s?(T|-)?(?<time>(?<hour>\\d{1,2})(:|\\.)(?<minute>\\d{1,2})(:|\\.)(?<second>\\d{1,2})\\.?(?<milli>\\d{0,7})?)?))";

    final Pattern regexPattern = Pattern.compile(DATETIME_REGEX);

    Pattern timeStampRegex;
    Pattern timeRegex;



    public ConnectSchemaRegistryConverter() {
    }

    public ConnectSchemaRegistryConverter(SchemaRegistryClient client) {
        this.schemaRegistry = client;
    }

    // Keep track of what additional (if any) converter properties were passed in to us.
    // This might include the format of timestamp, time, etc.
    // Also, whether to override mandatory field settings or not.
    private Map<String, ?> CsvProps;

    public void printProperties(Boolean stdout) {
        this.CsvProps.forEach((k,v) -> {
            String msg = String.format("Property Key is %s. Value is %s", k, v.toString());
            if (stdout) {
                System.out.println(msg);
            } else {
                log.debug(msg);
            }
        });
    }


    public void configure(Map<String, ?> configs, boolean isKey) {
        this.isKey = isKey;
        AvroConverterConfig avroConverterConfig = new AvroConverterConfig(configs);
        if (this.schemaRegistry == null) {
            this.schemaRegistry = new CachedSchemaRegistryClient(avroConverterConfig.getSchemaRegistryUrls(), avroConverterConfig.getMaxSchemasPerSubject(), Collections.singletonList(new AvroSchemaProvider()), configs, avroConverterConfig.requestHeaders());
        }

        this.serializer = new Serializer(configs, this.schemaRegistry);
        this.deserializer = null;
        this.avroData = new AvroData(new AvroDataConfig(configs));

        this.subjectCache = new HashMap<String, Schema>();

        this.CsvProps = configs;

        // Let the connector provide a regex to determine if a field should be treated as a timestamp
        timeStampRegex = Pattern.compile(CsvProps.containsKey("timestamp.format.regex")
                ? (String) CsvProps.get("timestamp.format.regex")
                : "[0-9]{4}-[0-9]{2}-[0-9]{2}[T-][0-9]{2}\\.[0-9]{2}\\.[0-9]{2}\\.[0-9]{0,6}");



        // Let the connector provide a regex to determine if a field should be treated as a time
        timeRegex = Pattern.compile(CsvProps.containsKey("time.format.regex")
                ? (String) CsvProps.get("time.format.regex")
                : "[0-9]{2}[:\\.][0-9]{2}[:\\.][0-9]{2}(.[0-9]{0,3}|)");

        log.info("CSVConverter is being used for " + (isKey ? "key" : "value") + " schemas.");

    }

    public byte[] fromConnectData(String topic, Schema schema, Object value) {

        // At this point in time, we DO NOT create keys - always returning a null key value if we are
        // configured in the Connector with `key.converer` properties.
        // This supports ensuring we partition data in a round-robin fashion.
        if (isKey) {
            byte[] emptyKey = new byte[0];
            return emptyKey;
        }

        final Schema targetSchema;
        Schema tmpSchema;

        try {
            // Assume TopicNameStrategy for Subjects
            String subject = topic + "-value";


            if (subjectCache.containsKey(subject)) {
                log.trace("Found schema " + subject + "for topic " + topic + " in our subject cache.");
                tmpSchema = subjectCache.get(subject);
            } else {
                log.info("Looking up value schema " + subject + " for topic " + topic + " from Schema Registry.");

                // Get metadata about the latest schema for a subject (ID, version, schema).
                SchemaMetadata md = schemaRegistry.getLatestSchemaMetadata(subject);
                org.apache.avro.Schema temp_avroSchema = new org.apache.avro.Schema.Parser().parse(md.getSchema());
                tmpSchema = this.avroData.toConnectSchema(temp_avroSchema);

                log.debug("Schema from Schema Registry has ID + " + md.getId() + ", and name of " + temp_avroSchema.getName());

                if (this.CsvProps.containsKey("all.fields.optional") && (
                        this.CsvProps.get("all.fields.optional").equals(true)
                        || this.CsvProps.get("all.fields.optional").equals("true"))) {
                    log.warn("CsvConverter configured to make all fields optional - this will result in a new Schema version being registered.");

                    tmpSchema = makeAllFieldsOptional(tmpSchema);
                }

                log.info("Putting Schema Registry-sourced schema in our cache.");
                subjectCache.put(subject, tmpSchema);
            }
        } catch (Exception e) {
            log.error("Exception while working with Schema Registry-sourced schema: " + e.toString());
            tmpSchema = null;
            e.printStackTrace();
        }

        // Assign the cached / looked up schema
        targetSchema = tmpSchema;
        log.trace("After cache / SR lookup, the target schema namespace/name is: " + targetSchema.name());
        schema = tmpSchema;


        try {
            // Map incoming struct fields to the correct data types from Schema Registry
            Struct mappedStruct = mapOldStructFieldsToTargetSchema((Struct) value, targetSchema);

            // We want an Avro Schema from our Connect schema
            org.apache.avro.Schema avroSchema = this.avroData.fromConnectSchema(targetSchema);

            // We want our schema and value object combined for serialisation
            IndexedRecord idxAndRec = (IndexedRecord) this.avroData.fromConnectData(targetSchema, mappedStruct);

            return this.serializer.serialize(
                    topic,
                    this.isKey,
                    idxAndRec,
                    new AvroSchema(avroSchema));

        } catch (SerializationException var5) {
            throw new DataException(String.format("Failed to serialize Avro data from topic %s :", topic), var5);
        } catch (InvalidConfigurationException var6) {
            throw new ConfigException(String.format("Failed to access Avro data from topic %s : %s", topic, var6.getMessage()));
        }
    }

    public static Schema makeAllFieldsOptional(Schema tmpSchema) {
        // Build a schema with all-fields-optional
        SchemaBuilder sb = new SchemaBuilder(Schema.Type.STRUCT).name(tmpSchema.name());

        // For each field in the source schema
        for (Field f: tmpSchema.fields()) {

            // Build up the correct field configuration based on the source field
            SchemaBuilder sbf = SchemaBuilder.type(f.schema().type()).optional();

            // If the field has a custom "type" (connect class name), then use it
            if (f.schema().name() != null) {
                sbf.name(f.schema().name());
            } else {
                sbf.name(f.name());
            }

            // If the field has parameters (scale, precision, etc), use it.
            if (f.schema().parameters() != null) {
                sbf.parameters(f.schema().parameters());
            }

            sbf.build();
            sb.field(f.name(), sbf.schema());


            log.trace("SR schema field name:  " + f.name() + ", and Struct type: " + f.schema().type() + ", schema name: " + f.schema().name());
            log.trace("Adj schema field name: " + f.name() + ", and Struct type: " + sbf.schema().type() + ", schema name: " + sbf.schema().name());

        }

        tmpSchema = sb.build();
        return tmpSchema;
    }

    public Struct mapOldStructFieldsToTargetSchema(Struct oldStruct, Schema targetSchema) {
        Struct newStruct = new Struct(targetSchema);

        // Iterate over struct fields
        //oldStruct.schema().fields().parallelStream().forEach(f -> {
        for (Field f : oldStruct.schema().fields()) {

                // We know that Spooldir always sets fields to String type
                String val = oldStruct.getString(f.name());

                if (val == null) {
                    newStruct.put(f.name(), null);
                    continue;
                }

                Field schemaField = targetSchema.field(f.name());

                // If we have a complex type, dig into it.
                // Assume a simple flat structure from the CSV input file (e.g. not nested structs or maps!)
                switch (schemaField.schema().type()) {
                    case BYTES:
                        // Find out what we're storing
                        String logicalType = schemaField.schema().name();
                        switch (logicalType) {
                            case "org.apache.kafka.connect.data.Decimal":
                                try {
                                    newStruct.put(f.name(), new BigDecimal(val));
                                } catch (Exception e) {
                                    log.error("Exception converting from String to BigDecimal: " + e.toString());
                                }
                                break;
                            case "timestamp-micros":
                                try {
                                    // Convert timestamp string into microseconds since epoch
                                    // Due to what appears to be a bug in ChronoUnit, we can't simply do
                                    // ChronoUnit.MICROS.between(Instant.EPOCH, dateFromDateString(val))
                                    // because we might get a Long overflow due to the difference being calculated
                                    // at a more granular time unit.
                                    // Instead, we calculate differences in seconds, then add microseconds to the result.

                                    Instant timestampField = dateFromDateString(val);
                                    Long microTs = (ChronoUnit.SECONDS.between(Instant.EPOCH, timestampField.truncatedTo(ChronoUnit.SECONDS)) * 1000000) + timestampField.getNano() / 1000;
                                    newStruct.put(f.name(), microTs);

                                } catch (Exception e) {
                                    log.error("Exception converting from String to Long with timestamp-micros logical name: " + e.toString());
                                }
                                break;
                            case "org.apache.kafka.connect.data.Date":
                                // Here for completeness - I'm not sure Connect will put Bytes/Date, preferring Int/Date instead
                                try {
                                    newStruct.put(f.name(), dateFromDateString(val));
                                } catch (Exception e) {
                                    log.error("Exception converting from String to Date: " + e.toString());
                                }
                                break;

                            default:
                                log.error("Not implemented / unhandled " +
                                                targetSchema.field(f.name()).schema().type().toString() +
                                                " type of ",
                                        targetSchema.field(f.name()).schema().name());
                        }
                        break;

                    case STRING:
                        newStruct.put(f.name(), val);
                        break;

                    case INT8:
                    case INT16:
                    case INT32:
                        if ("org.apache.kafka.connect.data.Date".equals(schemaField.schema().name())) {
                            newStruct.put(f.name(), Date.from(dateFromDateString(val)));
                        } else {
                            newStruct.put(f.name(), Integer.valueOf(val));
                        }
                        break;

                    case INT64:
                        // Check for logical type hints
                        if ("time-micros".equals(schemaField.schema().name())) {
                            newStruct.put(f.name(), LocalTime.parse(val).get(ChronoField.MICRO_OF_DAY));
                        } else if ("timestamp-micros".equals(schemaField.schema().name())) {
                            try {
                                // Convert timestamp string into microseconds since epoch
                                // Due to what appears to be a bug in ChronoUnit, we can't simply do
                                // ChronoUnit.MICROS.between(Instant.EPOCH, dateFromDateString(val))
                                // because we might get a Long overflow due to the difference being calculated
                                // at a more granular time unit.
                                // Instead, we calculate differences in seconds, then add microseconds to the result.

                                Instant timestampField = dateFromDateString(val);
                                Long microTs = (ChronoUnit.SECONDS.between(Instant.EPOCH, timestampField.truncatedTo(ChronoUnit.SECONDS)) * 1000000) + timestampField.getNano() / 1000;
                                newStruct.put(f.name(), microTs);
                            } catch (Exception e) {
                                log.error("Exception converting from String to Long with timestamp-micros name: " + e.toString());
                            }
                        } else {
                            // Does this field look like a timestamp?
                            if (timeStampRegex.matcher(val).matches()) {
                                log.trace("INT64 field " + schemaField.name() + " is timestamp string of the format " + timeStampRegex.toString());
                                try {
                                    // Convert timestamp string into microseconds since epoch
                                    // Due to what appears to be a bug in ChronoUnit, we can't simply do
                                    // ChronoUnit.MICROS.between(Instant.EPOCH, dateFromDateString(val))
                                    // because we might get a Long overflow due to the difference being calculated
                                    // at a more granular time unit.
                                    // Instead, we calculate differences in seconds, then add microseconds to the result.

                                    Instant timestampField = dateFromDateString(val);
                                    Long microTs = (ChronoUnit.SECONDS.between(Instant.EPOCH, timestampField.truncatedTo(ChronoUnit.SECONDS)) * 1000000) + timestampField.getNano() / 1000;
                                    newStruct.put(f.name(), microTs);

                                } catch (Exception e) {
                                    log.error("Exception converting from String to Long with timestamp-micros name: " + e.toString());
                                }
                            } else if (timeRegex.matcher(val).matches()) {
                                log.trace("INT64 field " + schemaField.name() + " is a time-string of the format " + timeRegex.toString());
                                try {
                                    newStruct.put(f.name(), LocalTime.parse(val).getLong(ChronoField.MICRO_OF_DAY));
                                } catch (Exception e) {
                                    log.error("Exception converting from String to Long with time-micros name: " + e.toString());
                                }
                            } else {
                                log.trace("Not a special timestamp of any kind, assuming string is a Long value.");
                                newStruct.put(f.name(), Long.valueOf(val));
                            }
                        }
                        break;

                    case BOOLEAN:
                        newStruct.put(f.name(), Boolean.valueOf(val));
                        break;

                    case FLOAT32:
                    case FLOAT64:
                        newStruct.put(f.name(), Float.valueOf(val));
                        break;

                    default:
                        log.error("Can't handle " + schemaField.schema().type().toString() + " field types with this converter!");
                }

            }

        return newStruct;
    }

    public Instant dateFromDateString(String timestamp) {
        // This is based on Debezium's timestamp converter at:
        //   https://github.com/oryanmoshe/debezium-timestamp-converter/blob/e8dc33e775b6da9c70b94dc1c2f69339472aa2be/src/main/java/oryanmoshe/kafka/connect/util/TimestampConverter.java#L134

        Matcher matches = regexPattern.matcher(timestamp);

        final Map<String, String> MONTH_MAP = Map.ofEntries(Map.entry("jan", "01"), Map.entry("feb", "02"),
                Map.entry("mar", "03"), Map.entry("apr", "04"), Map.entry("may", "05"), Map.entry("jun", "06"),
                Map.entry("jul", "07"), Map.entry("aug", "08"), Map.entry("sep", "09"), Map.entry("oct", "10"),
                Map.entry("nov", "11"), Map.entry("dec", "12"));

        if (matches.find()) {
            String year = (matches.group("year") != null ? matches.group("year")
                    : (matches.group("year2") != null ? matches.group("year2") : matches.group("year3")));
            String month = (matches.group("month") != null ? matches.group("month")
                    : (matches.group("month2") != null ? matches.group("month2") : matches.group("month3")));
            String day = (matches.group("day") != null ? matches.group("day")
                    : (matches.group("day2") != null ? matches.group("day2") : matches.group("day3")));
            String hour = matches.group("hour") != null ? matches.group("hour") : "00";
            String minute = matches.group("minute") != null ? matches.group("minute") : "00";
            String second = matches.group("second") != null ? matches.group("second") : "00";
            String milli = matches.group("milli") != null ? matches.group("milli") : "000000";

            if (milli.length() > 6)
                milli = milli.substring(0, 6);

            String dateStr = "";

            dateStr +=
                    ("00".substring(hour.length()) + hour) + ":" +
                    ("00".substring(minute.length()) + minute) + ":" +
                    ("00".substring(second.length()) + second) + "." +
                    (milli + "000000".substring(milli.length()));


            if (year != null) {
                if (month.length() > 2)
                    month = MONTH_MAP.get(month.toLowerCase());


                dateStr = year + "-" +
                        ("00".substring(month.length()) + month) + "-" +
                        ("00".substring(day.length()) + day) + "T" +
                        dateStr + "Z";
            } else {
                dateStr = "2020-01-01T" + dateStr + "Z";
            }

            //LocalDateTime dateObj = LocalDateTime.from(Instant.parse(dateStr));

            //return dateObj;
            return Instant.parse(dateStr);
        }

        log.debug("No match on incoming timestamp string " + timestamp);

        return null;
    }







    // We only use this converter for writing CSV data into Kafka.
    public SchemaAndValue toConnectData(String topic, byte[] value) {
        try {
            double a = 1 / 0.0;
        } catch (Exception e) {
            throw new ConfigException("Converter does not support deserialization! Use only for producing.");
        }
        return SchemaAndValue.NULL;
    }

    private static class Deserializer extends AbstractKafkaAvroDeserializer {
        public Deserializer(SchemaRegistryClient client) {
            this.schemaRegistry = client;
        }

        public Deserializer(Map<String, ?> configs, SchemaRegistryClient client) {
            this(client);
            this.configure(new KafkaAvroDeserializerConfig(configs));
        }

        public GenericContainerWithVersion deserialize(String topic, boolean isKey, byte[] payload) {
            return this.deserializeWithSchemaAndVersion(topic, isKey, payload);
        }
    }

    private static class Serializer extends AbstractKafkaAvroSerializer {
        public Serializer(SchemaRegistryClient client, boolean autoRegisterSchema) {
            this.schemaRegistry = client;
            this.autoRegisterSchema = autoRegisterSchema;
        }

        public Serializer(Map<String, ?> configs, SchemaRegistryClient client) {
            this(client, false);
            this.configure(new KafkaAvroSerializerConfig(configs));
        }

        public byte[] serialize(String topic, boolean isKey, Object value, AvroSchema schema) {
            return value == null ? null : this.serializeImpl(this.getSubjectName(topic, isKey, value, schema), value, schema);
        }
    }
}
