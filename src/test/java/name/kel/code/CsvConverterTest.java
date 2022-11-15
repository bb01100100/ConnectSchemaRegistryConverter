package io.confluent.ps.apac;


import io.confluent.connect.avro.AvroData;
import io.confluent.connect.avro.AvroDataConfig;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.event.Level;

import java.io.IOException;
import java.io.InputStream;
import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.Date;
import java.util.Map;
import java.util.Properties;

class CsvConverterTest {

    private static final Logger log = LoggerFactory.getLogger(CsvConverterTest.class);

    private Schema incoming;
    private Schema target;

    private CsvConverter csv;

    CsvConverterTest() {
        prepSchemas();
    }


    void prepSchemas() {
        Map<String, Object> props = (Map<String, Object>) (Map) getConnectionProperties();

        // Incoming "String" Schema from source connector
        String sourceSchema = "{\"type\":\"record\",\"name\":\"ASCHEMA\",\"namespace\":\"ANAMESPACE\",\"fields\":[" +
                "{\"name\":\"DECIMAL1\",\"type\":\"string\"}," +
                "{\"name\":\"DECIMAL2\",\"type\":\"string\"}," +
                "{\"name\":\"DATE1\",\"type\":\"string\"}," +
                "{\"name\":\"TIME1\",\"type\":\"string\"}," +
                "{\"name\":\"TIMESTAMP1\",\"type\":\"string\"}," +
                "{\"name\":\"STRING1\",\"type\":\"string\"}" +
                "]}\n";

        // Define our target Schema Registry Schema
        String targetSchema = "{\"type\":\"record\",\"name\":\"A_SCHEMA\",\"namespace\":\"A_NAMESPACE\",\"fields\":[" +
                "{\"name\":\"DECIMAL1\",\"type\":{\"type\":\"bytes\",\"logicalType\":\"decimal\",\"precision\":11,\"scale\":0}}," +
                "{\"name\":\"DECIMAL2\",\"type\":{\"type\":\"bytes\",\"logicalType\":\"decimal\",\"precision\":11,\"scale\":4}}," +
                "{\"name\":\"DATE1\",\"type\":{\"type\":\"int\",\"logicalType\":\"date\"}}," +
                "{\"name\":\"TIME1\",\"type\":{\"type\":\"long\",\"logicalType\":\"time-micros\"}}," +
                "{\"name\":\"TIMESTAMP1\",\"type\":{\"type\":\"long\",\"logicalType\":\"timestamp-micros\"}}," +
                "{\"name\":\"STRING1\",\"type\":\"string\"}" +
                "]}\n";


        // Define an Avro converter instance
        AvroData avroData = new AvroData(new AvroDataConfig(props));

        // Generate a schema from our source definition
        org.apache.avro.Schema av1 = new org.apache.avro.Schema.Parser().parse(sourceSchema);

        // Convert to Connect format (Struct)
        incoming = avroData.toConnectSchema(av1);

        // Parse the target schema
        org.apache.avro.Schema temp_avroSchema = new org.apache.avro.Schema.Parser().parse(targetSchema);

        // Convert the target Schema from Avro format to Connect format
        target = avroData.toConnectSchema(temp_avroSchema);

        log.info("Creating new CsvConverter instance");
        csv = new CsvConverter();
        csv.configure(props, false);
    }


    @Test
    void testTimestampVariations() {

        try {

            String DECIMAL1 = "1234";
            String DECIMAL2 = "1234.5678";
            String DATE1 = "1996-01-01";
            String TIME1 = "01:02:03";

            for (String TIMESTAMP1 : new String[]{
                    "2022-01-09-23.41.59.001230",
                    "1752-01-01-00.00.00.000000",
                    "0001-01-01-00.00.00.000000",
                    "9999-12-31-23.59.59.001230"}) {


                //By default all fields are mandatory
                Struct value = new Struct(incoming)
                        .put("DECIMAL1", DECIMAL1)
                        .put("DECIMAL2", DECIMAL2)
                        .put("DATE1", DATE1)
                        .put("TIME1", TIME1)
                        .put("STRING1", "This is a string")
                        .put("TIMESTAMP1", TIMESTAMP1);



                // Map the incoming string fields to correct Schema Registry data types.
                Struct mappedStruct = csv.mapOldStructFieldsToTargetSchema((Struct) value, target);

                log.info("Have successfully mapped incoming string struct to correct target Schema data types. Will validate field types.");

                // Validate that data types are correct.

                // Timestamp micros
                Long timestampValue = (Long) mappedStruct.get("TIMESTAMP1");
                Instant stampValue_tmp = ChronoUnit.MICROS.addTo(Instant.EPOCH, timestampValue);


                DateTimeFormatter timestampFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd-HH.mm.ss.SSSSSS").withZone(ZoneId.of("UTC"));

                log.info("TIMESTAMP1 original value is " + TIMESTAMP1 + ", serialised as long " + timestampValue + " and converted from long back to " + timestampFormatter.format(stampValue_tmp));

                // We convert to a real timestamp with correct seprators and 6dp accurazy with a timezone; so tweak
                // the STMP022 field value to align with that and do our assertion.
                assert (timestampFormatter.format(stampValue_tmp).equals(TIMESTAMP1));

            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }




    @Test
    void validateSchemaMapping() {


        try {

            // Create a test record that looks like it came from a source connector
            // E.g. all strings but note that the formatting of fields
            // expected to be dates, numbers, etc. should be accurate.

            String DECIMAL1 = "1234";
            String DECIMAL2 = "1234.5678";
            String DATE1 = "1996-01-01";
            String TIME1 = "01:02:03";
            String TIMESTAMP1 = "2022-01-09-23.41.59.001230";
            TIMESTAMP1 = "1752-01-01-00.00.00.000000";
            TIMESTAMP1 = "0001-01-01-00.00.00.000000";


            Struct value = new Struct(incoming)
                    .put("DECIMAL1", DECIMAL1)
                    .put("DECIMAL2", DECIMAL2)
                    .put("DATE1", DATE1)
                    .put("TIME1", TIME1)
                    .put("TIMESTAMP1", TIMESTAMP1)
                    .put("STRING1", "This is a string");


            // Map the incoming string fields to correct Schema Registry data types.
            Struct mappedStruct = csv.mapOldStructFieldsToTargetSchema((Struct) value, target);

            log.info("Have successfully mapped incoming string struct to correct target Schema data types. Will validate field types.");

            // Validate that data types are correct.

            // Timestamp micros
            Long timestampValue = (Long) mappedStruct.get("TIMESTAMP1");
            Instant stampValue_tmp = ChronoUnit.MICROS.addTo(Instant.EPOCH, timestampValue);

            long aLong = Long.valueOf("-9223372036854775808");
            Instant anInstant = ChronoUnit.MICROS.addTo(Instant.EPOCH, aLong);

            log.info("My instant at min long is " + anInstant.toString());
            aLong = Long.valueOf("9223372036854775807");
            anInstant = ChronoUnit.MICROS.addTo(Instant.EPOCH, aLong);

            log.info("My instant at max long is " + anInstant.toString());

            DateTimeFormatter timestampFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd-HH.mm.ss.SSSSSS").withZone(ZoneId.of("UTC"));

            log.info("TIMESTAMP1 original value is " + TIMESTAMP1 + ", serialised as long " + timestampValue + " and converted from long back to " + timestampFormatter.format(stampValue_tmp));

            // We convert to a real timestamp with correct seprators and 6dp accurazy with a timezone; so tweak
            // the STMP022 field value to align with that and do our assertion.
            assert (timestampFormatter.format(stampValue_tmp).equals(TIMESTAMP1));


            // DATE1 is mapped to a Date object by the Connect framework.
            Date dateValue = (Date) mappedStruct.get("DATE1");

            LocalDate dateValue_tmp = LocalDate.parse(DATE1);
            DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd").withZone(ZoneId.of("UTC"));

            log.info("DATE1 original value is " + DATE1 + ", serialised as a Java date object " + dateValue + " and converted value is " + formatter.format(dateValue.toInstant()));

            assert (formatter.format(dateValue.toInstant()).equals(formatter.format(LocalDate.parse(DATE1))));


            // Decimal value (bytes)
            BigDecimal bd = (BigDecimal) mappedStruct.get("DECIMAL2");
            log.info("DECIMAL2 original value is " + DECIMAL2 + ", serialised as a Java BigDecimal object " + bd + " and converted value is " + bd);
            assert (bd.equals(new BigDecimal(DECIMAL2)));

            long time1 = (long) mappedStruct.get("TIME1");
            LocalTime time1LocalTime = LocalTime.ofNanoOfDay(time1 * 1000);

            DateTimeFormatter timeFormatter = DateTimeFormatter.ofPattern("HH:mm:ss").withZone(ZoneId.of("UTC"));
            log.info("TIME1 original value is " + TIME1 + ", serialised as a long " + time1 + " and converted value is " + timeFormatter.format(time1LocalTime));

            assert (timeFormatter.format(time1LocalTime).equals(TIME1));

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    @Test
    void validateOptionalFields() {

        target = CsvConverter.makeAllFieldsOptional(target);

        try {

            // Create a test record that looks like it came from a source connector
            // E.g. all strings but note that the formatting of fields
            // expected to be dates, numbers, etc. should be accurate.

            String DECIMAL1 = "0987654321";
            String DECIMAL2 = "1234.5678";
            String DATE1 = "1996-01-01";
            String TIME1 = "01:02:03";
            String TIMESTAMP1 = "2022-01-09-23.41.59.00123";

            Struct value = new Struct(incoming)
                    .put("DECIMAL1", DECIMAL1)
                    ;

            // We didn't complete all the fields





            // Map the incoming string fields to correct Schema Registry data types.
            Struct mappedStruct = csv.mapOldStructFieldsToTargetSchema((Struct) value, target);

            log.info("Have successfully mapped incoming string struct to correct target Schema data types. Will validate field types.");


            // Decimal value (bytes)
            BigDecimal bd = (BigDecimal) mappedStruct.get("DECIMAL1");
            log.info("DECIMAL1 original value is " + DECIMAL1 + ", serialised as a Java BigDecimal object " + bd + " and converted value is " + bd);
            assert(bd.equals(new BigDecimal(DECIMAL1)));



        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    public static void main(String argv[]) {

        CsvConverter csv = new CsvConverter();

        // Timestamp tests
        for (String ts : new String[]{
                "2001-11-23-22.18.04.914100",
                "2001-11-23T22.18.04.914120",
                "2001-11-23T22:18:04.914123",
        }) {

            log.info(String.format("Timestamp micros is %s",
                    ts));
            log.info(String.format("Converted to this:  %s",
                    csv.dateFromDateString(ts).truncatedTo(ChronoUnit.MICROS)));

         }

        // Time tests
        for (String ts : new String[]{
                "22.18.04.914100",
                "22:18:04.914120",
                "22:18:04.914123",
        }) {

            log.info(String.format("Timestamp micros is %s",
                    ts));
            log.info(String.format("Converted to this:  %s",
                    csv.dateFromDateString(ts)));

            log.info(String.format("          truncated to micros:   %-33s",
                    csv.dateFromDateString(ts).truncatedTo(ChronoUnit.MICROS)));

            log.info(String.format("          truncated to millis:   %-33s",
                    csv.dateFromDateString(ts).truncatedTo(ChronoUnit.MILLIS)));

            log.info(String.format("          truncated to seconds:  %-32s",
                    csv.dateFromDateString(ts).truncatedTo(ChronoUnit.SECONDS)));

        }
    }

    Properties getConnectionProperties() {

        Properties props = new Properties();
        try (InputStream inputStream = CsvConverterTest.class.getResourceAsStream("/client.properties")) {
            props.load(inputStream);
        } catch (IOException e) {
            e.printStackTrace(System.out);
        }

        return props;
    }
}