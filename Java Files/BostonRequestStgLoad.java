package main.java.com.example;

import org.apache.beam.runners.dataflow.DataflowRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.parquet.ParquetIO;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Sample;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericData;

import java.time.LocalDate;
import java.time.LocalTime;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

public class BostonRequestStgLoad {

    public interface TransformOptions extends DataflowPipelineOptions {
        @Description("Path to input CSV file in GCS")
        @Default.String("gs://my-311-service-data-lake/bronze/Boston_311_2024.csv")
        String getInputPath();
        void setInputPath(String value);

        @Description("Path to output Parquet file in GCS (partitioned by Year)")
        @Default.String("gs://my-311-service-data-lake/silver/")
        String getOutputPath();
        void setOutputPath(String value);


    }

    public static void main(String[] args) {
        TransformOptions options = PipelineOptionsFactory.fromArgs(args)
                .withValidation()
                .as(TransformOptions.class);

        options.setProject("boston311servicerequest");
        options.setJobName("boston-311-stage-pipeline");
        options.setRegion("us-central1");
        options.setStagingLocation("gs://my-311-service-data-lake/staging/");
        options.setTempLocation("gs://my-311-service-data-lake/temp/"); 
        //options.setGcpTempLocation(options.getGcpTempLocation()); // Use the default or CLI override
        options.setRunner(DataflowRunner.class);

        Pipeline pipeline = Pipeline.create(options);

        String schemaJson = "{"
            + "\"type\": \"record\","
            + "\"name\": \"Boston311\","
            + "\"fields\": ["
            + "{\"name\": \"case_enquiry_id\", \"type\": \"string\"},"

            + "{\"name\": \"open_date\", \"type\": [\"null\", \"int\"], \"logicalType\": \"date\", \"default\": null},"
            + "{\"name\": \"open_time\", \"type\": [\"null\", \"long\"], \"logicalType\": \"time-millis\", \"default\": null},"

            + "{\"name\": \"sla_target_date\", \"type\": [\"null\", \"int\"], \"logicalType\": \"date\", \"default\": null},"
            + "{\"name\": \"sla_target_time\", \"type\": [\"null\", \"long\"], \"logicalType\": \"time-millis\", \"default\": null},"

            + "{\"name\": \"closed_date\", \"type\": [\"null\", \"int\"], \"logicalType\": \"date\", \"default\": null},"
            + "{\"name\": \"closed_time\", \"type\": [\"null\", \"long\"], \"logicalType\": \"time-millis\", \"default\": null},"

            + "{\"name\": \"on_time\", \"type\": [\"null\", \"string\"], \"default\": null},"
            + "{\"name\": \"case_status\", \"type\": [\"null\", \"string\"], \"default\": null},"
            + "{\"name\": \"closure_reason\", \"type\": [\"null\", \"string\"], \"default\": null},"
            + "{\"name\": \"case_title\", \"type\": [\"null\", \"string\"], \"default\": null},"
            + "{\"name\": \"subject\", \"type\": [\"null\", \"string\"], \"default\": null},"
            + "{\"name\": \"reason\", \"type\": [\"null\", \"string\"], \"default\": null},"
            + "{\"name\": \"type\", \"type\": [\"null\", \"string\"], \"default\": null},"
            + "{\"name\": \"queue\", \"type\": [\"null\", \"string\"], \"default\": null},"
            + "{\"name\": \"department\", \"type\": [\"null\", \"string\"], \"default\": null},"
            + "{\"name\": \"location\", \"type\": [\"null\", \"string\"], \"default\": null},"
            + "{\"name\": \"fire_district\", \"type\": [\"null\", \"string\"], \"default\": null},"
            + "{\"name\": \"pwd_district\", \"type\": [\"null\", \"string\"], \"default\": null},"
            + "{\"name\": \"city_council_district\", \"type\": [\"null\", \"string\"], \"default\": null},"
            + "{\"name\": \"police_district\", \"type\": [\"null\", \"string\"], \"default\": null},"
            + "{\"name\": \"neighborhood\", \"type\": [\"null\", \"string\"], \"default\": null},"
            + "{\"name\": \"neighborhood_services_district\", \"type\": [\"null\", \"string\"], \"default\": null},"
            + "{\"name\": \"ward\", \"type\": [\"null\", \"string\"], \"default\": null},"
            + "{\"name\": \"precinct\", \"type\": [\"null\", \"string\"], \"default\": null},"
            + "{\"name\": \"location_street_name\", \"type\": [\"null\", \"string\"], \"default\": null},"
            + "{\"name\": \"location_zipcode\", \"type\": [\"null\", \"string\"], \"default\": null},"
            + "{\"name\": \"latitude\", \"type\": [\"null\", \"double\"], \"default\": null},"
            + "{\"name\": \"longitude\", \"type\": [\"null\", \"double\"], \"default\": null},"
            + "{\"name\": \"source\", \"type\": [\"null\", \"string\"], \"default\": null},"

            + "{\"name\": \"db_created_datetime\", \"type\": \"long\", \"logicalType\": \"timestamp-millis\"},"
            + "{\"name\": \"db_modified_datetime\", \"type\": [\"null\", \"long\"], \"logicalType\": \"timestamp-millis\", \"default\": null},"
            + "{\"name\": \"created_by\", \"type\": [\"null\", \"string\"], \"default\": null},"
            + "{\"name\": \"modified_by\", \"type\": [\"null\", \"string\"], \"default\": null},"
            + "{\"name\": \"process_id\", \"type\": \"int\"}"
            + "]}";

        Schema avroSchema = new Schema.Parser().parse(schemaJson);

        PCollection<String> lines = pipeline.apply("Read CSV", TextIO.read().from(options.getInputPath()))
        .apply("Filter Header", ParDo.of(new DoFn<String, String>() {
private boolean firstLine = true;

@ProcessElement
public void processElement(@Element String line, OutputReceiver<String> receiver) {
if (firstLine) {
firstLine = false;  // Skip the first line (header)
} else {
receiver.output(line);
}
}
}));

        PCollection<GenericRecord> transformed = lines.apply("Transform Data", ParDo.of(new DoFn<String, GenericRecord>() {
            @ProcessElement
            public void processElement(@Element String line, OutputReceiver<GenericRecord> receiver) {
                String[] values = line.split(",");
                if (values[0].equalsIgnoreCase("case_enquiry_id")) {
                    return;
                }
                GenericRecord record = new GenericData.Record(avroSchema);
                record.put("case_enquiry_id", values[0]);
                DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

                String openDt = (values[1] == null || values[1].trim().isEmpty()) ? null : values[1].trim();
                if (openDt == null) {
                    record.put("open_date", null);
                    record.put("open_time", null);
                } else {
                    LocalDateTime openDateTime = LocalDateTime.parse(openDt, dateTimeFormatter);
                    LocalDate openDate = openDateTime.toLocalDate();
                    LocalTime openTime = openDateTime.toLocalTime();
                    record.put("open_date", (int) openDate.toEpochDay());
                    record.put("open_time", (long) openTime.toSecondOfDay() * 1000);
                }

                String slaDt = (values[2] == null || values[2].trim().isEmpty()) ? null : values[2].trim();
                if (slaDt == null) {
                    record.put("sla_target_date", null);
                    record.put("sla_target_time", null);
                } else {
                    LocalDateTime slaDateTime = LocalDateTime.parse(slaDt, dateTimeFormatter);
                    LocalDate slaDate = slaDateTime.toLocalDate();
                    LocalTime slaTime = slaDateTime.toLocalTime();
                    record.put("sla_target_date", (int) slaDate.toEpochDay());
                    record.put("sla_target_time", (long) slaTime.toSecondOfDay() * 1000);
                }

                String closedDt = (values[3] == null || values[3].trim().isEmpty()) ? null : values[3].trim();
                if (closedDt == null) {
                    record.put("closed_date", null);
                    record.put("closed_time", null);
                } else {
                    LocalDateTime closedDateTime = LocalDateTime.parse(closedDt, dateTimeFormatter);
                    LocalDate closedDate = closedDateTime.toLocalDate();
                    LocalTime closedTime = closedDateTime.toLocalTime();
                    record.put("closed_date", (int) closedDate.toEpochDay());
                    record.put("closed_time", (long) closedTime.toSecondOfDay() * 1000);
                }

                record.put("on_time", (values[4] == null || values[4].trim().isEmpty()) ? null : values[4].trim().toUpperCase());
                record.put("case_status", (values[5] == null || values[5].trim().isEmpty()) ? null : values[5].trim().toUpperCase());
                record.put("closure_reason", (values[6] == null || values[6].trim().isEmpty()) ? null : values[6].trim().substring(0, Math.min(values[6].trim().length(), 255)));
                record.put("case_title", (values[7] == null || values[7].trim().isEmpty()) ? null : values[7].trim().toUpperCase());
                record.put("subject", (values[8] == null || values[8].trim().isEmpty()) ? null : values[8].trim().toUpperCase());
                record.put("reason", (values[9] == null || values[9].trim().isEmpty()) ? null : values[9].trim().toUpperCase());
                record.put("type", (values[10] == null || values[10].trim().isEmpty()) ? null : values[10].trim().toUpperCase());
                record.put("queue", (values[11] == null || values[11].trim().isEmpty()) ? null : values[11].trim().toUpperCase());
                record.put("department", (values[12] == null || values[12].trim().isEmpty()) ? null : values[12].trim().toUpperCase());
                record.put("location", (values[15] == null || values[15].trim().isEmpty()) ? null : values[15].trim().toUpperCase());
                record.put("fire_district", (values[16] == null || values[16].trim().isEmpty()) ? null : values[16].trim().toUpperCase());
                record.put("pwd_district", (values[17] == null || values[17].trim().isEmpty()) ? null : values[17].trim().toUpperCase());
                record.put("city_council_district", (values[18] == null || values[18].trim().isEmpty()) ? null : values[18].trim().toUpperCase());
                record.put("police_district", (values[19] == null || values[19].trim().isEmpty()) ? null : values[19].trim().toUpperCase());
                record.put("neighborhood", (values[20] == null || values[20].trim().isEmpty()) ? null : values[20].trim().toUpperCase());
                record.put("neighborhood_services_district", (values[21] == null || values[21].trim().isEmpty()) ? null : values[21].trim().toUpperCase());
                record.put("ward", (values[22] == null || values[22].trim().isEmpty()) ? null : values[22].trim().toUpperCase());
                record.put("precinct", (values[23] == null || values[23].trim().isEmpty()) ? null : values[23].trim().toUpperCase());
                record.put("location_street_name", (values[24] == null || values[24].trim().isEmpty()) ? null : values[24].trim().toUpperCase());

                String zipcode = values[25] == null ? "" : values[25].trim();
                record.put("location_zipcode", zipcode.isEmpty() ? null : String.format("%05d", Integer.parseInt(zipcode)));

                String lat = values[26] == null ? "" : values[26].trim();
                String lon = values[27] == null ? "" : values[27].trim();
                record.put("latitude", lat.isEmpty() ? null : Double.parseDouble(lat));
                record.put("longitude", lon.isEmpty() ? null : Double.parseDouble(lon));

                record.put("source", (values[29] == null || values[29].trim().isEmpty()) ? null : values[29].trim().toUpperCase());

                record.put("db_created_datetime", System.currentTimeMillis());
                record.put("created_by", "system");
                record.put("modified_by", "");
                record.put("process_id", 1);

                receiver.output(record);
            }
        }));

        transformed
            .setCoder(AvroCoder.of(GenericRecord.class, avroSchema))
            .apply("Write Parquet",
                FileIO.<GenericRecord>write()
                    .via(ParquetIO.sink(avroSchema))
                    .to(options.getOutputPath())
                    .withPrefix("stage_boston_311_requests") 
                    .withSuffix(".parquet"));

        pipeline.run().waitUntilFinish();
    }
}