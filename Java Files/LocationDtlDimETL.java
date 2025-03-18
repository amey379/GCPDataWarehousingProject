package main.java.com.example;

import org.apache.beam.runners.dataflow.DataflowRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.parquet.ParquetIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.*;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;

import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.api.services.bigquery.model.TableFieldSchema;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import java.util.*;

public class LocationDtlDimETL {

    public interface TransformOptions extends DataflowPipelineOptions {
        @Description("Path to input Parquet file in GCS")
        @Default.String("gs://my-311-service-data-lake/silver/stage*.parquet")
        String getInputPath();
        void setInputPath(String value);

        @Description("BigQuery Table for dim_location_dtl")
        @Default.String("boston311servicerequest.dimensions.dim_location_dtl")
        String getBQTable();
        void setBQTable(String value);
    }

    public static void main(String[] args) {
        TransformOptions options = PipelineOptionsFactory.fromArgs(args)
                .withValidation()
                .as(TransformOptions.class);

        options.setRunner(DataflowRunner.class);
        options.setProject("boston311servicerequest");
        options.setRegion("us-east1");
        options.setStagingLocation("gs://my-311-service-data-lake/staging/");
        options.setGcpTempLocation("gs://my-311-service-data-lake/temp/");

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

        // Extract location details into KV pairs
        PCollection<KV<String, Void>> locationDtls = pipeline
                .apply("Read Parquet", ParquetIO.read(avroSchema).from(options.getInputPath()))
                .apply("Extract Location Details", ParDo.of(new ExtractLocationDetailsFn()))
                .apply("Remove Duplicates", Distinct.create());

        // Load existing dim_location_dtl as a side input
        PCollectionView<Map<String, Integer>> existingLocationDtls = pipeline
                .apply("Read Existing Location Details", BigQueryIO.readTableRows()
                        .from(options.getBQTable()))
                .apply("Convert to Map", ParDo.of(new ConvertBQToMapFn()))
                .apply(View.asMap());

        // Assign keys to new location details
        PCollection<TableRow> newLocationDtlsWithKeys = locationDtls
                .apply("Assign Location Dtl Key", ParDo.of(new AssignLocationDtlKeyFn(existingLocationDtls))
                        .withSideInputs(existingLocationDtls));

        // Add default "Not Available" row
        PCollection<TableRow> notAvailableRow = pipeline
                .apply("Create Default Row", Create.of(1))
                .apply("Generate Not Available Row", ParDo.of(new DoFn<Integer, TableRow>() {
                    @ProcessElement
                    public void processElement(@Element Integer input, OutputReceiver<TableRow> out) {
                        TableRow row = new TableRow();
                        row.set("location_dtl_key", -1);
                        row.set("neighborhood_services_district", -1);
                        row.set("police_district", "UNKNOWN");
                        row.set("fire_district", -1);
                        row.set("pwd_district", "UNKNOWN");
                        row.set("city_council_district", -1);
                        row.set("ward", -1);
                        row.set("precinct", "UNKNOWN");
                        out.output(row);
                    }
                }));

        // Combine and write to BigQuery
        PCollectionList<TableRow> combinedRows = PCollectionList.of(newLocationDtlsWithKeys).and(notAvailableRow);
        combinedRows.apply("Flatten", Flatten.pCollections())
                .apply("Write to BigQuery", BigQueryIO.writeTableRows()
                        .to(options.getBQTable())
                        .withSchema(getTableSchema())
                        .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
                        .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND));

        pipeline.run().waitUntilFinish();
    }

    static class ExtractLocationDetailsFn extends DoFn<GenericRecord, KV<String, Void>> {
        @ProcessElement
        public void processElement(@Element GenericRecord record, OutputReceiver<KV<String, Void>> out) {
            String neighborhoodServicesDistrict = record.get("neighborhood_services_district") != null ? record.get("neighborhood_services_district").toString().trim() : null;
            String policeDistrict = record.get("police_district") != null ? record.get("police_district").toString().trim() : null;
            String fireDistrictStr = record.get("fire_district") != null ? record.get("fire_district").toString().trim() : null;
            String pwdDistrict = record.get("pwd_district") != null ? record.get("pwd_district").toString().trim() : null;
            String cityCouncilDistrict = record.get("city_council_district") != null ? record.get("city_council_district").toString().trim() : null;
            String ward = record.get("ward") != null ? record.get("ward").toString().replace("WARD ", "").trim() : null;
            String precinct = record.get("precinct") != null ? record.get("precinct").toString().trim() : null;

            Integer neighborhoodServicesDistrictInt = parseIntegerOrNull(neighborhoodServicesDistrict, -1);
            policeDistrict = policeDistrict != null && !policeDistrict.equals("UNKNOWN") ? policeDistrict.substring(0, Math.min(3, policeDistrict.length())) : "UNKNOWN";
            Integer fireDistrict = parseIntegerOrNull(fireDistrictStr, -1);
            pwdDistrict = pwdDistrict != null && !pwdDistrict.equals("UNKNOWN") ? pwdDistrict.substring(0, Math.min(3, pwdDistrict.length())) : "UNKNOWN";
            Integer cityCouncilDistrictInt = parseIntegerOrNull(cityCouncilDistrict, -1);
            Integer wardInt = parseIntegerOrNull(ward, -1);
            precinct = precinct != null && !precinct.equals("UNKNOWN") ? precinct : "UNKNOWN";

            String compositeKey = String.format("%s|%s|%s|%s|%s|%s|%s",
                    neighborhoodServicesDistrictInt.toString(),
                    policeDistrict,
                    fireDistrict.toString(),
                    pwdDistrict,
                    cityCouncilDistrictInt.toString(),
                    wardInt.toString(),
                    precinct);

            out.output(KV.of(compositeKey, null));
        }

        private Integer parseIntegerOrNull(String value, int defaultValue) {
            if (value == null || value.isEmpty() || value.equals("UNKNOWN")) return defaultValue;
            try {
                return Integer.parseInt(value);
            } catch (NumberFormatException e) {
                return defaultValue;
            }
        }
    }

    static class ConvertBQToMapFn extends DoFn<TableRow, KV<String, Integer>> {
        @ProcessElement
        public void processElement(@Element TableRow row, OutputReceiver<KV<String, Integer>> out) {
            Integer neighborhoodServicesDistrict = row.get("neighborhood_services_district") != null ? ((Number) row.get("neighborhood_services_district")).intValue() : -1;
            String policeDistrict = (String) row.get("police_district");
            Integer fireDistrict = row.get("fire_district") != null ? ((Number) row.get("fire_district")).intValue() : -1;
            String pwdDistrict = (String) row.get("pwd_district");
            Integer cityCouncilDistrict = row.get("city_council_district") != null ? ((Number) row.get("city_council_district")).intValue() : -1;
            Integer ward = row.get("ward") != null ? ((Number) row.get("ward")).intValue() : -1;
            String precinct = (String) row.get("precinct");

            String compositeKey = String.format("%s|%s|%s|%s|%s|%s|%s",
                    neighborhoodServicesDistrict.toString(),
                    policeDistrict != null ? policeDistrict : "UNKNOWN",
                    fireDistrict.toString(),
                    pwdDistrict != null ? pwdDistrict : "UNKNOWN",
                    cityCouncilDistrict.toString(),
                    ward.toString(),
                    precinct != null ? precinct : "UNKNOWN");

            Integer locationDtlKey = ((Long) row.get("location_dtl_key")).intValue();
            out.output(KV.of(compositeKey, locationDtlKey));
        }
    }

    static class AssignLocationDtlKeyFn extends DoFn<KV<String, Void>, TableRow> {
        private final PCollectionView<Map<String, Integer>> existingLocationDtlsView;
        private int nextLocationDtlKey = 1;

        AssignLocationDtlKeyFn(PCollectionView<Map<String, Integer>> existingLocationDtlsView) {
            this.existingLocationDtlsView = existingLocationDtlsView;
        }

        @ProcessElement
        public void processElement(@Element KV<String, Void> kv, OutputReceiver<TableRow> out, ProcessContext c) {
            String compositeKey = kv.getKey();
            String[] fields = compositeKey.split("\\|", -1);
            Map<String, Integer> existingLocationDtls = c.sideInput(existingLocationDtlsView);
            Integer locationDtlKey = existingLocationDtls.getOrDefault(compositeKey, nextLocationDtlKey++);

            TableRow row = new TableRow();
            row.set("location_dtl_key", locationDtlKey);
            row.set("neighborhood_services_district", fields[0].equals("-1") ? -1 : Integer.parseInt(fields[0]));
            row.set("police_district", fields[1]);
            row.set("fire_district", fields[2].equals("-1") ? -1 : Integer.parseInt(fields[2]));
            row.set("pwd_district", fields[3]);
            row.set("city_council_district", fields[4].equals("-1") ? -1 : Integer.parseInt(fields[4]));
            row.set("ward", fields[5].equals("-1") ? -1 : Integer.parseInt(fields[5]));
            row.set("precinct", fields[6]);
            out.output(row);
        }
    }

    private static TableSchema getTableSchema() {
        return new TableSchema().setFields(Arrays.asList(
                new TableFieldSchema().setName("location_dtl_key").setType("INT64"),
                new TableFieldSchema().setName("neighborhood_services_district").setType("INT64").setMode("NULLABLE"),
                new TableFieldSchema().setName("police_district").setType("STRING").setMode("NULLABLE"),
                new TableFieldSchema().setName("fire_district").setType("INT64").setMode("NULLABLE"),
                new TableFieldSchema().setName("pwd_district").setType("STRING").setMode("NULLABLE"),
                new TableFieldSchema().setName("city_council_district").setType("INT64").setMode("NULLABLE"),
                new TableFieldSchema().setName("ward").setType("INT64").setMode("NULLABLE"),
                new TableFieldSchema().setName("precinct").setType("STRING").setMode("NULLABLE")
        ));
    }
}