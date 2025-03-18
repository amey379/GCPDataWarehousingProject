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

import java.math.BigDecimal;
import java.util.*;

public class LocationDimETL {

    public interface TransformOptions extends DataflowPipelineOptions {
        @Description("Path to input Parquet file in GCS")
        @Default.String("gs://my-311-service-data-lake/silver/stage*.parquet")
        String getInputPath();
        void setInputPath(String value);

        @Description("BigQuery Table for dim_location")
        @Default.String("boston311servicerequest.dimensions.dim_location")
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

        PCollection<KV<String, Void>> locations = pipeline
                .apply("Read Parquet", ParquetIO.read(avroSchema).from(options.getInputPath()))
                .apply("Extract Location Details", ParDo.of(new ExtractLocationDetailsFn()))
                .apply("Remove Duplicates", Distinct.create());

        PCollectionView<Map<String, Integer>> existingLocations = pipeline
                .apply("Read Existing Locations", BigQueryIO.readTableRows()
                        .from(options.getBQTable()))
                .apply("Convert to Map", ParDo.of(new ConvertBQToMapFn()))
                .apply(View.asMap());

        PCollection<TableRow> newLocationsWithKeys = locations
                .apply("Assign Location Key", ParDo.of(new AssignLocationKeyFn(existingLocations))
                        .withSideInputs(existingLocations));

        PCollection<TableRow> notAvailableRow = pipeline
                .apply("Create Default Row", Create.of(1))
                .apply("Generate Not Available Row", ParDo.of(new DoFn<Integer, TableRow>() {
                    @ProcessElement
                    public void processElement(@Element Integer input, OutputReceiver<TableRow> out) {
                        TableRow row = new TableRow();
                        row.set("location_key", -1);
                        row.set("is_intersection", 0);
                        row.set("street_name1", "UNKNOWN");
                        row.set("street_name2", null);
                        row.set("neighborhood", "Not Available");
                        row.set("city", "Boston");
                        row.set("state", "MA");
                        row.set("zipcode", null);
                        row.set("latitude", null);
                        row.set("longitude", null);
                        out.output(row);
                    }
                }));

        PCollectionList<TableRow> combinedRows = PCollectionList.of(newLocationsWithKeys).and(notAvailableRow);
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
            String locationStreetName = record.get("location_street_name") != null ? record.get("location_street_name").toString().trim() : null;
            String neighborhood = record.get("neighborhood") != null ? record.get("neighborhood").toString().trim().toUpperCase() : "Not Available";
            String zipcode = record.get("location_zipcode") != null ? record.get("location_zipcode").toString().trim() : null;
            Double latitude = record.get("latitude") != null ? (Double) record.get("latitude") : null;
            Double longitude = record.get("longitude") != null ? (Double) record.get("longitude") : null;

            int isIntersection = (locationStreetName != null && locationStreetName.contains("INTERSECTION")) ? 1 : 0;
            String streetName1, streetName2;
            if (isIntersection == 1) {
                String[] parts = locationStreetName.split("&");
                streetName1 = parts[0].substring("INTERSECTION".length()).trim();
                streetName2 = parts.length > 1 ? parts[1].trim() : null;
            } else {
                streetName1 = locationStreetName;
                streetName2 = null;
            }
            streetName1 = (streetName1 != null && !streetName1.isEmpty()) ? streetName1 : "UNKNOWN";

            BigDecimal latitudeDecimal = latitude != null ? BigDecimal.valueOf(latitude).setScale(7, BigDecimal.ROUND_HALF_UP) : null;
            BigDecimal longitudeDecimal = longitude != null ? BigDecimal.valueOf(longitude).setScale(7, BigDecimal.ROUND_HALF_UP) : null;

            String compositeKey = String.format("%s|%s|%s|%s|%s|%s",
                    streetName1, streetName2 != null ? streetName2 : "", neighborhood, zipcode != null ? zipcode : "",
                    latitudeDecimal != null ? latitudeDecimal.toString() : "null",
                    longitudeDecimal != null ? longitudeDecimal.toString() : "null");

            out.output(KV.of(compositeKey, null));
        }
    }

    static class ConvertBQToMapFn extends DoFn<TableRow, KV<String, Integer>> {
        @ProcessElement
        public void processElement(@Element TableRow row, OutputReceiver<KV<String, Integer>> out) {
            String streetName1 = (String) row.get("street_name1");
            String streetName2 = (String) row.get("street_name2");
            String neighborhood = (String) row.get("neighborhood");
            String zipcode = (String) row.get("zipcode");
            String latitude = row.get("latitude") != null ? row.get("latitude").toString() : "null";
            String longitude = row.get("longitude") != null ? row.get("longitude").toString() : "null";
            Integer locationKey = ((Long) row.get("location_key")).intValue();

            String compositeKey = String.format("%s|%s|%s|%s|%s|%s",
                    streetName1, streetName2 != null ? streetName2 : "", neighborhood, zipcode != null ? zipcode : "",
                    latitude, longitude);
            out.output(KV.of(compositeKey, locationKey));
        }
    }

    static class AssignLocationKeyFn extends DoFn<KV<String, Void>, TableRow> {
        private final PCollectionView<Map<String, Integer>> existingLocationsView;
        private int nextLocationKey = 1;

        AssignLocationKeyFn(PCollectionView<Map<String, Integer>> existingLocationsView) {
            this.existingLocationsView = existingLocationsView;
        }

        @ProcessElement
        public void processElement(@Element KV<String, Void> kv, OutputReceiver<TableRow> out, ProcessContext c) {
            String compositeKey = kv.getKey();
            String[] fields = compositeKey.split("\\|", -1);
            Map<String, Integer> existingLocations = c.sideInput(existingLocationsView);
            Integer locationKey = existingLocations.getOrDefault(compositeKey, nextLocationKey++);

            TableRow row = new TableRow();
            row.set("location_key", locationKey);
            row.set("is_intersection", fields[0].equals("UNKNOWN") && fields[1].isEmpty() ? 0 : (fields[1].isEmpty() ? 0 : 1));
            row.set("street_name1", fields[0]);
            row.set("street_name2", fields[1].isEmpty() ? null : fields[1]);
            row.set("neighborhood", fields[2]);
            row.set("city", "Boston");
            row.set("state", "MA");
            row.set("zipcode", fields[3].isEmpty() ? null : fields[3]);
            row.set("latitude", fields[4].equals("null") ? null : new BigDecimal(fields[4])); // NUMERIC
            row.set("longitude", fields[5].equals("null") ? null : new BigDecimal(fields[5]));
            out.output(row);
        }
    }

    private static TableSchema getTableSchema() {
        return new TableSchema().setFields(Arrays.asList(
                new TableFieldSchema().setName("location_key").setType("INT64"),
                new TableFieldSchema().setName("is_intersection").setType("INT64"),
                new TableFieldSchema().setName("street_name1").setType("STRING"),
                new TableFieldSchema().setName("street_name2").setType("STRING").setMode("NULLABLE"),
                new TableFieldSchema().setName("neighborhood").setType("STRING"),
                new TableFieldSchema().setName("city").setType("STRING"),
                new TableFieldSchema().setName("state").setType("STRING"),
                new TableFieldSchema().setName("zipcode").setType("STRING").setMode("NULLABLE"),
                new TableFieldSchema().setName("latitude").setType("NUMERIC").setMode("NULLABLE"),
                new TableFieldSchema().setName("longitude").setType("NUMERIC").setMode("NULLABLE")
        ));
    }
}