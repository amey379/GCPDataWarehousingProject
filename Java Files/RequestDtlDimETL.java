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

public class RequestDtlDimETL {

    public interface TransformOptions extends DataflowPipelineOptions {
        @Description("Path to input Parquet file in GCS")
        @Default.String("gs://my-311-service-data-lake/silver/stage*.parquet")
        String getInputPath();
        void setInputPath(String value);

        @Description("BigQuery Table for dim_request_dtl")
        @Default.String("boston311servicerequest.dimensions.dim_request_dtl")
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

        // Full Avro Schema from BostonRequestStgLoad
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

        // Step 1: Read Parquet and extract request detail columns
        PCollection<KV<String, Void>> requestDetails = pipeline
                .apply("Read Parquet", ParquetIO.read(avroSchema).from(options.getInputPath()))
                .apply("Extract Request Details", ParDo.of(new ExtractRequestDetailsFn()))
                .apply("Remove Duplicates", Distinct.create());

        // Step 2: Read existing request details from BigQuery
        PCollectionView<Map<String, Integer>> existingRequestDetails = pipeline
                .apply("Read Existing Request Details", BigQueryIO.readTableRows()
                        .from(options.getBQTable()))
                .apply("Convert to Map", ParDo.of(new ConvertBQToMapFn()))
                .apply(View.asMap());

        // Step 3: Assign auto-incrementing request_dtl_key
        PCollection<TableRow> newRequestDetailsWithKeys = requestDetails
                .apply("Assign Request Detail Key", ParDo.of(new AssignRequestDetailKeyFn(existingRequestDetails))
                        .withSideInputs(existingRequestDetails));

        // Step 4: Ensure "Not Available" row is added
        PCollection<TableRow> notAvailableRow = pipeline
                .apply("Create Default Row", Create.of(1))
                .apply("Generate Not Available Row", ParDo.of(new DoFn<Integer, TableRow>() {
                    @ProcessElement
                    public void processElement(@Element Integer input, OutputReceiver<TableRow> out) {
                        TableRow row = new TableRow();
                        row.set("request_dtl_key", -1);
                        row.set("case_title", "Not Available");
                        row.set("subject", "Not Available");
                        row.set("reason", "Not Available");
                        row.set("type", "Not Available");
                        row.set("department", "Not Available");
                        row.set("queue", "Not Available");
                        out.output(row);
                    }
                }));

        // Step 5: Merge and Write to BigQuery
        PCollectionList<TableRow> combinedRows = PCollectionList.of(newRequestDetailsWithKeys).and(notAvailableRow);
        combinedRows.apply("Flatten", Flatten.pCollections())
                .apply("Write to BigQuery", BigQueryIO.writeTableRows()
                        .to(options.getBQTable())
                        .withSchema(getTableSchema())
                        .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
                        .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND));

        pipeline.run().waitUntilFinish();
    }

    static class ExtractRequestDetailsFn extends DoFn<GenericRecord, KV<String, Void>> {
        @ProcessElement
        public void processElement(@Element GenericRecord record, OutputReceiver<KV<String, Void>> out) {
            String caseTitle = record.get("case_title") != null ? record.get("case_title").toString().trim().toUpperCase() : "Not Available";
            String subject = record.get("subject") != null ? record.get("subject").toString().trim().toUpperCase() : "Not Available";
            String reason = record.get("reason") != null ? record.get("reason").toString().trim().toUpperCase() : "Not Available";
            String type = record.get("type") != null ? record.get("type").toString().trim().toUpperCase() : "Not Available";
            String department = record.get("department") != null ? record.get("department").toString().trim().toUpperCase() : "Not Available";
            String queue = record.get("queue") != null ? record.get("queue").toString().trim().toUpperCase() : "Not Available";

            String compositeKey = String.format("%s|%s|%s|%s|%s|%s", caseTitle, subject, reason, type, department, queue);
            out.output(KV.of(compositeKey, null));
        }
    }

    static class ConvertBQToMapFn extends DoFn<TableRow, KV<String, Integer>> {
        @ProcessElement
        public void processElement(@Element TableRow row, OutputReceiver<KV<String, Integer>> out) {
            String caseTitle = (String) row.get("case_title");
            String subject = (String) row.get("subject");
            String reason = (String) row.get("reason");
            String type = (String) row.get("type");
            String department = (String) row.get("department");
            String queue = (String) row.get("queue");
            Integer requestDtlKey = null;
            if (row.get("request_dtl_key") != null) {
                Object keyObject = row.get("request_dtl_key");
            if (keyObject instanceof Long) {
                requestDtlKey = ((Long) keyObject).intValue();
            } else if (keyObject instanceof String) {
            try {
                requestDtlKey = Integer.parseInt((String) keyObject);
            } catch (NumberFormatException e) {
                requestDtlKey = null; // Handle invalid numbers gracefully
            }
    }
}


            String compositeKey = String.format("%s|%s|%s|%s|%s|%s", caseTitle, subject, reason, type, department, queue);
            out.output(KV.of(compositeKey, requestDtlKey));
        }
    }

    static class AssignRequestDetailKeyFn extends DoFn<KV<String, Void>, TableRow> {
        private final PCollectionView<Map<String, Integer>> existingRequestDetailsView;
        private int nextRequestDtlKey = 1;

        AssignRequestDetailKeyFn(PCollectionView<Map<String, Integer>> existingRequestDetailsView) {
            this.existingRequestDetailsView = existingRequestDetailsView;
        }

        @ProcessElement
        public void processElement(@Element KV<String, Void> kv, OutputReceiver<TableRow> out, ProcessContext c) {
            String compositeKey = kv.getKey();
            String[] fields = compositeKey.split("\\|", -1);
            Map<String, Integer> existingRequestDetails = c.sideInput(existingRequestDetailsView);
            Integer requestDtlKey = existingRequestDetails.getOrDefault(compositeKey, nextRequestDtlKey++);

            TableRow row = new TableRow();
            row.set("request_dtl_key", requestDtlKey);
            row.set("case_title", fields[0]);
            row.set("subject", fields[1]);
            row.set("reason", fields[2]);
            row.set("type", fields[3]);
            row.set("department", fields[4]);
            row.set("queue", fields[5]);
            out.output(row);
        }
    }

    private static TableSchema getTableSchema() {
        return new TableSchema().setFields(Arrays.asList(
                new TableFieldSchema().setName("request_dtl_key").setType("INT64"),
                new TableFieldSchema().setName("case_title").setType("STRING"),
                new TableFieldSchema().setName("subject").setType("STRING"),
                new TableFieldSchema().setName("reason").setType("STRING"),
                new TableFieldSchema().setName("type").setType("STRING"),
                new TableFieldSchema().setName("department").setType("STRING"),
                new TableFieldSchema().setName("queue").setType("STRING")
        ));
    }
}