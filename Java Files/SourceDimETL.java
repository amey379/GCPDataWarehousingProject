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

public class SourceDimETL {

    public interface TransformOptions extends DataflowPipelineOptions {
        @Description("Path to input Parquet file in GCS")
        @Default.String("gs://my-311-service-data-lake/silver/stage_boston*.parquet")  // Read all Parquet files
        String getInputPath();
        void setInputPath(String value);

        @Description("BigQuery Table for dim_source")
        @Default.String("boston311servicerequest.dimensions.dim_source")
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

        // Define Avro Schema for Parquet
        String schemaJson = "{"
                + "\"type\": \"record\","
                + "\"name\": \"Boston311\","
                + "\"fields\": ["
                + "{\"name\": \"source\", \"type\": [\"null\", \"string\"], \"default\": null}"
                + "]}";
        Schema avroSchema = new Schema.Parser().parse(schemaJson);

        // Step 1: Read Parquet and extract the source column
        PCollection<String> sources = pipeline
                .apply("Read Parquet", ParquetIO.read(avroSchema).from(options.getInputPath()))
                .apply("Extract Source Column", ParDo.of(new ExtractSourceFn()))
                .apply("Remove Duplicates", Distinct.create());

        // Step 2: Read existing sources from BigQuery
        PCollectionView<Map<String, Integer>> existingSources = pipeline
                .apply("Read Existing Sources", BigQueryIO.readTableRows()
                        .from(options.getBQTable()))
                .apply("Convert to Map", ParDo.of(new ConvertBQToMapFn()))
                .apply(View.asMap());

        // Step 3: Assign auto-incrementing source_key
        PCollection<TableRow> newSourcesWithKeys = sources
                .apply("Assign Source Key", ParDo.of(new AssignSourceKeyFn(existingSources))
                        .withSideInputs(existingSources));

        // Step 4: Ensure "Not Available" row is added
        PCollection<TableRow> notAvailableRow = pipeline
                .apply("Create Default Row", Create.of(1))  // Create a dummy PCollection with one element
                .apply("Generate Not Available Row", ParDo.of(new DoFn<Integer, TableRow>() {
                    @ProcessElement
                    public void processElement(@Element Integer input, OutputReceiver<TableRow> out) {
                        TableRow row = new TableRow();
                        row.set("source_key", -1);
                        row.set("source", "Not Available");
                        out.output(row);
                    }
                }));

        // Step 5: Merge and Write to BigQuery
        PCollectionList<TableRow> combinedRows = PCollectionList.of(newSourcesWithKeys).and(notAvailableRow);
        combinedRows.apply("Flatten", Flatten.pCollections())
                .apply("Write to BigQuery", BigQueryIO.writeTableRows()
                        .to(options.getBQTable())
                        .withSchema(getTableSchema())
                        .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
                        .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND));

        pipeline.run().waitUntilFinish();
    }

    // Function to extract the "source" column from Parquet
    static class ExtractSourceFn extends DoFn<GenericRecord, String> {
        @ProcessElement
        public void processElement(@Element GenericRecord record, OutputReceiver<String> out) {
            String source = record.get("source") != null ? record.get("source").toString().trim().toUpperCase() : null;
            if (source != null && !source.isEmpty()) {
                out.output(source);
            }
        }
    }

    // Function to Convert BigQuery Rows to a Lookup Map

    static class ConvertBQToMapFn extends DoFn<TableRow, KV<String, Integer>> {
        @ProcessElement
        public void processElement(@Element TableRow row, OutputReceiver<KV<String, Integer>> out) {
            String source = (String) row.get("source");
            Object sourceKeyObj = row.get("source_key");
            Integer sourceKey = null;
    
            if (sourceKeyObj != null) {
                if (sourceKeyObj instanceof Long) {
                    sourceKey = ((Long) sourceKeyObj).intValue();
                } else if (sourceKeyObj instanceof String) {
                    try {
                        sourceKey = Integer.parseInt((String) sourceKeyObj);
                    } catch (NumberFormatException e) {
                        sourceKey = -1;  // Default value in case of an error
                    }
                }
            }
    
            out.output(KV.of(source, sourceKey));
        }
    }
    

    // Function to Assign Auto-Incrementing Source Key
    static class AssignSourceKeyFn extends DoFn<String, TableRow> {
        private final PCollectionView<Map<String, Integer>> existingSourcesView;
        private int nextSourceKey = 1; // Default starting key

        AssignSourceKeyFn(PCollectionView<Map<String, Integer>> existingSourcesView) {
            this.existingSourcesView = existingSourcesView;
        }

        @ProcessElement
        public void processElement(@Element String source, OutputReceiver<TableRow> out, ProcessContext c) {
            Map<String, Integer> existingSources = c.sideInput(existingSourcesView);
            Integer sourceKey = existingSources.getOrDefault(source, nextSourceKey++);

            TableRow row = new TableRow();
            row.set("source_key", sourceKey);
            row.set("source", source);
            out.output(row);
        }
    }

    // Define BigQuery Schema
    private static TableSchema getTableSchema() {
        return new TableSchema().setFields(Arrays.asList(
                new TableFieldSchema().setName("source_key").setType("INT64"),
                new TableFieldSchema().setName("source").setType("STRING")
        ));
    }
}
