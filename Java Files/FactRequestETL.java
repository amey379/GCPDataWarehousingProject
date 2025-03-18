package com.example;  // Adjusted package to match typical Maven structure

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
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import java.math.BigDecimal;
import java.util.*;

public class FactRequestETL {

    public interface TransformOptions extends DataflowPipelineOptions {
        @Description("Path to input Parquet file in GCS")
        @Default.String("gs://my-311-service-data-lake/silver/*.parquet")
        String getInputPath();
        void setInputPath(String value);

        @Description("BigQuery Table for fact_request")
        @Default.String("boston311servicerequest.fact.fact_request")
        String getBQTable();
        void setBQTable(String value);
    }

    private static final DateTimeFormatter DATE_FORMAT = DateTimeFormat.forPattern("yyyy-MM-dd");
    private static final DateTimeFormatter TIME_FORMAT = DateTimeFormat.forPattern("HH:mm:ss");

    public static void main(String[] args) {
        TransformOptions options = PipelineOptionsFactory.fromArgs(args)
                .withValidation()
                .as(TransformOptions.class);

        options.setRunner(DataflowRunner.class);
        options.setProject("boston311servicerequest");
        options.setRegion("us-central1");
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

        // Read Parquet data
        PCollection<GenericRecord> input = pipeline
                .apply("Read Parquet", ParquetIO.read(avroSchema).from(options.getInputPath()));

        // Load dimension tables as side inputs
        PCollectionView<Map<String, Integer>> dateDimView = pipeline
                .apply("Read Dim Date", BigQueryIO.readTableRows().from("boston311servicerequest.dimensions.dim_date"))
                .apply("Map Date Keys", ParDo.of(new MapDateKeysFn()))
                .apply(View.asMap());

        PCollectionView<Map<String, Integer>> timeDimView = pipeline
                .apply("Read Dim Time", BigQueryIO.readTableRows().from("boston311servicerequest.dimensions.dim_time"))
                .apply("Map Time Keys", ParDo.of(new MapTimeKeysFn()))
                .apply(View.asMap());

        PCollectionView<Map<String, Integer>> locationDimView = pipeline
                .apply("Read Dim Location", BigQueryIO.readTableRows().from("boston311servicerequest.dimensions.dim_location"))
                .apply("Map Location Keys", ParDo.of(new MapLocationKeysFn()))
                .apply(View.asMap());

        PCollectionView<Map<String, Integer>> locationDtlDimView = pipeline
                .apply("Read Dim Location Dtl", BigQueryIO.readTableRows().from("boston311servicerequest.dimensions.dim_location_dtl"))
                .apply("Map Location Dtl Keys", ParDo.of(new MapLocationDtlKeysFn()))
                .apply(View.asMap());

        PCollectionView<Map<String, Integer>> requestDtlDimView = pipeline
                .apply("Read Dim Request Dtl", BigQueryIO.readTableRows().from("boston311servicerequest.dimensions.dim_request_dtl"))
                .apply("Map Request Dtl Keys", ParDo.of(new MapRequestDtlKeysFn()))
                .apply(View.asMap());

        PCollectionView<Map<String, Integer>> sourceDimView = pipeline
                .apply("Read Dim Source", BigQueryIO.readTableRows().from("boston311servicerequest.dimensions.dim_source"))
                .apply("Map Source Keys", ParDo.of(new MapSourceKeysFn()))
                .apply(View.asMap());

        // Transform and join with dimension keys
        PCollection<TableRow> factRows = input
                .apply("Transform to Fact", ParDo.of(new TransformToFactFn(
                        dateDimView, timeDimView, locationDimView, locationDtlDimView, requestDtlDimView, sourceDimView))
                        .withSideInputs(dateDimView, timeDimView, locationDimView, locationDtlDimView, requestDtlDimView, sourceDimView));

        // Write to BigQuery
        factRows.apply("Write to BigQuery", BigQueryIO.writeTableRows()
                .to(options.getBQTable())
                .withSchema(getTableSchema())
                .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
                .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND));

        pipeline.run().waitUntilFinish();
    }

    static class MapDateKeysFn extends DoFn<TableRow, KV<String, Integer>> {
        @ProcessElement
        public void processElement(@Element TableRow row, OutputReceiver<KV<String, Integer>> out) {
            String dateStr = (String) row.get("date");
            Integer dateKey = parseIntFromObject(row.get("date_key"), -1);
            out.output(KV.of(dateStr, dateKey));
        }
    }

    static class MapTimeKeysFn extends DoFn<TableRow, KV<String, Integer>> {
        @ProcessElement
        public void processElement(@Element TableRow row, OutputReceiver<KV<String, Integer>> out) {
            String timeStr = (String) row.get("time");
            Integer timeKey = parseIntFromObject(row.get("time_key"), -1);
            out.output(KV.of(timeStr, timeKey));
        }
    }

    static class MapLocationKeysFn extends DoFn<TableRow, KV<String, Integer>> {
        @ProcessElement
        public void processElement(@Element TableRow row, OutputReceiver<KV<String, Integer>> out) {
            String compositeKey = String.format("%s|%s|%s|%s|%s",
                    row.get("street_name1"),
                    row.get("neighborhood"),
                    row.get("zipcode") != null ? row.get("zipcode") : "",
                    row.get("latitude") != null ? row.get("latitude").toString() : "null",
                    row.get("longitude") != null ? row.get("longitude").toString() : "null");
            Integer locationKey = parseIntFromObject(row.get("location_key"), -1);
            out.output(KV.of(compositeKey, locationKey));
        }
    }

    static class MapLocationDtlKeysFn extends DoFn<TableRow, KV<String, Integer>> {
        @ProcessElement
        public void processElement(@Element TableRow row, OutputReceiver<KV<String, Integer>> out) {
            Integer neighborhoodServicesDistrict = parseIntFromObject(row.get("neighborhood_services_district"), -1);
            String policeDistrict = (String) row.get("police_district");
            Integer fireDistrict = parseIntFromObject(row.get("fire_district"), -1);
            String pwdDistrict = (String) row.get("pwd_district");
            Integer cityCouncilDistrict = parseIntFromObject(row.get("city_council_district"), -1);
            Integer ward = parseIntFromObject(row.get("ward"), -1);
            String precinct = (String) row.get("precinct");

            String compositeKey = String.format("%s|%s|%s|%s|%s|%s|%s",
                    neighborhoodServicesDistrict.toString(),
                    policeDistrict != null ? policeDistrict : "UNKNOWN",
                    fireDistrict.toString(),
                    pwdDistrict != null ? pwdDistrict : "UNKNOWN",
                    cityCouncilDistrict.toString(),
                    ward.toString(),
                    precinct != null ? precinct : "UNKNOWN");
            Integer locationDtlKey = parseIntFromObject(row.get("location_dtl_key"), -1);
            out.output(KV.of(compositeKey, locationDtlKey));
        }
    }

    static class MapRequestDtlKeysFn extends DoFn<TableRow, KV<String, Integer>> {
        @ProcessElement
        public void processElement(@Element TableRow row, OutputReceiver<KV<String, Integer>> out) {
            String compositeKey = String.format("%s|%s|%s|%s|%s",
                    row.get("case_title") != null ? row.get("case_title") : "",
                    row.get("subject") != null ? row.get("subject") : "",
                    row.get("reason") != null ? row.get("reason") : "",
                    row.get("queue") != null ? row.get("queue") : "",
                    row.get("type") != null ? row.get("type") : "");
            Integer requestDtlKey = parseIntFromObject(row.get("request_dtl_key"), -1);
            out.output(KV.of(compositeKey, requestDtlKey));
        }
    }

    static class MapSourceKeysFn extends DoFn<TableRow, KV<String, Integer>> {
        @ProcessElement
        public void processElement(@Element TableRow row, OutputReceiver<KV<String, Integer>> out) {
            String source = (String) row.get("source");
            Integer sourceKey = parseIntFromObject(row.get("source_key"), -1);
            out.output(KV.of(source != null ? source : "", sourceKey));
        }
    }

    static class TransformToFactFn extends DoFn<GenericRecord, TableRow> {
        private final PCollectionView<Map<String, Integer>> dateDimView;
        private final PCollectionView<Map<String, Integer>> timeDimView;
        private final PCollectionView<Map<String, Integer>> locationDimView;
        private final PCollectionView<Map<String, Integer>> locationDtlDimView;
        private final PCollectionView<Map<String, Integer>> requestDtlDimView;
        private final PCollectionView<Map<String, Integer>> sourceDimView;
        private int requestIdCounter;

        TransformToFactFn(PCollectionView<Map<String, Integer>> dateDimView,
                          PCollectionView<Map<String, Integer>> timeDimView,
                          PCollectionView<Map<String, Integer>> locationDimView,
                          PCollectionView<Map<String, Integer>> locationDtlDimView,
                          PCollectionView<Map<String, Integer>> requestDtlDimView,
                          PCollectionView<Map<String, Integer>> sourceDimView) {
            this.dateDimView = dateDimView;
            this.timeDimView = timeDimView;
            this.locationDimView = locationDimView;
            this.locationDtlDimView = locationDtlDimView;
            this.requestDtlDimView = requestDtlDimView;
            this.sourceDimView = sourceDimView;
            this.requestIdCounter = 1;
        }

        @ProcessElement
        public void processElement(ProcessContext c) {
            GenericRecord record = c.element();
            Map<String, Integer> dateDim = c.sideInput(dateDimView);
            Map<String, Integer> timeDim = c.sideInput(timeDimView);
            Map<String, Integer> locationDim = c.sideInput(locationDimView);
            Map<String, Integer> locationDtlDim = c.sideInput(locationDtlDimView);
            Map<String, Integer> requestDtlDim = c.sideInput(requestDtlDimView);
            Map<String, Integer> sourceDim = c.sideInput(sourceDimView);

            TableRow row = new TableRow();
            row.set("request_id", requestIdCounter++);

            // case_enquiry_id as STRING
            Object caseEnquiryId = record.get("case_enquiry_id");
            row.set("case_enquiry_id", caseEnquiryId != null ? caseEnquiryId.toString() : null);

            // Direct fields with null-safe string conversion
            row.set("closure_reason", record.get("closure_reason") != null ? record.get("closure_reason").toString() : null);
            row.set("status", record.get("case_status") != null ? record.get("case_status").toString() : null);
            row.set("on_time", record.get("on_time") != null ? record.get("on_time").toString() : null);

            // Date and Time transformations
            Integer openDateInt = record.get("open_date") != null ? (Integer) record.get("open_date") : null;
            Long openTimeLong = record.get("open_time") != null ? (Long) record.get("open_time") : null;
            Integer slaDateInt = record.get("sla_target_date") != null ? (Integer) record.get("sla_target_date") : null;
            Long slaTimeLong = record.get("sla_target_time") != null ? (Long) record.get("sla_target_time") : null;
            Integer closeDateInt = record.get("closed_date") != null ? (Integer) record.get("closed_date") : null;
            Long closeTimeLong = record.get("closed_time") != null ? (Long) record.get("closed_time") : null;

            String openDateStr = openDateInt != null 
                ? new org.joda.time.LocalDate(0).plusDays(openDateInt).toString(DATE_FORMAT) 
                : null;
            String openTimeStr = openTimeLong != null 
                ? new org.joda.time.LocalTime(openTimeLong.intValue(), org.joda.time.chrono.ISOChronology.getInstanceUTC()).toString(TIME_FORMAT) 
                : null;
            String slaDateStr = slaDateInt != null 
                ? new org.joda.time.LocalDate(0).plusDays(slaDateInt).toString(DATE_FORMAT) 
                : null;
            String slaTimeStr = slaTimeLong != null 
                ? new org.joda.time.LocalTime(slaTimeLong.intValue(), org.joda.time.chrono.ISOChronology.getInstanceUTC()).toString(TIME_FORMAT) 
                : null;
            String closeDateStr = closeDateInt != null 
                ? new org.joda.time.LocalDate(0).plusDays(closeDateInt).toString(DATE_FORMAT) 
                : null;
            String closeTimeStr = closeTimeLong != null 
                ? new org.joda.time.LocalTime(closeTimeLong.intValue(), org.joda.time.chrono.ISOChronology.getInstanceUTC()).toString(TIME_FORMAT) 
                : null;

            row.set("open_date", openDateStr);  // DATE type
            row.set("sla_date", slaDateStr != null ? slaDateStr + " " + (slaTimeStr != null ? slaTimeStr : "00:00:00") : null);  // DATETIME type
            row.set("close_date", closeDateStr != null ? closeDateStr + " " + (closeTimeStr != null ? closeTimeStr : "00:00:00") : null);  // DATETIME type

            row.set("open_date_key", openDateStr != null ? dateDim.getOrDefault(openDateStr, -1) : -1);
            row.set("open_time_key", openTimeStr != null ? timeDim.getOrDefault(openTimeStr, -1) : -1);
            row.set("sla_date_key", slaDateStr != null ? dateDim.getOrDefault(slaDateStr, -1) : -1);
            row.set("sla_time_key", slaTimeStr != null ? timeDim.getOrDefault(slaTimeStr, -1) : -1);
            row.set("close_date_key", closeDateStr != null ? dateDim.getOrDefault(closeDateStr, -1) : -1);
            row.set("close_time_key", closeTimeStr != null ? timeDim.getOrDefault(closeTimeStr, -1) : -1);

            // Location key
            String locationStreetName = record.get("location_street_name") != null ? record.get("location_street_name").toString().trim() : null;
            String streetName1 = (locationStreetName != null && locationStreetName.contains("INTERSECTION")) 
                ? locationStreetName.split("&")[0].substring("INTERSECTION".length()).trim() 
                : (locationStreetName != null && !locationStreetName.isEmpty() ? locationStreetName : "UNKNOWN");

            Double latitudeDouble = record.get("latitude") != null ? (Double) record.get("latitude") : null;
            Double longitudeDouble = record.get("longitude") != null ? (Double) record.get("longitude") : null;
            BigDecimal latitude = latitudeDouble != null ? BigDecimal.valueOf(latitudeDouble).setScale(7, BigDecimal.ROUND_HALF_UP) : null;
            BigDecimal longitude = longitudeDouble != null ? BigDecimal.valueOf(longitudeDouble).setScale(7, BigDecimal.ROUND_HALF_UP) : null;

            String locationCompositeKey = String.format("%s|%s|%s|%s|%s",
                    streetName1,
                    record.get("neighborhood") != null ? record.get("neighborhood").toString() : "",
                    record.get("location_zipcode") != null ? record.get("location_zipcode").toString() : "",
                    latitude != null ? latitude.toString() : "null",
                    longitude != null ? longitude.toString() : "null");
            row.set("location_key", locationDim.getOrDefault(locationCompositeKey, -1));

            // Location detail key
            String neighborhoodServicesDistrict = record.get("neighborhood_services_district") != null ? record.get("neighborhood_services_district").toString().trim() : "-1";
            String policeDistrict = record.get("police_district") != null ? record.get("police_district").toString().trim().substring(0, Math.min(3, record.get("police_district").toString().length())) : "UNKNOWN";
            String fireDistrictStr = record.get("fire_district") != null ? record.get("fire_district").toString().trim() : "-1";
            String pwdDistrict = record.get("pwd_district") != null ? record.get("pwd_district").toString().trim().substring(0, Math.min(3, record.get("pwd_district").toString().length())) : "UNKNOWN";
            String cityCouncilDistrict = record.get("city_council_district") != null ? record.get("city_council_district").toString().trim() : "-1";
            String ward = record.get("ward") != null ? record.get("ward").toString().replace("WARD ", "").trim() : "-1";
            String precinct = record.get("precinct") != null ? record.get("precinct").toString().trim() : "UNKNOWN";

            String locationDtlCompositeKey = String.format("%s|%s|%s|%s|%s|%s|%s",
                    neighborhoodServicesDistrict,
                    policeDistrict,
                    fireDistrictStr,
                    pwdDistrict,
                    cityCouncilDistrict,
                    ward,
                    precinct);
            row.set("location_dtl_key", locationDtlDim.getOrDefault(locationDtlCompositeKey, -1));

            // Request detail key
            String requestDtlCompositeKey = String.format("%s|%s|%s|%s|%s",
                    record.get("case_title") != null ? record.get("case_title").toString() : "",
                    record.get("subject") != null ? record.get("subject").toString() : "",
                    record.get("reason") != null ? record.get("reason").toString() : "",
                    record.get("queue") != null ? record.get("queue").toString() : "",
                    record.get("type") != null ? record.get("type").toString() : "");
            row.set("request_dtl_key", requestDtlDim.getOrDefault(requestDtlCompositeKey, -1));

            // Source key
            String source = record.get("source") != null ? record.get("source").toString() : "";
            row.set("source_key", sourceDim.getOrDefault(source, -1));

            c.output(row);
        }
    }

    private static Integer parseIntFromObject(Object value, int defaultValue) {
        if (value == null) return defaultValue;
        if (value instanceof Number) return ((Number) value).intValue();
        if (value instanceof String) {
            try {
                return Integer.parseInt((String) value);
            } catch (NumberFormatException e) {
                return defaultValue;
            }
        }
        return defaultValue;
    }

    private static Integer parseIntFromObject(Object value) {
        return parseIntFromObject(value, -1);
    }

    private static TableSchema getTableSchema() {
        return new TableSchema().setFields(Arrays.asList(
                new TableFieldSchema().setName("request_id").setType("INTEGER"),
                new TableFieldSchema().setName("case_enquiry_id").setType("STRING"),
                new TableFieldSchema().setName("open_date").setType("DATE"),
                new TableFieldSchema().setName("sla_date").setType("DATETIME"),
                new TableFieldSchema().setName("close_date").setType("DATETIME"),
                new TableFieldSchema().setName("open_date_key").setType("INTEGER"),
                new TableFieldSchema().setName("open_time_key").setType("INTEGER"),
                new TableFieldSchema().setName("sla_date_key").setType("INTEGER"),
                new TableFieldSchema().setName("sla_time_key").setType("INTEGER"),
                new TableFieldSchema().setName("close_date_key").setType("INTEGER"),
                new TableFieldSchema().setName("close_time_key").setType("INTEGER"),
                new TableFieldSchema().setName("request_dtl_key").setType("INTEGER"),
                new TableFieldSchema().setName("source_key").setType("INTEGER"),
                new TableFieldSchema().setName("location_key").setType("INTEGER"),
                new TableFieldSchema().setName("location_dtl_key").setType("INTEGER"),
                new TableFieldSchema().setName("closure_reason").setType("STRING").setMode("NULLABLE"),
                new TableFieldSchema().setName("status").setType("STRING").setMode("NULLABLE"),
                new TableFieldSchema().setName("on_time").setType("STRING").setMode("NULLABLE")
        ));
    }
}