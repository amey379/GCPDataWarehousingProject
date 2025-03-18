package main.java.com.example;

import org.apache.beam.runners.dataflow.DataflowRunner;
import org.apache.beam.sdk.Pipeline;
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

						   
						   
										  
import java.util.*;
								   
								  

public class DimDateTimeETL {

    public interface TransformOptions extends DataflowPipelineOptions {
        @Description("BigQuery Table for dim_date")
        @Default.String("boston311servicerequest.dimensions.dim_date")
        String getDateTable();
        void setDateTable(String value);

        @Description("BigQuery Table for dim_time")
        @Default.String("boston311servicerequest.dimensions.dim_time")
        String getTimeTable();
        void setTimeTable(String value);
    }

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

        // ✅ Generate Date Sequence (2015-01-01 to 2040-12-31)
        List<TableRow> dateRows = new ArrayList<>();
        Calendar calendar = Calendar.getInstance();
        calendar.set(2015, Calendar.JANUARY, 1);
        Calendar endCalendar = Calendar.getInstance();
        endCalendar.set(2040, Calendar.DECEMBER, 31);

        while (!calendar.after(endCalendar)) {
            int dateKey = Integer.parseInt(String.format("%tY%tm%td", calendar, calendar, calendar));
            String date = String.format("%tY-%tm-%td", calendar, calendar, calendar);
																			

            TableRow row = new TableRow();
            row.set("date_key", dateKey);
            row.set("date", date);
            row.set("year", calendar.get(Calendar.YEAR));
            row.set("month", calendar.get(Calendar.MONTH) + 1);
            row.set("day", calendar.get(Calendar.DAY_OF_MONTH));
            dateRows.add(row);

            calendar.add(Calendar.DAY_OF_MONTH, 1);
        }
											  
												 
																						   
																					   

        // ✅ Generate Time Sequence (00:00:00 to 23:59:59)
        List<TableRow> timeRows = new ArrayList<>();
        for (int h = 0; h < 24; h++) {
            for (int m = 0; m < 60; m++) {
                for (int s = 0; s < 60; s++) {
                    int timeKey = Integer.parseInt(String.format("%02d%02d%02d", h, m, s));
                    String time = String.format("%02d:%02d:%02d", h, m, s);

                    TableRow row = new TableRow();
                    row.set("time_key", timeKey);
                    row.set("time", time);
                    row.set("hour", h);
                    row.set("minute", m);
                    row.set("second", s);
                    timeRows.add(row);
                }
            }
        }

        // ✅ Load Dim Date Table into BigQuery
        pipeline
                .apply("Create Dim Date Rows", Create.of(dateRows))
                .apply("Write Dim Date to BigQuery", BigQueryIO.writeTableRows()
                        .to(options.getDateTable())
                        .withSchema(getDateTableSchema())
                        .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
                        .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND));

        // ✅ Load Dim Time Table into BigQuery
        pipeline
                .apply("Create Dim Time Rows", Create.of(timeRows))
                .apply("Write Dim Time to BigQuery", BigQueryIO.writeTableRows()
                        .to(options.getTimeTable())
                        .withSchema(getTimeTableSchema())
                        .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
                        .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND));
												   
												 
							
		 
	 

        pipeline.run().waitUntilFinish();
																	
					   
																						   
										  
																									  
											 
											
												
												
							
		 
    }

    // ✅ Define Schema for dim_date
    private static TableSchema getDateTableSchema() {
        return new TableSchema().setFields(Arrays.asList(
                new TableFieldSchema().setName("date_key").setType("INT64"),
                new TableFieldSchema().setName("date").setType("DATE"),
                new TableFieldSchema().setName("year").setType("INT64"),
                new TableFieldSchema().setName("month").setType("INT64"),
                new TableFieldSchema().setName("day").setType("INT64")
        ));
    }

    // ✅ Define Schema for dim_time
    private static TableSchema getTimeTableSchema() {
        return new TableSchema().setFields(Arrays.asList(
                new TableFieldSchema().setName("time_key").setType("INT64"),
                new TableFieldSchema().setName("time").setType("TIME"),
                new TableFieldSchema().setName("hour").setType("INT64"),
                new TableFieldSchema().setName("minute").setType("INT64"),
                new TableFieldSchema().setName("second").setType("INT64")
        ));
    }
}
