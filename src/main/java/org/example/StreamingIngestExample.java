package org.example;

import java.io.IOException;
import java.util.Iterator;
import com.google.api.services.bigquery.model.TableRow;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.StreamingOptions;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.Validation.Required;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.joda.time.Duration;
import org.json.simple.parser.JSONParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.json.JSONObject;

public class StreamingIngestExample {
    /* Define your own configuration options. Add your own arguments to be processed
    * by the command-line parser, and specify default values for them.
     */
    private static final Logger LOG = LoggerFactory.getLogger(StreamingIngestExample.class);

    public interface StreamingIngestExampleOptions extends StreamingOptions {
        @Description("The Cloud Pub/Sub topic to read from.")
        @Required
        String getInputTopic();

        void setInputTopic(String value);

        @Description("GCS output bucket")
        @Required
        String getDestinationBucket();

        void setDestinationBucket(String value);

        @Description("Full bq table name")
        @Required
        String getBQTable();

        void setBQTable(String value);
    }

    static class AggregateGroupBy extends DoFn<KV<String, Iterable<String>>, TableRow> {
        @ProcessElement
        public void processElement(ProcessContext c) {
            int clicks = 0;
            int purchases = 0;
            int views = 0;
            int counter = 0;
            String name = null;
            String email = null;
            String cust_id = null;

            KV<String, Iterable<String>> kvPair = (KV<String, Iterable<String>>) c.element();
            String key = kvPair.getKey();
            Iterable<String> iterable = kvPair.getValue();
            Iterator iterator = iterable.iterator();
            while (iterator.hasNext()) {

                String next = (String) iterator.next();
                JSONObject jsonObject = new JSONObject(next);
                String action = (String) jsonObject.get("action");

                if (counter == 0) {
                    cust_id = (String) jsonObject.get("cust_id");
                    name = (String) jsonObject.get("name");
                    email = (String) jsonObject.get("email");
                }

                switch (action) {
                    case "click":
                        clicks++;
                        break;
                    case "purchase":
                        purchases++;
                        break;
                    case "view":
                        views++;
                        break;
                }
            }
            TableRow row = new TableRow();
            row.set("cust_id", cust_id);
            row.set("name", name);
            row.set("email", email);
            row.set("clicks", clicks);
            row.set("purchases", purchases);
            row.set("views", views);

            c.output(row);
        }
    }
    public static void main(String[] args) throws IOException {
        // The maximum number of shards when writing output.
        int numShards = 1;
        JSONParser parser = new JSONParser();
        // PipelineOptions options =
        //         PipelineOptionsFactory.fromArgs(args).create();

        StreamingIngestExampleOptions options =
            PipelineOptionsFactory.fromArgs(args).withValidation().as(StreamingIngestExampleOptions.class);

        Pipeline pipeline = Pipeline.create(options);

        PCollection<String> messages = pipeline
                // 1) Read string messages from a Pub/Sub topic.
                .apply("Read PubSub Messages", PubsubIO.readStrings().fromTopic(options.getInputTopic()))
                // 2) Group the messages into fixed-sized minute intervals.
                .apply(Window.into(FixedWindows.of(Duration.standardMinutes(1))));

        messages.apply(TextIO
                        .write()
                        .to(options.getDestinationBucket())
                        .withWindowedWrites()
                        .withSuffix(".json"));

        PCollection<KV<String, String>> messages2 = messages
                .apply("Re-align to KV",
                        ParDo.of(
                                new DoFn<String, KV<String, String>>() {
                                    @ProcessElement
                                    public void processElement(ProcessContext c) {
                                        JSONObject json = new JSONObject(c.element());
                                        c.output(KV.of((String)json.get("cust_id"), c.element()));
                                    }
                                }
                        ));

        PCollection<KV<String, Iterable<String>>> groupByResult = messages2.apply("GroupBy Customer ID", GroupByKey.create());

        PCollection<TableRow> rowCollection = groupByResult.apply(ParDo.of(
                new AggregateGroupBy()));

        rowCollection.apply("Write to BigQuery", BigQueryIO.writeTableRows()
                .to(options.getBQTable())
                //.withSchema(schema)
                .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_NEVER)
                .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND));

        // Execute the pipeline and wait until it finishes running.
        pipeline.run().waitUntilFinish();
    }
}