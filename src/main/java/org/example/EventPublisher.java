
package org.example;

import com.google.api.core.ApiFuture;
import com.google.cloud.pubsub.v1.Publisher;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.PubsubMessage;
import com.google.pubsub.v1.TopicName;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.io.StringWriter;

import org.json.simple.JSONObject;

public class EventPublisher {
    private static List<String> names = Arrays.asList("Albert Sunwoo",
            "Andrew Panger",
            "Dustin Williams",
            "Joe Montana",
            "Tom Brady",
            "Drew Bledsoe",
            "Patrick Mahomes",
            "Lionel Messi",
            "Alex Morgan",
            "Josh Allen");

    private static  List<String> emails = Arrays.asList("Albert_Sunwoo",
            "Andrew_Panger",
            "Dustin_Williams",
            "Joe_Montana",
            "Tom_Brady",
            "Drew_Bledsoe",
            "Patrick_Mahomes",
            "Lionel_Messi",
            "Alex_Morgan",
            "Josh_Allen");

    private static int eventDurationInMinutes = 30;
    private static final int eventIntervalInMS = 250;

    private static List<String> actions = Arrays.asList("click", "purchase", "view");
    public static void main(String... args) throws Exception {
        // TODO(developer): Replace these variables before running the sample.
        String projectId = "red-road-356318";
        String topicId = "streaming-ingest";

        publisherExample(projectId, topicId);
    }

    public static void publisherExample(String projectId, String topicId)
            throws IOException, ExecutionException, InterruptedException {
        TopicName topicName = TopicName.of(projectId, topicId);

        Publisher publisher = null;
        List<JSONObject> custList = createMessages();
        Random rnd = new Random();

        List<String> actions = Arrays.asList("click", "purchase", "view");

        try {
            // Create a publisher instance with default settings bound to the topic
            publisher = Publisher.newBuilder(topicName).build();

            JSONObject jsonObject = null;
            String message = null;
            int timeDuration = 0;
            int maxTimeLength = eventDurationInMinutes * 60 * 1000;
            while(timeDuration < maxTimeLength) {
                int randomSelection = rnd.nextInt(10);

                jsonObject = custList.get(randomSelection);

                jsonObject.put("action", actions.get(rnd.nextInt(3)));
                jsonObject.put("event_id",getSaltString());

                StringWriter out = new StringWriter();
                try {
                    jsonObject.writeJSONString(out);
                }catch(java.io.IOException ioe){
                    System.out.println(ioe);
                }
                message = out.toString();

                ByteString data = ByteString.copyFromUtf8(message);
                PubsubMessage pubsubMessage = PubsubMessage.newBuilder().setData(data).build();

                // Once published, returns a server-assigned message id (unique within the topic)
                ApiFuture<String> messageIdFuture = publisher.publish(pubsubMessage);
                String messageId = messageIdFuture.get();
                System.out.println("Published message ID: " + messageId + " : " + message);
                Thread.sleep(eventIntervalInMS);
                timeDuration = timeDuration + eventIntervalInMS;
            }

        } finally {
            if (publisher != null) {
                // When finished with the publisher, shutdown to free up resources.
                publisher.shutdown();
                publisher.awaitTermination(1, TimeUnit.MINUTES);
            }
        }
    }

    private static String getSaltString() {
        String SALTCHARS = "ABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890";
        StringBuilder salt = new StringBuilder();
        Random rnd = new Random();
        while (salt.length() < 18) { // length of the random string.
            int index = (int) (rnd.nextFloat() * SALTCHARS.length());
            salt.append(SALTCHARS.charAt(index));
        }
        String saltStr = salt.toString();
        return saltStr;
    }

    public static List<JSONObject> createMessages(){
        List<JSONObject> messageList= new ArrayList<JSONObject>();

        for(int i=0; i<10; i++){
            JSONObject obj = new JSONObject();
            String idString = getSaltString();
            obj.put("cust_id",idString);
            obj.put("name",names.get(i));
            obj.put("email",emails.get(i)+"@foo.org");
//            obj.put("action", actions.get(rnd.nextInt(3)));
//            obj.put("event_id",getSaltString());

            messageList.add(obj);
        }

        return messageList;
    }
}