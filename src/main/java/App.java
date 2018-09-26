import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutures;
import com.google.cloud.ServiceOptions;
import com.google.cloud.pubsub.v1.Publisher;
import com.google.cloud.pubsub.v1.Subscriber;
import com.google.cloud.pubsub.v1.SubscriptionAdminClient;
import com.google.cloud.pubsub.v1.TopicAdminClient;
import com.google.common.base.Charsets;
import com.google.common.base.Stopwatch;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.*;
import org.threeten.bp.Duration;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

public class App {

    private static final int MESSAGE_COUNT = 1000;

    public static void main(String[] args) throws Exception {
        final String projectId = ServiceOptions.getDefaultProjectId();
        final ProjectTopicName topicName = ProjectTopicName.of(projectId, "pubsub-fun-" + System.currentTimeMillis());
        final ProjectSubscriptionName subscriptionName = ProjectSubscriptionName.of(projectId, topicName.getTopic() + "-subscription");

        final TopicAdminClient topicAdminClient = TopicAdminClient.create();
        final SubscriptionAdminClient subscriptionAdminClient = SubscriptionAdminClient.create();
        final AtomicReference<Topic> topic = new AtomicReference<>();
        final AtomicReference<Subscription> subscription = new AtomicReference<>();
        try {
            Runtime.getRuntime().addShutdownHook(new Thread(() -> cleanup(topicName, subscriptionName, topicAdminClient, subscriptionAdminClient, topic, subscription)));

            System.out.println(String.format("Creating topic %s", topicName));
            topic.set(topicAdminClient.createTopic(topicName));

            System.out.println(String.format("Creating subscription %s", subscriptionName));
            subscription.set(subscriptionAdminClient.createSubscription(subscriptionName, topicName, PushConfig.newBuilder().build(), 10));

            System.out.println(String.format("Publishing %d messages", MESSAGE_COUNT));
            final Publisher publisher = Publisher.newBuilder(topicName).build();
            final List<ApiFuture<String>> futures = new ArrayList<>(MESSAGE_COUNT);
            for (int i = 0; i < MESSAGE_COUNT; i++) {
                futures.add(publisher.publish(createMessage(i)));
            }
            ApiFutures.allAsList(futures).get();
            publisher.shutdown();
            publisher.awaitTermination(10, TimeUnit.SECONDS);
            futures.clear();

            System.out.println("Consuming messages from subscription");
            final AtomicInteger received = new AtomicInteger(0);
            final Stopwatch sw = Stopwatch.createUnstarted();
            final Subscriber subscriber = Subscriber.newBuilder(subscriptionName, (msg, ack) -> {
                    try {
                        sw.reset();
                        Thread.sleep(1000);
                        received.incrementAndGet();
                        ack.ack();
                        sw.start();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                })
                .setMaxAckExtensionPeriod(Duration.ZERO)
                .build();
            subscriber.startAsync().awaitRunning();
            sw.start();

            while (true) {
                System.out.println(String.format("\u001b[1A\u001b[1000D%d received", received.get()));
                if (sw.elapsed(TimeUnit.SECONDS) >= 10 || received.get() > MESSAGE_COUNT * 2) {
                    break;
                }
                Thread.sleep(1000);
            }
            subscriber.stopAsync().awaitTerminated(10, TimeUnit.SECONDS);

            System.out.println(String.format("Received %d messages", received.get()));
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            cleanup(topicName, subscriptionName, topicAdminClient, subscriptionAdminClient, topic, subscription);
        }
    }

    private static PubsubMessage createMessage(int index) {
        return PubsubMessage.newBuilder()
            .setData(ByteString.copyFrom(String.valueOf(index), Charsets.UTF_8))
            .build();
    }

    private static void cleanup(ProjectTopicName topicName, ProjectSubscriptionName subscriptionName, TopicAdminClient topicAdminClient, SubscriptionAdminClient subscriptionAdminClient, AtomicReference<Topic> topic, AtomicReference<Subscription> subscription) {
        if (subscription.get() != null) {
            subscription.set(null);
            System.out.println("Deleting subscription");
            subscriptionAdminClient.deleteSubscription(subscriptionName);
            subscriptionAdminClient.close();
        }

        if (topic.get() != null) {
            topic.set(null);
            System.out.println("Deleting topic");
            topicAdminClient.deleteTopic(topicName);
            topicAdminClient.close();
        }
    }

}
