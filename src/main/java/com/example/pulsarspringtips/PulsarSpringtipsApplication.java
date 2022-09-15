package com.example.pulsarspringtips;

import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.TypedMessageBuilder;
import org.apache.pulsar.common.schema.SchemaType;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.pulsar.annotation.PulsarListener;
import org.springframework.pulsar.core.PulsarTemplate;
import org.springframework.pulsar.core.PulsarTopic;
import org.springframework.pulsar.core.TypedMessageBuilderCustomizer;

@SpringBootApplication
public class PulsarSpringtipsApplication {

    public static void main(String[] args) {
        SpringApplication.run(PulsarSpringtipsApplication.class, args);
    }

    private final static String TOPIC_NAME = "hello-pulsar";

    @Bean
    PulsarTopic pulsarTopic() {
        // if you dont specify a MessageRouter, then itll round robin pick a partition for you
        return PulsarTopic.builder("thetopic").numberOfPartitions(5).build();
    }

    record Customer(Integer id, String name) {
    }

    @Bean
    ApplicationRunner runner(
            PulsarTemplate<String> stringPulsarTemplate,
            PulsarTemplate<Customer> customerPulsarTemplate) {
        return (args) -> {
            stringPulsarTemplate
                    .newMessage("Hello Pulsar World!")
                    /*         .withCustomRouter(new MessageRouter() {
                                 @Override
                                 public int choosePartition(Message<?> msg, TopicMetadata metadata) {
                                     // if u want to make sure that msg foo ends up in partition 0, then use this interface
                                     return MessageRouter.super.choosePartition(msg, metadata);
                                 }
                             })*/
                    .withMessageCustomizer(new TypedMessageBuilderCustomizer<>() {
                        @Override
                        public void customize(TypedMessageBuilder<String> messageBuilder) {
//                        messageBuilder.key("routingkey"  ).value( "routingkeyvalue");
                            messageBuilder.property("header", "header value");
                        }
                    }) // key for key shared subscription
                    .withTopic(TOPIC_NAME)
                    .send();

            customerPulsarTemplate.setSchema(Schema.JSON(Customer.class));
            customerPulsarTemplate.send("jsonSchema", new Customer(1, "Chris"));
        };
    }

    @PulsarListener(topics = "jsonSchema", schemaType = SchemaType.JSON)
    void listen3(Customer message) {
        System.out.println("Message Received: " + message);
    }

    //
    @PulsarListener(topics = "anotherone")
    void listen2(String message) {
        System.out.println("Message Received: " + message);
    }

    //
    @PulsarListener(topics = TOPIC_NAME, subscriptionType = "failover",
             concurrency = "5", //"${my-concurrency-value:5}", // only use this is the subscription type is failover
            subscriptionName = "hello-pulsar-subscription")
    void listen(Message<String> message) {
        System.out.println("Message Received: " + message.getValue());
    }
}


