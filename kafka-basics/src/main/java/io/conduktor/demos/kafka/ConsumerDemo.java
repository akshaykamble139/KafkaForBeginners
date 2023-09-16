package io.conduktor.demos.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Properties;


public class ConsumerDemo {

    private static final Logger log = LoggerFactory.getLogger(ConsumerDemo.class.getSimpleName());

    public static void main(String[] args) {

        log.info("Hello world!");

        String groupId = "my-java-application";
        String topic = "demo_java";

        // create Producer properties
        Properties properties = new Properties();

        //localhost
//        properties.setProperty("bootstrap.servers","127.0.0.1:9092");

        //conduktor
        properties.setProperty("bootstrap.servers","cluster.playground.cdkt.io:9092");

        properties.setProperty("security.protocol","SASL_SSL");
        properties.setProperty("sasl.jaas.config","org.apache.kafka.common.security.plain.PlainLoginModule required username=\"3vPWJ5fo4th9elT6CD67ph\" password=\"eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJpc3MiOiJodHRwczovL2F1dGguY29uZHVrdG9yLmlvIiwic291cmNlQXBwbGljYXRpb24iOiJhZG1pbiIsInVzZXJNYWlsIjpudWxsLCJwYXlsb2FkIjp7InZhbGlkRm9yVXNlcm5hbWUiOiIzdlBXSjVmbzR0aDllbFQ2Q0Q2N3BoIiwib3JnYW5pemF0aW9uSWQiOjc2NjA3LCJ1c2VySWQiOjg5MTI3LCJmb3JFeHBpcmF0aW9uQ2hlY2siOiIxMmYxOGQwNy03NzliLTRjYzUtYTU2Yi0xNTI3MGEyZTgyN2UifX0.h_kqMObEdXAdGo0CtBsCxnIzsg-ILUjqlIZVPkrOzJw\";");
        properties.setProperty("sasl.mechanism","PLAIN");

        //set consumer config
        properties.setProperty("key.deserializer", StringDeserializer.class.getName());
        properties.setProperty("value.deserializer",StringDeserializer.class.getName());
        properties.setProperty("group.id", groupId);
//        properties.setProperty("auto.offset.reset", "none/earliest/latest");
        properties.setProperty("auto.offset.reset", "earliest");

        // create consumer
        KafkaConsumer<String,String> kafkaConsumer = new KafkaConsumer<>(properties);

        // subscribe to a topic
        kafkaConsumer.subscribe(Arrays.asList(topic));

        // poll for data
        while(true){

            log.info("Polling");

            ConsumerRecords<String,String> records = kafkaConsumer.poll(1000);

            for(ConsumerRecord<String,String> record : records){
                log.info("Key: " + record.key() + ", Value: " + record.value());
                log.info("Partition: " + record.partition() + ", Offset: " + record.offset());

            }
        }



    }
}
