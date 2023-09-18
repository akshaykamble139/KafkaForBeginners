package io.conduktor.demos.kafka.advanced;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Properties;

public class ConsumerDemoRebalanceListener {
    private static Logger log = LoggerFactory.getLogger(ConsumerDemoRebalanceListener.class);

    public static void main(String[] args) {

        String groupId = "my-java-application";
        String topic = "demo_java";

        // create Producer properties
        Properties properties = new Properties();

        //conduktor
        properties.setProperty("bootstrap.servers","cluster.playground.cdkt.io:9092");

        properties.setProperty("security.protocol","SASL_SSL");
        properties.setProperty("sasl.jaas.config","org.apache.kafka.common.security.plain.PlainLoginModule required username=\"3vPWJ5fo4th9elT6CD67ph\" password=\"eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJpc3MiOiJodHRwczovL2F1dGguY29uZHVrdG9yLmlvIiwic291cmNlQXBwbGljYXRpb24iOiJhZG1pbiIsInVzZXJNYWlsIjpudWxsLCJwYXlsb2FkIjp7InZhbGlkRm9yVXNlcm5hbWUiOiIzdlBXSjVmbzR0aDllbFQ2Q0Q2N3BoIiwib3JnYW5pemF0aW9uSWQiOjc2NjA3LCJ1c2VySWQiOjg5MTI3LCJmb3JFeHBpcmF0aW9uQ2hlY2siOiIxMmYxOGQwNy03NzliLTRjYzUtYTU2Yi0xNTI3MGEyZTgyN2UifX0.h_kqMObEdXAdGo0CtBsCxnIzsg-ILUjqlIZVPkrOzJw\";");
        properties.setProperty("sasl.mechanism","PLAIN");

        //set consumer config
        properties.setProperty("key.deserializer", StringDeserializer.class.getName());
        properties.setProperty("value.deserializer",StringDeserializer.class.getName());
        properties.setProperty("group.id", groupId);

        properties.setProperty("auto.offset.reset", "earliest");
        properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

        // create consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);

        ConsumerRebalanceListenerImpl listener = new ConsumerRebalanceListenerImpl(consumer);

        final Thread mainThread = Thread.currentThread();

        Runtime.getRuntime().addShutdownHook(new Thread(){
            @Override
            public void run() {
                log.info("Detected a shutdown, let's call consumer.wakeup()...");

                consumer.wakeup();

                try {
                    mainThread.join();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        });

        consumer.subscribe(Arrays.asList(topic));

        try{
            // poll for data
            while(true){

                ConsumerRecords<String,String> records = consumer.poll(100);

                for(ConsumerRecord<String,String> record : records){
                    log.info("Key: " + record.key() + ", Value: " + record.value());
                    log.info("Partition: " + record.partition() + ", Offset: " + record.offset());

                    listener.addOffsetToTrack(record.topic(), record.partition(), record.offset());

                }

                consumer.commitAsync();
            }
        }
        catch (WakeupException ex){
            log.info("Consumer is starting to shut down");
        }
        catch (Exception e){
            log.error("Unexpected exception occurred", e);
        }
        finally {
            consumer.commitSync(listener.getCurrentOffsets());
            consumer.close();
            log.info("Consumer is now gracefully shutdown");
        }





    }

}
