package io.conduktor.demos.kafka;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;


public class ProducerDemoWithCallbacks {

    private static final Logger log = LoggerFactory.getLogger(ProducerDemoWithCallbacks.class.getSimpleName());

    public static void main(String[] args) {

        log.info("Hello world!");

        // create Producer properties
        Properties properties = new Properties();

        //localhost
//        properties.setProperty("bootstrap.servers","127.0.0.1:9092");

        //conduktor
        properties.setProperty("bootstrap.servers","cluster.playground.cdkt.io:9092");

        properties.setProperty("security.protocol","SASL_SSL");
        properties.setProperty("sasl.jaas.config","org.apache.kafka.common.security.plain.PlainLoginModule required username=\"3vPWJ5fo4th9elT6CD67ph\" password=\"eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJpc3MiOiJodHRwczovL2F1dGguY29uZHVrdG9yLmlvIiwic291cmNlQXBwbGljYXRpb24iOiJhZG1pbiIsInVzZXJNYWlsIjpudWxsLCJwYXlsb2FkIjp7InZhbGlkRm9yVXNlcm5hbWUiOiIzdlBXSjVmbzR0aDllbFQ2Q0Q2N3BoIiwib3JnYW5pemF0aW9uSWQiOjc2NjA3LCJ1c2VySWQiOjg5MTI3LCJmb3JFeHBpcmF0aW9uQ2hlY2siOiIxMmYxOGQwNy03NzliLTRjYzUtYTU2Yi0xNTI3MGEyZTgyN2UifX0.h_kqMObEdXAdGo0CtBsCxnIzsg-ILUjqlIZVPkrOzJw\";");
        properties.setProperty("sasl.mechanism","PLAIN");

        //set producer properties
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer",StringSerializer.class.getName());

        properties.setProperty("batch.size","400");

//        properties.setProperty("partitioner.class", RoundRobinPartitioner.class.getName());


        // create the producer
        try(KafkaProducer<String,String> producer = new KafkaProducer<>(properties)){

            for(int j=0;j<10;j++){
                for (int i=0;i<30;i++) {
                    ProducerRecord<String, String> producerRecord =
                            new ProducerRecord<>("demo_java", "hello world " + i);

                    //send data
                    producer.send(producerRecord, new Callback() {
                        @Override
                        public void onCompletion(RecordMetadata metadata, Exception exception) {
                            if (exception == null) {
                                log.info("Received new metadata: \n" +
                                        "Topic: " + metadata.topic() + "\n" +
                                        "Partition: " + metadata.partition() + "\n" +
                                        "Offset: " + metadata.offset() + "\n" +
                                        "Timestamp: " + metadata.timestamp()
                                );
                            } else {
                                log.error("Error while producing " + exception);
                            }
                        }
                    });
                }

                try {
                    Thread.sleep(500);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }

        }
    }
}
