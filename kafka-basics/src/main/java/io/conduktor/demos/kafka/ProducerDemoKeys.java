package io.conduktor.demos.kafka;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;


public class ProducerDemoKeys {

    private static final Logger log = LoggerFactory.getLogger(ProducerDemoKeys.class.getSimpleName());

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

        // create the producer
        try(KafkaProducer<String,String> producer = new KafkaProducer<>(properties)){

            for(int j=0;j<2;j++){
                for (int i=0;i<10;i++) {

                    String topic = "demo_java";
                    String key = "id_" + i;
                    String value = "hello world " + i;
                    ProducerRecord<String, String> producerRecord =
                            new ProducerRecord<>(topic,key,value);

                    //send data
                    producer.send(producerRecord, new Callback() {
                        @Override
                        public void onCompletion(RecordMetadata metadata, Exception exception) {
                            if (exception == null) {
                                log.info("Key: " + key + " | Partition: " + metadata.partition());
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
