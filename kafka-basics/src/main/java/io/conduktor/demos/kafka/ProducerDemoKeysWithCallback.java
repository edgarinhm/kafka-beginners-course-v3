package io.conduktor.demos.kafka;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
public class ProducerDemoKeysWithCallback {

    private static final Logger log = LoggerFactory.getLogger(ProducerDemoKeysWithCallback.class.getSimpleName());
    public static void main(String[] args){
        log.info("I am a Kafka Producer");

        // create Producer Properties
        Properties properties = new Properties();

        // connect to Docker
        //properties.setProperty("bootstrap.servers", "host.docker.internal:29092");

        // connect to Localhost
        properties.setProperty("bootstrap.servers", "127.0.0.1:9092");

        // set Producer Properties
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer",StringSerializer.class.getName());

        properties.setProperty("batch.size", "400"); // you would keep default 16k of batch size not small size in production

        // create the Producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        for (int j=0; j<10; j++){

            for (int i=0; i<30; i++){
                // create a Producer Rercord
                ProducerRecord<String, String> producerRecord = new ProducerRecord<>("demo_java","hello world " + i);

                // send data
                producer.send(producerRecord, new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata metadata, Exception e) {
                        if(e==null){
                            log.info("Received new metadata \n" +
                                    "Topic: " + metadata.topic() + "\n" +
                                    "Partition: " + metadata.partition() + "\n" +
                                    "Offset: " + metadata.offset() + "\n" +
                                    "Timestamp: " + metadata.timestamp() + "\n"
                            );
                        }else {
                            log.error("Error while producing", e);
                        }
                    }
                });
            }

            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

        }
        // tell the producer send all data and block until done -- synchronous
        producer.flush();

        // flush and close the producer
        producer.close();
    }
}
