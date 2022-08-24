package com.cg.producers;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class KafkaApplication {

	public static void main(String[] args) {
		
		//Step1 set the mandatory properties.
	    Properties props= new Properties();
	    props.put("bootstrap.servers","localhost:9092");
	    props.put("key.serializer","org.apache.kafka.common.serialization.StringSerializer");
	    props.put("value.serializer","org.apache.kafka.common.serialization.StringSerializer");

	    //Create Kafka Producer.
	    KafkaProducer<String,String> producer= new KafkaProducer<String, String>(props);

	    try{
	        for (int i=210;i<=220;i++){
	            ProducerRecord<String,String> record= new ProducerRecord<String ,String >("firstTopic","test value"+i);
	            producer.send(record);
	            System.out.println("Producer record number send"+i);



	        }
	    }
	    catch(Exception ex){
	        ex.printStackTrace();
	    }
	    finally {
	            producer.close();
	    }
	}

}
