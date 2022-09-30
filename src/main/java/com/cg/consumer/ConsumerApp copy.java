package com.cg.consumer;

import java.util.Properties;
import java.util.ArrayList;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

public class ConsumerApp {

	public static void main(String [] args) {

		//Step1 set the mandatory properties.
		Properties props= new Properties();
		props.put("bootstrap.servers","localhost:9092");
		props.put("group.id","test");
		props.put("enable.auto.commit", "true");
		props.put("auto.commit.interval.ms", "1000");
		props.put("session.timeout.ms", "30000");
		props.put("key.deserializer","org.apache.kafka.common.serialization.StringDeserializer");  
		props.put("value.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
		
		//Create Kafka consumer.
		KafkaConsumer<String,String> consumer= new KafkaConsumer<String, String>(props);
		
		ArrayList<String> topicList =new ArrayList();
		topicList.add("firstTopic"); //topics to subscribe.
		// Subscribe to list of topics
		consumer.subscribe(topicList);
		
		try{
			while(true) {
				ConsumerRecords<String, String> records = consumer.poll(1000);
				for(ConsumerRecord<String,String>record : records) {
					System.out.println("****"+record.toString());
				}
			}
		}
		catch(Exception ex){
		    ex.printStackTrace();
		}
		finally {
		        consumer.close();
		}
		}
	}

