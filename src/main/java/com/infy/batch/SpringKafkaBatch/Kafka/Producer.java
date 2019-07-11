package com.infy.batch.SpringKafkaBatch.Kafka;

import java.util.concurrent.atomic.AtomicLong;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.NonTransientResourceException;
import org.springframework.batch.item.ParseException;
import org.springframework.batch.item.UnexpectedInputException;
import org.springframework.batch.item.kafka.KafkaItemWriter;
import org.springframework.batch.item.kafka.builder.KafkaItemWriterBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.convert.converter.Converter;
import org.springframework.kafka.core.KafkaTemplate;

import com.infy.batch.SpringKafkaBatch.domain.Customer;

@EnableBatchProcessing
@Configuration
public class Producer {

	@Autowired
	private JobBuilderFactory jobBuilderFactory;
	
	
	@Autowired
	private StepBuilderFactory stepBuilderFactory;
	
	
	
	@Autowired
	private KafkaTemplate<Long,Customer> kafkaTemplate;

	
	@Bean
	public Job job() {
		return jobBuilderFactory.get("producer").start(start()).incrementer(new RunIdIncrementer()).build();
	}

	@Bean
	public Step start() {
		return this.stepBuilderFactory.get("step 1").<Customer,Customer>chunk(10).reader(reader()).writer(writer()).build();
	}

	@Bean
	public ItemReader<? extends Customer> reader() {
		AtomicLong id= new AtomicLong();
		
		return new ItemReader<Customer>() {

			@Override
			public Customer read()
					throws Exception, UnexpectedInputException, ParseException, NonTransientResourceException {
				if(id.getAndIncrement()<10_1000) {
					return new Customer(id.get(), Math.random()>0.5?"James":"John");
				}
				return null;
			}
		};
	}

	@Bean
	public KafkaItemWriter<Long,Customer> writer() {
		
		return new KafkaItemWriterBuilder<Long,Customer>().kafkaTemplate(kafkaTemplate).itemKeyMapper(new Converter<Customer,Long>(){

			@Override
			public Long convert(Customer source) {
				return source.getId();
			}
			
		}).build(); 
	}

}
