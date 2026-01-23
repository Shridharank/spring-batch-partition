package com.shri.spring_batch;

import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
@EnableBatchProcessing
public class SpringBatchPartitionApplication {

	public static void main(String[] args) {
		SpringApplication.run(SpringBatchPartitionApplication.class, args);
	}

}
