package com.example.batchproject;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

@SpringBootApplication
@EnableBatchProcessing
public class BatchProjectApplication {

	@Autowired
	public JobBuilderFactory jobBuilderFactory;

	@Autowired
	public StepBuilderFactory stepBuilderFactory;

	@Bean
	public Step packageItemStep() {
		return this.stepBuilderFactory.get("packageItemStep").tasklet()
	}

	@Bean
	public Job deliverPackageJob(){
		return this.jobBuilderFactory.get("deliverPackageJob").start(packageItemStep()).build();
	}



	public static void main(String[] args) {

		SpringApplication.run(BatchProjectApplication.class, args);
	}

}
