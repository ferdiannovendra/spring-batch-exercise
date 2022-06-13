package com.example.batchproject;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.StepExecutionListener;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.job.builder.FlowBuilder;
import org.springframework.batch.core.job.flow.Flow;
import org.springframework.batch.core.job.flow.JobExecutionDecider;
import org.springframework.batch.core.job.flow.support.SimpleFlow;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.core.task.SimpleAsyncTaskExecutor;

import java.util.List;

@SpringBootApplication
@EnableBatchProcessing
public class BatchProjectApplication {

	@Autowired
	public JobBuilderFactory jobBuilderFactory;

	@Autowired
	public StepBuilderFactory stepBuilderFactory;

	@Bean
	public ItemReader<String> itemReader() {
		return new SimpleItemReader();
	}
	@Bean
	public Step chunkBasedStep(){
		return this.stepBuilderFactory.get("chunkBasedStep")
				.<String, String>chunk(3)
				.reader(itemReader())
				.writer(new ItemWriter<String>() {
					@Override
					public void write(List<? extends String> list) throws Exception {
						System.out.println(String.format("Received list of size : %s", list.size()));
						list.forEach(System.out::println);
					}
				}).build();
	}



	@Bean
	public Job job(){
		return this.jobBuilderFactory.get("job")
				.start(chunkBasedStep())
				.build();
	}


	public static void main(String[] args) {

		SpringApplication.run(BatchProjectApplication.class, args);
	}

}
