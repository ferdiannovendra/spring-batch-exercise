package com.example.batchproject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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
import org.springframework.batch.item.database.JdbcPagingItemReader;
import org.springframework.batch.item.database.PagingQueryProvider;
import org.springframework.batch.item.database.builder.JdbcBatchItemWriterBuilder;
import org.springframework.batch.item.database.builder.JdbcCursorItemReaderBuilder;
import org.springframework.batch.item.database.builder.JdbcPagingItemReaderBuilder;
import org.springframework.batch.item.database.support.SqlPagingQueryProviderFactoryBean;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.FlatFileItemWriter;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.transform.BeanWrapperFieldExtractor;
import org.springframework.batch.item.file.transform.DelimitedLineAggregator;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.batch.item.json.JacksonJsonObjectMarshaller;
import org.springframework.batch.item.json.builder.JsonFileItemWriterBuilder;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.task.SimpleAsyncTaskExecutor;

import javax.sql.DataSource;
import java.io.File;
import java.util.List;

@SpringBootApplication
@EnableBatchProcessing
public class BatchProjectApplication {

	Logger logger = LoggerFactory.getLogger(getClass());
	public static String[] names = new String[] {"orderId", "firstName", "lastName", "email", "cost", "itemId", "itemName", "shipDate"};

	public static String ORDER_SQL = "select order_id, first_name, last_name, email, cost, item_id, item_name, ship_date "
			+ "from SHIPPED_ORDER order by order_id";
	public static String INSERT_ORDER_SQL = "insert into "
			+ "shipped_order_output(order_id, first_name, last_name, email, cost, item_id, item_name, ship_date)"
			+ " values(:orderId,:firstName,:lastName,:email,:cost,:itemId,:itemName,:shipDate)";
	@Autowired
	public JobBuilderFactory jobBuilderFactory;

	@Autowired
	public StepBuilderFactory stepBuilderFactory;

	@Autowired
	public DataSource dataSource;

	@Bean
	public ItemWriter<Order> itemWriter() {
		FileSystemResource fileSystemResource = new FileSystemResource("/data/shipped_order_output.json");
		System.out.println(fileSystemResource);
		return new JsonFileItemWriterBuilder<Order>()
				.jsonObjectMarshaller(new JacksonJsonObjectMarshaller<Order>())
				.resource(fileSystemResource)
				.name("jsonItemWriter")
				.build();
	}

	@Bean
	public PagingQueryProvider queryProvider() throws Exception {
		SqlPagingQueryProviderFactoryBean factory = new SqlPagingQueryProviderFactoryBean();
		factory.setSelectClause("select order_id, first_name, last_name, email, cost, item_id, item_name, ship_date");
		factory.setFromClause("from SHIPPED_ORDER");
		factory.setSortKey("order_id");
		factory.setDataSource(dataSource);
		return factory.getObject();
	}

	//Multi Thread
	@Bean
	public ItemReader<Order> itemReader() throws Exception {
		return new JdbcPagingItemReaderBuilder<Order>()
				.dataSource(dataSource)
				.name("jdbcCursorItemReader")
				.queryProvider(queryProvider())
				.rowMapper(new OrderRowMapper())
				.pageSize(10)
				.build();
	}

	//Single-thread
//	@Bean
//	public ItemReader<Order> itemReader() {
//		return new JdbcCursorItemReaderBuilder<Order>()
//				.dataSource(dataSource)
//				.name("jdbcCursorItemReader")
//				.sql(ORDER_SQL)
//				.rowMapper(new OrderRowMapper())
//				.build();
//	}

	//This code for read Item from CSV. If needed just uncomment this code below
//	@Bean
//	public ItemReader<Order> itemReader() {
//		FlatFileItemReader<Order> itemReader = new FlatFileItemReader<>();
//		itemReader.setLinesToSkip(1);
//		ClassPathResource classPathResource = new ClassPathResource("data/shipped_orders.csv");
//
////		FileSystemResource fileSystemResource = new FileSystemResource("src/main/resources/data/shipped_orders.csv");
//		itemReader.setResource(classPathResource);
//
//		System.out.println(classPathResource);
//		System.out.println(classPathResource.exists());
////		logger.info(fileSystemResource.toString());
//
//		DefaultLineMapper<Order> lineMapper = new DefaultLineMapper<>();
//		DelimitedLineTokenizer tokenizer = new DelimitedLineTokenizer();
//		tokenizer.setNames(tokens);
//
//		lineMapper.setLineTokenizer(tokenizer);
//
//		lineMapper.setFieldSetMapper(new OrderFieldSetMapper());
//
//		itemReader.setLineMapper(lineMapper);
//		return itemReader;
//	}
	@Bean
	public Step chunkBasedStep() throws Exception {
		return this.stepBuilderFactory.get("chunkBasedStep")
				.<Order, Order>chunk(10)
				.reader(itemReader())
				.writer(itemWriter())
				.build();
	}


	@Bean
	public Job job() throws Exception {
		return this.jobBuilderFactory.get("job")
				.start(chunkBasedStep())
				.build();
	}


	public static void main(String[] args) {

		SpringApplication.run(BatchProjectApplication.class, args);
	}

}
