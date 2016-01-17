package com.shekhar.spring.batch;

import java.sql.SQLException;

import javax.sql.DataSource;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.DefaultBatchConfigurer;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.repository.support.JobRepositoryFactoryBean;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.database.BeanPropertyItemSqlParameterSourceProvider;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.database.JdbcCursorItemReader;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.jdbc.core.BeanPropertyRowMapper;
import org.springframework.transaction.PlatformTransactionManager;

import oracle.jdbc.pool.OracleConnectionPoolDataSource;

@Configuration
@EnableBatchProcessing
@EnableAutoConfiguration
public class BatchConfig extends DefaultBatchConfigurer {

	@Autowired
	PlatformTransactionManager transactionManager;
	
	@Override
	protected JobRepository createJobRepository() throws Exception {
		JobRepositoryFactoryBean factory = new JobRepositoryFactoryBean();
		factory.setDataSource(batchDataSource());
		factory.setTransactionManager(transactionManager);
		factory.afterPropertiesSet();
		return  factory.getObject();
	}

	@Autowired
	private JobBuilderFactory jobBuilderFactory;

	@Autowired
	private StepBuilderFactory stepBuilderFactory;

	@Bean
	public Job job() throws SQLException{
		return jobBuilderFactory.get("My Job").start(step1()).build();
	}
	@Bean
	public Step step1() throws SQLException {
		return stepBuilderFactory.get("Load My Entity").<MyEntity, MyEntity> chunk(10).reader(reader()).writer(writer())
				.build();
	}

	@Bean
	public ItemReader<MyEntity> reader() throws SQLException {
		JdbcCursorItemReader<MyEntity> reader = new JdbcCursorItemReader<>();
		reader.setSql("Select entity_code entityCode, entity_name entityName, entity_description entityDescription from source_table");
		reader.setFetchSize(100);
		reader.setDataSource(sourceDataSource());
		reader.setName("My entity reader");
		reader.setRowMapper(new BeanPropertyRowMapper<MyEntity>(MyEntity.class, true));

		return reader;
	}

	@Bean
	public ItemWriter<MyEntity> writer() throws SQLException {
		JdbcBatchItemWriter<MyEntity> writer = new JdbcBatchItemWriter<>();
		writer.setSql(
				"INSERT INTO target_table(entity_code, entity_name, entity_description) values (:entityCode, :entityName, :entityDescription)");
		writer.setDataSource(targetDataSource());
		writer.setAssertUpdates(true);
		writer.setItemSqlParameterSourceProvider(new BeanPropertyItemSqlParameterSourceProvider<MyEntity>());

		return writer;
	}

	@Bean
	@Qualifier("source")
	public DataSource sourceDataSource() throws SQLException {
		return createOracleDataSource("jdbc:oracle:thin:@localhost:1521:xe", "source", "source");
	}

	@Bean
	@Primary
	public DataSource batchDataSource() throws SQLException {
		return createOracleDataSource("jdbc:oracle:thin:@localhost:1521:xe", "batch", "batch");
	}

	@Bean
	@Qualifier("target")
	public DataSource targetDataSource() throws SQLException {
		return createOracleDataSource("jdbc:oracle:thin:@localhost:1521:xe", "target", "target");
	}

	private DataSource createOracleDataSource(String url, String userId, String password) throws SQLException {
		OracleConnectionPoolDataSource dataSource = new OracleConnectionPoolDataSource();
		dataSource.setURL(url);
		dataSource.setUser(userId);
		dataSource.setPassword(password);
		
		return dataSource;
	}

}
