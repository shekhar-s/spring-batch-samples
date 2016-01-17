package com.shekhar.spring.batch;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

import javax.sql.DataSource;

import org.junit.After;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.batch.core.configuration.JobRegistry;
import org.springframework.batch.core.configuration.annotation.DefaultBatchConfigurer;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.SpringApplicationConfiguration;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.test.jdbc.JdbcTestUtils;

@RunWith(SpringJUnit4ClassRunner.class)
@SpringApplicationConfiguration(classes = BatchDemoApplication.class)
public class BatchDemoApplicationTests {

	@Autowired
	JobLauncher launcher;

	@Autowired
	JobRegistry registry;

	private JdbcTemplate targetJdbcTemplate;

	private JdbcTemplate sourceJdbcTemplate;

	@Autowired
	public void setTargetDataSource(@Qualifier("target") DataSource targetDataSource) {
		targetJdbcTemplate = new JdbcTemplate(targetDataSource);
	}

	@Autowired
	public void setSourceDataSource(@Qualifier("source")DataSource sourceDataSource) {
		sourceJdbcTemplate = new JdbcTemplate(sourceDataSource);
	}

	@After
	public void before(){
		//JdbcTestUtils.deleteFromTables(targetJdbcTemplate, "target_table");
	}
	
	@Test
	public void contextLoads() {
		assertThat("All rows did not move into target", 
				JdbcTestUtils.countRowsInTable(targetJdbcTemplate, "Target_Table"), 
				is(JdbcTestUtils.countRowsInTable(sourceJdbcTemplate, "Source_Table")));
	}

	//@Test
	public void entityLoads() throws Exception {
		System.out.println("Target " + targetJdbcTemplate.getDataSource().getConnection().getMetaData().getUserName());
		System.out.println("Source " + sourceJdbcTemplate.getDataSource().getConnection().getMetaData().getUserName());
		
		JdbcTestUtils.deleteFromTables(targetJdbcTemplate, "target_table");
		launcher.run(registry.getJob("My Job"), null);
		assertThat("All rows did not move into target", 
				JdbcTestUtils.countRowsInTable(targetJdbcTemplate, "Target_Table"), 
				is(JdbcTestUtils.countRowsInTable(sourceJdbcTemplate, "Source_Table")));
	}
}
