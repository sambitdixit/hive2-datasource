package org.apache.hadoop.hive.datasource;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import junit.framework.Assert;

import org.apache.hadoop.hive.datasource.Hive2DataSource;
import org.apache.hadoop.hive.datasource.Hive2Database;
import org.junit.Test;

public class Hive2DataSourceTest {

	@Test
	public void testGetConnectionFailureWithRetry() {
		Hive2Database server1 = new Hive2Database();
		server1.setDriverClassName("org.apache.hive.jdbc.HiveDriver");
		server1.setJdbcUrl("jdbc:hive2://localhost:10000");
		server1.setUserName("scott");
		server1.setPassword("password");

		Hive2Database server2 = new Hive2Database();
		server2.setDriverClassName("org.apache.hive.jdbc.HiveDriver");
		server2.setJdbcUrl("jdbc:hive2://localhost:10010");
		server2.setUserName("scott");
		server2.setPassword("password");

		Hive2Database server3 = new Hive2Database();
		server3.setDriverClassName("org.apache.hive.jdbc.HiveDriver");
		server3.setJdbcUrl("jdbc:hive2://localhost:10020");
		server3.setUserName("scott");
		server3.setPassword("password");

		List<Hive2Database> hiveServers = new ArrayList<Hive2Database>();
		hiveServers.add(server1);
		hiveServers.add(server2);
		hiveServers.add(server3);
		Hive2DataSource dataSource = new Hive2DataSource(hiveServers, 5,
				TimeUnit.SECONDS);
		Connection conn = null;

		try {
			conn = dataSource.getConnection();
		} catch (Exception e) {
			Assert.assertEquals(e.getClass(),SQLException.class);
		}

	}

}
