package org.apache.hadoop.hive.datasource;

import java.sql.Driver;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author sambitdixit
 *
 */
public class Hive2Database {

	private static Logger log = LoggerFactory.getLogger(Hive2Database.class);

	private Driver driver;
	private String driverClassName;

	private String jdbcUrl;
	private String userName;
	private String password;

	public String getJdbcUrl() {
		return jdbcUrl;
	}

	public void setJdbcUrl(String jdbcUrl) {
		this.jdbcUrl = jdbcUrl;
	}

	public String getUserName() {
		return userName;
	}

	public void setUserName(String userName) {
		this.userName = userName;
	}

	public String getPassword() {
		return password;
	}

	public void setPassword(String password) {
		this.password = password;
	}

	public Driver getDriver() {
		return driver;
	}

	public void setDriver(Driver driver) {
		this.driver = driver;
		log.debug("driver set is {}", driver);
	}

	public String getDriverClassName() {
		return driverClassName;
	}

	public void setDriverClassName(String driverClassName) {
		this.driverClassName = driverClassName;
		log.debug("driverClassName set is {}", driverClassName);
		if (this.driverClassName != null) {
			try {
				Class<?> driverClass = Class.forName(driverClassName);
				if (driverClass != null) {
					Object obj = driverClass.newInstance();
					if (obj != null && obj instanceof Driver) {
						this.driver = (Driver) obj;
					}
				}
			} catch (ClassNotFoundException e) {
				log.error("ClassNotFoundException", e);
			} catch (InstantiationException e) {
				log.error("InstantiationException", e);
			} catch (IllegalAccessException e) {
				log.error("IllegalAccessException", e);
			}
		}
	}

	@Override
	public String toString() {
		return "Hive2Database [driver=" + driver + ", driverClassName="
				+ driverClassName + ", jdbcUrl=" + jdbcUrl + ", userName="
				+ userName + ", password=" + password + "]";
	}

}
