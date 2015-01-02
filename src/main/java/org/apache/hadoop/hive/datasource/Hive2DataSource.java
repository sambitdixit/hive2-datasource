package org.apache.hadoop.hive.datasource;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import javax.annotation.Nonnull;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.rholder.retry.RetryException;
import com.github.rholder.retry.Retryer;
import com.github.rholder.retry.RetryerBuilder;
import com.github.rholder.retry.StopStrategies;
import com.github.rholder.retry.WaitStrategies;

/**
 * @author sambitdixit
 *
 */
public class Hive2DataSource extends AbstractDataSource {

	private static Logger log = LoggerFactory.getLogger(Hive2DataSource.class);
	
	private Queue<Hive2Database> databaseQueue = new LinkedList<Hive2Database>();
    
	private final Lock lock = new ReentrantLock();

	private Retryer<Connection> retryer = null;

	/**
	 * The default constructor to accept Hive2Database configs, maximum time to
	 * sleep for exponential backoff policy of retry, unit of maximum time. The
	 * default attemptNumber is 5 if no database config is supplied otherwise
	 * the attemptNumber is equal to size of number of database configs.
	 * 
	 * @param databases
	 *            list of Hive2Database configs.
	 * @param maximumTime
	 *            the maximum time to sleep
	 * @param maximumTimeUnit
	 *            the unit of the maximum time
	 */
	public Hive2DataSource(List<Hive2Database> databases, long maximumTime,
			@Nonnull TimeUnit maximumTimeUnit) {
		setDatabases(databases);
		int attemptNumber = 5;
		if (databases != null && databases.size() > 0) {
			attemptNumber = databases.size();
		}
		retryer = RetryerBuilder
				.<Connection> newBuilder()
				.retryIfExceptionOfType(SQLException.class)
				.retryIfRuntimeException()
				.withWaitStrategy(
						WaitStrategies.exponentialWait(maximumTime,
								maximumTimeUnit))
				.withStopStrategy(
						StopStrategies.stopAfterAttempt(attemptNumber)).build();
	}

	
	/*
	 * (non-Javadoc)
	 * 
	 * @see javax.sql.DataSource#getConnection()
	 * 
	 * public Connection getConnection() throws SQLException { Hive2Database
	 * database = nextDatabase(); Connection conn = null; try { conn =
	 * getConnection(database.getDriver(), database.getJdbcUrl(),
	 * database.getUserName(), database.getPassword()); } catch (SQLException
	 * se) { database = nextDatabase(); conn =
	 * getConnection(database.getDriver(), database.getJdbcUrl(),
	 * database.getUserName(), database.getPassword()); } return conn; }
	 */

	/*
	 * (non-Javadoc)
	 * 
	 * @see javax.sql.DataSource#getConnection()
	 */
	public Connection getConnection() throws SQLException {
		Callable<Connection> callable = new Callable<Connection>() {
			public Connection call() throws Exception {
				return getRawConnection();
			}
		};
		Connection conn = null;
		try {
			conn = retryer.call(callable);
		} catch (RetryException e) {
			log.error("Error while retrying",e);
		} catch (ExecutionException e) {
			log.error("Error while retrying",e);
		}
		return conn;
	}

	/**
	 * @return
	 * @throws SQLException
	 */
	private Connection getRawConnection() throws SQLException {
		Hive2Database database = nextDatabase();
		log.debug("Trying to acquire connection for driver {} with jdbc url {}",database.getDriverClassName(),database.getJdbcUrl());
		Connection conn = getConnection(database.getDriver(),
				database.getJdbcUrl(), database.getUserName(),
				database.getPassword());
		return conn;
	}


	/*
	 * (non-Javadoc)
	 * 
	 * @see javax.sql.DataSource#getConnection(java.lang.String,
	 * java.lang.String)
	 */
	public Connection getConnection(final String username, final String password)
			throws SQLException {
		Callable<Connection> callable = new Callable<Connection>() {
			public Connection call() throws Exception {
				return getRawConnection(username, password);
			}
		};
		Connection conn = null;
		try {
			conn = retryer.call(callable);
		} catch (RetryException e) {
			log.error("Error while retrying",e);
		} catch (ExecutionException e) {
			log.error("Error while retrying",e);
		}

		return conn;
	}

	/**
	 * @param username
	 * @param password
	 * @return
	 * @throws SQLException
	 */
	private Connection getRawConnection(String username, String password)
			throws SQLException {
		Hive2Database database = nextDatabase();
		log.debug("Trying to acquire connection for driver {} with jdbc url {}",database.getDriverClassName(),database.getJdbcUrl());
		Connection conn = getConnection(database.getDriver(),
				database.getJdbcUrl(), username, password);
		return conn;
	}

	
	/**
	 * @param databases
	 */
	private void setDatabases(List<Hive2Database> databases) {
		if (databases != null && databases.size() > 0) {
			for (Hive2Database database : databases) {
				this.databaseQueue.add(database);
			}
		}
	}

	

	/**
	 * @return
	 * @throws SQLException
	 */
	private Hive2Database nextDatabase() throws SQLException {
		this.lock.lock();
		try {
			if (this.databaseQueue.isEmpty()) {
				throw new SQLException("No database is configured");
			}
			if (this.databaseQueue.size() == 1) {
				return this.databaseQueue.element();
			}
			Hive2Database database = this.databaseQueue.remove();
			this.databaseQueue.add(database);
			return database;
		} finally {
			this.lock.unlock();
		}
	}

}