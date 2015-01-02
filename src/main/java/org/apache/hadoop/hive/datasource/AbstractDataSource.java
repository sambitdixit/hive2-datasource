package org.apache.hadoop.hive.datasource;

import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.SQLException;
import java.util.Properties;
import java.util.logging.Logger;

import javax.sql.DataSource;

/**
 * @author sambitdixit
 *
 */
public abstract class AbstractDataSource implements DataSource {

	/**
	 * Returns 0, indicating the default system timeout is to be used.
	 */
	public int getLoginTimeout() throws SQLException {
		return 0;
	}

	/**
	 * Setting a login timeout is not supported.
	 */
	public void setLoginTimeout(int timeout) throws SQLException {
		throw new UnsupportedOperationException("setLoginTimeout");
	}

	/**
	 * LogWriter methods are not supported.
	 */
	/* (non-Javadoc)
	 * @see javax.sql.CommonDataSource#getLogWriter()
	 */
	public PrintWriter getLogWriter() {
		throw new UnsupportedOperationException("getLogWriter");
	}

	/**
	 * LogWriter methods are not supported.
	 */
	public void setLogWriter(PrintWriter pw) throws SQLException {
		throw new UnsupportedOperationException("setLogWriter");
	}

	// ---------------------------------------------------------------------
	// Implementation of JDBC 4.0's Wrapper interface
	// ---------------------------------------------------------------------

	/* (non-Javadoc)
	 * @see java.sql.Wrapper#unwrap(java.lang.Class)
	 */
	@SuppressWarnings("unchecked")
	public <T> T unwrap(Class<T> iface) throws SQLException {
		if (iface.isInstance(this)) {
			return (T) this;
		}
		throw new SQLException("DataSource of type [" + getClass().getName()
				+ "] cannot be unwrapped as [" + iface.getName() + "]");
	}

	/* (non-Javadoc)
	 * @see java.sql.Wrapper#isWrapperFor(java.lang.Class)
	 */
	public boolean isWrapperFor(Class<?> iface) throws SQLException {
		return iface.isInstance(this);
	}

	// ---------------------------------------------------------------------
	// Implementation of JDBC 4.1's getParentLogger method
	// ---------------------------------------------------------------------

	/* (non-Javadoc)
	 * @see javax.sql.CommonDataSource#getParentLogger()
	 */
	public Logger getParentLogger() {
		return Logger.getLogger(Logger.GLOBAL_LOGGER_NAME);
	}
	
	/**
	 * @param userName
	 * @param password
	 * @return
	 */
	private Properties getConnectionProperties(String userName, String password) {
		Properties mergedProps = new Properties();
		if (userName != null) {
			mergedProps.setProperty("user", userName);
		}
		if (password != null) {
			mergedProps.setProperty("password", password);
		}
		return mergedProps;
	}
	
	/**
	 * @param driver
	 * @param jdbcUrl
	 * @param userName
	 * @param password
	 * @return
	 * @throws SQLException
	 */
	protected Connection getConnection(Driver driver, String jdbcUrl,
			String userName, String password) throws SQLException {
		Properties props = getConnectionProperties(userName, password);
		Connection conn = driver.connect(jdbcUrl, props);
		return conn;
	}

}
