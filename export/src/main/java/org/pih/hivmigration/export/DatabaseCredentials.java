package org.pih.hivmigration.export;

import oracle.jdbc.driver.OracleDriver;

public class DatabaseCredentials {

	//***** PROPERTIES *****

	private String driver = OracleDriver.class.getName();
	private String url;
	private String user;
	private String password;

	//***** CONSTRUCTORS *****

	public DatabaseCredentials() {}

	public DatabaseCredentials(String url, String user, String password) {
		this.url = url;
		this.user = user;
		this.password = password;
	}

	public DatabaseCredentials(String driver, String url, String user, String password) {
		this(url, user, password);
		this.driver = driver;
	}

	//***** PROPERTY ACCESS *****

	public String getDriver() {
		return driver;
	}

	public void setDriver(String driver) {
		this.driver = driver;
	}

	public String getUrl() {
		return url;
	}

	public void setUrl(String url) {
		this.url = url;
	}

	public String getUser() {
		return user;
	}

	public void setUser(String user) {
		this.user = user;
	}

	public String getPassword() {
		return password;
	}

	public void setPassword(String password) {
		this.password = password;
	}
}
