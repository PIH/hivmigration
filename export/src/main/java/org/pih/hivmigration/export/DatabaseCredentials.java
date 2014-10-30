/**
 * The contents of this file are subject to the OpenMRS Public License
 * Version 1.0 (the "License"); you may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 * http://license.openmrs.org
 *
 * Software distributed under the License is distributed on an "AS IS"
 * basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
 * License for the specific language governing rights and limitations
 * under the License.
 *
 * Copyright (C) OpenMRS, LLC.  All Rights Reserved.
 */
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
