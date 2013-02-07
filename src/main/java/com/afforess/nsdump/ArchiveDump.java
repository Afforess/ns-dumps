/*
 * Copyright (c) 2013 Afforess
 * 
 * Permission is hereby granted, free of charge, to any person obtaining
 * a copy of this software and associated documentation files (the
 * "Software"), to deal in the Software without restriction, including
 * without limitation the rights to use, copy, modify, merge, publish,
 * distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so, subject to
 * the following conditions:
 * 
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 * 
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
 * NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE
 * LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
 * OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION
 * WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 */
package com.afforess.nsdump;

abstract class ArchiveDump {
	private String connectionUrl;
	private String username;
	private String password;

	/**
	 * Gets the connection url that is used to connect to the database
	 * @return database connection url
	 */
	public String getConnectionUrl() {
		return connectionUrl;
	}

	/**
	 * Sets the connection url that is used to connect to the database
	 * @param connectionUrl
	 */
	public void setConnectionUrl(String connectionUrl) {
		this.connectionUrl = connectionUrl;
	}

	/**
	 * Gets the username used when connecting to the database
	 * 
	 * @return username
	 */
	public String getUsername() {
		return username;
	}

	/**
	 * Sets the username used when connecting to the database
	 * 
	 * @param username
	 */
	public void setUsername(String username) {
		this.username = username;
	}

	/**
	 * Gets the password used when connecting to the database
	 * 
	 * @return password
	 */
	public String getPassword() {
		return password;
	}

	/**
	 * Sets the password used when connecting to the database
	 * 
	 * @param password
	 */
	public void setPassword(String password) {
		this.password = password;
	}
}
