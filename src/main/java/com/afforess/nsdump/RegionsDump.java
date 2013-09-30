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

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.zip.GZIPInputStream;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;

import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

/**
 * Created with a stream of data that is gzipped region xml, and can be parsed into a database table
 * 
 * <p><b>regions table format</b><pre>
 * <span style="padding: 0 10px">&nbsp;</span>name           - varchar
 * <span style="padding: 0 10px">&nbsp;</span>title          - varchar
 * <span style="padding: 0 10px">&nbsp;</span>factbook       - clob
 * <span style="padding: 0 10px">&nbsp;</span>numnations     - int
 * <span style="padding: 0 10px">&nbsp;</span>nations        - clob
 * <span style="padding: 0 10px">&nbsp;</span>delegate       - varchar
 * <span style="padding: 0 10px">&nbsp;</span>delegatevotes  - int
 * <span style="padding: 0 10px">&nbsp;</span>founder        - varchar
 * <span style="padding: 0 10px">&nbsp;</span>power          - varchar
 * <span style="padding: 0 10px">&nbsp;</span>flag           - varchar
 * <span style="padding: 0 10px">&nbsp;</span>embassies      - clob
 * </pre><p>
 */
public class RegionsDump extends ArchiveDump {
	private final static String REGIONS_DUMP_URL = "http://www.nationstates.net/pages/regions.xml.gz";
	private final InputStream stream;
	private boolean parsed;
	/**
	 * Constructs a region dump using the given file as the data source
	 * @param file regions.xml.gz file
	 * @throws FileNotFoundException 
	 */
	public RegionsDump(File file) throws FileNotFoundException {
		this(new FileInputStream(file));
	}

	/**
	 * Opens a url connection to the ns region dump and uses the latest dump as the data stream
	 * @throws IOException 
	 */
	public RegionsDump() throws IOException {
		this(getRegionDump());
	}

	/**
	 * Constructs a region dump using the given inputstream as the data stream
	 * @param stream of data in gzipped xml
	 */
	public RegionsDump(InputStream stream) {
		this.stream = stream;
		this.setConnectionUrl("jdbc:h2:./ns-db");
		this.setUsername("sa");
		this.setPassword("sa");
	}

	/**
	 * True if the region data has been parsed already
	 * 
	 * @return parsed
	 */
	public boolean isParsed() {
		return parsed;
	}

	/**
	 * Parses the region data into the database
	 */
	public void parse() {
		Connection conn = null;
		try {
			conn = getDatabaseConnection();
			
			try {
				conn.prepareStatement("DROP TABLE regions").execute();
			} catch (SQLException ignore) {	}
			conn.prepareStatement("CREATE TABLE regions (name varchar(50), title varchar(50), factbook clob, numnations int, nations clob," +
					"delegate varchar(50), delegatevotes int, founder varchar(50), power varchar(50), flag varchar(255), embassies clob)").execute();

			conn.prepareStatement("CREATE INDEX region_name ON regions(name);").execute();
			SAXParserFactory factory = SAXParserFactory.newInstance();
			SAXParser parser = factory.newSAXParser();
			parser.parse(new GZIPInputStream(stream), new RegionParser(conn));
		} catch (SQLException e) {
			throw new RuntimeException("SQL Exception", e);
		} catch (SAXException e) {
			throw new RuntimeException("Sax Parsing Exception", e);
		} catch (IOException e) {
			throw new RuntimeException("IOException", e);
		} catch (ParserConfigurationException e) {
			throw new RuntimeException("Parsing Exception", e);
		} finally {
			parsed = true;
			if (conn != null) {
				try {
					conn.close();
				} catch (SQLException e) { }
			}
		}
	}

	/**
	 * Creates a new connection to the database
	 * @return database connection
	 * @throws SQLException
	 */
	public Connection getDatabaseConnection() throws SQLException {
		return DriverManager.getConnection(this.getConnectionUrl(), this.getUsername(), this.getPassword());
	}

	private static InputStream getRegionDump() throws IOException {
		URL url = new URL(REGIONS_DUMP_URL);
		HttpURLConnection conn = (HttpURLConnection) url.openConnection();
		conn.setRequestProperty("User-Agent", "ns-dumps api by Afforess (https://github.com/Afforess/ns-dumps)");
		return conn.getInputStream();
	}

	private static class RegionParser extends DefaultHandler {
		private final Connection conn;
		//Element data
		private final StringBuilder builder = new StringBuilder();
		private boolean reset = true;

		//Region Data
		private String name;
		private String factbook;
		private String numNations;
		private String nations;
		private String delegate;
		private String delegateVotes;
		private String founder;
		private String power;
		private String flag;
		private final ArrayList<String> embassies = new ArrayList<String>();

		public RegionParser(Connection conn) {
			this.conn = conn;
		}

		@Override
		public void startElement(String s, String s1, String elementName, Attributes attributes) throws SAXException {
			if (elementName.equalsIgnoreCase("embassies")) {
				embassies.clear();
			}
			reset = true;
		}

		@Override
		public void endElement(String s, String s1, String element) throws SAXException {
			reset = true;
			if (element.equalsIgnoreCase("region")) {
				try {
					PreparedStatement statement = conn.prepareStatement("INSERT INTO regions (name, title, factbook, numnations, nations, " +
							"delegate, delegatevotes, founder, power, flag, embassies)  VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");
					statement.setString(1, name.toLowerCase().replaceAll(" ", "_"));
					statement.setString(2, name);
					statement.setClob(3, new StringReader(factbook));
					statement.setInt(4, Integer.parseInt(numNations));
					statement.setClob(5, new StringReader(nations));
					statement.setString(6, delegate);
					statement.setInt(7, Integer.parseInt(delegateVotes));
					statement.setString(8, founder);
					statement.setString(9, power);
					statement.setString(10, flag);
					if (embassies.size() > 0) {
						StringBuilder builder = new StringBuilder();
						for (String embassy : embassies) {
							builder.append(embassy);
							builder.append(":");
						}
						builder.deleteCharAt(builder.length() - 1);
						statement.setClob(11, new StringReader(builder.toString()));
					} else {
						statement.setClob(11, new StringReader(""));
					}
					statement.execute();
				} catch (SQLException e) {
					throw new RuntimeException(e);
				}
			} else if (element.equalsIgnoreCase("name")) {
				name = builder.toString();
			} else if (element.equalsIgnoreCase("factbook")) {
				factbook = builder.toString();
			} else if (element.equalsIgnoreCase("numnations")) {
				numNations = builder.toString();
			} else if (element.equalsIgnoreCase("nations")) {
				nations = builder.toString();
			} else if (element.equalsIgnoreCase("delegate")) {
				delegate = builder.toString();
			} else if (element.equalsIgnoreCase("delegatevotes")) {
				delegateVotes = builder.toString();
			} else if (element.equalsIgnoreCase("founder")) {
				founder = builder.toString();
			} else if (element.equalsIgnoreCase("power")) {
				power = builder.toString();
			} else if (element.equalsIgnoreCase("flag")) {
				flag = builder.toString();
			} else if (element.equalsIgnoreCase("embassy")) {
				embassies.add(builder.toString());
			} 
		}

		@Override
		public void characters(char[] ac, int i, int j) throws SAXException {
			if (reset) {
				builder.delete(0, builder.length());
				builder.append(ac, i, j);
				reset = false;
			} else {
				builder.append(ac, i, j);
			}
		}
	}
}
