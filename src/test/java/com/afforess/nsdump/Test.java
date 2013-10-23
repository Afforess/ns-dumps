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
import java.io.IOException;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.commons.lang.StringEscapeUtils;

public class Test {

	public static void main(String[] args) throws IOException, SQLException {
		testNationDump();
	}

	public static void testRegionDump() throws IOException, SQLException {
		RegionsDump dump = new RegionsDump();
		dump.parse();
		
		Connection conn = dump.getDatabaseConnection();
		PreparedStatement statement = conn.prepareStatement("SELECT (numnations) FROM regions");
		ResultSet result = statement.executeQuery();
		int total = 0, regions = 0;
		while(result.next()) {
			total += result.getInt(1);
			regions++;
		}
		System.out.println("Total nations: " + total);
		System.out.println("Total regions: " + regions);
		result.close();
		conn.close();
		
		File db = new File("./ns-db.h2.db");
		db.delete();
	}

	public static void testNationDump() throws IOException, SQLException {
		NationsDump dump = new NationsDump();
		dump.parse();
				
		Connection conn = dump.getDatabaseConnection();
		PreparedStatement statement = conn.prepareStatement("SELECT (name) FROM nations");
		ResultSet result = statement.executeQuery();
		int total = 0;
		while(result.next()) {
			total++;
		}
		result.close();
		
		System.out.println("Total nations: " + total);
		
		statement = conn.prepareStatement("SELECT * FROM nations WHERE name = 'sakhovelo'");
		result = statement.executeQuery();
		result.next();
		for (int i = 1; i <= 10; i++) {
			if (i == 4) {
				Clob clob = result.getClob(i);
				String motto = clob.getSubString(1, (int) clob.length());
				String mottoEscaped = StringEscapeUtils.unescapeHtml(motto);
				System.out.println("Raw: " + motto + " Escaped: " + mottoEscaped);
			} else {
				System.out.println(result.getString(i));
			}
		}
		
		
		File db = new File("./ns-db.h2.db");
		db.delete();
	}
}
