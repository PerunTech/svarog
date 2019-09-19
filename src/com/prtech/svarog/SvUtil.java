/*******************************************************************************
 * Copyright (c) 2013, 2017 Perun Technologii DOOEL Skopje.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License
 * Version 2.0 or the Svarog License Agreement (the "License");
 * You may not use this file except in compliance with the License. 
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See LICENSE file in the project root for the specific language governing 
 * permissions and limitations under the License.
 *
 *******************************************************************************/
package com.prtech.svarog;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.math.BigInteger;
import java.security.MessageDigest;
import java.util.UUID;

import org.apache.commons.io.IOUtils;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.prtech.svarog_common.DbDataArray;
import com.prtech.svarog_common.DbDataObject;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.PrecisionModel;

public class SvUtil {

	public static PrecisionModel sdiPrecision = new PrecisionModel(SvConf.getSDIPrecision());
	public static GeometryFactory sdiFactory = new GeometryFactory(sdiPrecision, SvConf.getSDISrid());

	/***
	 * Method for generating a MD5 hash from a string
	 * 
	 * @param pass
	 *            The string from which a hash should be generated
	 * @return The hash in string format
	 */
	public static String getMD5(String pass) {
		try {
			MessageDigest m = MessageDigest.getInstance("MD5");
			byte[] data = pass.getBytes();
			m.update(data, 0, data.length);
			BigInteger i = new BigInteger(1, m.digest());
			return String.format("%1$032X", i);
		} catch (Exception ex) {
			System.out.print("Error generating MD5. " + ex.getMessage() + "\n");
		}
		return "";
	}

	/**
	 * Method for uppercasing an ASCII string. This method ONLY uppercases the
	 * standard asci chars from a-z to A-Z. It doesn't perform ANSI/unicode
	 * uppercase.
	 * 
	 * @param inStr
	 *            The string to be uppercased
	 * @return array of uppercased chars
	 */
	public static String svUpperCase(String inStr) {
		char value[] = inStr.toCharArray();
		int firstLower;
		final int len = inStr.length();

		/* Now check if there are any characters that need to be changed. */

		for (firstLower = 0; firstLower < len;) {
			int c = (int) value[firstLower];
			if (c >= 97 && c <= 122) {
				value[firstLower] = (char) (c - 32);
			}
			firstLower += 1;
		}
		return new String(value);
	}

	/**
	 * Method for uppercasing an ASCII string. This method ONLY uppercases the
	 * standard asci chars from a-z to A-Z. It doesn't perform ANSI/unicode
	 * uppercase.
	 * 
	 * @param inStr
	 *            The string to be uppercased
	 * @return array of uppercased chars
	 */
	public static String svUpperCase(char[] inStr) {
		char value[] = new char[inStr.length];
		System.arraycopy(inStr, 0, value, 0, inStr.length);
		int firstLower;
		final int len = inStr.length;

		/* Now check if there are any characters that need to be changed. */

		for (firstLower = 0; firstLower < len;) {
			int c = (int) value[firstLower];
			if (c >= 97 && c <= 122) {
				value[firstLower] = (char) (c - 32);
			}
			firstLower += 1;
		}
		return new String(value);
	}

	public static String getUUID() {
		return UUID.randomUUID().toString();
	}

	static JsonObject readJsonFromFile(String fileName, boolean replaceSvarogTags) {
		// init the table configs
		InputStream fis = null;
		JsonObject jobj = null;
		try {
			fis = new FileInputStream(fileName);
			String json = IOUtils.toString(fis, "UTF-8");
			if (replaceSvarogTags) {
				json = json.replace("{MASTER_REPO}", SvConf.getMasterRepo());
				json = json.replace("{DEFAULT_SCHEMA}", SvConf.getDefaultSchema());
				json = json.replace("{REPO_TABLE_NAME}", SvConf.getMasterRepo());
			}
			Gson gson = new Gson();
			jobj = gson.fromJson(json, JsonElement.class).getAsJsonObject();
		} catch (IOException e) {
			System.out.println("File "+fileName+" was not found or its not readable");// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			if (fis != null)
				try {
					fis.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
		}
		return jobj;
	}

	/**
	 * Simple function for writing a string variable to a file
	 * 
	 * @param fileName
	 *            the file to which the string should be written
	 * @param strValue
	 *            the String which should be written
	 */
	public static void saveStringToFile(String fileName, String strValue) {
		FileOutputStream fop = null;
		File file;

		try {

			file = new File(fileName);
			if (file.getParentFile() != null) {
				file.getParentFile().mkdirs();
			}
			file.createNewFile();
			fop = new FileOutputStream(file);

			Writer out = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(file), "UTF8"));

			// if file doesnt exists, then create it
			if (!file.exists()) {
				file.createNewFile();
			}

			// get the content in bytes
			out.append(strValue);

			out.flush();
			out.close();

		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				if (fop != null) {
					fop.close();
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

	}

}
