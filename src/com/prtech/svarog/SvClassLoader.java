/*******************************************************************************
 *   Copyright (c) 2013, 2019 Perun Technologii DOOEL Skopje.
 *   All rights reserved. This program and the accompanying materials
 *   are made available under the terms of the Apache License
 *   Version 2.0 or the Svarog License Agreement (the "License");
 *   You may not use this file except in compliance with the License. 
 *  
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See LICENSE file in the project root for the specific language governing 
 *   permissions and limitations under the License.
 *  
 *******************************************************************************/
package com.prtech.svarog;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.jar.JarEntry;
import java.util.jar.JarInputStream;

public final class SvClassLoader extends ClassLoader {

	/**
	 * Lazy loader's private instance
	 */
	private static volatile SvClassLoader instance = null;

	/**
	 * List of JAR IDs which were loaded
	 */
	private static ConcurrentMap<String, byte[]> listOfLoadedJars = new ConcurrentHashMap<String, byte[]>();

	/**
	 * Default class loader
	 * 
	 * @param parent
	 */
	private SvClassLoader(ClassLoader parent) {
		super(parent);
	}

	/**
	 * Lazy loader to get class loader instance
	 * 
	 * @return The ClassLoader instance
	 */
	public static SvClassLoader getInstance() {

		if (instance == null) {
			synchronized (SvClassLoader.class) {
				if (instance == null) {
					instance = new SvClassLoader(SvClassLoader.class.getClassLoader());
				}
			}

		}

		return instance;
	}

	/**
	 * Method to check if a JAR with the requested ID was already loaded
	 * 
	 * @param unqId The ID of the jar file
	 * @return True if the jar is loaded
	 */
	public static Boolean isJarLoaded(String unqId) {
		Boolean isLoaded = false;
		isLoaded = listOfLoadedJars.containsKey(unqId);

		return isLoaded;
	}

	/**
	 * Method to check if a JAR with the requested ID was already loaded
	 * 
	 * @param unqId   The ID of the jar file
	 * @param byteval The the byte value of the jar file
	 * @return True if the jar is loaded
	 */
	public static Boolean isJarLoaded(String unqId, byte[] byteval) {
		Boolean isLoaded = false;
		isLoaded = listOfLoadedJars.containsKey(unqId);
		if (!isLoaded)
			for (byte[] currBytes : listOfLoadedJars.values()) {
				if (Arrays.equals(byteval, currBytes)) {
					isLoaded = true;
					listOfLoadedJars.put(unqId, currBytes);
					break;
				}
			}
		// contains(jarUnqId);
		return isLoaded;
	}

	/**
	 * The class loader first checks if the JAR with the appropriate ID is loaded
	 * then tries to load
	 * 
	 * @param className The class name to be loaded
	 * @param unqId  The JAR unique ID. When running in the rule engine, this is
	 *                  usually the Action ID
	 * @return The class definition
	 * @throws SvException
	 */
	public Class<?> loadClass(String className, String unqId) throws SvException {
		if (isJarLoaded(unqId)) {
			try {
				Class<?> c = null;
				c = loadClass(className);
				return c;
			} catch (ClassNotFoundException e) {
				throw (new SvException("system.error.re_unknown_classname", svCONST.systemUser, null, unqId, e));
			}
		} else
			throw (new SvException("system.error.re_unknown_action_type", svCONST.systemUser, null, unqId));
	}

	/**
	 * Method to load all classes from a JAR file and associate the unique ID as
	 * already loaded so we don't try to load it again if there is no need.
	 * 
	 * @param jarByteCode The byte[] array containing the jar file
	 * @param jarUnqId    The unique identifier under which this jar is loaded. For
	 *                    the rule engine it is ACTION ID
	 * @throws SvException
	 */
	public void loadJar(byte[] jarByteCode, String unqId) throws SvException {
		try {
			synchronized (listOfLoadedJars) {
				if (isJarLoaded(unqId, jarByteCode))
					return;

				InputStream is = null;
				is = new ByteArrayInputStream(jarByteCode);

				JarInputStream ji = new JarInputStream(is);
				JarEntry je = null;

				while ((je = ji.getNextJarEntry()) != null) {

					if (je.isDirectory() || !je.getName().endsWith(".class")) {
						continue;
					}
					// -6 because of .class
					String jarClass = je.getName().substring(0, je.getName().length() - 6);
					jarClass = jarClass.replace('/', '.');

					ByteArrayOutputStream baos = new ByteArrayOutputStream();
					while (true) {
						int qwe = ji.read();
						if (qwe == -1)
							break;
						baos.write(qwe);
					}
					byte[] classbytes = baos.toByteArray();
					defineClass(jarClass, classbytes, 0, classbytes.length);
				}
				listOfLoadedJars.put(unqId, jarByteCode);
			}
		} catch (IOException e) {
			throw (new SvException("system.error.re_unknown_action_type", svCONST.systemUser, null, unqId, e));
		}
	}

	public void unloadClassLoader() {
		this.unloadClassLoader();
	}

	public static byte[] getJarData(String unqId) {
		byte[] retval = null;
		Boolean isLoaded = false;
		isLoaded = listOfLoadedJars.containsKey(unqId);
		if (isLoaded) {
			retval = listOfLoadedJars.get(unqId);
		}
		return retval;
	}

}
