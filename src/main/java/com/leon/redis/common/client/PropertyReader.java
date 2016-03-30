package com.leon.redis.common.client;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class PropertyReader {
	public static String getProperty(String propertyName, String key) throws Exception {
		Properties property = new Properties();
		InputStream in = null;
		try {
			in = PropertyReader.class.getClassLoader().getResourceAsStream(propertyName);
			property.load(in);
		} catch (Exception e) {
			throw e;
		} finally {
			try {
				in.close();
			} catch (IOException e) {
				throw e;
			}
		}

		return property.getProperty(key);
	}
}
