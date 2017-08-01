package com.apaceh.ew.kafka.utils;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * RestServer configuration.
 * @author EthanWang
 *
 */
public class ServerConfiguration {
	private static Properties conf;
	
	public static Properties getConf(){
		if (conf == null) {
			synchronized(ServerConfiguration.class){
				if (conf == null) {
					new ServerConfiguration();
				}
			}
		}
		return conf;
	}
	
	private ServerConfiguration(){
		loadConf();
	}

	private void loadConf() {
		InputStream inputStream = null;
		try {
			String base = this.getClass().getResource("/").getPath();
			String confpath = base + "config.properties";
			inputStream = new FileInputStream(new File(confpath));
			conf = new Properties();
			conf.load(inputStream);	
		} catch (Exception e) {
			e.printStackTrace();
			throw new RuntimeException(e);
		}
		finally {
			if (inputStream != null) {
				try {
					inputStream.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
	}
}
