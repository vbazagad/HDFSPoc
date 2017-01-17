package com.bbva.hdfs.rest;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.boot.web.support.SpringBootServletInitializer;

@SpringBootApplication
public class RestHDFSLauncher extends SpringBootServletInitializer {
 
    @Override
    protected SpringApplicationBuilder configure(SpringApplicationBuilder application) {
        return application.sources(RestHDFSLauncher.class);
    }
 

	public static void main(String[] args) {
		SpringApplication.run(RestHDFSLauncher.class, args);
	}
	
	@Bean
	public static Configuration buidConfig(){
		return new Configuration();
	}
	
	@Bean
	public static FileSystem buildFileSystem(@Value("${hdfs.url}") String hdfsUrl, @Autowired Configuration conf) throws IOException{
		return FileSystem.get(URI.create(hdfsUrl), conf);
	}

}
