package com.bbva.hdfs.rest.controllers;

import java.net.URI;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;

public abstract class AbstractHDFSController {

	@Value("${hdfs.url}")
	private String hdfsUrl;

	@Autowired
	private FileSystem fs;

	public AbstractHDFSController() {
		super();
	}

	protected Path obtainPath(String name) {
		URI uri = URI.create(hdfsUrl+name);
		Path path = new Path(uri);
		return path;
	}

	/**
	 * @return the hdfsUrl
	 */
	public String getHdfsUrl() {
		return hdfsUrl;
	}

	/**
	 * @param hdfsUrl the hdfsUrl to set
	 */
	public void setHdfsUrl(String hdfsUrl) {
		this.hdfsUrl = hdfsUrl;
	}

	/**
	 * @return the fs
	 */
	public FileSystem getFs() {
		return fs;
	}

	/**
	 * @param fs the fs to set
	 */
	public void setFs(FileSystem fs) {
		this.fs = fs;
	}

}