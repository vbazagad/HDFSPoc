package com.bbva.hdfs.rest.controllers;

import java.io.IOException;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpEntity;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RequestPart;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.multipart.MultipartFile;

import com.bbva.hdfs.rest.response.Response;


@RestController
public class FileController extends AbstractHDFSController{
	
	Logger LOG = Logger.getLogger(FileController.class);


	@RequestMapping(value="hdfs/file", consumes=MediaType.MULTIPART_FORM_DATA_VALUE, method=RequestMethod.PUT)
	public Response<Void> upload(@RequestPart("file") MultipartFile file, @RequestParam("datos") String path){
		try{
			FSDataOutputStream os = getFs().create(obtainPath(path));
			IOUtils.copy(file.getInputStream(), os);
			os.hflush();
			os.close();
			return new Response<Void>();
		}catch(Exception e){
			LOG.error("Error creating file",e);
			return new Response<Void>(e.getMessage());
		}
	}
	
	@RequestMapping(value="hdfs/file", method=RequestMethod.GET)
	public HttpEntity download(@RequestParam("path") String path){
		try{
			FSDataInputStream is = getFs().open(obtainPath(path));
			return new HttpEntity<byte[]>(IOUtils.toByteArray(is));
		} catch (IOException e) {
			LOG.error("Error obtaining bytes form file",e);
			return new HttpEntity<Response<Void>>(new Response<Void>(e.getMessage()));
		}		
	}
	
}
