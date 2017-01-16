package com.bbva.hdfs.rest.controllers;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import javax.servlet.http.HttpServletRequest;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RequestPart;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.context.request.RequestAttributes;
import org.springframework.web.context.request.RequestContextHolder;
import org.springframework.web.context.request.ServletRequestAttributes;

import com.bbva.hdfs.rest.response.NamedReference;
import com.bbva.hdfs.rest.response.Response;

@RestController
public class DirectoryController extends AbstractHDFSController {
	Logger LOG= Logger.getLogger(DirectoryController.class);
	
	
	@RequestMapping(value="/hdfs/directory", method=RequestMethod.PUT)
	public Response<Void> create(@RequestParam("path") String path){
		
		try {
			return new Response<Void>(getFs().mkdirs(obtainPath(path)),"Could not create directory");
		} catch (Exception e) {
			LOG.error("Error creating directory",e);
			return new Response<Void>(e.getMessage());
		}
	}

	
	@RequestMapping(value="/hdfs/directory", method=RequestMethod.GET)
	public Response<List<NamedReference>> listFiles(@RequestParam("path") String path){
		List<NamedReference> result= new ArrayList<NamedReference>();
		FileStatus[] statuses;
		try {
			statuses = getFs().listStatus(obtainPath(path));
			if(statuses!=null){
				for(FileStatus item:statuses){
					result.add(buildReference(item));
				}
			}
		} catch (IllegalArgumentException | IOException e) {
			LOG.error("Could not list path content",e);
			return new Response<List<NamedReference>>(e.getMessage());
		}
		return new Response<List<NamedReference>>(result);
	}
	
	private NamedReference buildReference(FileStatus item) {
		NamedReference ref= new NamedReference();
		ref.setName(item.getPath().getName());
		String contextUrl = extracted();
		if(item.isDirectory()){
			ref.setHref(contextUrl+"hdfs/directory?path="+item.getPath().toUri().getRawPath());
		}else{
			ref.setHref(contextUrl+"hdfs/file?path="+item.getPath().toUri().getRawPath());
		}
		return ref;
	}


	private String extracted() {
		RequestAttributes requestAttributes = RequestContextHolder.getRequestAttributes();
		String contextUrl = null;
		if(null != requestAttributes && requestAttributes instanceof ServletRequestAttributes) {
			HttpServletRequest request = ((ServletRequestAttributes)requestAttributes).getRequest();
			contextUrl = request.getRequestURL().toString().replaceAll("hdfs/directory", "");
		}
		return contextUrl;
	}


	@RequestMapping(value="/hdfs/directory", method=RequestMethod.DELETE)
	public Response<Void> delete(@RequestParam("path") String path, @RequestParam(name="recursive", defaultValue="false") boolean recursive){
		try {
			return new Response<Void>(getFs().delete(obtainPath(path), recursive),"Could not delete directory");
		} catch (Exception e) {
			LOG.error("Error deleting directory",e);
			return new Response<Void>(e.getMessage());
		}
	}
	
}
