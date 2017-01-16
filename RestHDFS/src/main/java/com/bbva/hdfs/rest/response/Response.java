package com.bbva.hdfs.rest.response;

public class Response<T extends Object> {
	
	private static String LINE_BREAK=System.getProperty("line.separator");
	
	private boolean valid=true;
	public String message;
	public T response;
	
	private String processMessage(String message) {
		if(message.contains(LINE_BREAK)){
			return message.substring(0,message.indexOf(LINE_BREAK));
		}else{
			return  message;
		}
	}
	
	public Response(){
		
	}
	
	public Response(T response){
		this.response=response;
	}
	
	public Response(String message){
		valid=false;
		this.message=processMessage(message);
	}
	
	public Response(boolean valid){
		this.valid=valid;
	}
	public Response(boolean valid, String mensaje) {
		this.valid=valid;
		if(!valid){
			this.message=processMessage(mensaje);
		}
	}

	/**
	 * @return the valid
	 */
	public boolean isValid() {
		return valid;
	}
	/**
	 * @param valid the valid to set
	 */
	public void setValid(boolean valid) {
		this.valid = valid;
	}
	/**
	 * @return the message
	 */
	public String getMessage() {
		return message;
	}
	/**
	 * @param message the message to set
	 */
	public void setMessage(String message) {
		this.message = processMessage(message);
	}

	/**
	 * @return the response
	 */
	public T getResponse() {
		return response;
	}
	/**
	 * @param response the response to set
	 */
	public void setResponse(T response) {
		this.response = response;
	}
	
}
