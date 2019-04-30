package com.ibm.adlload;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.URISyntaxException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

public class Logging {

	private static String currentDir = "";
	private static String logsDir = "";

	public Logging() {
		currentDir = getExecuteFileLoc();
		logsDir = currentDir+"\\logs";
		File logDir = new File(logsDir);
		if(!logDir.exists()) {
			logDir.mkdirs();
		}
	}

	public void writeLog(String input) {
		String logFileName = getFileName();
		try {
			FileWriter fw = new FileWriter(logFileName,true);
			fw.append(getCurrentDate() + "\tINFO\t" + input);
			fw.append("\n");
			fw.close();
		} catch (IOException e) {
			e.printStackTrace();
			//log.writeStackTrace(e);
		}
	}
	
	public void writeLog(Exception exc){
		String logFileName = getFileName();
		StringWriter stackTrace = new StringWriter();
		exc.printStackTrace(new PrintWriter(stackTrace));
		
		try {
			FileWriter fw = new FileWriter(logFileName,true);
			fw.append(getCurrentDate() + "\tERROR\t" + stackTrace.toString());
			fw.append("\n");
			fw.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public String getFileNameOnly() {
		return "LoadToADL_Log_"+getCurrentDateFile()+".log";
	}

	public String getFileName() {
		return logsDir+"\\LoadToADL_Log_"+getCurrentDateFile()+".log";
	}

	private String getCurrentDate() {
		DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		Date date = new Date();
		return dateFormat.format(date);
	}
	
	private String getCurrentDateFile() {
		DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
		Date date = new Date();
		return dateFormat.format(date);
	}

	private String getExecuteFileLoc(){
		try {
			return new File(Logging.class.getProtectionDomain().getCodeSource().getLocation().toURI().getPath()).getParent();
		} catch (URISyntaxException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}
}
