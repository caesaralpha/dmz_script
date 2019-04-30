package com.ibm.adlload;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.PrintStream;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Scanner;

import org.apache.http.HttpEntity;
import org.apache.http.HttpHeaders;
import org.apache.http.HttpHost;
import org.apache.http.HttpRequest;
import org.apache.http.HttpResponse;
import org.apache.http.NameValuePair;
import org.apache.http.ProtocolException;
import org.apache.http.client.config.CookieSpecs;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.client.protocol.HttpClientContext;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.DefaultRedirectStrategy;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.protocol.HttpContext;
import org.apache.http.util.Args;
import org.json.JSONException;
import org.json.JSONObject;

import com.microsoft.azure.datalake.store.ADLFileOutputStream;
import com.microsoft.azure.datalake.store.ADLStoreClient;
import com.microsoft.azure.datalake.store.IfExists;
import com.microsoft.azure.datalake.store.oauth2.AccessTokenProvider;
import com.microsoft.azure.datalake.store.oauth2.ClientCredsTokenProvider;

public class LoadToAdl {

	public static String token = "";
	public static String dir = "C:\\DMZ\\";
	//	public static String dir = "C:\\DMZTEST\\";
	public static String dirScript = "C:\\Script\\";

	private static final String filterExt = ".csv";
	private static Logging log;
	String proxySetup = "";
	ADLStoreClient client;

	public LoadToAdl(String proxy) {
		try {
			log = new Logging();
			proxySetup = proxy;

			if(proxySetup.equalsIgnoreCase("proxy")) {
				//				System.out.println("Running With Proxy");
				token = getAuth();
			}
			else {
				System.setProperty("java.net.useSystemProxies", "true");
				System.out.println("Running With System Proxy");
			}

		} catch (Exception e) {
			e.printStackTrace();
			log.writeLog("Failed to connect through proxy, exiting");
			log.writeLog(e);
			System.exit(0);
		}
	}

	public void runUpload() {
		sentByEnv("dev",proxySetup);
		sentByEnv("prd",proxySetup);
	}

	private void sentByEnv(String env, String proxy) {

		if(!proxy.equalsIgnoreCase("proxy")) {
			String clientId = "f9cada24-10b3-4f2f-a866-b43421e87348";
			String authTokenEndpoint = "https://login.microsoftonline.com/0893afe9-eb86-43fe-982e-abd6fdc13c62/oauth2/token";
			String clientKey = "Sh/4eCjaMEwgjJflrc69CRhusBYvs08ITroOM1uY6zk=";
			String accountFQDN = "pertaminadtlndng"+env+"dl.azuredatalakestore.net";

			AccessTokenProvider provider = new ClientCredsTokenProvider(authTokenEndpoint, clientId, clientKey);
			client = ADLStoreClient.createClient(accountFQDN, provider);
		}

		System.out.println();
		System.out.println("//////////////////");
		System.out.println("RUNNING FOR "+env.toUpperCase());
		System.out.println("//////////////////");
		log.writeLog("RUNNING FOR "+env.toUpperCase());

		ArrayList<File>  allFiles = new ArrayList<File>();
		walk(dir+env, allFiles);

		if(allFiles.size()<1) {
			System.out.println("No new files found, skipping this env");
			log.writeLog("No new files found, skipping this env");
			return;
		}

		for(File f : allFiles) {
			System.out.println("Sent file : "+f.getAbsolutePath());
			log.writeLog("Sent file : "+f.getAbsolutePath());

			runOMTBatch(f.getAbsolutePath(), env, "RUNNING");

			String source = f.getAbsolutePath();
			String dest = getDestinationPath(source, env);

			System.out.println("Sent file to : "+dest);
			log.writeLog("Sent file to : "+dest);

			try {
				if(proxy.equalsIgnoreCase("proxy")) {
					uploadFile(env, dest, source);
					deleteFile(f);
					runOMTBatch(f.getAbsolutePath(), env, "SUCCESS");
				}
				else {
					uploadFileViaJDK(env, dest, source);
					deleteFile(f);
					runOMTBatch(f.getAbsolutePath(), env, "SUCCESS");
				}
			} catch (Exception e) {
				// TODO Auto-generated catch block
				try {
					runOMTBatch(f.getAbsolutePath(), env, "ABORT");
				} catch (Exception e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
					log.writeLog(e1);
				}
				e.printStackTrace();
				log.writeLog(e);
			}
			System.out.println("\tFile succesfully transfered");
			log.writeLog("File succesfully transfered");
		}
		try {
			sentLogging(env);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	private String removeExtensionFile(String str) {
		return str.substring(0, str.lastIndexOf('.'));
	}

	public void updateOMT(String fileStr, String env, String jobStatus) throws Exception {
		File file = new File(fileStr);

		String fileName = file.getName();
		fileName = removeExtensionFile(fileName).toUpperCase();

		String workflowname = "WF"+fileName.replace("_", "");
		String pipelinename=workflowname+"_1_OP_TO_ADL";
		String prcdt=getCurrentDate();

		String url = "pertaminaomtrest"+env+"wapp.azurewebsites.net",
				proxyUrl = "10.32.0.134";
		int port = 3128;

		HttpHost proxyServer = new HttpHost(proxyUrl,port,"http");
		HttpHost targetServer = new HttpHost(url,443,"https");

		CloseableHttpClient httpclient = HttpClients.createDefault();

		RequestConfig config = RequestConfig.custom()
				.setProxy(proxyServer)
				.setCookieSpec(CookieSpecs.IGNORE_COOKIES)
				.build();

		HttpGet get = new HttpGet("/api?env="+env+"&method=updateOMT&workflowname="+workflowname
				+"&pipelinename="+pipelinename
				+"&jobStatus="+jobStatus
				+"&prcdt="+prcdt);
		get.setConfig(config);

		CloseableHttpResponse response = httpclient.execute(targetServer, get);

		//		System.out.println("\nSending 'GET' request to URL : " + url);
		//		System.out.println("Response Code : " +response.getStatusLine().getStatusCode());

		BufferedReader rd = new BufferedReader(
				new InputStreamReader(response.getEntity().getContent()));

		StringBuffer result = new StringBuffer();
		String line = "";
		while ((line = rd.readLine()) != null) {
			result.append(line);
		}

		response.close();
		httpclient.close();
		rd.close();
	}

	private void updateOMTWithoutProxy(File file, String env, String jobStatus) throws Exception {
		String fileName = file.getName();
		fileName = removeExtensionFile(fileName).toUpperCase();

		String workflowname = "WF"+fileName.replace("_", "");
		String pipelinename=workflowname+"_1_OP_TO_ADL";
		String prcdt=getCurrentDate();

		String url = "pertaminaomtrest"+env+"wapp.azurewebsites.net";

		HttpHost targetServer = new HttpHost(url,443,"https");

		CloseableHttpClient httpclient = HttpClients.createDefault();

		RequestConfig config = RequestConfig.custom()
				.setCookieSpec(CookieSpecs.IGNORE_COOKIES)
				.build();

		HttpGet get = new HttpGet("/api?env="+env+"&method=updateOMT&workflowname="+workflowname
				+"&pipelinename="+pipelinename
				+"&jobStatus="+jobStatus
				+"&prcdt="+prcdt);
		get.setConfig(config);

		CloseableHttpResponse response = httpclient.execute(targetServer, get);

		//		System.out.println("\nSending 'GET' request to URL : " + url);
		//		System.out.println("Response Code : " +response.getStatusLine().getStatusCode());

		BufferedReader rd = new BufferedReader(
				new InputStreamReader(response.getEntity().getContent()));

		StringBuffer result = new StringBuffer();
		String line = "";
		while ((line = rd.readLine()) != null) {
			result.append(line);
		}

		response.close();
		httpclient.close();
		rd.close();
	}

	private void deleteFile(File fileSource) {
		if(fileSource.exists()) {
			fileSource.delete();
		}
	}

	private void uploadFile(String env, String fileDestination, String fileSource) throws Exception {
		String url = "pertaminadtlndng"+env+"dl.azuredatalakestore.net",
				proxyUrl = "10.32.0.134";
		int port = 3128;


		HttpHost proxyServer = new HttpHost(proxyUrl,port,"http");
		HttpHost targetServer = new HttpHost(url,443,"https");

		HttpClientBuilder hcb = HttpClients.custom();
		hcb.setRedirectStrategy(new DefaultRedirectStrategy() {
			public boolean isRedirected(HttpRequest request, HttpResponse response, HttpContext context) throws ProtocolException {
				Args.notNull(request, "HTTP request");
				Args.notNull(response, "HTTP response");
				int statusCode = response.getStatusLine().getStatusCode();
				switch(statusCode) {
				case 301:
				case 307:
				case 302:
				case 308:
				case 303:
					return true;
				case 304:
				case 305:
				case 306:
				default:
					return false;
				}
			}});

		CloseableHttpClient httpclient = hcb.build(); 
		HttpClientContext context = HttpClientContext.create();

		RequestConfig config = RequestConfig.custom()
				.setProxy(proxyServer)				
				.build();

		HttpPut post = new HttpPut("/webhdfs/v1/"+fileDestination+"?op=CREATE&overwrite=true");
		post.setConfig(config);
		post.setHeader(HttpHeaders.AUTHORIZATION,"Bearer "+token);

		HttpEntity entity = new ByteArrayEntity(getRawBody(fileSource));
		post.setEntity(entity);

		CloseableHttpResponse response = httpclient.execute(targetServer, post, context);

		System.out.println("\tSending file to : " + fileDestination);
		System.out.println("\tPost parameters : " + post.getEntity());
		System.out.println("\tResponse Code : " + response.getStatusLine().getStatusCode());
		log.writeLog("Sending file to : " + fileDestination);
		log.writeLog("Post parameters : " + post.getEntity());
		log.writeLog("Response Code : " + response.getStatusLine().getStatusCode());

		BufferedReader rd = new BufferedReader(new InputStreamReader(response.getEntity().getContent()));

		StringBuffer result = new StringBuffer();
		String line = "";
		while ((line = rd.readLine()) != null) {
			result.append(line);
		}

		response.close();
		httpclient.close();
		rd.close();
	}

	private void uploadFileWithoutProxya(String env, String fileDestination, String fileSource) throws Exception {
		String url = "pertaminadtlndng"+env+"dl.azuredatalakestore.net";

		HttpHost targetServer = new HttpHost(url,443,"https");

		HttpClientBuilder hcb = HttpClients.custom();
		hcb.setRedirectStrategy(new DefaultRedirectStrategy() {
			public boolean isRedirected(HttpRequest request, HttpResponse response, HttpContext context) throws ProtocolException {
				Args.notNull(request, "HTTP request");
				Args.notNull(response, "HTTP response");
				int statusCode = response.getStatusLine().getStatusCode();
				switch(statusCode) {
				case 301:
				case 307:
				case 302:
				case 308:
				case 303:
					return true;
				case 304:
				case 305:
				case 306:
				default:
					return false;
				}
			}});

		CloseableHttpClient httpclient = hcb.build(); 
		HttpClientContext context = HttpClientContext.create();

		RequestConfig config = RequestConfig.custom()			
				.build();

		HttpPut post = new HttpPut("/webhdfs/v1/"+fileDestination+"?op=CREATE&overwrite=true");
		post.setConfig(config);
		post.setHeader(HttpHeaders.AUTHORIZATION,"Bearer "+token);

		HttpEntity entity = new ByteArrayEntity(getRawBody(fileSource));
		post.setEntity(entity);

		CloseableHttpResponse response = httpclient.execute(targetServer, post, context);

		System.out.println("\tSending file to : " + fileDestination);
		System.out.println("\tPost parameters : " + post.getEntity());
		System.out.println("\tResponse Code : " + response.getStatusLine().getStatusCode());
		log.writeLog("Sending file to : " + fileDestination);
		log.writeLog("Post parameters : " + post.getEntity());
		log.writeLog("Response Code : " + response.getStatusLine().getStatusCode());

		BufferedReader rd = new BufferedReader(new InputStreamReader(response.getEntity().getContent()));

		StringBuffer result = new StringBuffer();
		String line = "";
		while ((line = rd.readLine()) != null) {
			result.append(line);
		}

		response.close();
		httpclient.close();
		rd.close();
	}

	//	private void uploadFileViaJDK(String env, String fileDestination, String fileSource) throws Exception {
	//		OutputStream stream = client.createFile(fileDestination, IfExists.OVERWRITE);
	//		byte[] buf = getRawBody(fileSource);
	//		stream.write(buf);
	//		stream.close();
	////		PrintStream out = new PrintStream(stream);
	////		out.println(readFile(fileSource, Charset.defaultCharset()));
	//
	//		System.out.println("\tSending file to : " + fileDestination);
	//		System.out.println("\tResponse Code : 201");
	//		log.writeLog("Sending file to : " + fileDestination);
	//		log.writeLog("Response Code : 201");
	//
	//		System.out.println("File created using byte array.");
	//	}

	private void uploadFileViaJDK(String env, String fileDestination, String fileSource) throws Exception {
		OutputStream stream = client.createFile(fileDestination, IfExists.OVERWRITE);
		PrintStream out = new PrintStream(stream);
		
		FileInputStream inputStream = new FileInputStream(new File(fileSource));
		Scanner sc = new Scanner(inputStream, "UTF-8");

		while (sc.hasNextLine()) {
			String line = sc.nextLine();
			out.println(line);
		} 

		out.close();
		stream.close();
		sc.close();
		//		PrintStream out = new PrintStream(stream);
		//		out.println(readFile(fileSource, Charset.defaultCharset()));

		System.out.println("\tSending file to : " + fileDestination);
		System.out.println("\tResponse Code : 201");
		log.writeLog("Sending file to : " + fileDestination);
		log.writeLog("Response Code : 201");

		System.out.println("File created using byte array.");
	}

	private String readFile(String path, Charset encoding) 
			throws IOException 
	{
		byte[] encoded = Files.readAllBytes(Paths.get(path));
		return new String(encoded, encoding);
	}

	private byte[] getRawBody(String source) throws Exception {

		FileInputStream fileInputStream = null;
		byte[] encoded = null;

		try {

			File file = new File(source);
			encoded = new byte[(int) file.length()];

			//read file into bytes[]
			fileInputStream = new FileInputStream(file);
			fileInputStream.read(encoded);

		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			if (fileInputStream != null) {
				try {
					fileInputStream.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}

		}

		//		byte[] encoded = Files.readAllBytes(Paths.get(source));
		//		return new String(encoded, Charset.defaultCharset());
		return encoded;
	}

	private String getDestinationPath(String sourcePath, String env) {
		String destPath = "";
		String currentDate = getCurrentDate();
		String regexFilter = "\\\\\\d{8}\\\\";

		destPath = sourcePath.replaceFirst(regexFilter, "\\\\"+currentDate+"\\\\");
		destPath = destPath.replace(dir+env, "");
		destPath = destPath.replace("\\", "/");
		return destPath;
	}

	private String getCurrentDate() {
		DateFormat dateFormat = new SimpleDateFormat("yyyyMMdd");
		Date date = new Date();
		return dateFormat.format(date);
	}

	private ArrayList<File> getListFileSource(String parentDir, ArrayList<File> list){	
		System.out.println("checking.. : "+parentDir);
		File[] filesInDirectory = new File(parentDir).listFiles();
		for (File f : filesInDirectory) {
			if(f.isDirectory()){
				getListFileSource(f.getAbsolutePath(), list);
			}
			else {
				String fileExtenstion = getFileExtension(f);
				if(fileExtenstion.equalsIgnoreCase(filterExt)){ 
					list.add(f);
				}
			}
		}

		return list;
	}

	private void walk( String path , ArrayList<File> listFiles) {

		File root = new File( path );
		File[] list = root.listFiles();

		if (list == null) return;

		for ( File f : list ) {
			if ( f.isDirectory() ) {
				walk( f.getAbsolutePath(), listFiles );
				//System.out.println( "Dir:" + f.getAbsoluteFile() );
			}
			else {
				//				System.out.println( "File:" + f.getAbsoluteFile() );
				if(f.getName().toLowerCase().contains("csv")) {
					listFiles.add(f.getAbsoluteFile());
				}
			}
		}
	}

	private String getFileExtension(File file) {
		String name = file.getName();
		int lastIndexOf = name.lastIndexOf(".");
		if (lastIndexOf == -1) {
			return ""; // empty extension
		}
		return name.substring(lastIndexOf);
	}

	private String getAuth() throws IOException, JSONException
	{
		String url = "login.microsoftonline.com",
				proxyUrl = "10.32.0.134";
		int port = 3128;


		HttpHost proxyServer = new HttpHost(proxyUrl,port,"http");
		HttpHost targetServer = new HttpHost(url,443,"https");

		CloseableHttpClient httpclient = HttpClients.createDefault();
		RequestConfig config = RequestConfig.custom()
				.setProxy(proxyServer)
				.build();
		HttpPost post = new HttpPost("/0893afe9-eb86-43fe-982e-abd6fdc13c62/oauth2/token");
		post.setConfig(config);

		// set header
		post.setHeader("Content-Type", "application/x-www-form-urlencoded");

		ArrayList<NameValuePair> urlParameters = new ArrayList<NameValuePair>();
		urlParameters.add(new BasicNameValuePair("grant_type", "client_credentials"));
		urlParameters.add(new BasicNameValuePair("resource", "https://management.core.windows.net/"));
		urlParameters.add(new BasicNameValuePair("client_id", "f9cada24-10b3-4f2f-a866-b43421e87348"));
		urlParameters.add(new BasicNameValuePair("client_secret", "Sh/4eCjaMEwgjJflrc69CRhusBYvs08ITroOM1uY6zk="));
		urlParameters.add(new BasicNameValuePair("AccessTokenLifetime", "3600"));

		post.setEntity(new UrlEncodedFormEntity(urlParameters));

		CloseableHttpResponse response = httpclient.execute(targetServer, post);

		//		System.out.println("\nSending 'POST' request to URL : " + url);
		//		System.out.println("Post parameters : " + post.getEntity());
		//		System.out.println("Response Code : " + response.getStatusLine().getStatusCode());

		BufferedReader rd = new BufferedReader(
				new InputStreamReader(response.getEntity().getContent()));

		StringBuffer result = new StringBuffer();
		String line = "";
		while ((line = rd.readLine()) != null) {
			result.append(line);
		}

		response.close();
		JSONObject tokenObj = new JSONObject(result.toString()); 
		String accessToken = (String) tokenObj.get("access_token"); 
		//		System.out.println("the access token is --> "+accessToken);

		return accessToken;

	}

	private String getAuthWithoutProxy() throws IOException, JSONException
	{
		String url = "login.microsoftonline.com";
		HttpHost targetServer = new HttpHost(url,443,"https");

		CloseableHttpClient httpclient = HttpClients.createDefault();
		RequestConfig config = RequestConfig.custom()
				.build();
		HttpPost post = new HttpPost("/0893afe9-eb86-43fe-982e-abd6fdc13c62/oauth2/token");
		post.setConfig(config);

		// set header
		post.setHeader("Content-Type", "application/x-www-form-urlencoded");

		ArrayList<NameValuePair> urlParameters = new ArrayList<NameValuePair>();
		urlParameters.add(new BasicNameValuePair("grant_type", "client_credentials"));
		urlParameters.add(new BasicNameValuePair("resource", "https://management.core.windows.net/"));
		urlParameters.add(new BasicNameValuePair("client_id", "f9cada24-10b3-4f2f-a866-b43421e87348"));
		urlParameters.add(new BasicNameValuePair("client_secret", "Sh/4eCjaMEwgjJflrc69CRhusBYvs08ITroOM1uY6zk="));
		urlParameters.add(new BasicNameValuePair("AccessTokenLifetime", "3600"));

		post.setEntity(new UrlEncodedFormEntity(urlParameters));

		CloseableHttpResponse response = httpclient.execute(targetServer, post);

		//		System.out.println("\nSending 'POST' request to URL : " + url);
		//		System.out.println("Post parameters : " + post.getEntity());
		//		System.out.println("Response Code : " + response.getStatusLine().getStatusCode());

		BufferedReader rd = new BufferedReader(
				new InputStreamReader(response.getEntity().getContent()));

		StringBuffer result = new StringBuffer();
		String line = "";
		while ((line = rd.readLine()) != null) {
			result.append(line);
		}

		response.close();
		JSONObject tokenObj = new JSONObject(result.toString()); 
		String accessToken = (String) tokenObj.get("access_token"); 
		//		System.out.println("the access token is --> "+accessToken);

		return accessToken;

	}

	private void sentLogging(String env) throws Exception {
		String logFile = log.getFileName();
		String logFileDestination = log.getFileNameOnly();
		String logDirDestination = "/logs/";
		if(proxySetup.equalsIgnoreCase("proxy")) {
			uploadFile(env, logDirDestination+"/"+logFileDestination, logFile);
		}
		else {
			uploadFileViaJDK(env, logDirDestination+"/"+logFileDestination, logFile);
		}	
	}

	public void testViaJDK() {

		String clientId = "f9cada24-10b3-4f2f-a866-b43421e87348";
		String authTokenEndpoint = "https://login.microsoftonline.com/0893afe9-eb86-43fe-982e-abd6fdc13c62/oauth2/token";
		String clientKey = "Sh/4eCjaMEwgjJflrc69CRhusBYvs08ITroOM1uY6zk=";
		String accountFQDN = "pertaminadtlndngprddl.azuredatalakestore.net";

		AccessTokenProvider provider = new ClientCredsTokenProvider(authTokenEndpoint, clientId, clientKey);
		ADLStoreClient client = ADLStoreClient.createClient(accountFQDN, provider);
		try {
			client.createDirectory("/test");
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	private void runOMTBatch(String file, String dest, String source) {

		Runtime runtime = Runtime.getRuntime();
		try {
			Process p1 = runtime.exec("cmd /c start java -jar "+dirScript+"\\runomt.jar "+file+" "+dest+" "+source+"");
			InputStream is = p1.getInputStream();
			int i = 0;
			while( (i = is.read() ) != -1) {
				System.out.print((char)i);
			}
		} catch(IOException ioException) {
			System.out.println(ioException.getMessage() );
		}

	}
}

