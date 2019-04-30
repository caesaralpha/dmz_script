package com.ibm.adlload;

public class MainLoadAdl {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		String proxy = "";
		int MAXRETRY = 3;
		
		try {
			proxy = args[0];
			if(!proxy.equalsIgnoreCase("proxy")) {
				proxy = "";
			}
		}
		catch (Exception e){
			
		}
		
		LoadToAdl lta;
		lta = new LoadToAdl(proxy);
		
//		lta.testViaJDK();
		while(MAXRETRY > 0) {
			lta.runUpload();
			MAXRETRY--;
		}
		
	}

}
