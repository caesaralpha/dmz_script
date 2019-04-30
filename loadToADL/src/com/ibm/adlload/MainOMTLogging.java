package com.ibm.adlload;

public class MainOMTLogging {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		LoadToAdl lta;
		lta = new LoadToAdl("");
		System.out.println("Running logging : "+args[0]+" | "+args[1]+" | "+args[2]);
		try {
			lta.updateOMT(args[0],args[1], args[2]);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}

}
