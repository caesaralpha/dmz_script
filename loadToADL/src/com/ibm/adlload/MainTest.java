package com.ibm.adlload;

import java.io.IOException;

import com.microsoft.azure.datalake.store.ADLStoreClient;
import com.microsoft.azure.datalake.store.oauth2.AccessTokenProvider;
import com.microsoft.azure.datalake.store.oauth2.ClientCredsTokenProvider;

public class MainTest {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		String env = "prd";
		System.out.println("start with system proxy");
		System.setProperty("java.net.useSystemProxies", "true");
		
		String clientId = "f9cada24-10b3-4f2f-a866-b43421e87348";
		String authTokenEndpoint = "https://login.microsoftonline.com/0893afe9-eb86-43fe-982e-abd6fdc13c62/oauth2/token";
		String clientKey = "Sh/4eCjaMEwgjJflrc69CRhusBYvs08ITroOM1uY6zk=";
		String accountFQDN = "pertaminadtlndng"+env+"dl.azuredatalakestore.net";

		AccessTokenProvider provider = new ClientCredsTokenProvider(authTokenEndpoint, clientId, clientKey);
		ADLStoreClient client = ADLStoreClient.createClient(accountFQDN, provider);
		
		try {
			client.createDirectory("/test22");
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
