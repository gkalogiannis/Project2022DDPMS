package Project.app;

import org.elasticsearch.client.RestHighLevelClient;

import Project.tools.*;

import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;

import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.conn.ssl.TrustSelfSignedStrategy;
import org.apache.http.ssl.SSLContextBuilder;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;

import javax.net.ssl.SSLContext;




public class ESManager {

    public RestHighLevelClient CreateHighLevelClient()
            throws IOException, KeyStoreException, NoSuchAlgorithmException, CertificateException {

        boolean useSSL = true;        
        
        HttpHost httpHost = new HttpHost("localhost", 9200, "https");
        CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        credentialsProvider.setCredentials(AuthScope.ANY,new UsernamePasswordCredentials(Project_Properties.Elasticsearch_username, Project_Properties.Elasticsearch_password));//ibByBITMZhbIe10BRtWG
        
        RestClientBuilder lowLevelClientBuilder = RestClient.builder(httpHost);

        if (!useSSL) {  // Without TLS 
                lowLevelClientBuilder.setHttpClientConfigCallback(new 
                RestClientBuilder.HttpClientConfigCallback() {
                @Override
                public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpClientBuilder) {
                    return httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
                        }
                    }
                );
            } else {  // With TLS


                
                Path trustStorePath = Paths.get(Project_Properties.trustStorePath);
                Path keyStorePath = Paths.get(Project_Properties.keyStorePath);
                KeyStore trustStore = KeyStore.getInstance("pkcs12");
                KeyStore keyStore = KeyStore.getInstance("pkcs12");
                String trustStorePass="";
                String keyStorePass="";
                try (InputStream is = Files.newInputStream(trustStorePath)) {
                    
                    trustStore.load(is, trustStorePass.toCharArray());
                }
                try (InputStream is = Files.newInputStream(keyStorePath)) {
                    
                    keyStore.load(is, keyStorePass.toCharArray());
                }    
                
                try {
                final SSLContext sslcontext = SSLContextBuilder.create().loadTrustMaterial(keyStore, new 
                     TrustSelfSignedStrategy()).build();

                     lowLevelClientBuilder.setHttpClientConfigCallback(new 
                     RestClientBuilder.HttpClientConfigCallback() {
                       @Override
                       public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpClientBuilder) {
                         return httpClientBuilder.setSSLContext(sslcontext)
                                      .setDefaultCredentialsProvider(credentialsProvider);
                         }
                     });       
                    }
                catch (Exception e) {
                    System.out.println(ConsoleColors.RED+e.getMessage()+ConsoleColors.RESET);
                }
            }           
            RestHighLevelClient highLevelClient = new RestHighLevelClient(lowLevelClientBuilder);
            InfoService infoservice = new InfoService(highLevelClient);
            infoservice.ShowClusterInfo();
            return highLevelClient;
    }

    public RestClient CreateLowLevelClient(RestHighLevelClient HighClient) {

        RestClient LowLevelClient = HighClient.getLowLevelClient();
        RestClientBuilder builder = RestClient.builder(
            new HttpHost("localhost", 9200, "https"));
            builder.toString();
            return LowLevelClient;
      }
      
    
      public void CloseHighLevelCLient(RestHighLevelClient HighClient) throws IOException {

        System.out.println(ConsoleColors.WHITE+"Shutting down HighLevel Connection....."+ConsoleColors.RED+"OK");
        HighClient.close();
      }
      public void CloseLowLevelCLient(RestClient LowLevelClient) throws IOException {
        System.out.println(ConsoleColors.WHITE+"Shutting down LowLevel Connection....."+ConsoleColors.RED+"OK");
        LowLevelClient.close();
        
    }
    public void CloseAllCLients(RestHighLevelClient HighClient, RestClient LowLevelClient) throws IOException {

        HighClient.close();
        LowLevelClient.close();
    }
        
                
                
        
}
