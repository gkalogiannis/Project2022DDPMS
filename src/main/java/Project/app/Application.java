package Project.app;

import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import Project.tools.*;

import java.io.IOException;

import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.NoSuchProviderException;
import java.security.cert.CertificateException;




@SpringBootApplication

public class Application {
    private static String[] _args;
    

    public static void main(String[] args)
            throws KeyStoreException, NoSuchAlgorithmException, CertificateException, IOException, NoSuchProviderException {

        SpringApplication.run(Application.class, args);

        ESManager EsManager = new ESManager();
        RestHighLevelClient HighClient = EsManager.CreateHighLevelClient();
        RestClient LowLevelClient = EsManager.CreateLowLevelClient(HighClient);
        InfoService infoService = new InfoService(HighClient);
        System.out.println(ConsoleColors.BLUE_BRIGHT + "Number of Indexes    ----> " + ConsoleColors.RESET
                + infoService.CountIndexes());
       
      
                
    }

}






