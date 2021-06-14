package Project.app;

import java.io.IOException;

import org.elasticsearch.action.admin.cluster.health.ClusterHealthRequest;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.indices.alias.get.GetAliasesRequest;
import org.elasticsearch.client.GetAliasesResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.cluster.health.ClusterHealthStatus;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import Project.tools.ConsoleColors;

public class InfoService {
    static RestHighLevelClient client;

    public InfoService(RestHighLevelClient client) {
        InfoService.client = client;
    }



    public void ShowClusterInfo() throws IOException {

        System.out.println(ConsoleColors.BLUE_BRIGHT + "Elasticsearch node name is    ----> " + ConsoleColors.WHITE
                + client.info(RequestOptions.DEFAULT).getNodeName());
        System.out.println(ConsoleColors.BLUE_BRIGHT + "Elasticsearch cluster name is ----> " + ConsoleColors.WHITE
                + client.info(RequestOptions.DEFAULT).getClusterName());
        System.out.println(ConsoleColors.BLUE_BRIGHT + "Elasticsearch version  is     ----> " + ConsoleColors.WHITE
                + client.info(RequestOptions.DEFAULT).getVersion().getNumber());
        System.out.println(ConsoleColors.BLUE_BRIGHT + "Lucent version is             ----> " + ConsoleColors.WHITE
                + client.info(RequestOptions.DEFAULT).getVersion().getLuceneVersion());
        System.out.println(ConsoleColors.RESET);

    }

    public int CountIndexes() {
        int count = 0;
        try {
            ClusterHealthRequest request = new ClusterHealthRequest();
            ClusterHealthResponse response = client.cluster().health(request, RequestOptions.DEFAULT);

            count = response.getActiveShards();

        } catch (IOException e1) {
            e1.printStackTrace();
        }
        return count;
    }

    public ClusterHealthStatus ShowHealthStatus(String indexname, boolean legend) {
        try {
        ClusterHealthRequest request = new ClusterHealthRequest().indices(indexname);
        ClusterHealthResponse response = client.cluster().health(request, RequestOptions.DEFAULT);
            String status = response.getStatus().toString();
            switch (status) {
                case "RED": 
                {
                    System.out.println("Index :"+indexname+" | Status :"+ConsoleColors.RED+
                    response.getStatus().toString()+ConsoleColors.RESET);
                }
                case "GREEN":
                {
                    System.out.println("Index :"+indexname+" | Status :"+ConsoleColors.GREEN+
                    response.getStatus().toString()+ConsoleColors.RESET);
                }
                case "YELLOW":
                {
                    System.out.println("Index :"+indexname+" | Status :"+ConsoleColors.YELLOW+
                    response.getStatus().toString()+ConsoleColors.RESET);
                }
            }
            if (legend) ShowIndexHealthLegend();
            return response.getStatus();

                 } catch (IOException e1) {
                    e1.printStackTrace();
                    return null;
                }
        

            }        

      
            private void ShowIndexHealthLegend() {
            }

            public void ShowAllIndexes() throws IOException, JSONException {
        GetAliasesRequest request = new GetAliasesRequest();
        GetAliasesResponse response = client.indices().getAlias(request, RequestOptions.DEFAULT);
        XContentBuilder builder = XContentFactory.jsonBuilder();
        response.toXContent(builder, ToXContent.EMPTY_PARAMS);

        JSONObject json = new JSONObject(response.getAliases().toString());
        //System.out.println(json.toString());
              
        JSONArray key = json.names ();
        for (int i = 0; i < key.length (); ++i) {
           String keys = key.getString (i); 
           System.out.println(keys.toString());
                      
        }
        System.out.println("\n");
      }

      public String GetIndexName(String Beatname) throws IOException, JSONException {
        GetAliasesRequest request = new GetAliasesRequest();
        GetAliasesResponse response = client.indices().getAlias(request, RequestOptions.DEFAULT);
        XContentBuilder builder = XContentFactory.jsonBuilder();
        response.toXContent(builder, ToXContent.EMPTY_PARAMS);

        JSONObject json = new JSONObject(response.getAliases().toString());
                     
        JSONArray key = json.names ();
        for (int i = 0; i < key.length (); ++i) {
            String keys = key.getString (i); 
            if (keys.toLowerCase().contains(Beatname))
           {
            return keys.toString();
           }
        }
        System.out.println(ConsoleColors.RED+"No index name found      ----> "+ConsoleColors.WHITE
        +"name : "+Beatname.toString()+ConsoleColors.RESET);
        return null;
      }

      public JSONObject  GetAllIndexes() throws IOException, JSONException {
        GetAliasesRequest request = new GetAliasesRequest();
        GetAliasesResponse response = client.indices().getAlias(request, RequestOptions.DEFAULT);
        XContentBuilder builder = XContentFactory.jsonBuilder();
        response.toXContent(builder, ToXContent.EMPTY_PARAMS);

        JSONObject json = new JSONObject(response.getAliases().toString());
        return json;
      }
}
