package Project.app;

import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import Project.tools.*;

import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;

import java.io.IOException;

public class IndexService {

    static RestHighLevelClient client;

    public IndexService(RestHighLevelClient client) {
        IndexService.client = client;
    }

    public void CreateIndex(String json, String indexname) {
        try {
            IndexRequest req = new IndexRequest(indexname);
            req.source(json, XContentType.JSON);
            IndexResponse response = client.index(req, RequestOptions.DEFAULT);
            // TODO : check if index already exists
            System.out.println(ConsoleColors.BLUE + "Creating Index       ---->" + ConsoleColors.RESET + " name : "
                    + indexname + " | result : " + ConsoleColors.RED + response.getResult().toString()
                    + ConsoleColors.RESET);
        } catch (IOException e1) {
            e1.printStackTrace();
        }
    }

    public void DeleteIndex(String indexname) {
        try {
            DeleteIndexRequest req = new DeleteIndexRequest(indexname);
            AcknowledgedResponse response = client.indices().delete(req, RequestOptions.DEFAULT);
            // TODO : check if index exists
            System.out.println(ConsoleColors.BLUE + "Deleting Index       ---->" + ConsoleColors.RESET + " name : "
            + indexname + " | Acknowledged : " + ConsoleColors.RED + response.isAcknowledged()
            + ConsoleColors.RESET);
        } catch (IOException e1) {
            e1.printStackTrace();
        }
    }

    public void ShowIndexes(String json) {
        //String json = "{" + "\"name\":\"greg\"" + ",\"age\":\"42\"" + "}";
        try {
            IndexRequest req = new IndexRequest("test");

            req.source(json, XContentType.JSON);
            IndexResponse response = client.index(req, RequestOptions.DEFAULT);
            System.out.println(response.toString());
        } catch (IOException e1) {
            e1.printStackTrace();
        }
    }

    public void ViewIndex(String indexname) {
        SearchRequest searchRequest = new SearchRequest(indexname); 
		SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
		//searchSourceBuilder.size(1000);
		searchSourceBuilder.sort("_id");
		searchSourceBuilder.query(QueryBuilders.matchAllQuery());
		searchRequest.source(searchSourceBuilder);
		try {
			SearchResponse searchResponse = client.search(searchRequest,RequestOptions.DEFAULT);
			SearchHit[] values = searchResponse.getHits().getHits();
			if(values.length > 0) {
				for(SearchHit s : values) {
					System.out.println(s.getSourceAsString());
				}	
			} else {
				System.out.println("No results found!");
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
    }
}

