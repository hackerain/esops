package com.example;

import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.io.*;

import org.apache.http.HttpHost;

import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.client.RestClientBuilder.HttpClientConfigCallback;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.composite.CompositeAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.composite.CompositeValuesSourceBuilder;
import org.elasticsearch.search.aggregations.bucket.composite.ParsedComposite;
import org.elasticsearch.search.aggregations.bucket.composite.TermsValuesSourceBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;

/**
 * Elasticsearch term aggregations to get unique field values
 *
 */
public class Agg
{
    public static void main( String[] args )
    {
        String username = "elastic";
        String password = "ustack";
        String hostname = "localhost";
        int port = 9200;
        String indexName = "index1";
        int pageSize = 3;
        String fieldName = "rec_no.keyword";

        // Create the low-level client
        final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(username, password));

        RestClientBuilder builder = RestClient.builder(
            new HttpHost(hostname, port))
            .setHttpClientConfigCallback(new HttpClientConfigCallback() {
                @Override
                public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpClientBuilder) {
                    return httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
                }
            });

        RestHighLevelClient client = new RestHighLevelClient(builder);

        try {
            SearchRequest searchRequest = new SearchRequest(indexName);
            SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
            List<CompositeValuesSourceBuilder<?>> sourceBuilderList = new ArrayList<>();
            sourceBuilderList.add(new TermsValuesSourceBuilder("rec_no").field(fieldName));
            CompositeAggregationBuilder compositeAggregationBuilder = AggregationBuilders.composite("rec_nos", sourceBuilderList).size(pageSize);

            RangeQueryBuilder rangeQueryBuilder = QueryBuilders.rangeQuery("timestamp").from("2022-07-01").to("2022-07-03");

            searchSourceBuilder.query(rangeQueryBuilder);
            searchSourceBuilder.aggregation(compositeAggregationBuilder);
            searchSourceBuilder.size(0);
            searchRequest.source(searchSourceBuilder);

            SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
            Map<String, Object> afterKey;

            File file = new File(indexName + ".txt");
            if (file.exists()) {
                file.delete();
            }
            BufferedWriter out = new BufferedWriter(new FileWriter(file.getName(), true));

            while (searchResponse != null) {
                Aggregations aggregations = searchResponse.getAggregations();
                ParsedComposite parsedComposite = aggregations.get("rec_nos");
                afterKey = parsedComposite.afterKey();
                List<ParsedComposite.ParsedBucket> buckets = parsedComposite.getBuckets();
                for (ParsedComposite.ParsedBucket parsedBucket : buckets) {
                    for (Map.Entry<String, Object> m : parsedBucket.getKey().entrySet()) {
                        String  value = String.valueOf(m.getValue());
                        String[] items = value.split(",");
                        for (String item : items) {
                            out.write(item+"\n");
                        }
                    }
                }
                if (afterKey != null) {
                    System.out.printf("Get after %s%n", afterKey.get("rec_no"));
                    compositeAggregationBuilder.aggregateAfter(afterKey);
                    searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
                } else {
                    searchResponse = null;
                }
            }


            out.close();
            client.close();

        } catch (Exception e) {
            System.out.print(e);
        }
    }
}
