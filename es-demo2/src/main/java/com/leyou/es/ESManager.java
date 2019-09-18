package com.leyou.es;

import com.alibaba.fastjson.JSON;
import com.leyou.pojo.Item;
import org.apache.http.HttpHost;
import org.apache.lucene.index.Term;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightField;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;


public class ESManager {
    RestHighLevelClient client = null;

    @Before
    public void init(){
        client = new RestHighLevelClient(
                RestClient.builder(
                        new HttpHost("127.0.0.1", 9201, "http"),
                        new HttpHost("127.0.0.1", 9202, "http"),
                        new HttpHost("127.0.0.1", 9203, "http")));
    }

    //新增一个

    @Test
    public void testDoc() throws Exception{
        Item item = new Item("1","小米500手机","手机","小米",1199.0,"q3311");
        //IndexRequest专门用于插入索引数据的对象
        IndexRequest request = new IndexRequest("item","docs",item.getId());
        //把对象转成json字符串
        String jsonString = JSON.toJSONString(item);
        request.source(jsonString, XContentType.JSON);
        client.index(request, RequestOptions.DEFAULT);
    }

    //批量新增
    @Test
    public void testBulkAddDoc() throws Exception{
        List<Item> list = new ArrayList<>();
        list.add(new Item("1", "小米手机7", "手机", "小米", 3299.00,"http://image.leyou.com/13123.jpg"));
        list.add(new Item("2", "坚果手机R1", "手机", "锤子", 3699.00,"http://image.leyou.com/13123.jpg"));
        list.add(new Item("3", "华为META10", "手机", "华为", 4499.00,"http://image.leyou.com/13123.jpg"));
        list.add(new Item("4", "小米Mix2S", "手机", "小米", 4299.00, "http://image.leyou.com/13123.jpg"));
        list.add(new Item("5", "荣耀V10", "手机", "华为", 2799.00,"http://image.leyou.com/13123.jpg"));

        BulkRequest request = new BulkRequest();
        for (Item item : list) {
            IndexRequest indexRequest = new IndexRequest("item","docs",item.getId());
            String jsonString = JSON.toJSONString(item);
            indexRequest.source(jsonString,XContentType.JSON);
            request.add(indexRequest);
        }
        client.bulk(request,RequestOptions.DEFAULT);
    }

    //删除
    @Test
    public void testDeleteDoc() throws Exception{
        DeleteRequest request = new DeleteRequest("item","docs","1");
        client.delete(request,RequestOptions.DEFAULT);
    }

    //各种查询
    @Test
    public void testSearch() throws Exception {
        //构建一个用来查询的对象
        SearchRequest searchRequest = new SearchRequest("item").types("docs");
        //构建查询方式
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        //term查询(其它略)
        //searchSourceBuilder.query(QueryBuilders.matchAllQuery());
        searchSourceBuilder.query(QueryBuilders.termQuery("title","机"));
        HighlightBuilder highlightBuilder = new HighlightBuilder();
        highlightBuilder.field("title");
        highlightBuilder.preTags("<span style='color:red'>");
        highlightBuilder.postTags("</span>");
        searchSourceBuilder.highlighter(highlightBuilder);
        //显示字段的过滤
        //searchSourceBuilder.fetchSource(new String[]{"id","title"},null);
        //数据的过滤
        //searchSourceBuilder.postFilter(QueryBuilders.termQuery("price","3299"));
        //查询结果放入到searchRequest中
        searchRequest.source(searchSourceBuilder);
        //执行查询
        SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
        SearchHits responseHits = searchResponse.getHits();
        System.out.println("总条数"+responseHits.getTotalHits());
        SearchHit[] responseHitsHits = responseHits.getHits();
        for (SearchHit responseHitsHit : responseHitsHits) {
            String jsonString = responseHitsHit.getSourceAsString();
            Item item = JSON.parseObject(jsonString, Item.class);
            //高亮
            Map<String, HighlightField> highlightFields = responseHitsHit.getHighlightFields();
            HighlightField highlightField = highlightFields.get("title");
            Text[] fragments = highlightField.getFragments();
            if(fragments!=null&&fragments.length>0){
                String title = fragments[0].toString();
                item.setTitle(title); //把item的title替换成高亮的数据
            }
            System.out.println(item);
        }
    }

    //聚合
    @Test
    public void testSearch2() throws Exception {
        //构建一个用来查询的对象
        SearchRequest searchRequest = new SearchRequest("item").types("docs");
        //构建查询方式
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        //聚合查询
        searchSourceBuilder.aggregation(AggregationBuilders.terms("brandCount").field("brand"));
        //放入到searchRequest
        searchRequest.source(searchSourceBuilder);
        //执行查询
        SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
        Aggregations aggregations = searchResponse.getAggregations();
        Terms terms = aggregations.get("brandCount");
        List<? extends Terms.Bucket> buckets = terms.getBuckets();
        buckets.forEach(bucket->{
            System.out.println(bucket.getKeyAsString()+":"+bucket.getDocCount());
        });
    }

    @After
    public void end() throws Exception {
        client.close();
    }

}
