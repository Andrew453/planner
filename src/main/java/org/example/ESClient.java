package org.example;

import org.apache.http.HttpHost;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.search.TotalHits;
import org.elasticsearch.action.get.*;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.script.Script;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.filter.Filter;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.elasticsearch.search.aggregations.bucket.histogram.Histogram;
import org.elasticsearch.search.aggregations.metrics.Cardinality;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.xcontent.XContentType;


import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.*;

public class ESClient {
    static {
        System.setProperty("log4j.configurationFile", "C:\\Users\\senio\\IdeaProjects\\planner\\src\\main\\java\\org\\example\\log4j2.xml");
    }

    private static Logger LOG = LogManager.getLogger();


    RestHighLevelClient client;
    String INDEX_NAME = "nekich_news";

    ESClient() {
        client = new RestHighLevelClient(
                RestClient.builder(
                        new HttpHost("localhost", 9200, "http"),
                        new HttpHost("localhost", 9201, "http")));

        createIndex(INDEX_NAME);
    }

    public void close() throws IOException {
        client.close();
    }

    private void createIndex(String indexName) {
        try {
            boolean indexExists = client.indices().exists(new GetIndexRequest(indexName), RequestOptions.DEFAULT);
            if (indexExists) {
                LOG.debug("Индекс был создан: " + indexName);
                return;
            }

            CreateIndexRequest request = new CreateIndexRequest(indexName);

            request.mapping("{\n" +
                    "      \"properties\": {\n" +
                    "        \"date\": {\n" +
                    "          \"type\": \"date\",\n" +
                    "          \"format\": \"yyyy-MM-dd\"\n" +
                    "        }\n" +
                    "  }\n" +
                    "}", XContentType.JSON);
            client.indices().create(request, RequestOptions.DEFAULT);
            LOG.debug("Создан индекс: " + indexName);
        } catch (IOException e) {
            LOG.error("Ошибка: " + e.getMessage());
            return;
        }
    }


    public boolean store(News n) throws IOException {
        IndexRequest request = new IndexRequest(INDEX_NAME);
        request.id(n.getHash());

        // Установить формат даты
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
        String formattedDate = dateFormat.format(n.getDate());

        // Создать карту для хранения источника документа
        Map<String, Object> source = new HashMap<>();
        source.putAll(n.toMap());
        source.put("date", formattedDate);
        LOG.debug(source);
        // Установить источник документа
        request.source(source, XContentType.JSON);

        try {
            IndexResponse response = client.index(request, RequestOptions.DEFAULT);
            LOG.debug("Записано с идентификатором: " + response.getId());
            return true;
        } catch (IOException e) {
            LOG.error("Ошибка записи новости: " + e.getMessage());
            return false;
        }
    }


    public News searchByHash(String hash) throws IOException {
        GetRequest request = new GetRequest(INDEX_NAME, hash);

        try {
            GetResponse response = client.get(request, RequestOptions.DEFAULT);
            if (response.isExists()) {
                Map<String, Object> sourceAsMap = response.getSourceAsMap();
                News n = new News();
                n.fromMap(sourceAsMap);
                LOG.debug("Новость найдена: " + n.getHeader());
                return n;
            } else {
                LOG.debug("Новость не найдена");
                return null;
            }
        } catch (IOException e) {
            LOG.error("Ошибка поиска по хэшу новости: " + e.getMessage());
            return null;
        }
    }

    public News searchAnd(String title, String link) throws IOException {
        BoolQueryBuilder queryBuilder = QueryBuilders.boolQuery();
        queryBuilder.must(QueryBuilders.matchQuery("header", title));
        queryBuilder.must(QueryBuilders.matchQuery("link", link));

        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.query(queryBuilder);

        SearchRequest request = new SearchRequest(INDEX_NAME);
        request.source(sourceBuilder);

        try {
            SearchResponse response = client.search(request, RequestOptions.DEFAULT);
            TotalHits total = response.getHits().getTotalHits();

            SearchHit[] hits = response.getHits().getHits();
            for (SearchHit hit : hits) {
                Map<String, Object> sourceAsMap = hit.getSourceAsMap();
                News  n = new News();
                n.fromMap(sourceAsMap);
                if (n != null) {
                    LOG.debug("Найдена новость " + n.getHeader());
                    return n;
                } else {
                    LOG.debug("n == null");
                    break;
                }
            }
        } catch (IOException e) {
            LOG.error("Ошибка поиска новости: " + e.getMessage());
            return null;
        }
        return null;
    }

    public News searchOr(String title, String link) throws IOException {
        BoolQueryBuilder queryBuilder = QueryBuilders.boolQuery();
        queryBuilder.should(QueryBuilders.matchQuery("header", title));
        queryBuilder.should(QueryBuilders.matchQuery("link", link));
        queryBuilder.minimumShouldMatch(1);

        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.query(queryBuilder);

        SearchRequest request = new SearchRequest(INDEX_NAME);
        request.source(sourceBuilder);

        try {
            SearchResponse response = client.search(request, RequestOptions.DEFAULT);
            TotalHits total = response.getHits().getTotalHits();
            SearchHit[] hits = response.getHits().getHits();
            for (SearchHit hit : hits) {
                Map<String, Object> sourceAsMap = hit.getSourceAsMap();
                News n = new News();
                n.fromMap(sourceAsMap);
                if (n != null) {
                    LOG.debug("Новость найдена" + n.getHeader());
                    return n;
                } else {
                    LOG.error("n == null");
                    break;
                }
            }
        } catch (IOException e) {
            LOG.error("Ошибка поиска новости: " + e.getMessage());
            return null;
        }
        return null;
    }

    public Map<String, Long> searchSortByDate() throws IOException {
        DateHistogramAggregationBuilder aggregationBuilder = AggregationBuilders.dateHistogram("news_date_aggregation")
                .field("date")
                .format("yyyy-MM-dd")
                .calendarInterval(DateHistogramInterval.DAY);

        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.aggregation(aggregationBuilder);

        SearchRequest request = new SearchRequest(INDEX_NAME);
        request.source(sourceBuilder);

        try {
            SearchResponse response = client.search(request, RequestOptions.DEFAULT);
            Histogram newsDateAggregation = response.getAggregations().get("news_date_aggregation");
            Map<String, Long> result = new HashMap<>();
            for (Histogram.Bucket bucket : newsDateAggregation.getBuckets()) {
                result.put(bucket.getKeyAsString(), bucket.getDocCount());
            }

            return result;
        } catch (IOException e) {
            LOG.error("Ошибка поиска новостей: " + e.getMessage());
            return null;
        }
    }


    public List<News> multiGet(List<String> hashes) throws IOException {
        MultiGetRequest multiGetRequest = new MultiGetRequest();
        for (String hash : hashes) {
            multiGetRequest.add(new MultiGetRequest.Item(INDEX_NAME, hash));
        }

        try {
            MultiGetResponse multiGetResponse = client.mget(multiGetRequest, RequestOptions.DEFAULT);
            List<News> ns = new ArrayList<>();
            for (MultiGetItemResponse itemResponse : multiGetResponse.getResponses()) {
                GetResponse getResponse = itemResponse.getResponse();
                if (getResponse.isExists()) {
                    Map<String, Object> sourceAsMap = getResponse.getSourceAsMap();
                    News n = new News();
                    n.fromMap(sourceAsMap);
                    ns.add(n);
                }
            }
            return ns;
        } catch (IOException e) {
            LOG.error("Ошибка MultiGet: " + e.getMessage());
            return null;
        }
    }



    public Map<String, String> searchByText(String query) {
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        HashMap<String, String> res = new HashMap<>();
        // устанавливаем multi_match query
        sourceBuilder.query(QueryBuilders.multiMatchQuery(query, "text"));

        SearchRequest searchRequest = new SearchRequest(INDEX_NAME);
        searchRequest.source(sourceBuilder);

        try {
            SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
            SearchHit[] hits = searchResponse.getHits().getHits();
            for (SearchHit hit : hits) {
                String hash = (String) hit.getSourceAsMap().get("hash");
                String text = (String) hit.getSourceAsMap().get("text");
                res.put(hash, text);
            }
        } catch (IOException e) {
            LOG.error("Ошибка поиска: " + e.getMessage());
            return null;
        }
        return res;
    }

    public Map<String, String> searchByHeaderAndText(String header, String text, String date) {
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        HashMap<String, String> res = new HashMap<>();

        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
        boolQueryBuilder.must(QueryBuilders.matchQuery("header", header));
        boolQueryBuilder.should(QueryBuilders.matchQuery("text", text));
        boolQueryBuilder.filter(QueryBuilders.rangeQuery("date").gte(date));

        sourceBuilder.query(boolQueryBuilder);

        SearchRequest searchRequest = new SearchRequest(INDEX_NAME);
        searchRequest.source(sourceBuilder);

        try {
            SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
            SearchHit[] hits = searchResponse.getHits().getHits();
            for (SearchHit hit : hits) {
                String hash = (String) hit.getSourceAsMap().get("hash");
                String head = (String) hit.getSourceAsMap().get("header");
                res.put(hash, head);
            }
        } catch (IOException e) {
            LOG.error("Ошибка поиска: " + e.getMessage());
            return null;
        }
        return res;
    }


    public long countByDate(String date) {
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();

        // устанавливаем фильтр по дате
        sourceBuilder.query(QueryBuilders.termQuery("date", date));

        // добавляем metrics агрегацию по количеству меовов
        sourceBuilder.aggregation(AggregationBuilders.cardinality("news_count").field("hash.keyword"));

        // устанавливаем размер результата в 0, чтобы не возвращались сами документы
        sourceBuilder.size(0);

        SearchRequest searchRequest = new SearchRequest(INDEX_NAME);
        searchRequest.source(sourceBuilder);

        try {
            SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
            Cardinality newsCount = searchResponse.getAggregations().get("news_count");
            long count = newsCount.getValue();
            return count;
        } catch (IOException e) {
            LOG.error("Ошибка поиска: " + e.getMessage());
            return 0;
        }
    }

    public long countLogsByLevel(String level) throws IOException {
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();

        // устанавливаем фильтр по уровню лога с использованием multi-match query
        sourceBuilder.aggregation(AggregationBuilders.filter("info_count",
                QueryBuilders.multiMatchQuery(level, "message")));

        // устанавливаем размер результата в 0, чтобы не возвращались сами документы
        sourceBuilder.size(0);

        SearchRequest searchRequest = new SearchRequest("logs");
        searchRequest.source(sourceBuilder);

        try {
            SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
            Filter infoCount = searchResponse.getAggregations().get("info_count");
            long count = infoCount.getDocCount();
//            log.debug("Level: " + level + ", количество логов: " + count);
            return count;
        } catch (IOException e) {
            LOG.error("Ошибка поиска логов: " + e.getMessage());
            return 0;
        }
    }

    public List<String> searchWithLink(int size) throws IOException {
        SearchRequest request = new SearchRequest("meows");
        request.source().size(size);

        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
        boolQueryBuilder.must(QueryBuilders.existsQuery("link"));
        boolQueryBuilder.filter(QueryBuilders.scriptQuery(
                new Script("doc['link.keyword'].value instanceof String")));

        request.source().query(boolQueryBuilder);
        request.source().fetchSource(new String[]{"header", "link"}, null);

        try {
            SearchResponse searchResponse = client.search(request, RequestOptions.DEFAULT);
            SearchHit[] hits = searchResponse.getHits().getHits();
            List<String> results = new ArrayList<>();
            for (SearchHit hit : hits) {
                Map<String, Object> sourceAsMap = hit.getSourceAsMap();
                if (sourceAsMap.containsKey("header")) {
                    String header = (String) sourceAsMap.get("header");
                    results.add(header);
                }
            }
            return results;
        } catch (IOException e) {
            LOG.error("Ошибка поиска: " + e.getMessage());
            return null;
        }
    }








}
