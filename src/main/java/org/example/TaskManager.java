package org.example;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DeliverCallback;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.JSONArray;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.util.*;

//TODO: single channel for single connection

public class TaskManager {
    private static final String USER = "guest";
    private static final String HOST = "127.0.0.1";
    private static final String PASS = "guest";
    Map<Integer, PreNews> cache = new HashMap<>();
    private String SEND_QUEUE_NAME = "planner_queue";
    private String RECIEVE_QUEUE_NAME = "crawler_queue";
    String SEND_KEY = "pl_to_cr";
    private String RECIEVE_KEY = "cr_to_pl";
    Listener listener;

    Channel channel;
    ESClient esClient;

    static {
        System.setProperty("log4j.configurationFile", "C:\\Users\\senio\\IdeaProjects\\planner\\src\\main\\java\\org\\example\\log4j2.xml");
    }

    private static Logger LOG = LogManager.getLogger();
    private static final String EXCHANGE = "parser";

    TaskManager() throws IOException {
        this.listener = new Listener(RECIEVE_KEY, EXCHANGE, RECIEVE_QUEUE_NAME);
        this.esClient = new ESClient();

        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(HOST);
        factory.setUsername(USER);
        factory.setPassword(PASS);
        factory.useNio();
        factory.setConnectionTimeout(50000);
        factory.setRequestedHeartbeat(100);

        Connection connection;
        try {
            connection = factory.newConnection();
        } catch (Exception e) {
            LOG.info("Queue()");
            LOG.info(e);
            return;
        }
        Channel channel;
        try {
            channel = connection.createChannel();
        } catch (Exception e) {
            LOG.info("connection.createChannel");
            LOG.info(e);
            return;
        }
        try {
            channel.exchangeDeclare(EXCHANGE, "direct");
        } catch (Exception e) {
            LOG.info("channel.exchangeDeclare");
            LOG.info(e);
            return;
        }

        try {
            channel.queueDeclare(SEND_QUEUE_NAME, false, false, false, null);
        } catch (Exception e) {
            LOG.info("channel.queueDeclare");
            LOG.info(e);
            return;
        }
        this.channel = channel;
    }

    public void stop() throws IOException {
        if (esClient != null) {
            esClient.close();
        }
    }

    public boolean getNews(String url) {
        try {
            Document doc;
            try {
                doc = Jsoup.connect(url).get();
            } catch (Exception e) {
                LOG.info("Ошибка получения HTML страницы");
                LOG.info(e);
                return false;
            }
            int code = doc.connection().response().statusCode();
            switch (code) {
                case 200:
                    break;
                case 404:
                    LOG.error("Error 404");
                    return false;
                case 500:
                    LOG.error("Error 500");
                    return false;
                default:
                    LOG.error("Неверный статус-код: %d\n", code);
                    return false;
            }
            Elements timeline = doc.select("div.timeline");
            Elements divs = timeline.select("div");
            for (Element div : divs) {
                Elements dates = div.select("time");
                Elements title = div.select("a");
                PreNews nif = new PreNews(title.attr("title"), dates.attr("datetime"), title.attr("href"));
                if (!nif.ok) {
                    continue;
                }
                nif.hashMD5 = MD5ByPreNews(nif);
                nif.print();
                cache.put(nif.ID, nif);
            }
            Proccess();
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
        return true;
    }

    public void Listen() throws IOException {
        listener.Listen(store);
    }

    public void Send(String msg) {
        try {
            channel.basicPublish(EXCHANGE, SEND_KEY, null, msg.getBytes());
        } catch (Exception e) {
            LOG.error("channel.basicPublish");
            LOG.error(e);
        }
    }

    DeliverCallback store = (consumerTag, delivery) -> {
        String jsonString = new String(delivery.getBody(), StandardCharsets.UTF_8);
        ObjectMapper objectMapper = new ObjectMapper();

        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
        objectMapper.setDateFormat(dateFormat);

        News ni;
        try {
            ni = objectMapper.readValue(jsonString, News.class);
            if (ni == null) {
                return;
            }
        } catch (JsonProcessingException e) {
            LOG.error("objectMapper.writeValueAsString(ni)");
            LOG.error(e);
            return;
        }
        listener.getChannel().basicAck(delivery.getEnvelope().getDeliveryTag(), false);
        if (!ni.Valid()) {
            return;
        }
        String hashtext = MD5ByNews(ni);
        ni.setHash(hashtext);

        News storedni = esClient.searchByHash(hashtext);

//      Сохранение в базу
        if (storedni == null) {
            LOG.info("Записывается: " + ni.header);
            esClient.store(ni);
//            ni.print();
        }

    };

    //  Отправка на обработку в crawler
    private void Proccess() {
        List<String> urls = new ArrayList<>();
        this.cache.forEach((key, value) -> {
            try {
                String hash = MD5ByPreNews(value);
                News foundNews = esClient.searchByHash(hash);
//              Если записи в базе нет
                if (foundNews == null) {
                    LOG.debug("В базе данных новость не найдена: " + value.link);
                    urls.add(value.link);
                }
            } catch (IOException e) {
                LOG.error(e);
            }

        });
        JSONArray jarr = new JSONArray(urls);

        String json = jarr.toString();
        LOG.info(json);
        Send(json);
    }

    private String MD5ByNews(News ni) {
        MessageDigest md = null;
        try {
            md = MessageDigest.getInstance("MD5");
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }
        byte[] messageDigest = md.digest((ni.header + ni.link).getBytes());
        BigInteger no = new BigInteger(1, messageDigest);
        String hashtext = no.toString(16);
        return hashtext;
    }

    private String MD5ByPreNews(PreNews ni) {
        MessageDigest md = null;
        try {
            md = MessageDigest.getInstance("MD5");
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }
        byte[] messageDigest = md.digest((ni.title + ni.link).getBytes());
        BigInteger no = new BigInteger(1, messageDigest);
        String hashtext = no.toString(16);
        return hashtext;
    }
}
