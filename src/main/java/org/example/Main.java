package org.example;


import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class Main {
    static String MAIN_LINK = "https://www.interfax.ru/";
    public static void main(String[] args) throws Exception {

        TaskManager p = new TaskManager();

        ExecutorService executor = Executors.newSingleThreadExecutor();
        executor.submit(() -> {
            p.getNews(MAIN_LINK);
        });
        p.Listen();

//        ESClient esClient = new ESClient();
//
//
//        News news = esClient.searchByHash("39d6b28fe08d96330a7a879bb687099d");
//        news.sout();
//        System.out.println();
//
//        news = esClient.searchAnd("Климатические активисты приклеили постер к картине Моне в парижском музее", "https://www.interfax.ru/culture/964479");
//        news.sout();
//        System.out.println();
//
//        news = esClient.searchOr("Мишустин и Пашинян обсудили актуальные вопросы взаимодействия РФ и Армении", "https://www.interfax.ru/russia/964458");
//        news.sout();
//        System.out.println();
//
//        Map<String,Long> m = esClient.searchSortByDate();
//        System.out.println("esClient.searchNewsSortByDate()");
//        System.out.println(m);
//        System.out.println();
//
//        List<String> hashes = new ArrayList<String>();
//        hashes.add("ddd9c790dedabbeecba2a774d4ea664e");
//        hashes.add("93cc5ea64bb7f365768abbd7625b29b9");
//        List<News> nis = esClient.multiGet(hashes);
//        System.out.println("esClient.multiGetNews(hashes)");
//        for (News n: nis) {
//            n.sout();
//        }
//        System.out.println();
//
//
//
//        Map<String,String> m2 = esClient.searchByText("INTERFAX.RU");
//        System.out.println(m2);
//        System.out.println();
//
//        long c = esClient.countByDate("2024-06-01");
//        System.out.println("Новостей по дате: " + c);
//        System.out.println();
//
//        c = esClient.countLogsByLevel("ERROR");;
//        System.out.println("Логов по level: " + c);
//        System.out.println();
//
//        Map<String,String> m3 = esClient.searchByHeaderAndText("Житель Шебекино получил осколочное ранение в результате обстрела ВСУ","INTERFAX.RU","2024-06-01");
//        System.out.println(m3);
//        System.out.println();
//
//        List<String> l = esClient.searchWithLink(3);
//        System.out.println(l);
//        System.out.println();

    }

}