package org.example;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public class News {
    String hash;
    String text;
    String header;
    Date date;
    String link;


    public Map<String, String> toMap() {
        Map<String, String> map = new HashMap<>();
        map.put("hash", hash);
        map.put("text", text);
        map.put("header", header);
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
        String dateString = dateFormat.format(date);
        map.put("date", dateString);
        map.put("link", link);
        return map;
    }

    public boolean Valid() {
        if (Objects.equals(header, "")) {
            return false;
        }
        if (Objects.equals(text, "")) {
            return false;
        }
        if (date.toString().isEmpty()) {
            return false;
        }
        if (Objects.equals(link, "")) {
            return false;
        }
        return true;
    }


    public void fromMap(Map<String, Object> map) {
        this.hash = map.get("hash").toString();
        this.text = map.get("text").toString();
        this.header = map.get("header").toString();
        // Преобразовать дату из формата "yyyy-MM-dd" в объект Date
        String dateString = map.get("date").toString();
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
        try {
            this.date = dateFormat.parse(dateString);
        } catch (ParseException e) {
            throw new RuntimeException("Cannot parse date: " + dateString, e);
        }
        this.link = map.get("link").toString();
    }

    public void sout() {
        System.out.println(hash);
        System.out.println(link);
        System.out.println(header);
        System.out.println(link);

        System.out.println();
        System.out.println(text);
        System.out.println();
    }


    public String getText() {
        return text;
    }

    public String getHeader() {
        return header;
    }

    public java.util.Date getDate() {
        return date;
    }

    public String getLink() {
        return link;
    }

    public void setText(String text) {
        this.text = text;
    }

    public void setHeader(String header) {
        this.header = header;
    }

    public void setDate(Date date) {
        this.date = date;
    }

    public void setLink(String link) {
        this.link = link;
    }

    public String getHash() {
        return hash;
    }

    public void setHash(String hash) {
        this.hash = hash;
    }
}
