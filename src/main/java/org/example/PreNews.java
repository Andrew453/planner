package org.example;

import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.UUID;

public class PreNews {
    String title;
    String link;
    String date;
    Boolean ok;
    String hashMD5;
    Boolean read = false;
    int ID;

    PreNews(String title, String date, String link) {
        this.title = title;
        if (link != "") {
            if (link.contains("sport")) {
                this.ok = false;
                return;
            }
            this.link = "https://www.interfax.ru" + link;
        } else {
            this.ok = false;
            return;
        }
        this.date = date;

        MessageDigest md = null;
        try {
            md = MessageDigest.getInstance("MD5");
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }
        byte[] messageDigest = md.digest((this.title + this.link).getBytes());
        BigInteger no = new BigInteger(1, messageDigest);
        this.hashMD5 = no.toString(16);
        this.ID = (int) UUID.randomUUID().getMostSignificantBits();
        this.ok = true;
    }

    void print() {
        System.out.println("Title: " + title);
        System.out.println("Link:" + link);
        System.out.println("Date:" + date);
        System.out.println("Hash: " + hashMD5);
    }
}
