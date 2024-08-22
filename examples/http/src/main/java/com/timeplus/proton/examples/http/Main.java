package com.timeplus.proton.examples.http;

import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import com.timeplus.proton.client.ProtonClient;
import com.timeplus.proton.client.ProtonConfig;
import com.timeplus.proton.client.ProtonCredentials;
import com.timeplus.proton.client.ProtonException;
import com.timeplus.proton.client.ProtonFormat;
import com.timeplus.proton.client.ProtonNode;
import com.timeplus.proton.client.ProtonProtocol;
import com.timeplus.proton.client.ProtonRecord;
import com.timeplus.proton.client.ProtonRequest;
import com.timeplus.proton.client.ProtonResponse;
import com.timeplus.proton.client.ProtonResponseSummary;
import com.timeplus.proton.client.data.BinaryStreamUtils;
import com.timeplus.proton.client.data.ProtonPipedStream;

public class Main {
    
    static void ping(ProtonNode server) throws Exception {
        try (ProtonClient client = ProtonClient.newInstance(server.getProtocol())) {
            client.ping(server,3);
        }
    }
    static void dropAndCreateTable(ProtonNode server, String table) throws ProtonException {
        try (ProtonClient client = ProtonClient.newInstance(server.getProtocol())) {
            ProtonRequest<?> request = client.connect(server);
            // or use future chaining
            request.query("drop stream if exists " + table).execute().get();
            request.query("create stream " + table + "(a string, b nullable(string))")
                    .execute().get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw ProtonException.forCancellation(e, server);
        } catch (ExecutionException e) {
            throw ProtonException.of(e, server);
        }
    }

    static long insert(ProtonNode server, String table) throws ProtonException {
        try (ProtonClient client = ProtonClient.newInstance(server.getProtocol())) {
            ProtonRequest<?> request = client.connect(server);
            request.query("insert into " + table + "(a,b) values ('a','1'),('b','2')")
                    .execute().get();
            return 2;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw ProtonException.forCancellation(e, server);
        } catch (ExecutionException e) {
            throw ProtonException.of(e, server);
        }
    }

    static int query(ProtonNode server, String table) throws ProtonException {
        try (ProtonClient client = ProtonClient.newInstance(server.getProtocol());
                ProtonResponse response = client.connect(server).query("select * from table(" + table + ")").execute().get()) {
            int count = 0;
            // or use stream API via response.stream()
            for (ProtonRecord rec : response.records()) {
                count++;
            }
            return count;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw ProtonException.forCancellation(e, server);
        } catch (ExecutionException e) {
            throw ProtonException.of(e, server);
        }
    }

    public static void main(String[] args) {
        ProtonNode server = ProtonNode.builder()
                .host(System.getProperty("chHost", "192.168.3.16"))
                .port(ProtonProtocol.HTTP, Integer.parseInt(System.getProperty("chPort", "8123")))
                .database("default").credentials(ProtonCredentials.fromUserAndPassword(
                        System.getProperty("chUser", "default"), System.getProperty("chPassword", "")))
                .build();

        String table = "http_example_table";

        try {
            ping(server);
            
            dropAndCreateTable(server, table);

            insert(server, table);

            query(server, table);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
