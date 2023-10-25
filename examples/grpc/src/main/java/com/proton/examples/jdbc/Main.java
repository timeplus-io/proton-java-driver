package com.proton.examples.jdbc;

import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import com.proton.client.ProtonClient;
import com.proton.client.ProtonConfig;
import com.proton.client.ProtonCredentials;
import com.proton.client.ProtonException;
import com.proton.client.ProtonFormat;
import com.proton.client.ProtonNode;
import com.proton.client.ProtonProtocol;
import com.proton.client.ProtonRecord;
import com.proton.client.ProtonRequest;
import com.proton.client.ProtonResponse;
import com.proton.client.ProtonResponseSummary;
import com.proton.client.data.BinaryStreamUtils;
import com.proton.client.data.ProtonPipedStream;

public class Main {
    static void dropAndCreateTable(ProtonNode server, String table) throws ProtonException {
        try (ProtonClient client = ProtonClient.newInstance(server.getProtocol())) {
            ProtonRequest<?> request = client.connect(server);
            // or use future chaining
            request.query("drop table if exists " + table).execute().get();
            request.query("create table " + table + "(a String, b Nullable(String)) engine=MergeTree() order by a")
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
            ProtonRequest.Mutation request = client.connect(server).write().table(table)
                    .format(ProtonFormat.RowBinary);
            ProtonConfig config = request.getConfig();
            CompletableFuture<ProtonResponse> future;
            // back-pressuring is not supported, you can adjust the first two arguments
            try (ProtonPipedStream stream = new ProtonPipedStream(config.getMaxBufferSize(),
                    config.getMaxQueuedBuffers(), config.getSocketTimeout())) {
                // in async mode, which is default, execution happens in a worker thread
                future = request.data(stream.getInput()).execute();

                // writing happens in main thread
                for (int i = 0; i < 1000000; i++) {
                    BinaryStreamUtils.writeString(stream, String.valueOf(i % 16));
                    BinaryStreamUtils.writeNonNull(stream);
                    BinaryStreamUtils.writeString(stream, UUID.randomUUID().toString());
                }
            }

            // response should be always closed
            try (ProtonResponse response = future.get()) {
                ProtonResponseSummary summary = response.getSummary();
                return summary.getWrittenRows();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw ProtonException.forCancellation(e, server);
        } catch (ExecutionException | IOException e) {
            throw ProtonException.of(e, server);
        }
    }

    static int query(ProtonNode server, String table) throws ProtonException {
        try (ProtonClient client = ProtonClient.newInstance(server.getProtocol());
                ProtonResponse response = client.connect(server).query("select * from " + table).execute().get()) {
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
                .port(ProtonProtocol.GRPC, Integer.parseInt(System.getProperty("chPort", "9100")))
                .database("system").credentials(ProtonCredentials.fromUserAndPassword(
                        System.getProperty("chUser", "default"), System.getProperty("chPassword", "")))
                .build();

        String table = "grpc_example_table";

        try {
            dropAndCreateTable(server, table);

            insert(server, table);

            query(server, table);
        } catch (ProtonException e) {
            e.printStackTrace();
        }
    }
}
