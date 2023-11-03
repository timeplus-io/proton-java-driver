package com.timeplus.proton.client.naming;

import java.net.InetSocketAddress;

import com.timeplus.proton.client.ProtonDnsResolver;
import com.timeplus.proton.client.ProtonProtocol;
import com.timeplus.proton.client.logging.Logger;
import com.timeplus.proton.client.logging.LoggerFactory;

import org.xbill.DNS.Lookup;
import org.xbill.DNS.Record;
import org.xbill.DNS.SRVRecord;
import org.xbill.DNS.TextParseException;
import org.xbill.DNS.Type;

public class SrvResolver extends ProtonDnsResolver {
    private static final Logger log = LoggerFactory.getLogger(SrvResolver.class);

    protected SRVRecord lookup(String srvDns, boolean basedOnWeight) {
        Record[] records = null;
        try {
            records = new Lookup(srvDns, Type.SRV).run();
        } catch (TextParseException e) {
            // fallback to a cached entry?
            log.warn("Not able to resolve given DNS query: [%s]", srvDns, e);
        }

        SRVRecord record = null;
        if (records != null) {
            if (basedOnWeight) {
                for (int i = 0; i < records.length; i++) {
                    SRVRecord rec = (SRVRecord) records[i];
                    if (record == null || record.getWeight() > rec.getWeight()) {
                        record = rec;
                    }
                }
            } else {
                record = (SRVRecord) records[0];
            }
        }

        return record;
    }

    @Override
    public InetSocketAddress resolve(ProtonProtocol protocol, String host, int port) {
        if (protocol == null || host == null) {
            throw new IllegalArgumentException("Non-null protocol and host are required");
        }

        SRVRecord r = lookup(host, false);
        if (r != null) {
            host = r.getName().canonicalize().toString(true);
            port = r.getPort();
        }
        return new InetSocketAddress(host, port);
    }
}
