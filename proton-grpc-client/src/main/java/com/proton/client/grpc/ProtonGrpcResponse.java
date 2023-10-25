package com.proton.client.grpc;

import java.io.IOException;
import java.util.Map;

import com.proton.client.ProtonConfig;
import com.proton.client.ProtonInputStream;
import com.proton.client.ProtonResponseSummary;
import com.proton.client.data.ProtonStreamResponse;
import com.proton.client.grpc.impl.Progress;
import com.proton.client.grpc.impl.Result;
import com.proton.client.grpc.impl.Stats;

public class ProtonGrpcResponse extends ProtonStreamResponse {
    private final ProtonStreamObserver observer;
    private final Result result;

    protected ProtonGrpcResponse(ProtonConfig config, Map<String, Object> settings,
            ProtonStreamObserver observer) throws IOException {
        super(config, ProtonInputStream.of(observer.getInputStream()), settings, null, observer.getSummary());

        this.observer = observer;
        this.result = null;
    }

    protected ProtonGrpcResponse(ProtonConfig config, Map<String, Object> settings, Result result)
            throws IOException {
        super(config, ProtonInputStream.of(result.getOutput().newInput()), settings, null,
                new ProtonResponseSummary(null, null));

        this.observer = null;
        this.result = result;
        if (result.hasProgress()) {
            Progress p = result.getProgress();
            summary.update(new ProtonResponseSummary.Progress(p.getReadRows(), p.getReadBytes(),
                    p.getTotalRowsToRead(), p.getWrittenRows(), p.getWrittenBytes()));
        }

        if (result.hasStats()) {
            Stats s = result.getStats();
            summary.update(new ProtonResponseSummary.Statistics(s.getRows(), s.getBlocks(), s.getAllocatedBytes(),
                    s.getAppliedLimit(), s.getRowsBeforeLimit()));
        }
    }

    @Override
    public ProtonResponseSummary getSummary() {
        return summary;
    }
}
