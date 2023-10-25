package com.timeplus.proton.response;

import com.timeplus.proton.ResponseFactory;

import java.io.IOException;
import java.io.InputStream;

import com.proton.client.data.JsonStreamUtils;

public class ProtonResponseFactory implements ResponseFactory<ProtonResponse> {
    @Override
    public ProtonResponse create(InputStream response) throws IOException {
        return JsonStreamUtils.readObject(response, ProtonResponse.class);
    }
}
