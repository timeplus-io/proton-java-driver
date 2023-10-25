package com.proton.jdbc;

import java.sql.SQLException;
import java.sql.Statement;

import com.proton.client.ProtonConfig;
import com.proton.client.ProtonRequest;
import com.proton.client.ProtonRequest.Mutation;

public interface ProtonStatement extends Statement {
    @Override
    ProtonConnection getConnection() throws SQLException;

    ProtonConfig getConfig();

    ProtonRequest<?> getRequest();

    default Mutation write() {
        return getRequest().write();
    }
}
