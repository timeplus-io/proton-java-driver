package com.timeplus.proton.jdbc;

import java.sql.SQLException;
import java.sql.Statement;

import com.timeplus.proton.client.ProtonConfig;
import com.timeplus.proton.client.ProtonRequest;
import com.timeplus.proton.client.ProtonRequest.Mutation;

public interface ProtonStatement extends Statement {
    @Override
    ProtonConnection getConnection() throws SQLException;

    ProtonConfig getConfig();

    ProtonRequest<?> getRequest();

    default Mutation write() {
        return getRequest().write();
    }
}
