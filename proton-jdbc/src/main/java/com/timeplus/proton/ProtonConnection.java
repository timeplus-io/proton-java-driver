package com.timeplus.proton;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.TimeZone;


public interface ProtonConnection extends Connection {
    TimeZone getServerTimeZone();
    
    TimeZone getTimeZone();

    @Override
    ProtonStatement createStatement() throws SQLException;

    @Override
    ProtonStatement createStatement(int resultSetType, int resultSetConcurrency) throws SQLException;

    String getServerVersion() throws SQLException;
}
