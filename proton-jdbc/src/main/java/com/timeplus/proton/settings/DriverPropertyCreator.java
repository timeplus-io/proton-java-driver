package com.timeplus.proton.settings;

import java.sql.DriverPropertyInfo;
import java.util.Properties;

public interface DriverPropertyCreator {
    DriverPropertyInfo createDriverPropertyInfo(Properties properties);
}
