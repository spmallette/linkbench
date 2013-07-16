package com.facebook.LinkBench;

import java.util.Properties;

public class OrientdbMemoryGraphTest extends OrientdbLocalGraphTest {

    @Override
    protected void addCustomGraphProps(Properties props) {
        props.setProperty("tinkerpop.blueprints.orientdb.url", "memory:tinkerpop");
    }
}
