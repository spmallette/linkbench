package com.facebook.LinkBench;

import java.util.Properties;

public class Neo4JGraphTest extends BaseBlueprintsGraphStoreTest {

    LinkStoreBlueprints store;

    @Override
    protected void initStore(Properties props) throws Exception {
        store = new LinkStoreBlueprints();
    }

    @Override
    protected LinkStoreBlueprints getStore() {
        return store;
    }

    @Override
    protected void addCustomGraphProps(Properties props) {
        props.setProperty("tinkerpop.blueprints.neo4j.directory","/tmp/neo4j");
    }

    @Override
    protected String getGraphProviderClassName() {
        return "com.tinkerpop.blueprints.impls.neo4j.Neo4jGraph";
    }

}
