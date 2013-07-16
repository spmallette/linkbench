package com.facebook.LinkBench;

import java.util.Properties;

public class OrientdbLocalGraphTest extends BaseBlueprintsGraphStoreTest {

    LinkStoreBlueprints store;

    @Override
    protected void initStore(Properties props) throws Exception {
        store = new LinkStoreBlueprints();
    }

    @Override
    protected LinkStoreBlueprints getStore() {
        return store;
    }

    /**
     * blueprints.graph              com.tinkerpop.blueprints.impls.orient.OrientGraph
     * blueprints.orientdb.url       The connection URL for the OrientGraph instance.
     * blueprints.orientdb.username  Username to connect to OrientGraph instance.
     * blueprints.orientdb.password  Password to connect to OrientGraph instance.
     */
    @Override
    protected void addCustomGraphProps(Properties props) {
        props.setProperty("tinkerpop.blueprints.orientdb.url", "local:/tmp/orientdb");
    }

    @Override
    protected String getGraphProviderClassName() {
        return "com.tinkerpop.blueprints.impls.orient.OrientGraph";
    }

}
