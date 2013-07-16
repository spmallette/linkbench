package com.facebook.LinkBench;

import java.util.Properties;

public class TitanGraphTest extends BaseBlueprintsGraphStoreTest {

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
        props.setProperty("tinkerpop.storage.directory","/tmp/titan");
        props.setProperty("tinkerpop.storage.backend","berkeleyje");
    }

    @Override
    protected String getGraphProviderClassName() {
        return "com.thinkaurelius.titan.core.TitanFactory";
    }

}

