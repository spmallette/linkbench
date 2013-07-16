package com.facebook.LinkBench;

import java.util.Properties;

public class TinkerGraphTest extends BaseBlueprintsGraphStoreTest {

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
    protected String getGraphProviderClassName() {
        return "com.tinkerpop.blueprints.impls.tg.TinkerGraph";
    }

}
