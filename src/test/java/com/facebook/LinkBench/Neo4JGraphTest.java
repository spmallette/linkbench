package com.facebook.LinkBench;

import com.facebook.LinkBench.blueprints.Neo4JGraphProvider;

import java.util.Properties;

public class Neo4JGraphTest extends BaseBlueprintsGraphStoreTest {

    LinkStoreBlueprints store;

    @Override
    protected void initStore(Properties props) throws Exception {
        store = new LinkStoreBlueprints(new Neo4JGraphProvider());
    }

    @Override
    LinkStoreBlueprints getStore() {
        return store;
    }
}
