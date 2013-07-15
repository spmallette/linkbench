package com.facebook.LinkBench;

import com.facebook.LinkBench.blueprints.TitanGraphProvider;

import java.util.Properties;

public class TitanGraphTest extends BaseBlueprintsGraphStoreTest {

    LinkStoreBlueprints store;

    @Override
    protected void initStore(Properties props) throws Exception {
        store = new LinkStoreBlueprints(new TitanGraphProvider());
    }

    @Override
    LinkStoreBlueprints getStore() {
        return store;
    }
}
