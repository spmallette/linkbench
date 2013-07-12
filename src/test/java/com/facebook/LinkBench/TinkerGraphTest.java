package com.facebook.LinkBench;

import com.facebook.LinkBench.blueprints.TinkerGraphProvider;

import java.util.Properties;

public class TinkerGraphTest extends BaseBlueprintsGraphStoreTest {

    LinkStoreBlueprints store;

    @Override
    protected void initStore(Properties props) throws Exception {
        store = new LinkStoreBlueprints(new TinkerGraphProvider());
    }

    @Override
    LinkStoreBlueprints getStore() {
        return store;
    }
}
