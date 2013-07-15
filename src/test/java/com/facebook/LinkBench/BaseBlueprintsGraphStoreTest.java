package com.facebook.LinkBench;

import com.facebook.LinkBench.blueprints.TinkerGraphProvider;

import java.io.IOException;
import java.util.Properties;

abstract public class BaseBlueprintsGraphStoreTest extends GraphStoreTestBase {

    @Override
    protected DummyLinkStore getStoreHandle(boolean initialize) throws IOException, Exception {
        DummyLinkStore result = new DummyLinkStore(getStore());
        return result;
    }

    abstract LinkStoreBlueprints getStore();
}
