package com.facebook.LinkBench;

import java.io.IOException;
import java.util.Properties;

abstract public class BaseBlueprintsGraphStoreTest extends GraphStoreTestBase {

    @Override
    protected DummyLinkStore getStoreHandle(boolean initialize) throws IOException, Exception {
        DummyLinkStore result = new DummyLinkStore(getStore());
        return result;
    }

    @Override
    final protected void addCustomProps(Properties props) {
        String graphProviderClassName = getGraphProviderClassName();
        props.setProperty("tinkerpop.blueprints.graph", graphProviderClassName);
        addCustomGraphProps(props);
    }

    protected void addCustomGraphProps(Properties props){};

    abstract protected LinkStoreBlueprints getStore();

    abstract protected String getGraphProviderClassName();

}
