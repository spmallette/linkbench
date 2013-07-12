package com.facebook.LinkBench.blueprints;

import com.tinkerpop.blueprints.Graph;
import com.tinkerpop.blueprints.impls.tg.TinkerGraph;

public class TinkerGraphProvider extends GraphProvider {

    @Override
    Graph getGraphInterface() {
        return new TinkerGraph();
    }
}
