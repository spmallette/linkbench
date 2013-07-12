package com.facebook.LinkBench.blueprints;

import com.tinkerpop.blueprints.impls.neo4j.Neo4jGraph;

public class Neo4JGraphProvider extends GraphProvider {

    @Override
    Neo4jGraph getGraphInterface() {
        final String path = "/tmp/neo4j-linkbench";
        return new Neo4jGraph(path);
    }
}
