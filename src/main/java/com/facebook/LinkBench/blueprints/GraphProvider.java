package com.facebook.LinkBench.blueprints;

import com.facebook.LinkBench.ConfigUtil;
import com.facebook.LinkBench.LinkStoreBlueprints;
import com.tinkerpop.blueprints.Graph;
import com.tinkerpop.blueprints.KeyIndexableGraph;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.impls.neo4j.Neo4jGraph;

import org.apache.log4j.Logger;

public abstract class GraphProvider {

    private final Logger logger = Logger.getLogger(ConfigUtil.LINKBENCH_LOGGER);
    private static Graph g;

    /**
     * Basically keeps track of graph instances grabbed/killed.  When the connection count drops
     * to zero then the graph gets shutdown and nulled out. This is an admitted hack, but at
     * least stays within the confines of what LinkBench framework currently is without
     * introducing a big complex pull request with tons of refactoring.
     */
    private int openedConnections;

    public synchronized Graph getGraph() {
        if (g == null) {

            g = getGraphInterface();

            if (!(g instanceof KeyIndexableGraph)) {
                throw new RuntimeException(String.format("Graph must be of type %s",
                                                         KeyIndexableGraph.class
                                                             .getCanonicalName()));
            }

            final KeyIndexableGraph kg = ((KeyIndexableGraph) g);
            if (!kg.getIndexedKeys(Vertex.class).contains(LinkStoreBlueprints.PROPERTY_IID)) {
                kg.createKeyIndex(LinkStoreBlueprints.PROPERTY_IID, Vertex.class);
            }

            logger.info(String.format("Initialized graph - %s", g));
        }

        openedConnections++;

        logger.info(String.format("OPENED - Graph 'connection' count = %s", openedConnections));

        return g;
    }

    abstract Graph getGraphInterface();

    public synchronized void shutdown() {
        openedConnections--;

        logger.info(String.format("CLOSED - Graph 'connection' count = %s", openedConnections));

        if (openedConnections == 0) {
            g.shutdown();

            logger.info(String.format("Graph shutdown - %s", g));

            g = null;
        }
    }
}
