package com.facebook.LinkBench;

import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Graph;
import com.tinkerpop.blueprints.KeyIndexableGraph;
import com.tinkerpop.blueprints.TransactionalGraph;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.impls.neo4j.Neo4jGraph;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;

/**
 * A rough implementation for using LinkBench with Blueprints-enabled graphs.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class LinkStoreBlueprints extends GraphStore {

    private static String PROPERTY_DATA = "data";
    private static String PROPERTY_IID = "iid";
    private static String PROPERTY_TIME = "time";
    private static String PROPERTY_TYPE = "type";
    private static String PROPERTY_VERSION = "version";
    private static String PROPERTY_VISIBILITY = "visibility";

    private static GraphProvider graphProvider = new GraphProvider();
    private Graph g;

    @Override
    public void initialize(Properties p, Phase currentPhase, int threadId) throws IOException, Exception {
        g = graphProvider.getGraph();
    }

    @Override
    public void close() {
        graphProvider.shutdown();
    }

    @Override
    public void clearErrors(int threadID) {

    }

    @Override
    public boolean addLink(String dbid, Link a, boolean noinverse) throws Exception {
        // Blueprints doesn't care about the dbid
        try {
            final Vertex v1 = findVertex(a.id1);
            final Vertex v2 = findVertex(a.id2);

            if (v1 == null || v2 == null) {
                return false;
            }

            final Edge e = v1.addEdge(String.valueOf(a.link_type), v2);
            updateEdgeProperties(e, a);

            if (!noinverse) {
                final Edge eInverse = v2.addEdge(String.valueOf(a.link_type), v1);
                updateEdgeProperties(eInverse, a);
            }

            tryCommit();
            return true;
        } catch (Exception ex) {
            tryRollback();
            return false;
        }
    }

    @Override
    public boolean deleteLink(String dbid, long id1, long link_type, long id2, boolean noinverse, boolean expunge) throws Exception {
        // Blueprints doesn't care about the dbid
        try {
            final Vertex v1 = findVertex(id1);
            final Vertex v2 = findVertex(id2);

            if (v1 == null || v2 == null) {
                return false;
            }

            final Iterable<Edge> edges = v1.getEdges(Direction.OUT, String.valueOf(link_type));
            for (Edge e : edges) {
                if (e.getVertex(Direction.IN).equals(v2)) {
                    if (expunge) {
                        e.remove();
                    } else {
                        e.setProperty(PROPERTY_VISIBILITY, VISIBILITY_HIDDEN);
                    }
                }
            }

            if (!noinverse) {
                final Iterable<Edge> inverseEdges = v2.getEdges(Direction.OUT, String.valueOf(link_type));
                for (Edge e : inverseEdges) {
                    if (e.getVertex(Direction.IN).equals(v1)) {
                        if (expunge) {
                            e.remove();
                        } else {
                            e.setProperty(PROPERTY_VISIBILITY, VISIBILITY_HIDDEN);
                        }
                    }
                }
            }

            tryCommit();

            return true;
        } catch (Exception ex) {
            tryRollback();
            return false;
        }
    }

    @Override
    public boolean updateLink(String dbid, Link a, boolean noinverse) throws Exception {
        // Blueprints doesn't care about the dbid
        try {
            final Vertex v1 = findVertex(a.id1);
            final Vertex v2 = findVertex(a.id2);

            if (v1 == null || v2 == null) {
                return false;
            }

            final Iterable<Edge> edges = v1.getEdges(Direction.OUT, String.valueOf(a.link_type));
            for (Edge e : edges) {
                if (e.getVertex(Direction.IN).equals(v2)) {
                    updateEdgeProperties(e, a);
                }
            }

            if (!noinverse) {
                final Iterable<Edge> inverseEdges = v2.getEdges(Direction.OUT, String.valueOf(a.link_type));
                for (Edge e : inverseEdges) {
                    if (e.getVertex(Direction.IN).equals(v1)) {
                        updateEdgeProperties(e, a);
                    }
                }
            }

            tryCommit();
            return true;
        } catch (Exception ex) {
            tryRollback();
            return false;
        }
    }

    @Override
    public Link getLink(String dbid, long id1, long link_type, long id2) throws Exception {
        // Blueprints doesn't care about the dbid
        final Vertex v1 = findVertex(id1);
        final Vertex v2 = findVertex(id2);

        if (v1 == null || v2 == null) {
            return null;
        }

        final Iterable<Edge> edges = v1.getEdges(Direction.OUT, String.valueOf(link_type));
        for (Edge e : edges) {
            Vertex v;
            try {
                v = e.getVertex(Direction.IN);
            } catch (Exception ex) {
                v = null;
            }

            if (v != null && v.equals(v2)) {
                return createLink(id1, link_type, id2, e);
            }
        }

        return null;
    }

    @Override
    public Link[] getLinkList(String dbid, long id1, long link_type) throws Exception {
        // Blueprints doesn't care about the dbid
        final Vertex v1 = findVertex(id1);

        if (v1 == null) {
            return null;
        }

        final List<Link> links  = new ArrayList<Link>();
        final Iterable<Edge> edges = v1.getEdges(Direction.OUT, String.valueOf(link_type));

        for (Edge e : edges) {
            final Link l = createLink(id1, link_type, e);
            if (l != null) {
                links.add(createLink(id1, link_type, e));
            }
        }

        return links.toArray(new Link[0]);
    }

    @Override
    public Link[] getLinkList(String dbid, long id1, long link_type, long minTimestamp, long maxTimestamp, int offset, int limit) throws Exception {
        // Blueprints doesn't care about the dbid
        final Vertex v1 = findVertex(id1);

        if (v1 == null) {
            return null;
        }

        int skipped = 0;
        int matching = 0;

        final List<Link> links  = new ArrayList<Link>();
        final Iterable<Edge> edges = v1.getEdges(Direction.OUT, String.valueOf(link_type));
        for (Edge e : edges) {
            final Link l = createLink(id1, link_type, e);

            if (l != null && l.visibility == VISIBILITY_DEFAULT &&
                    l.time >= minTimestamp && l.time <= maxTimestamp) {
                if (skipped < offset) {
                    skipped++;
                    continue;
                }

                links.add(l);

                if (matching >= limit) {
                    break;
                }

            }
        }

        return links.toArray(new Link[0]);
    }

    @Override
    public long countLinks(String dbid, long id1, long link_type) throws Exception {
        // Blueprints doesn't care about the dbid
        final Vertex v1 = findVertex(id1);

        int count = 0;
        if (v1 != null) {
            final Iterable<Edge> edges = v1.getEdges(Direction.OUT, String.valueOf(link_type));
            for(Edge e : edges) {
                count++;
            }
        }

        return count;
    }

    @Override
    public void resetNodeStore(String dbid, long startID) throws Exception {
        // Blueprints doesn't care about the dbid
        int pos = 0;
        final Iterable<Vertex> vertices = g.getVertices();
        for (Vertex v : vertices) {
            if (pos < startID) {
                continue;
            }

            pos++;
            v.remove();
        }
    }

    @Override
    public long addNode(String dbid, Node node) throws Exception {
        // Blueprints doesn't care about the dbid
        try {
            // most blueprints implementations will ignore the id
            Vertex v = findVertex(node.id);
            if (v == null) {
                v = g.addVertex(node.id);
            }

            updateVertexProperties(v, node);
            tryCommit();

            return node.id;
        } catch (Exception ex) {
            tryRollback();
            throw ex;
        }
    }

    @Override
    public Node getNode(String dbid, int type, long id) throws Exception {
        // Blueprints doesn't care about the dbid or the type. The id is identifier enough to uniquely identify it.
        final Vertex v = findVertex(id);
        if (v == null) {
            return null;
        }

        final Node n = new Node(id,
                Integer.parseInt(v.getProperty(PROPERTY_TYPE).toString()),
                Long.parseLong(v.getProperty(PROPERTY_VERSION).toString()),
                Integer.parseInt(v.getProperty(PROPERTY_TIME).toString()),
                (byte[]) v.getProperty(PROPERTY_DATA));

        return n.clone();
    }

    @Override
    public boolean updateNode(String dbid, Node node) throws Exception {
        try {
            final Vertex v = findVertex(node.id);
            if (v == null) {
                return false;
            }

            updateVertexProperties(v, node);
            tryCommit();

            return true;
        } catch (Exception ex) {
            tryRollback();
            return false;
        }
    }

    @Override
    public boolean deleteNode(String dbid, int type, long id) throws Exception {
        // Blueprints doesn't care about the dbid or the type. The id is identifier enough to uniquely identify it.
        try {
            final Vertex v = findVertex(id);
            if (v != null) {
                v.remove();
                tryCommit();
            }

            return true;
        } catch (Exception ex) {
            tryRollback();
            return false;
        }
    }

    private Vertex findVertex(long id) throws Exception {
        final Iterator<Vertex> itty = g.getVertices(PROPERTY_IID, id).iterator();
        Vertex v = null;
        if (itty.hasNext()) {
            v = itty.next();
        }

        return v;
    }

    private void tryCommit() throws Exception {
        if (g.getFeatures().supportsTransactions)
            ((TransactionalGraph) g).commit();
    }

    private void tryRollback() throws Exception {
        if (g.getFeatures().supportsTransactions)
            ((TransactionalGraph) g).rollback();
    }

    private static void updateVertexProperties(final Vertex v, final Node node) {
        v.setProperty(PROPERTY_DATA, node.data);
        v.setProperty(PROPERTY_TIME, node.time);
        v.setProperty(PROPERTY_TYPE, node.type);
        v.setProperty(PROPERTY_VERSION, node.version);
        v.setProperty(PROPERTY_IID, node.id);
    }

    private static void updateEdgeProperties(final Edge e, final Link a) {
        e.setProperty(PROPERTY_VISIBILITY, a.visibility);
        e.setProperty(PROPERTY_DATA, a.data);
        e.setProperty(PROPERTY_TIME, a.time);
        e.setProperty(PROPERTY_VERSION, a.version);
    }

    private static Link createLink(final long id1, final long link_type, final Edge e) {
        try {
            // defensively check for the vertex in case it got blown away in a delete.  need try-catch as some
            // graph implementations will throw an exception here if the vertex is no longer present
            final Vertex v = e.getVertex(Direction.IN);
            if (v == null) {
                return null;
            }

            return createLink(id1, Long.parseLong(v.getProperty(PROPERTY_IID).toString()), link_type, e);
        } catch (Exception ex) {
            return null;
        }
    }

    private static Link createLink(final long id1, final long id2, final long link_type, final Edge e) {
        return new Link(id1, link_type, id2,
                Byte.parseByte(e.getProperty(PROPERTY_VISIBILITY).toString()),
                (byte[]) e.getProperty(PROPERTY_DATA),
                Integer.parseInt(e.getProperty(PROPERTY_VERSION).toString()),
                Long.parseLong(e.getProperty(PROPERTY_TIME).toString()));
    }

    static class GraphProvider {

        private final Logger logger = Logger.getLogger(ConfigUtil.LINKBENCH_LOGGER);
        private static Graph g = null;

        /**
         * Basically keeps track of graph instances grabbed/killed.  When the connection count drops to zero then
         * the graph gets shutdown and nulled out. This is an admitted hack, but at least stays within the confines
         * of what LinkBench framework currently is without introducing a big complex pull request with tons of
         * refactoring.
         */
        private int openedConnections;

        public synchronized Graph getGraph() {
            if (g == null) {
                final String path = "/tmp/neo4j-linkbench";
                g = new Neo4jGraph(path);

                if (!(g instanceof KeyIndexableGraph)) {
                    throw new RuntimeException(String.format("Graph must be of type %s", KeyIndexableGraph.class.getCanonicalName()));
                }

                final KeyIndexableGraph kg = ((KeyIndexableGraph) g);
                if (!kg.getIndexedKeys(Vertex.class).contains(PROPERTY_IID)) {
                    kg.createKeyIndex(PROPERTY_IID, Vertex.class);
                }

                logger.info(String.format("Initialized graph - %s", g));
            }

            openedConnections++;

            logger.info(String.format("OPENED - Graph 'connection' count = %s", openedConnections));

            return g;
        }

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
}
