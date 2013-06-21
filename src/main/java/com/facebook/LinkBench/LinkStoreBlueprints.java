package com.facebook.LinkBench;

import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Graph;
import com.tinkerpop.blueprints.KeyIndexableGraph;
import com.tinkerpop.blueprints.TransactionalGraph;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.impls.neo4j.Neo4jGraph;

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
                        e.setProperty("visibility", VISIBILITY_HIDDEN);
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
                            e.setProperty("visibility", VISIBILITY_HIDDEN);
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
            if (e.getVertex(Direction.IN).equals(v2)) {
                return new Link(id1, link_type, id2,
                        Byte.parseByte(e.getProperty("visibility").toString()),
                        e.getProperty("data").toString().getBytes(),
                        Integer.parseInt(e.getProperty("version").toString()),
                        Long.parseLong(e.getProperty("time").toString()));
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
            links.add(new Link(id1, link_type, Long.parseLong(e.getVertex(Direction.IN).getProperty("iid").toString()),
                    Byte.parseByte(e.getProperty("visibility").toString()),
                    (byte[]) e.getProperty("data"),
                    Integer.parseInt(e.getProperty("version").toString()),
                    Long.parseLong(e.getProperty("time").toString())));
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
            final Link l = new Link(id1, link_type, Long.parseLong(e.getVertex(Direction.IN).getProperty("iid").toString()),
                    Byte.parseByte(e.getProperty("visibility").toString()),
                    (byte[]) e.getProperty("data"),
                    Integer.parseInt(e.getProperty("version").toString()),
                    Long.parseLong(e.getProperty("time").toString()));

            if (l.visibility == VISIBILITY_DEFAULT &&
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
                Integer.parseInt(v.getProperty("type").toString()),
                Long.parseLong(v.getProperty("version").toString()),
                Integer.parseInt(v.getProperty("time").toString()),
                (byte[]) v.getProperty("data"));

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
        final Iterator<Vertex> itty = g.getVertices("iid", id).iterator();
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
        v.setProperty("data", node.data);
        v.setProperty("time", node.time);
        v.setProperty("type", node.type);
        v.setProperty("version", node.version);
        v.setProperty("iid", node.id);
    }

    private static void updateEdgeProperties(final Edge e, final Link a) {
        e.setProperty("visibility", a.visibility);
        e.setProperty("data", a.data);
        e.setProperty("time", a.time);
        e.setProperty("version", a.version);
    }

    static class GraphProvider {

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
                g = new Neo4jGraph("/tmp/neo4j-linkbench");
                ((KeyIndexableGraph) g).createKeyIndex("iid", Vertex.class);
            }

            openedConnections++;

            return g;
        }

        public synchronized void shutdown() {
            openedConnections--;
            if (openedConnections == 0) {
                g.shutdown();
                g = null;
            }
        }
    }
}
