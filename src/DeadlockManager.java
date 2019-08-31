import java.util.*;

/**
 * This class serves as the deadlock manager. It will
 * construct a graph and use BFS topological sort
 * to detect any cycle.
 *
 * @author Weiqiang Li
 * Updated: 12/05/2018
 */
public class DeadlockManager {

    protected static class Vertex {

        private final int vertexId; // vertex id, hence transaction id
        private Set<Vertex> children; // children of this vertex

        public Vertex(int vertexId) {
            this.vertexId = vertexId;
            this.children = new HashSet<>();
        }

        /**
         * Get vertex id.
         * @return vertex id
         */
        public int getVertexId() {
            return vertexId;
        }

        /**
         * Get children set.
         * @return children set
         */
        public Set<Vertex> getChildren() {
            return children;
        }

        /**
         * Check if the vertex has a specific child.
         * @param child
         * @return true if the vertex has this child
         */
        public boolean containsChild(Vertex child) {
            return children.contains(child);
        }

        /**
         * Add a child to child set.
         * @param vertex
         */
        public void addChild(Vertex vertex) {
            children.add(vertex);
        }

        /**
         * Remove a child from child set.
         * @param vertex
         */
        public void removeChild(Vertex vertex) {
            children.remove(vertex);
        }

        /**
         * Remove a child from child set by vertex id.
         * @param vertexId
         */
        public void removeChild(int vertexId) {
            children.removeIf(v -> v.getVertexId() == vertexId);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Vertex vertex = (Vertex) o;
            return vertexId == vertex.vertexId;
        }

        @Override
        public int hashCode() {
            return Objects.hash(vertexId);
        }
    }

    private Map<Vertex, Integer> graph; // <key: vertex, val: indegree>

    public DeadlockManager() {
        graph = new HashMap<>();
    }

    /**
     * Add a new vertex to the graph
     * @param vertexId
     */
    public void addVertex(int vertexId) {
        graph.put(new Vertex(vertexId), 0);
    }

    /**
     * Check if this graph contains the vertex.
     * @param vertexId - the vertex id to be tested
     * @return true if the graph contains this vertex
     */
    public boolean containsVertex(int vertexId) {
        return getVertex(vertexId) != null;
    }

    /**
     * Get vertex by vertex id, or null if it does not contain the vertex.
     * @param vertexId
     * @return vertex, or null if not exists in graph
     */
    private Vertex getVertex(int vertexId) {
        for (Vertex vertex : graph.keySet()) {
            if (vertex.getVertexId() == vertexId) {
                return vertex;
            }
        }
        return null;
    }

    /**
     * Add a child to a specific vertex.
     * @param vertexId - the parent vertex id
     * @param childId - the child vertex id
     */
    public void addChild(int vertexId, int childId) {
        if (vertexId == childId) { // Ignore self-loop
            return;
        }
        Vertex vertex = getVertex(vertexId);
        Vertex child = getVertex(childId);
        if (!vertex.containsChild(child)) {
            vertex.addChild(child);
            graph.put(child, graph.get(child) + 1);
        }
    }

    /**
     * Remove a single vertex by its id from the graph.
     * @param vertexId
     */
    public void removeVertex(int vertexId) {
        Vertex vertex = getVertex(vertexId);
        if (vertex == null) {
            return;
        }
        for (Vertex child : vertex.getChildren()) {
            graph.put(child, graph.get(child) - 1);
        }
        for (Vertex v : graph.keySet()) {
            v.removeChild(vertexId);
        }
        graph.remove(vertex);
    }

    /**
     * Detect if there is any deadlock using BFS topological sort.
     * @return a list of deadlocked transaction id; the list is empty if there is no deadlock
     */
    public List<Integer> detectDeadlock() {
        Map<Integer, Integer> detectionGraph = constructDetectionGraph();
        Queue<Integer> queue = new LinkedList<>();
        for (int vertexId : detectionGraph.keySet()) {
            if (detectionGraph.get(vertexId) == 0) {
                queue.offer(vertexId);
            }
        }
        while (!queue.isEmpty()) {
            int vertexId = queue.poll();
            Vertex vertex = getVertex(vertexId);
            for (Vertex child : vertex.getChildren()) {
                int childId = child.getVertexId();
                detectionGraph.put(childId, detectionGraph.get(childId) - 1);
                if (detectionGraph.get(childId) == 0) {
                    queue.offer(childId);
                }
            }
        }
        List<Integer> deadlockList = new ArrayList<>();
        for (int vertexId : detectionGraph.keySet()) {
            if (detectionGraph.get(vertexId) > 0) {
                deadlockList.add(vertexId);
            }
        }
        return deadlockList;
    }

    /**
     * Helper function for deadlock detection.
     * @return the detection graph used in deadlock detection
     */
    private Map<Integer, Integer> constructDetectionGraph() {
        Map<Integer, Integer> detectionGraph = new HashMap<>();
        for (Vertex vertex : graph.keySet()) {
            int vertexId = vertex.getVertexId();
            int indegree = graph.get(vertex);
            detectionGraph.put(vertexId, indegree);
        }
        return detectionGraph;
    }

    /**
     * Debug only: print the whole graph in a specific format.
     */
    public void printGraph() {
        for (Vertex vertex : graph.keySet()) {
            Set<Vertex> children = vertex.getChildren();
            StringBuilder childrenString = new StringBuilder();
            for (Vertex child : children) {
                childrenString.append(" " + child.getVertexId());
            }
            System.out.println(vertex.getVertexId() + ", " + graph.get(vertex) + " :" + childrenString.toString());
        }
    }

}
