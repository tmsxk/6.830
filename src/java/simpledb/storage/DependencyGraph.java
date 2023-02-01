package simpledb.storage;

import simpledb.transaction.TransactionId;

import java.util.*;

public class DependencyGraph {
    private Map<TransactionId, Set<TransactionId>> graph;

    public DependencyGraph() {
        graph = new HashMap<>();
    }

    public synchronized boolean addDependency(TransactionId u, TransactionId v) {
        if (u == null || v == null) {
            return false;
        }
        if (u.equals(v)) {
            return true;
        }
        if (graph.get(u) == null){
            graph.put(u, new HashSet<>());
        }
        if (find(v, u)) {
            return false;
        } else {
            if (!graph.get(u).contains(v)) {
                graph.get(u).add(v);
            }
            return true;
        }
    }

    public synchronized void removeDependency(TransactionId u, TransactionId v) {
        if (graph.get(u) != null && graph.get(u).contains(v)) {
            graph.get(u).remove(v);
        }
    }

    private boolean find(TransactionId u, TransactionId v) {
        Queue<TransactionId>  queue = new LinkedList<>();
        queue.add(u);
        while (!queue.isEmpty()) {
            TransactionId tid = queue.remove();
            if (tid.equals(v))
                return true;
            if (graph.get(tid) != null) {
                for (TransactionId neigh: graph.get(tid)) {
                    queue.add(neigh);
                }
            }
        }
        return false;
    }
}
