package io.aeron.koans.cluster;

import io.aeron.cluster.ClusterTool;

import java.io.File;

/**
 * Requests a snapshot from the running Aeron Cluster leader.
 *
 * <p>Run while all three {@link ClusterNode} processes are running.
 * Pass the target node ID (default 0) as {@code args[0]} to select
 * which node's cluster directory to connect through.
 *
 * <p>The cluster leader will write a snapshot; followers receive it via log replication.
 * After stopping and restarting all nodes, the counter state will be restored.
 */
public class ClusterSnapshot {

    public static void main(final String[] args) {
        final int nodeId = args.length > 0 ? Integer.parseInt(args[0]) : 0;
        final File clusterDir = new File(
                System.getProperty("java.io.tmpdir") + "/aeron-cluster-consensus-" + nodeId);

        System.out.println("[Snapshot] Requesting snapshot via node " + nodeId +
                " clusterDir=" + clusterDir);

        if (!clusterDir.exists()) {
            System.err.println("[Snapshot] ERROR: cluster dir not found — is ClusterNode " +
                    nodeId + " running?");
            System.exit(1);
        }

        final boolean ok = ClusterTool.snapshot(clusterDir, System.out);
        System.out.println("[Snapshot] " + (ok ? "SUCCESS — snapshot accepted by leader" :
                "FAILED — check that the cluster is running and has a leader"));
    }
}
