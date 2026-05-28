package io.aeron.koans.cluster;

import io.aeron.archive.Archive;
import io.aeron.archive.client.AeronArchive;
import io.aeron.cluster.ClusteredMediaDriver;
import io.aeron.cluster.ConsensusModule;
import io.aeron.cluster.service.ClusteredServiceContainer;
import io.aeron.driver.MediaDriver;
import org.agrona.concurrent.SigInt;

import java.io.File;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Aeron Cluster – Node.
 *
 * <p>Launches one member of a 3-node Raft cluster hosting {@link CounterMapService}.
 * Pass node ID (0, 1, or 2) as {@code args[0]}.
 *
 * <p>Port layout per node (base = 20000 + nodeId * 100):
 * <pre>
 *   +0  ingress   client-facing endpoint
 *   +1  consensus inter-node Raft
 *   +2  log       Raft log replication
 *   +3  transfer  log catch-up for rejoining nodes
 *   +4  archive   local archive control
 * </pre>
 *
 * <p>Start all three nodes before running {@link ClusterClient}.
 */
public class ClusterNode {

    static final String CLUSTER_MEMBERS =
            "0,localhost:20000,localhost:20001,localhost:20002,localhost:20003,localhost:20004|" +
            "1,localhost:20100,localhost:20101,localhost:20102,localhost:20103,localhost:20104|" +
            "2,localhost:20200,localhost:20201,localhost:20202,localhost:20203,localhost:20204";

    static final String INGRESS_ENDPOINTS = "0=localhost:20000,1=localhost:20100,2=localhost:20200";

    public static void main(final String[] args) throws Exception {
        if (args.length < 1) {
            System.err.println("Usage: ClusterNode <nodeId: 0|1|2>");
            System.exit(1);
        }

        final int nodeId = Integer.parseInt(args[0]);
        final int base   = 20000 + nodeId * 100;

        final String aeronDir   = System.getProperty("java.io.tmpdir") + "/aeron-cluster-node-" + nodeId;
        final String archiveDir = System.getProperty("java.io.tmpdir") + "/aeron-cluster-archive-" + nodeId;
        final String clusterDir = System.getProperty("java.io.tmpdir") + "/aeron-cluster-consensus-" + nodeId;

        final AtomicBoolean running = new AtomicBoolean(true);
        SigInt.register(() -> running.set(false));

        System.out.println("[Node " + nodeId + "] Starting...");
        System.out.printf("[Node %d]   ingress  : localhost:%d%n", nodeId, base);
        System.out.printf("[Node %d]   aeron    : %s%n", nodeId, aeronDir);
        System.out.printf("[Node %d]   archive  : %s%n", nodeId, archiveDir);

        final String archiveChannel = "aeron:udp?endpoint=localhost:" + (base + 4);

        try (
            ClusteredMediaDriver clusteredDriver = ClusteredMediaDriver.launch(
                new MediaDriver.Context()
                    .aeronDirectoryName(aeronDir)
                    .dirDeleteOnStart(true),
                new Archive.Context()
                    .aeronDirectoryName(aeronDir)
                    .archiveDir(new File(archiveDir))
                    .controlChannel(archiveChannel)
                    .replicationChannel("aeron:udp?endpoint=localhost:0")
                    .deleteArchiveOnStart(true),
                new ConsensusModule.Context()
                    .aeronDirectoryName(aeronDir)
                    .clusterDir(new File(clusterDir))
                    .clusterMemberId(nodeId)
                    .clusterMembers(CLUSTER_MEMBERS)
                    .ingressChannel("aeron:udp?endpoint=localhost:" + base)
                    .archiveContext(new AeronArchive.Context()
                            .controlRequestChannel(archiveChannel)
                            .controlResponseChannel("aeron:udp?endpoint=localhost:" + (base + 5))
                            .aeronDirectoryName(aeronDir)));
            ClusteredServiceContainer serviceContainer = ClusteredServiceContainer.launch(
                new ClusteredServiceContainer.Context()
                    .aeronDirectoryName(aeronDir)
                    .clusterDir(new File(clusterDir))
                    .archiveContext(new AeronArchive.Context()
                            .controlRequestChannel(archiveChannel)
                            .controlResponseChannel("aeron:udp?endpoint=localhost:" + (base + 6))
                            .aeronDirectoryName(aeronDir))
                    .clusteredService(new CounterMapService()))
        ) {
            System.out.println("[Node " + nodeId + "] Running — Ctrl-C to stop");
            while (running.get()) {
                Thread.sleep(100);
            }
        }

        System.out.println("[Node " + nodeId + "] Stopped");
    }
}
