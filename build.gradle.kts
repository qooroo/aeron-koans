plugins {
    java
}

group = "io.aeron.koans"
version = "1.0-SNAPSHOT"

java {
    sourceCompatibility = JavaVersion.VERSION_17
    targetCompatibility = JavaVersion.VERSION_17
}

repositories {
    mavenCentral()
}

val aeronVersion = "1.44.0"
val sbeVersion   = "1.30.0"

// Separate configuration so SbeTool is not on the runtime classpath
val sbeCodegen: Configuration by configurations.creating

dependencies {
    implementation("io.aeron:aeron-all:$aeronVersion")
    implementation("uk.co.real-logic:sbe-all:$sbeVersion")
    sbeCodegen("uk.co.real-logic:sbe-tool:$sbeVersion")
    testImplementation("org.junit.jupiter:junit-jupiter:5.10.2")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
}

// ---------------------------------------------------------------------------
// SBE code generation
// ---------------------------------------------------------------------------
val sbeGeneratedDir = layout.buildDirectory.dir("generated/sbe/main/java")

val generateSbeSources by tasks.registering(JavaExec::class) {
    group = "build"
    description = "Generate SBE codecs from schema"
    inputs.file(layout.projectDirectory.file("src/main/resources/sbe/messages.xml"))
    outputs.dir(sbeGeneratedDir)

    classpath = sbeCodegen
    mainClass.set("uk.co.real_logic.sbe.SbeTool")
    args = listOf(layout.projectDirectory.file("src/main/resources/sbe/messages.xml").asFile.absolutePath)
    systemProperties(
        mapOf(
            "sbe.output.dir"             to sbeGeneratedDir.get().asFile.absolutePath,
            "sbe.target.language"        to "Java",
            "sbe.java.generate.interfaces" to "true"
        )
    )
}

sourceSets {
    main {
        java {
            srcDir(sbeGeneratedDir)
        }
    }
}

tasks.named("compileJava") {
    dependsOn(generateSbeSources)
}

tasks.test {
    useJUnitPlatform()
}

// ---------------------------------------------------------------------------
// Run tasks – each mini-application has its own task
// ---------------------------------------------------------------------------
fun runTask(name: String, mainClass: String, description: String, appArgs: List<String> = emptyList()) =
    tasks.register<JavaExec>(name) {
        this.group = "run"
        this.description = description
        this.classpath = sourceSets.main.get().runtimeClasspath
        this.mainClass.set(mainClass)
        this.args = appArgs
        this.jvmArgs = listOf(
            "-Daeron.term.buffer.length=1048576",
            "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
            "--add-opens=java.base/java.lang=ALL-UNNAMED",
            "--add-opens=java.base/java.util=ALL-UNNAMED",
            "--add-opens=java.base/java.nio=ALL-UNNAMED"
        )
    }

runTask("runMessagingClient", "io.aeron.koans.messaging.MessagingClient",
        "Run the Messaging Client (sends NewOrderSingle, receives ExecutionReport)")
runTask("runMessagingVenue",  "io.aeron.koans.messaging.MessagingVenue",
        "Run the Messaging Venue  (receives NewOrderSingle, replies ExecutionReport)")
runTask("runArchiveServer",   "io.aeron.koans.archive.ArchiveServer",
        "Run the Archive Server   (receives Pings, writes to local archive)")
runTask("runArchiveClient",   "io.aeron.koans.archive.ArchiveClient",
        "Run the Archive Client   (sends Pings, receives Pongs, writes to local archive)")
runTask("runArchiveReader",   "io.aeron.koans.archive.ArchiveReader",
        "Run the Archive Reader   (replays & decodes recordings from server or client archive)")
runTask("runClusterNode0",   "io.aeron.koans.cluster.ClusterNode",
        "Run Cluster Node 0  (nodeId=0, ingress=20000)", listOf("0"))
runTask("runClusterNode1",   "io.aeron.koans.cluster.ClusterNode",
        "Run Cluster Node 1  (nodeId=1, ingress=20100)", listOf("1"))
runTask("runClusterNode2",   "io.aeron.koans.cluster.ClusterNode",
        "Run Cluster Node 2  (nodeId=2, ingress=20200)", listOf("2"))
runTask("runClusterClient",  "io.aeron.koans.cluster.ClusterClient",
        "Run Cluster Client  (interactive named-counter REPL)")
runTask("runClusterSnapshot","io.aeron.koans.cluster.ClusterSnapshot",
        "Request a snapshot from the running cluster leader")
