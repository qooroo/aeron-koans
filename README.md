# aeron-koans

A series of self-contained, runnable mini-applications that teach core
[Aeron](https://github.com/real-logic/aeron) concepts using
[SBE](https://github.com/real-logic/simple-binary-encoding) for message encoding.

---

## Requirements

| Tool | Version |
|------|---------|
| JDK  | 17+     |
| Gradle | bundled via `./gradlew` |

---

## Build

```bash
./gradlew build
```

SBE codecs are generated automatically from
`src/main/resources/sbe/messages.xml` during the build.

---

## Koan 1 – Aeron Messaging (Point-to-Point with SBE)

Two separate Aeron instances communicate via UDP.

* **MessagingVenue** – subscribes for `NewOrderSingle` orders, decodes the SBE
  payload, and sends back an `ExecutionReport`.
* **MessagingClient** – encodes a `NewOrderSingle` using SBE, publishes it to
  the Venue, and decodes the incoming `ExecutionReport`.

Messages use minimal FIX models (`NewOrderSingle` and `ExecutionReport`) encoded
with SBE.

### Run

Open two terminals:

```bash
# Terminal 1 – start Venue first
./gradlew runMessagingVenue

# Terminal 2 – send an order
./gradlew runMessagingClient
```

---

## Koan 2 – Aeron Archive (Record, Store & Replay)

Three applications demonstrate recording messages to Aeron Archive and later
replaying them.

* **ArchiveServer** – receives `Ping` messages from the Client over UDP,
  **writes each incoming Ping to its local Aeron Archive**, and sends `Pong`
  replies.
* **ArchiveClient** – sends five `Ping` messages, **records the incoming Pong
  replies to its own local Aeron Archive**, then waits until interrupted so its
  archive remains accessible.
* **ArchiveReader** – connects to either the Server's or the Client's running
  archive, lists all recordings, replays each one, and decodes/logs every SBE
  message to the console.

### Run

Open three terminals in order:

```bash
# Terminal 1 – start Server (keeps running)
./gradlew runArchiveServer

# Terminal 2 – start Client (sends 5 pings, then waits)
./gradlew runArchiveClient

# Terminal 3 – read recordings from the Server archive
./gradlew runArchiveReader

# … or read from the Client archive:
./gradlew runArchiveReader --args="client"
```

Press `Ctrl-C` in terminals 1 and 2 when you are done.

---

## SBE Schema

All messages are defined in `src/main/resources/sbe/messages.xml`.

| Template ID | Message            | Used in         |
|-------------|--------------------|-----------------|
| 1           | `NewOrderSingle`   | Koan 1          |
| 2           | `ExecutionReport`  | Koan 1          |
| 3           | `Ping`             | Koan 2          |
| 4           | `Pong`             | Koan 2          |

---

## Project structure

```
src/
  main/
    resources/sbe/messages.xml          ← SBE schema
    java/io/aeron/koans/
      messaging/
        MessagingClient.java            ← Koan 1: client (sends orders)
        MessagingVenue.java             ← Koan 1: venue  (fills orders)
      archive/
        ArchiveServer.java              ← Koan 2: server (records Pings)
        ArchiveClient.java              ← Koan 2: client (records Pongs)
        ArchiveReader.java              ← Koan 2: replays archive recordings
build/generated/sbe/main/java/          ← SBE-generated codecs (auto-generated)
```
