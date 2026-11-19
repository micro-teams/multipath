# MultiPath

MultiPath carries one client's traffic redundantly over several network paths ("lines") to one
server, so that the traffic survives lines dying. Every byte is sent on every live line and the
receiver keeps the first copy; as long as one line works, nothing stalls and nothing breaks.

The wire protocol is defined in [`PROTOCOL.md`](./PROTOCOL.md).

## Components

| Path | What it is |
|---|---|
| `go/cmd/multipath-server` | The server: a reverse proxy that accepts MultiPath links and splices every stream to one TCP target. |
| `go/cmd/multipath-client` | The client: a local TCP port whose every connection becomes one stream over all lines. |
| `conformance/` | Fault-injecting proxy and scenarios that run the real client and server binaries. |
| `ts/` | Legacy: the protocol-version-2 browser client, kept buildable until its successor exists. |

Deployment and configuration are described below once the components exist.

## License

MIT, see [LICENSE](./LICENSE).
