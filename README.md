# mandatory-5-Distributed-Auction-System
You must implement a **distributed auction system** using replication:
a distributed component which handles auctions, and provides
operations for bidding and querying the state of an auction. The
component must faithfully implement the semantics of the system
described below, and must at least be resilient to one (1) crash
failure.

# Building and Running
Run everything from the repository root.

Start the registry. By default it spawns 3 nodes and waits for them to register:
```
go run ./main.go --port=5000 --n=3
```

Each node logs `Node listening on port <port>`. Connect a client with a comma-separated list of node ports:
```
go run ./client/client.go --nodes=<port1>,<port2>,<port3>
```
The client starts with the first port. If that node can't be reached, it switches to the next one in the list.

Client commands: `name <text>` (required first), `bid <amount>`, `result`, `exit`.

### Running nodes manually
```
go run ./main.go --spawn=false --n=3
go run ./node --registry-port=5000
```
Run the node command once per node (3 times for `--n=3`). Optional node flags: `--port` (default: random free port), `--rpc-timeout` (default `1s`).