# Mars Distributed Storage System

A peer-to-peer distributed key-value storage system with quorum-based replication and sequential consistency, built using Akka actors.

## Features

- Quorum-based replication (configurable N, R, W parameters)
- Sequential consistency using Lamport logical clocks
- Circular ring topology with consistent hashing
- Node crash recovery and graceful leave operations
- Configurable read/write quorums with constraint validation

## Configuration

System parameters are configured at runtime:
- **N**: Replication factor (number of replicas)
- **R**: Read quorum size
- **W**: Write quorum size
- **T**: Timeout in milliseconds


## Project Structure

```
src/
├── main/
│   ├── java/ds/
│   │   ├── Main.java              # Entry point with interactive TUI
│   │   ├── ManagementService.java # Actor system lifecycle and node management
│   │   ├── actors/
│   │   │   ├── Client.java        # Client actor for requests
│   │   │   ├── Handler.java       # Request handler coordinator
│   │   │   └── Node.java          # Storage node actor
│   │   ├── config/
│   │   │   └── Settings.java      # Configuration parameters and validation
│   │   └── model/
│   │       ├── Delayer.java       # Network delay simulation
│   │       ├── Request.java       # Request message wrapper
│   │       └── Types.java         # Shared message types
│   └── resources/
│       ├── application.conf       # Akka configuration
│       └── logback.xml            # Logging configuration
└── test/java/ds/
    └── SystemBehaviorTest.java    # Comprehensive system tests
```

## Requirements

- Java 11 or higher
- Gradle (wrapper included)

## Building the Project

```bash
# Build without running
gradle build

# Clean build artifacts
gradle clean
```

## Running the Project

```bash
# Run the system with interactive menu
gradle run --console=plain

# Or on Windows
gradlew.bat run --console=plain
```


## Running Tests

```bash
# Run all tests
gradle test

# Run specific test suite
gradle test --tests SystemBehaviorTest

# View test results
open build/reports/tests/test/index.html
```
