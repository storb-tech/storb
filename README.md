# Storb

Storb is a decentralized object storage subnet built on the Bittensor network. It aims to be a distributed, fault-tolerant, and efficient digital storage solution.

## Features

- **Decentralization**: Utilizes a network of nodes to store data redundantly.

- **Erasure Coding**: Enhances data reliability and storage efficiency by fragmenting and distributing data across multiple nodes, allowing reconstruction even if some fragments are lost.

- **Incentivized Storage**: Storb leverages the power of the Bittensor network. The subnet rewards miners for contributing reliable and responsive storage resources, with validators ensuring data integrity. Bittensor serves as the ideal incentive layer for this.

For an overview of how the subnet works, [see here](docs/overview.md).

## Installation

1. **Clone the repository**:

   ```bash
   git clone https://github.com/storb-tech/storb.git
   cd storb
   ```

2. **Set up environment and dependencies**:

   Install the Rust nightly toolchain.

   Ensure that the following dependencies are installed:

   ```bash
   sudo apt update
   sudo apt install build-essential clang libclang-dev libssl-dev pkg-config
   ```

   **OR**

   If you use NixOS or the Nix package manager, you can use the provided flakes in this repository to get set up. It will install the necessary dependencies and Rust toolchains for you.

3. **Compile**:

   ```bash
   cargo build --release

   # Your executable:
   ./target/release/storb
   ```

4. **Configure and run node**:

   Copy the `settings.toml.example` file and name it `settings.toml`, then update the configuration options as needed.

   Specific instructions for the miner and validator:

   - [**Miner**](docs/miner.md)
   - [**Validator**](docs/validator.md)

## Contributing

We welcome contributions to enhance Storb. Please fork the repository and submit a pull request with your improvements.

## License

This project is licensed under the MIT License. See the [LICENSE](./LICENSE) file for details.

## Contact

For questions or support, please open an issue in this repository or contact the maintainers on the Storb or Bittensor Discord server.
