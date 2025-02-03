# storb-rs

Storb is a decentralized object storage subnet built on the Bittensor network. It aims to be a distributed, fault-tolerant, and efficient digital storage solution.

## Features

- **Decentralization**: Utilizes a network of nodes to store data redundantly.

- **Erasure Coding**: Enhances data reliability and storage efficiency by fragmenting and distributing data across multiple nodes, allowing reconstruction even if some fragments are lost.

- **Incentivized Storage**: Storb leverages the power of the Bittensor network. The subnet rewards miners for contributing reliable and responsive storage resources, with validators ensuring data integrity. Bittensor serves as the ideal incentive layer for this.

For an overview of how the subnet works, [see here](docs/overview.md).

## Installation

1. **Clone the Repository**:

   ```bash
   git clone https://github.com/fr34kcoders/storb.git
   cd storb
   ```

2. **Set Up Virtual Environment**:

   You will need to have Rust installed.

   ```bash
   cargo build --release
   ```

3. **Configure and run node**:

- [**Miner**](docs/miner.md)
- [**Validator**](docs/validator.md)

## Contributing

We welcome contributions to enhance Storb. Please fork the repository and submit a pull request with your improvements.

## License

This project is licensed under the MIT License. See the [LICENSE](./LICENSE) file for details.

## Contact

For questions or support, please open an issue in this repository or contact the maintainers on the Bittensor discord server.
