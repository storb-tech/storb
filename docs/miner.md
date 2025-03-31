# Storb Miners

Miners are the backbone of Storb. They are responsible for storing and serving pieces of files to validators, and by proxy, to the end users.

## Setup

### Configuration

Have a look over the `settings.toml` file. There are various parameters there that can be modified. Alternatively, one can just set these parameters through the CLI as shown in the following step.

### Running miner

- Mainnet
    ```bash
    ./target/release/storb miner \
        --netuid 269 --external-ip EXTERNAL_IP \
        --api-port API_PORT \
        --quic-port QUIC_PORT \
        --wallet-name WALLET_NAME \
        --hotkey-name HOTKEY_NAME \
        --subtensor.address wss://entrypoint-finney.opentensor.ai:443 \
        --neuron.sync-frequency 120 \
        --min-stake-threshold 10000 \
        --post-ip
    ```

- Testnet
    ```bash
    ./target/release/storb miner \
        --netuid 26 --external-ip EXTERNAL_IP \
        --api-port API_PORT \
        --quic-port QUIC_PORT \
        --wallet-name WALLET_NAME \
        --hotkey-name HOTKEY_NAME \
        --subtensor.address wss://test.finney.opentensor.ai:443 \
        --neuron.sync-frequency 120 \
        --min-stake-threshold 0 \
        --post-ip
    ```
