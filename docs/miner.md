# Storb Miners

Miners are the backbone of Storb. They are responsible for storing and serving pieces of files to validators, and by proxy, to the end users.

## Running a Miner

1. Have a look over the `settings.toml` file. There are various parameters there that can be modified. Alternatively, one can just set these parameters through the CLI as shown in the following step.

2. Run on Bittensor testnet

    ```bash
    ./target/release/storb miner \
        --netuid 269 --external-ip EXTERNAL_IP \
        --api-port API_PORT \
        --quic-port QUIC_PORT \
        --wallet-name WALLET_NAME \
        --hotkey-name HOTKEY_NAME \
        --subtensor.address wss://test.finney.opentensor.ai:443 \
        --neuron.sync-frequency 120 \
        --dht-dir storb_data/vali-dht.storb-local \
        --pem-file storb_data/validator.pem \
        --min-stake-threshold 0 \
        --post-ip
    ```
