# Storb Validators

Validators play a crucial role in the Storb network by serving as gateways to the storage subnet. They handle the storage and retrieval of files, ensuring data integrity and availability.

## Running a validator

1. Have a look over the `settings.toml` file. There are various parameters there that can be modified. Alternatively, one can just set these parameters through the CLI as shown in the following step.

2. You'll also need to set up the local database using SQLx.

   ```bash
   cargo install sqlx-cli
   sqlx database create --database-url "sqlite://storb_data/database.db"
   sqlx migrate run --database-url "sqlite://storb_data/database.db"
   ```

3. Run on testnet

    ```bash
    ./target/release/storb validator \
        --netuid 269 \
        --external-ip EXTERNAL_IP \
        --api-port API_PORT \
        --wallet-name WALLET_NAME \
        --hotkey-name HOTKEY_NAME \
        --subtensor.address wss://test.finney.opentensor.ai:443 \
        --neuron.sync-frequency 120 \
        --dht-dir storb_data/vali-dht.storb-local \
        --pem-file storb_data/validator.pem \
        --post-ip
    ```
