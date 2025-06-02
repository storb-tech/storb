# Storb Validators

Validators play a crucial role in the Storb network by serving as gateways to the storage subnet. They handle the storage and retrieval of files, ensuring data integrity and availability.

## Setup

### Configuration

Have a look over the `settings.toml` file. There are various parameters there that can be modified. Alternatively, one can just set these parameters through the CLI as shown in the following steps.

### Setting up databases

You'll also need to set up the local databases using SQLx.

#### Install SQLx CLI
cargo install sqlx-cli

#### Score database

```bash
sqlx database create --database-url "sqlite://storb_data/database.db"
sqlx migrate run --source migrations/scoresdb/ --database-url "sqlite://storb_data/database.db"
```
#### Metadata database

```bash
sqlx database create  --database-url "sqlite://storb_data/metadata.db"
sqlx migrate run --source migrations/metadatadb/ --database-url "sqlite://storb_data/metadata.db"
```

#### Installing cr-sqlite
You will also need to install the cr-sqlite extension for sqlite.

First, visit their releases page: https://github.com/vlcn-io/cr-sqlite/releases
then download the latest release for your platform and extract it. For example, if you are on Linux, and running on a x86_64 platform, you would download `crsqlite-linux-x86_64.zip`
```bash
wget https://github.com/vlcn-io/cr-sqlite/releases/download/v0.16.3/crsqlite-linux-x86_64.zip
unzip crsqlite-linux-x86_64.zip
```

Then, copy the `crsqlite.so` file to the `crsqlite` directory under the root directory of the repository.
```bash
mkdir -p /path/to/storb/repo/crsqlite
mv /path/to/unzipped/crsqlite.so /path/to/storb/repo/crsqlite
```

Then update the `crsqlite_file` parameter in `settings.toml` to point to the location of the `crsqlite.so` file if it isn't already. For example:
```toml
[validator]
crsqlite_file = "/path/to/storb/repo/crsqlite/crsqlite.so"
```

### Running validator

#### Mainnet

    ```bash
    ./target/release/storb validator \
        --netuid 26 \
        --external-ip EXTERNAL_IP \
        --api-port API_PORT \
        --wallet-name WALLET_NAME \
        --hotkey-name HOTKEY_NAME \
        --subtensor.address wss://entrypoint-finney.opentensor.ai:443 \
        --post-ip
    ```

#### Testnet

    ```bash
    ./target/release/storb validator \
        --netuid 269 \
        --external-ip EXTERNAL_IP \
        --api-port API_PORT \
        --wallet-name WALLET_NAME \
        --hotkey-name HOTKEY_NAME \
        --subtensor.address wss://test.finney.opentensor.ai:443 \
        --post-ip
    ```

#### Using Docker and Watchtower

Make sure that you have first cloned this repository and filled out the `settings.toml` file with the necessary parameters. You will also need to specify environment variables for `API_PORT`, `QUIC_PORT`, and `DHT_PORT` if they differ from the defaults for port forwarding in the Docker container.

Then, run the following:

```bash
docker compose up -f config/validator.docker-compose.yaml -d
```

### API Access

Validators can serve as gateways to the subnet, thereby letting users upload and download files to and from miners. We provide a cli tool to help manage api access.

#### Generating an API Key

The following example generates an api key that has a capped download and upload quota, as well as a rate limit of 60 requests/min.

```bash
$ ./target/release/storb apikey create --name "capped" --rate-limit 60 --upload-limit 10485983 --download-limit 10485983

✨ Created API key: storb_bac03afd-cc44-4362-8d11-d604e10aebe7
Name: capped
Rate limit: 100 requests/minute
Upload limit: 10485983 bytes
Download limit: 10485983 bytes
```

#### Deleting an API Key

```bash
$ ./target/release/storb apikey delete storb_bac03afd-cc44-4362-8d11-d604e10aebe7
✅ API key deleted successfully
```

For more information on how to use the cli tool run

```bash
./target/release/storb apikey --help
```
