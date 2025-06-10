# Storb Validators

Validators play a crucial role in the Storb network by serving as gateways to the storage subnet. They handle the storage and retrieval of files, ensuring data integrity and availability.

## Setup

### Configuration

Have a look over the `settings.toml` file. There are various parameters there that can be modified. Alternatively, one can just set these parameters through the CLI as shown in the following steps.

### Setting up databases

You'll also need to set up the local databases using SQLx.

#### Install SQLx CLI

```bash
cargo install sqlx-cli
```

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
The cr-sqlite extension for SQLite is also required. We automatically download the correct library for the target system during the build step. By default, it is downloaded to a `crsqlite` folder in the project root, but you can specify a directory to use with the `CRSQLITE_LIB_DIR` environment variable.

If you used a custom install directory (or are not using Linux), update the `crsqlite_file` parameter in `settings.toml` to point to the location of the `crsqlite.so` file if it isn't already. For example:

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
