# Storb - An Overview

## TLDR

The subnet can be used as shown below:

![overview](../assets/overview.png)

Uploading files

- Client hits a validator endpoint to upload a file. This can be done by sending a file to its http endpoint:

    ```bash
    curl -X POST http://{ip}:{port}/file -F "file=@{path/to/file}"
    ```

- The validator splits up the file into erasure-coded pieces that are then distributed to miners.
- The validator distributes the file metadata to other neurons through a DHT.
- Returns an infohash which can be used by user to download the file from the network:

    ```
    4e44d931392d68ec0318f09d48267d05c4a8d9fe852832adeeaefc47a892d23c
    ```

Retrieving files

- Client requests for a file through a validator. Also done through its http endpoint:

    ```bash
    curl -X GET http://{ip}:{port}/file?infohash={infohash}
    ```

- The validator uses the DHT to determine where the file pieces are stored then requests the pieces from the miners.
- The validator reconstructs the file with the pieces and sends it back to the client.

## Scoring Mechanism

![scoring](../assets/weight-scoring.png)

Scoring is made up of multiple components:

- **Latency**: Miners are scored based on how quickly they respond to storage and retrieval requests.
- **Response Rate**: Reliable miners are the name of the game. The less a miner responds with valid data to storage and retrieval requests, the lower it is scored.
- **Data validation**: There's no point in having a miner with low latency and high response rates if the data they return is invalid. To ensure that miners are consistently returning the correct data, the integrity of the data they return is also taken into account.

### Window-Based Scoring

The scoring system uses a sliding window approach to ensure scores remain current and representative:

- **Stats Reset**: After every 2,500 requests to a miner, their statistics (storage successes, storage attempts, retrieval successes, retrieval attempts) are reset
- **Score History**: Before resetting stats, the current score is stored as a "previous score" to maintain historical performance context
- **Score Calculation**: 
  - For miners with fewer than 50 requests in the current window, their previous score is used
  - For miners with sufficient requests, the score is calculated as:
    - 70% weight on current window performance
    - 30% weight on previous historical score
  - The final score combines:
    - 75% weight on response rates (storage/retrieval success)
    - 25% weight on latency performance

This window-based approach ensures that:
- Scores reflect recent performance more heavily than historical data
- Miners can't coast on past performance indefinitely
- New or recently reset miners have a grace period using their previous score
- Performance is evaluated over a meaningful sample size of requests

## Chunking and Piecing

Files are split into erasure-coded chunks, and subsequently split into pieces and stored across various miners for redundancy.

![chunk](../assets/chunk.png)

## DHT for File Metadata

File metadata — which is useful for querying miners for pieces, and, eventually, reconstructing files — is replicated and stored across neurons in the subnet in the form of a DHT.

![metadata](../assets/metadata.png)
