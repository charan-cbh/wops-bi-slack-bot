import asyncio

from glide import (
    ClosingError,
    ConnectionError,
    GlideClusterClient,
    GlideClusterClientConfiguration,
    Logger,
    LogLevel,
    NodeAddress,
    RequestError,
    TimeoutError,
)

async def main():
    # Set logger configuration
    Logger.set_logger_config(LogLevel.INFO)

    # Configure the Glide Cluster Client
    addresses = [
        NodeAddress("wops-bi-slack-bot-hqi6hw.serverless.use1.cache.amazonaws.com", 6380)
    ]
    config = GlideClusterClientConfiguration(addresses=addresses, use_tls=True)
    client = None

    try:
        print("Connecting to Valkey Glide...")

        # Create the client
        client = await GlideClusterClient.create(config)
        print("Connected successfully.")

        # Perform SET operation
        result = await client.set("key", "value")
        print(f"Set key 'key' to 'value': {result}")

        # Perform GET operation
        value = await client.get("key")
        print(f"Get response for 'key': {value}")

        # Perform PING operation
        ping_response = await client.ping()
        print(f"PING response: {ping_response}")

    except (TimeoutError, RequestError, ConnectionError, ClosingError) as e:
        print(f"An error occurred: {e}")
    finally:
        # Close the client connection
        if client:
            try:
                await client.close()
                print("Client connection closed.")
            except ClosingError as e:
                print(f"Error closing client: {e}")


asyncio.run(main())