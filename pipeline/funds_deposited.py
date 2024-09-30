import asyncio
import os
import polars as pl
from hypermanager.events import EventConfig
from hypermanager.manager import HyperManager
from hypermanager.protocols.across import (
    SpokePoolAddresses,
    client_config,
    base_event_configs,
)  # Import across schema


async def get_v3_funds_deposited():
    """
    Function to get only V3FundsDeposited events for relevant clients (Across protocol).
    """
    # Iterate through relevant HyperSync clients and associated SpokePool addresses
    for client, spoke_pool_address in client_config.items():
        print(f"Querying V3FundsDeposited events for {client.name}...")

        v3_funds_deposited_config = next(
            (
                config
                for config in base_event_configs
                if config["name"] == "V3FundsDeposited"
                # if config["name"] == "FilledV3Relay"
            ),
            None,  # Return None if not found
        )
        try:
            manager = HyperManager(url=client.client)

            # Create an EventConfig for the V3FundsDeposited event, assigning the contract dynamically
            event_config = EventConfig(
                name=v3_funds_deposited_config["name"],
                signature=v3_funds_deposited_config["signature"],
                contract=spoke_pool_address.value,  # Assign contract address dynamically
                column_mapping=v3_funds_deposited_config["column_mapping"],
            )

            # Query the V3FundsDeposited events
            df: pl.DataFrame = await manager.execute_event_query(
                event_config, save_data=False, tx_data=True, block_range=2_500_000
            )

            # Check if the DataFrame is empty
            if df.is_empty():
                print(
                    f"No events found for {event_config.name} on {client.name}, continuing..."
                )
                continue

            # Process the non-empty DataFrame
            print(f"Events found for {event_config.name} on {client.name}: {df.shape}")

            # Create the folder if it doesn't exist
            folder_path = f"data/across/{event_config.name}"
            if not os.path.exists(folder_path):
                os.makedirs(folder_path)

            # Save the file with the naming convention `V3FundsDeposited_{client.name}.parquet`
            df.write_parquet(f"{folder_path}/{event_config.name}_{client.name}.parquet")

        except Exception as e:
            print(f"Error querying {event_config.name} on {client.name}: {e}")


if __name__ == "__main__":
    asyncio.run(get_v3_funds_deposited())
