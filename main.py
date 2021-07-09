from libraries.datalake import ConnectionAzureDataLake

connection_datalake = ConnectionAzureDataLake()

connection_datalake.initialize_storage_account_ad_env_variable()

print(connection_datalake.list_containers())