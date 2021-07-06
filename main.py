from libraries.datalake import ConnectionAzureDataLake
import json
import pandas as pd
from datetime import datetime
import argparse

parser = argparse.ArgumentParser(description='credentials of datalake')
parser.add_argument('--client_id', '-c', type=str, help='client id of app service')
parser.add_argument('--clent_secret', '-s', type=str, help='client secret of app service')
parser.add_argument('--tenant_id', '-t', type=str, help='tenant id')

arg = parser.parse_args()

client_id = arg.client_id
client_secret = arg.clent_secret
tenant_id = arg.tenant_id

connection_datalake = ConnectionAzureDataLake()

connection_datalake.initialize_storage_account_ad(
    storage_account_name='studystoragewest',
    client_id=client_id, 
    client_secret=client_secret,
    tenant_id=tenant_id
)

now = datetime.now()
time = f"{now.year}{now.strftime('%m')}{now.strftime('%d')}{now.strftime('%H')}{now.strftime('%M')}"

connection_datalake.create_directory(container='pipeline', path=time)
connection_datalake.save_string_to_file('pipeline', time, 'teste', 'Hello World!')