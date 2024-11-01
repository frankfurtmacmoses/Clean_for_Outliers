'''
// To do: modify the below for S3 sink connector 
'''

import boto3
import json

def createConnector():
    config = loadFormatedConfig()
    client = boto3.client('kafkaconnect',region_name='us-east-1')
    response = client.create_connector(
        capacity={
            'autoScaling': {
                'maxWorkerCount': 10,
                'mcuCount': 1,
                'minWorkerCount': 2,
                'scaleInPolicy': {
                    'cpuUtilizationPercentage': 60
                },
                'scaleOutPolicy': {
                    'cpuUtilizationPercentage': 80
                }
            }
        },
        connectorConfiguration={
            'string': config
        },
        connectorDescription='This connector connected to seg database to pull all tables belonging to CTG schemal in the apperser sever',
        connectorName='testing',
        kafkaCluster={
            'apacheKafkaCluster': {
                'bootstrapServers': 'b-1.seshybdev.bvj0c2.c20.kafka.us-east-1.amazonaws.com:9098',
                'vpc': {
                    'securityGroups': ['sg-0f47a5b1af6830287'],
                    'subnets': ['subnet-020ee60424186a2fc']
                }
            }
        },
        kafkaClusterClientAuthentication={
            'authenticationType': 'IAM'
        },
        kafkaClusterEncryptionInTransit={
            'encryptionType': 'TLS'
        },
        kafkaConnectVersion='2.7.1',
        logDelivery={
            'workerLogDelivery': {
                'cloudWatchLogs': {
                    'enabled': True,
                    'logGroup': 'source-connector-logs'
                },
                's3': {
                    'bucket': 'ses-kafka-connector',
                    'enabled': True,
                    'prefix': 'ctgschema'
                }
            }
        },
        plugins=[
            {
                'customPlugin': {
                    'customPluginArn': 'arn:aws:kafkaconnect:us-east-1:802803944024:connector/source-conector1/3c43f33e-8bdb-4557-a456-639faff42798-4',
                    'revision': 1
                }
            },
        ],
        serviceExecutionRoleArn='arn:aws:iam::802803944024:role/ses-dev ',
        
        workerConfiguration={
            'revision': 1,
            'workerConfigurationArn': 'connector-wk-config'
        }
    )

    print(response)

def loadFormatedSourceConfig():
    myconfig = 'config/source-connectors/debazium-source-connector-2.json'
    keytoreplace = 'table.include.list'
    with open(myconfig)as config:
        myconfig = json.load(config)

    # Load the list of table from t
    with open('tables_config/tablelist.txt') as tablelist:
        tablelist = [line.strip() for line in tablelist]
        
    
    # Replace the 'table.include.list' of the config file with values from tablelist.txt
    myconfig['config'][keytoreplace] = tablelist

    # Return updated configuration file
    return str(myconfig)
def loadFormatedSinkConfig():
    myconfig = 'config/source-connectors/debazium-source-connector-2.json'
    keytoreplace = 'table.include.list'
    with open(myconfig)as config:
        myconfig = json.load(config)

    # Load the list of table from t
    with open('tables_config/tablelist.txt') as tablelist:
        tablelist = [line.strip() for line in tablelist]
    # Iterate append on the list to change it to topic name
    myconfig['config'][keytoreplace] = tablelist

    # Return updated list of topics
    return str(myconfig)

if __name__ == '__main__':
    createConnector()