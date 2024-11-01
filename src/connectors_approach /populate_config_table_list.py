import json
def loadFormatedConfig():
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

if __name__ == '__main__':
    loadFormatedConfig()