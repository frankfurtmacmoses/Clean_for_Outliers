-- Check for all the tables enable for cdc

USE esg;

SELECT 
    s.name AS SchemaName,
    t.name AS TableName,
    ct.capture_instance AS CaptureInstance,
    ct.start_lsn AS StartLsn,
    ct.end_lsn AS EndLsn,
    ct.supports_net_changes AS SupportsNetChanges
FROM 
    cdc.change_tables ct
JOIN 
    sys.tables t ON ct.object_id = t.object_id
JOIN 
    sys.schemas s ON t.schema_id = s.schema_id
ORDER BY 
    s.name, t.name;


-- Check if a table is enable for cdc

USE esg

SELECT 
    s.name AS SchemaName,
    t.name AS TableName
FROM 
    cdc.change_tables ct
JOIN 
    sys.tables t ON ct.object_id = t.object_id
JOIN 
    sys.schemas s ON t.schema_id = s.schema_id
WHERE 
    t.name = 'YourTableName';  -- Replace with your table name



-- Enable a table for CDC
USE esg

EXEC sys.sp_cdc_enable_table
    @source_schema = N'CTGout',
    @source_name = N'CTGout.INV_4000_Customer',
    @role_name = NULL;  -- Replace with a role if needed, or leave as NULL for no specific role.
