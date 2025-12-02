/*==============================================================================
    Stored Procedure: bronze.load_bronze
    Purpose:
        Loads all CRM and ERP source files into the Bronze Layer.
        Each table is truncated and reloaded using BULK INSERT.
==============================================================================*/

CREATE OR ALTER PROCEDURE bronze.load_bronze 
AS
BEGIN
    DECLARE 
        @start_time DATETIME,
        @end_time DATETIME,
        @batch_start_time DATETIME,
        @batch_end_time DATETIME;

    BEGIN TRY
        ----------------------------------------------------------------------
        -- Start Batch
        ----------------------------------------------------------------------
        SET @batch_start_time = GETDATE();

        PRINT '================================================';
        PRINT 'Starting Bronze Layer Load';
        PRINT '================================================';


        /*======================================================================
            CRM TABLES
        ======================================================================*/
        PRINT '------------------------------------------------';
        PRINT 'Loading CRM Tables';
        PRINT '------------------------------------------------';


        -------------------------------
        -- Load crm_cust_info
        -------------------------------
        SET @start_time = GETDATE();
        PRINT '>> Truncating: bronze.crm_cust_info';
        TRUNCATE TABLE bronze.crm_cust_info;

        PRINT '>> Loading Data: bronze.crm_cust_info';
        BULK INSERT bronze.crm_cust_info
        FROM 'D:\DATA ANALYSIS PROJECT DATA\SQL PROJECT\SQL DATA WARE HOUSE PROJECT MINE\datasets\source_crm\cust_info.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );
        SET @end_time = GETDATE();
        PRINT '>> Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' sec';
        PRINT '------------------------------------------------';


        -------------------------------
        -- Load crm_prd_info
        -------------------------------
        SET @start_time = GETDATE();
        PRINT '>> Truncating: bronze.crm_prd_info';
        TRUNCATE TABLE bronze.crm_prd_info;

        PRINT '>> Loading Data: bronze.crm_prd_info';
        BULK INSERT bronze.crm_prd_info
        FROM 'D:\DATA ANALYSIS PROJECT DATA\SQL PROJECT\SQL DATA WARE HOUSE PROJECT MINE\datasets\source_crm\prd_info.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );
        SET @end_time = GETDATE();
        PRINT '>> Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' sec';
        PRINT '------------------------------------------------';


        -------------------------------
        -- Load crm_sales_details
        -------------------------------
        SET @start_time = GETDATE();
        PRINT '>> Truncating: bronze.crm_sales_details';
        TRUNCATE TABLE bronze.crm_sales_details;

        PRINT '>> Loading Data: bronze.crm_sales_details';
        BULK INSERT bronze.crm_sales_details
        FROM 'D:\DATA ANALYSIS PROJECT DATA\SQL PROJECT\SQL DATA WARE HOUSE PROJECT MINE\datasets\source_crm\sales_details.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );
        SET @end_time = GETDATE();
        PRINT '>> Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' sec';
        PRINT '------------------------------------------------';



        /*======================================================================
            ERP TABLES
        ======================================================================*/
        PRINT '------------------------------------------------';
        PRINT 'Loading ERP Tables';
        PRINT '------------------------------------------------';


        -------------------------------
        -- Load erp_loc_a101
        -------------------------------
        SET @start_time = GETDATE();
        PRINT '>> Truncating: bronze.erp_loc_a101';
        TRUNCATE TABLE bronze.erp_loc_a101;

        PRINT '>> Loading Data: bronze.erp_loc_a101';
        BULK INSERT bronze.erp_loc_a101
        FROM 'D:\DATA ANALYSIS PROJECT DATA\SQL PROJECT\SQL DATA WARE HOUSE PROJECT MINE\datasets\source_erp\loc_a101.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );
        SET @end_time = GETDATE();
        PRINT '>> Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' sec';
        PRINT '------------------------------------------------';


        -------------------------------
        -- Load erp_cust_az12
        -------------------------------
        SET @start_time = GETDATE();
        PRINT '>> Truncating: bronze.erp_cust_az12';
        TRUNCATE TABLE bronze.erp_cust_az12;

        PRINT '>> Loading Data: bronze.erp_cust_az12';
        BULK INSERT bronze.erp_cust_az12
        FROM 'D:\DATA ANALYSIS PROJECT DATA\SQL PROJECT\SQL DATA WARE HOUSE PROJECT MINE\datasets\source_erp\cust_az12.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );
        SET @end_time = GETDATE();
        PRINT '>> Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' sec';
        PRINT '------------------------------------------------';


        -------------------------------
        -- Load erp_px_cat_g1v2
        -------------------------------
        SET @start_time = GETDATE();
        PRINT '>> Truncating: bronze.erp_px_cat_g1v2';
        TRUNCATE TABLE bronze.erp_px_cat_g1v2;

        PRINT '>> Loading Data: bronze.erp_px_cat_g1v2';
        BULK INSERT bronze.erp_px_cat_g1v2
        FROM 'D:\DATA ANALYSIS PROJECT DATA\SQL PROJECT\SQL DATA WARE HOUSE PROJECT MINE\datasets\source_erp\px_cat_g1v2.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );
        SET @end_time = GETDATE();
        PRINT '>> Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' sec';
        PRINT '------------------------------------------------';


        ----------------------------------------------------------------------
        -- End Batch
        ----------------------------------------------------------------------
        SET @batch_end_time = GETDATE();

        PRINT '==========================================';
        PRINT 'Bronze Layer Load Completed Successfully';
        PRINT 'Total Load Duration: ' + CAST(DATEDIFF(SECOND, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' sec';
        PRINT '==========================================';

    END TRY


    BEGIN CATCH
        PRINT '==========================================';
        PRINT 'ERROR OCCURRED WHILE LOADING BRONZE LAYER';
        PRINT 'Error Message: ' + ERROR_MESSAGE();
        PRINT 'Error Number : ' + CAST(ERROR_NUMBER() AS NVARCHAR);
        PRINT 'Error State  : ' + CAST(ERROR_STATE() AS NVARCHAR);
        PRINT '==========================================';
    END CATCH
END;
