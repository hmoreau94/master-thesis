/*
AUTHOR:
	Hugo Moreau - hugo.moreau@epfl.ch
	Msc in Communication Systems
	Minor in Management and Technological Entrepreneurship

DESCRIPTION:
	As a package containing references to non-existant tables cannot compile we
	need an external package to help us create such tables. This package will help to 
	relaunch the POC once tables have been deleted. 

EXECUTION:
	BEGIN
	    HUMOREAU.CPE_FAIL_DETECTION_POC_INIT.INITIALIZE;
	END;
*/

CREATE OR REPLACE PACKAGE CPE_FAIL_DETECTION_POC_INIT 
IS
    /*
    First we declare the necessary variables
    */
    var_day_0                       DATE 		:= SYSDATE - 1; -- sets day 0 for which the data is computed at execution
    var_via_day_0 					DATE 		:= var_day_0 - 8; -- the day for which we look for failing CPEs
    v_cpe_type                      number(10)  := 55; -- node type in topology database that references CPEs
    v_online_status                 number(10)  := 8; -- status from SAA that corresponds to an online state
    v_percentile_limit              number(10)  := 99; -- the percent of CPEs that we wish to keep in offline outlier detection
    debug_size                      number(10)  := 0; -- default size of a table
    v_n_days_offline_history        number(10);
    n_existing_tables				number(10);

    -- cleans up intermediary tables once they are not needed anymore
    save_space                      BOOLEAN 	:= TRUE; -- set to true to truncate intermediary tables after execution
    print_perf                      BOOLEAN		:= TRUE; -- to print the execution time of procedure in log_table
    debug                           BOOLEAN		:= TRUE; -- to print the size of intermediary tables
    
    -- to test performance    
    v_end_time_up                   number(10);
    v_end_time_dn                   number(10);
    v_end_time_cpe                  number(10);
    v_end_time_vector				number(10); 
    v_end_time                      number(10); 
    
    v_start_time                    number(10); 
    v_start_time_total              number(10);

    
    /*
    Then the procedures
    */
    PROCEDURE CLEAN_UP;
    PROCEDURE CHECK_STATE;

	-- INITIALIZATION
	PROCEDURE INITIALIZE_STEP_NO_DIFFS(day_to_consider  IN DATE);
	PROCEDURE INITIALIZE_STEP_WITH_DIFFS(day_to_consider IN DATE);
	PROCEDURE INITIALIZE_STEP_VIA(day_to_consider IN DATE);
	PROCEDURE INIT_LOGTABLE;
	PROCEDURE INITIALIZE;

	-- Exception
    INVALID_STATE					EXCEPTION;


END CPE_FAIL_DETECTION_POC_INIT;
/
CREATE OR REPLACE PACKAGE BODY CPE_FAIL_DETECTION_POC_INIT 
IS

	/*
	Checks that indeed none of the tables exist already. 

	@raises:
	- INVALID_STATE: if one of the table that should be created by the INIT procedure already exists.
	*/
	PROCEDURE CHECK_STATE IS
	BEGIN
		SELECT COUNT(*) INTO n_existing_tables 
		FROM ALL_TABLES 
		WHERE 	OWNER = 'HUMOREAU' AND
				TABLE_NAME IN 
					('DN_EXTRACTED','DN_HOURLY', 'CMTS_DN_HOURLY',
					'CMTS_DN','CMTS_DN_VECTOR','UP_EXTRACTED',
					'UP_HOURLY','CMTS_UP_HOURLY','CMTS_UP',
					'CMTS_UP_VECTOR','EXTRA_INFOS','OFFLINE_CPE',
					'UNAVAILABILITY_PCT','CENTILES','OUTLIERS',
					'SAA_SVGP_ENRICHED','SVG_AVG','STD_EXTRACTED_MES',
					'DAY_0_6H_WINDOWS','CPE_VECTOR','VECTOR',
					'DAILY_AVG_DAY_0','DAILY_AVG_DIFFS','SUBSET_MILESTONES',
					'FULL_FLOWS','FLAGGED_EVENT','TAGGED_MILESTONE',
					'MILESTONES','VIA_DETAILS','SUCCESS_FLAGS',
					'TENTATIVE_MATCH','FLAGGED_SUCESSFUL_SESSIONS',
					'VIA_MACS_NOT_CONFIRMED','FTR_FLAGGED_MACS',
					'VIA_MACS','LOG_TABLE');
		if(n_existing_tables>0) Then
			RAISE INVALID_STATE;
		end if;
	END CHECK_STATE;

	/*
	Will empty all the intermediary tables that are used to fill in the buffer tables.
	*/
    PROCEDURE CLEAN_UP IS
	BEGIN
        EXECUTE IMMEDIATE 'TRUNCATE TABLE HUMOREAU.DN_EXTRACTED DROP STORAGE';
        EXECUTE IMMEDIATE 'TRUNCATE TABLE HUMOREAU.DN_HOURLY DROP STORAGE';
        EXECUTE IMMEDIATE 'TRUNCATE TABLE HUMOREAU.CMTS_DN_HOURLY DROP STORAGE';
        EXECUTE IMMEDIATE 'TRUNCATE TABLE HUMOREAU.CMTS_DN DROP STORAGE';
        EXECUTE IMMEDIATE 'TRUNCATE TABLE HUMOREAU.CMTS_DN_VECTOR DROP STORAGE';
        EXECUTE IMMEDIATE 'TRUNCATE TABLE HUMOREAU.UP_EXTRACTED DROP STORAGE';
        EXECUTE IMMEDIATE 'TRUNCATE TABLE HUMOREAU.UP_HOURLY DROP STORAGE';
        EXECUTE IMMEDIATE 'TRUNCATE TABLE HUMOREAU.CMTS_UP_HOURLY DROP STORAGE';
        EXECUTE IMMEDIATE 'TRUNCATE TABLE HUMOREAU.CMTS_UP DROP STORAGE';
        EXECUTE IMMEDIATE 'TRUNCATE TABLE HUMOREAU.CMTS_UP_VECTOR DROP STORAGE';
        EXECUTE IMMEDIATE 'TRUNCATE TABLE HUMOREAU.EXTRA_INFOS DROP STORAGE';
        EXECUTE IMMEDIATE 'TRUNCATE TABLE HUMOREAU.OFFLINE_CPE DROP STORAGE';
        EXECUTE IMMEDIATE 'TRUNCATE TABLE HUMOREAU.UNAVAILABILITY_PCT DROP STORAGE';
        EXECUTE IMMEDIATE 'TRUNCATE TABLE HUMOREAU.CENTILES DROP STORAGE';
        EXECUTE IMMEDIATE 'TRUNCATE TABLE HUMOREAU.OUTLIERS DROP STORAGE';
        EXECUTE IMMEDIATE 'TRUNCATE TABLE HUMOREAU.SAA_SVGP_ENRICHED DROP STORAGE';
        EXECUTE IMMEDIATE 'TRUNCATE TABLE HUMOREAU.SVG_AVG DROP STORAGE';
        EXECUTE IMMEDIATE 'TRUNCATE TABLE HUMOREAU.STD_EXTRACTED_MES DROP STORAGE';
        EXECUTE IMMEDIATE 'TRUNCATE TABLE HUMOREAU.DAY_0_6H_WINDOWS DROP STORAGE'; 
        EXECUTE IMMEDIATE 'TRUNCATE TABLE HUMOREAU.CPE_VECTOR DROP STORAGE';	
	END CLEAN_UP;


	/*
	In order to restart the package. It allows to recreate all tables necessary for the execution of MAIN_PROC and 
	fill the buffer tables with the correct data such that the induction can be done by executing MAIN_PROC.
	*/
	PROCEDURE INITIALIZE AS
	BEGIN
		CHECK_STATE;
		INIT_LOGTABLE;
		INITIALIZE_STEP_NO_DIFFS(var_day_0 - 5);
		INITIALIZE_STEP_WITH_DIFFS(var_day_0 - 5);
		-- and we initialize VIA tables (the day doesn't matter as it only creates empty tables)
		INITIALIZE_STEP_VIA(var_day_0 - 5);	
		CLEAN_UP;
	END;
    

	------------------------------------------------------------------------------------------------

	/*
	Fills in all the intermediary tables and DAILY_AVG_DAY_0 with the values for a given date

	@params:
	- day_to_consider: the date to interpret as day 0 to fill in the tables.
	*/
	PROCEDURE INITIALIZE_STEP_NO_DIFFS(day_to_consider  IN DATE) AS
	BEGIN
	    -- Fill in the Vector Table
		EXECUTE IMMEDIATE '
			CREATE TABLE HUMOREAU.DN_EXTRACTED AS (
		        -- We extract the measurement that we are interested in for the time period of interest. 
		        SELECT * FROM
		            (
		            WITH 
		            EXTRACTED AS(
		                    -- Extract the measurements for day_0
		                SELECT  /*+ PARALLEL(16) */ 
		                        CMTS_NAME,
		                        DOWNSTREAM_NAME, 
		                        HOUR,
		                        RXPOWER, 
		                        AVG_SNR,
		                        CCER, 
		                        CER, 
		                        UTILIZATION
		                FROM SAA.CMTS_HOUR_DOWNSTREAM_STATS 
		                WHERE  TRUNC(HOUR) = ''' || TRUNC(day_to_consider) || ''' 
		                )
		            -- Extract the measurements for day_0
		            SELECT  /*+ PARALLEL(16) */ 
		                    CMTS_NAME,
		                    REGEXP_SUBSTR(DOWNSTREAM_NAME,''(Downstream [[:alnum:]/]+)'') AS IFC_DESCR, 
		                    CASE 
		                        WHEN to_number(to_char(HOUR,''HH24'')) IN (23,22,21,20,19,18) THEN 0 -- those hours belongs to the most recent 6h window
		                        WHEN to_number(to_char(HOUR,''HH24'')) IN (17,16,15,14,13,12) THEN 1
		                        WHEN to_number(to_char(HOUR,''HH24'')) IN (11,10,9,8,7,6)     THEN 2
		                        WHEN to_number(to_char(HOUR,''HH24'')) IN (5,4,3,2,1,0)       THEN 3 -- while these belong to the least recent
		                    END AS HOUR_WINDOW, 
		                    RXPOWER, 
		                    AVG_SNR,
		                    CCER, 
		                    CER, 
		                    UTILIZATION
		            FROM EXTRACTED 
		            )
		    )' ;

		EXECUTE IMMEDIATE '
			CREATE TABLE HUMOREAU.DN_HOURLY AS (
		        -- Get the average for 6hours window over the last 24h
		        SELECT * FROM(
		            -- We want to get the average of each measurement over 6h windows (23h to 17h then 17h to 11h ... ) for day_0
		            SELECT 
		                CMTS_NAME, IFC_DESCR, HOUR_WINDOW,
		                AVG(RXPOWER) AS RXPOWER,
		                AVG(AVG_SNR) AS AVG_SNR, 
		                AVG(CCER) AS CCER, 
		                AVG(CER) AS CER, 
		                AVG(UTILIZATION) AS UTILIZATION
		            FROM DN_EXTRACTED
		            GROUP BY CMTS_NAME, IFC_DESCR, HOUR_WINDOW
		            )
		    )' ;

		EXECUTE IMMEDIATE '
			CREATE TABLE  HUMOREAU.CMTS_DN_HOURLY AS (
		        -- Pivoting the window average to obtain a table with only one entry per Interface
		        SELECT *
		        FROM HUMOREAU.DN_HOURLY
		        PIVOT 
		            ( 
		              MAX(RXPOWER) AS RXPOWER_DN, 
		              MAX(AVG_SNR) AS AVG_SNR_DN, 
		              MAX(CCER) AS CCER_DN, 
		              MAX(CER) AS CER_DN, 
		              MAX(UTILIZATION) AS UTILIZATION_DN
		              FOR HOUR_WINDOW IN (0,1,2,3)
		            )      
		    )' ;

		EXECUTE IMMEDIATE '
			CREATE TABLE HUMOREAU.CMTS_DN AS (
		        -- We add the service group information
		        SELECT * FROM (
		            WITH IFC2SVG AS (
		                select CMTS_NAME, SERVICE_GROUP_NAME AS SERVICE_GROUP, IFC_DESCR 
		                from DM_DIM.ETL_STG_TOPO_CMTS2NODE
		            )
		            SELECT 
		                IFC2SVG.SERVICE_GROUP,
		                to_date(''' || TRUNC(day_to_consider) || ''',''DD/MM/YYYY HH24:MI:SS'') AS DAY_0,
		                CMTS_DN.*
		            FROM HUMOREAU.CMTS_DN_HOURLY CMTS_DN INNER JOIN IFC2SVG
		                ON  CMTS_DN.CMTS_NAME = IFC2SVG.CMTS_NAME 
		                    AND CMTS_DN.IFC_DESCR = IFC2SVG.IFC_DESCR
		            )
		    )' ;

		EXECUTE IMMEDIATE '
			CREATE TABLE HUMOREAU.CMTS_DN_VECTOR AS (
		        SELECT 
		            -- and can finally obtain the average of interface 
		            -- measurements grouped by Service group 
		            CMTS_NAME,
		            SERVICE_GROUP,
		            DAY_0,
		            ---
		            AVG("0_RXPOWER_DN") AS "0_CMTS_RX_DN",
		            AVG("1_RXPOWER_DN") AS "1_CMTS_RX_DN",
		            AVG("2_RXPOWER_DN") AS "2_CMTS_RX_DN",
		            AVG("3_RXPOWER_DN") AS "3_CMTS_RX_DN",
		            --,
		            AVG("0_AVG_SNR_DN") AS "0_CMTS_SNR_DN",
		            AVG("1_AVG_SNR_DN") AS "1_CMTS_SNR_DN",
		            AVG("2_AVG_SNR_DN") AS "2_CMTS_SNR_DN",
		            AVG("3_AVG_SNR_DN") AS "3_CMTS_SNR_DN",
		            --,
		            AVG("0_CCER_DN") AS "0_CMTS_CCER_DN",
		            AVG("1_CCER_DN") AS "1_CMTS_CCER_DN",
		            AVG("2_CCER_DN") AS "2_CMTS_CCER_DN",
		            AVG("3_CCER_DN") AS "3_CMTS_CCER_DN",
		            --,
		            AVG("0_CER_DN") AS "0_CMTS_CER_DN",
		            AVG("1_CER_DN") AS "1_CMTS_CER_DN",
		            AVG("2_CER_DN") AS "2_CMTS_CER_DN",
		            AVG("3_CER_DN") AS "3_CMTS_CER_DN",
		            --,
		            AVG("0_UTILIZATION_DN") AS "0_CMTS_UTILIZATION_DN",
		            AVG("1_UTILIZATION_DN") AS "1_CMTS_UTILIZATION_DN",
		            AVG("2_UTILIZATION_DN") AS "2_CMTS_UTILIZATION_DN",
		            AVG("3_UTILIZATION_DN") AS "3_CMTS_UTILIZATION_DN"
		        FROM HUMOREAU.CMTS_DN
		        GROUP BY CMTS_NAME,
		            SERVICE_GROUP,
		            DAY_0
		    )';

	    EXECUTE IMMEDIATE '
		CREATE TABLE HUMOREAU.UP_EXTRACTED AS(
	        -- We extract the measurement that we are interested into for the time period of interest. 
	        SELECT * FROM (
	            WITH
	            EXTRACTED AS (
	            -- Extract the column of interest for all days of interest
	            SELECT  /*+ PARALLEL(16) */ 
	                    CMTS_NAME, 
	                    UPSTREAM_NAME,
	                    HOUR,
	                    RXPOWER, 
	                    TXPOWER, 
	                    CER, 
	                    UTILIZATION, 
	                    MS_UTILIZATION, 
	                    FREECONT_MS_UTILIZATION
	            FROM SAA.CMTS_HOUR_UPSTREAM_STATS 
	            WHERE TRUNC(HOUR) = ''' ||  TRUNC(day_to_consider)||'''
	            )
	            SELECT /*+ PARALLEL(16) */ 
	                CMTS_NAME,
	                REGEXP_SUBSTR(UPSTREAM_NAME,''[[:alnum:] .]+/[0-9 .]+/[0-9 .]+$'') AS IFC_DESCR, 
	                CASE 
	                    WHEN to_number(to_char(HOUR,''HH24'')) IN (23,22,21,20,19,18) THEN 0 -- those hours belongs to the most recent 6h window
	                    WHEN to_number(to_char(HOUR,''HH24'')) IN (17,16,15,14,13,12) THEN 1
	                    WHEN to_number(to_char(HOUR,''HH24'')) IN (11,10,9,8,7,6)     THEN 2
	                    WHEN to_number(to_char(HOUR,''HH24'')) IN (5,4,3,2,1,0)       THEN 3 -- while these belong to the least recent
	                END AS HOUR_WINDOW, 
	                RXPOWER, 
	                TXPOWER, 
	                CER, 
	                UTILIZATION, 
	                MS_UTILIZATION, 
	                FREECONT_MS_UTILIZATION
	            FROM EXTRACTED
	            )
	    )' ;

	    EXECUTE IMMEDIATE '
		CREATE TABLE HUMOREAU.UP_HOURLY AS (
	        -- Get the average for 6hours window over the last 24h
	        SELECT * FROM (
	            -- We want to get the average of each measurement over 6h windows (23h to 17h then 17h to 11h ... ) for day_0
	            SELECT 
	                CMTS_NAME, IFC_DESCR, HOUR_WINDOW,
	                AVG(RXPOWER) AS RXPOWER,
	                AVG(TXPOWER) AS TXPOWER, 
	                AVG(CER) AS CER, 
	                AVG(UTILIZATION) AS UTILIZATION,
	                AVG(MS_UTILIZATION) AS MS_UTILIZATION,
	                AVG(FREECONT_MS_UTILIZATION) AS FREECONT_MS_UTILIZATION
	            FROM UP_EXTRACTED
	            GROUP BY CMTS_NAME, IFC_DESCR, HOUR_WINDOW
	            )
		)' ;

		EXECUTE IMMEDIATE '
		CREATE TABLE HUMOREAU.CMTS_UP_HOURLY AS(
	        -- Pivoting the window average to obtain a table with only one entry per Interface
	        SELECT *
	        FROM HUMOREAU.UP_HOURLY
	        PIVOT 
	            ( 
	              MAX(RXPOWER) AS RXPOWER_UP, 
	              MAX(TXPOWER) AS TXPOWER_UP, 
	              MAX(CER) AS CER_UP, 
	              MAX(UTILIZATION) AS UTILIZATION_UP, 
	              MAX(MS_UTILIZATION) AS MS_UTILIZATION_UP, 
	              MAX(FREECONT_MS_UTILIZATION) AS FREECONT_MS_UTILIZATION_UP
	              FOR HOUR_WINDOW IN (0,1,2,3,4)
	            )
		)' ;

		EXECUTE IMMEDIATE '
		CREATE TABLE HUMOREAU.CMTS_UP AS (
	        -- We add the service group information
	        SELECT * FROM(
	            WITH IFC2SVG AS (
	                select CMTS_NAME, SERVICE_GROUP_NAME AS SERVICE_GROUP, IFC_DESCR 
	                from DM_DIM.ETL_STG_TOPO_CMTS2NODE
	            )
	            SELECT 
	                IFC2SVG.SERVICE_GROUP,
	                to_date(''' || TRUNC(day_to_consider) || ''',''DD/MM/YYYY HH24:MI:SS'') AS DAY_0,
	                CMTS_UP.*
	            FROM HUMOREAU.CMTS_UP_HOURLY CMTS_UP INNER JOIN IFC2SVG
	                ON  CMTS_UP.CMTS_NAME = IFC2SVG.CMTS_NAME 
	                    AND CMTS_UP.IFC_DESCR = IFC2SVG.IFC_DESCR
	            )
		)' ;

		EXECUTE IMMEDIATE '
		CREATE TABLE HUMOREAU.CMTS_UP_VECTOR AS (
	        SELECT 
	            -- and can finally obtain the average of interface 
	            -- measurements grouped by Service group 
	            CMTS_NAME,
	            SERVICE_GROUP,
	            DAY_0,
	            ---
	            AVG("0_RXPOWER_UP") AS "0_CMTS_RX_UP",
	            AVG("1_RXPOWER_UP") AS "1_CMTS_RX_UP",
	            AVG("2_RXPOWER_UP") AS "2_CMTS_RX_UP",
	            AVG("3_RXPOWER_UP") AS "3_CMTS_RX_UP",
	            --
	            AVG("0_TXPOWER_UP") AS "0_CMTS_TX_UP",
	            AVG("1_TXPOWER_UP") AS "1_CMTS_TX_UP",
	            AVG("2_TXPOWER_UP") AS "2_CMTS_TX_UP",
	            AVG("3_TXPOWER_UP") AS "3_CMTS_TX_UP",
	            --
	            AVG("0_CER_UP") AS "0_CMTS_CER_UP",
	            AVG("1_CER_UP") AS "1_CMTS_CER_UP",
	            AVG("2_CER_UP") AS "2_CMTS_CER_UP",
	            AVG("3_CER_UP") AS "3_CMTS_CER_UP",
	            --
	            AVG("0_UTILIZATION_UP") AS "0_CMTS_UTILIZATION_UP",
	            AVG("1_UTILIZATION_UP") AS "1_CMTS_UTILIZATION_UP",
	            AVG("2_UTILIZATION_UP") AS "2_CMTS_UTILIZATION_UP",
	            AVG("3_UTILIZATION_UP") AS "3_CMTS_UTILIZATION_UP",
	            --
	            AVG("0_MS_UTILIZATION_UP") AS "0_CMTS_MS_UTILIZATION_UP",
	            AVG("1_MS_UTILIZATION_UP") AS "1_CMTS_MS_UTILIZATION_UP",
	            AVG("2_MS_UTILIZATION_UP") AS "2_CMTS_MS_UTILIZATION_UP",
	            AVG("3_MS_UTILIZATION_UP") AS "3_CMTS_MS_UTILIZATION_UP",
	            --
	            AVG("0_FREECONT_MS_UTILIZATION_UP") AS "0_CMTS_F_MS_UTILIZATION_UP",
	            AVG("1_FREECONT_MS_UTILIZATION_UP") AS "1_CMTS_F_MS_UTILIZATION_UP",
	            AVG("2_FREECONT_MS_UTILIZATION_UP") AS "2_CMTS_F_MS_UTILIZATION_UP",
	            AVG("3_FREECONT_MS_UTILIZATION_UP") AS "3_CMTS_F_MS_UTILIZATION_UP"
	        FROM HUMOREAU.CMTS_UP
	        GROUP BY CMTS_NAME,
	            SERVICE_GROUP,
	            DAY_0
		)' ;

		EXECUTE IMMEDIATE '
		CREATE TABLE HUMOREAU.EXTRA_INFOS AS (
	        SELECT * FROM (
	            WITH HW_INFO AS (
	                -- get the hardware model
	                SELECT
	                    /*+ PARALLEL(16) */ 
	                    REPLACE(NODE_KEY,'':'') AS MAC, 
	                    SRC_NODE_MODEL AS HARDWARE_MODEL, 
	                    SRC_NODE_BUILDING_ID AS BUILDING_ID, 
	                    CLY_ACCT_NUMBER as CLY_ACCOUNT_NUMBER
	                FROM DM_TOPO_CH.ETL_STG_NW_TOPO_NODES 
	                WHERE topo_node_type_id = ''' || v_cpe_type ||'''

	            ),N_CPE_2_BUILDING AS (
	                -- get the number of CPE in the same building
	                SELECT  BUILDING_ID, 
	                    COUNT(DISTINCT MAC) AS N_CPE
	                FROM HW_INFO 
	                GROUP BY BUILDING_ID
	            ), EXTENDED_VALIDITY AS (
	                -- sometimes some entries get invalidated for no reason so we join the information
	                SELECT /*+ PARALLEL(16) */ CMTS, SERVICE_GROUP, MAC, MIN(Modemloss_valid_from) AS V_FROM, MAX(modemloss_valid_to) AS V_TO
	                FROM CTSP_HIST.CTSP_HIST_MODEMLOSS_CH
	                GROUP BY  CMTS, SERVICE_GROUP, MAC
	            ), SVG_CMTS_INFO AS (
	                SELECT /*+ PARALLEL(16) */ 
	                    CMTS, SERVICE_GROUP, UPPER(REPLACE(MAC,'':'')) AS MAC
	                FROM EXTENDED_VALIDITY 
	                WHERE TRUNC(V_FROM) <= ''' || TRUNC(day_to_consider)|| ''' AND TRUNC(V_TO) >=''' ||  TRUNC(day_to_consider)|| '''
	            )
	            SELECT
	                /*+ PARALLEL(16) */ 
	                HW.MAC,
	                HW.HARDWARE_MODEL,
	                HW.CLY_ACCOUNT_NUMBER ,
	                N.N_CPE,
	                S.SERVICE_GROUP,
	                S.CMTS
	            FROM HW_INFO HW 
	            INNER JOIN N_CPE_2_BUILDING N 
	                ON HW.BUILDING_ID = N.BUILDING_ID
	            INNER JOIN SVG_CMTS_INFO S 
	                ON S.MAC = HW.MAC
	            ) 
	        WHERE HARDWARE_MODEL IN (
	            ''CONNECT BOX CH7465LG COMPAL'',
	            -- OLD MODEMS
	            ''UBEE EVM3236 (ED 3.0) - CPE'',
	            ''UBEE EVM3206 (ED 3.0) - CPE'',
	            ''WLAN MODEM TC7200 - CPE'',
	            ''WLAN MODEM EVW3226 - CPE'',
	            ''WLAN MODEM TC7200 V2 - CPE'',
	            ''WLAN MODEM TWG870 - CPE''
	            ) -- WE LIMIT OURSELVES TO models that do not have deep sleep mode
		)' ;

		EXECUTE IMMEDIATE '
		CREATE TABLE HUMOREAU.OFFLINE_CPE AS (
	        SELECT * FROM (
	            WITH SELECTED_MACS AS (
	                SELECT /*+ PARALLEL(16) */ MAC, HOUR_STAMP
	                FROM  SAA.CM_HOUR_HEALTH
	                WHERE 
	                    CM_STATUS != '''|| v_online_status || '''
	                    AND TRUNC(HOUR_STAMP) <= TRUNC(SYSDATE-1) -- To limit ourselves to days where we have FULL DAYS history
	            )
	            SELECT  /*+ PARALLEL(16) */ 
	                    MAC,
	                    to_number(to_char(HOUR_STAMP,''HH24'')) H, 
	                    to_number(to_char(HOUR_STAMP,''DD'')) D
	            FROM SELECTED_MACS
	            )
		)' ;

		EXECUTE IMMEDIATE '
		CREATE TABLE HUMOREAU.UNAVAILABILITY_PCT AS (
	        SELECT * FROM (
	            WITH HOURLY_UNAVAILABILITY AS (
	                -- For each CPE, For each hour we compute the percentage over the full day history of offline days
	                SELECT /*+ PARALLEL(16) */ MAC, H, 100*COUNT(D)/('''|| v_n_days_offline_history || ''') UNAVAILABLE
	                FROM HUMOREAU.OFFLINE_CPE
	                GROUP BY MAC, H
	            )
	            -- we pivot the results to have one entry per cpe
	            SELECT /*+ PARALLEL(16) */
	                MAC,
	                COALESCE("0_UNAVAILABLE",0) AS "0_UNAVAILABLE",
	                COALESCE("1_UNAVAILABLE",0) AS "1_UNAVAILABLE",
	                COALESCE("2_UNAVAILABLE",0) AS "2_UNAVAILABLE",
	                COALESCE("3_UNAVAILABLE",0) AS "3_UNAVAILABLE",
	                COALESCE("4_UNAVAILABLE",0) AS "4_UNAVAILABLE",
	                COALESCE("5_UNAVAILABLE",0) AS "5_UNAVAILABLE",
	                COALESCE("6_UNAVAILABLE",0) AS "6_UNAVAILABLE",
	                COALESCE("7_UNAVAILABLE",0) AS "7_UNAVAILABLE",
	                COALESCE("8_UNAVAILABLE",0) AS "8_UNAVAILABLE",
	                COALESCE("9_UNAVAILABLE",0) AS "9_UNAVAILABLE",
	                COALESCE("10_UNAVAILABLE",0) AS "10_UNAVAILABLE",
	                COALESCE("11_UNAVAILABLE",0) AS "11_UNAVAILABLE",
	                COALESCE("12_UNAVAILABLE",0) AS "12_UNAVAILABLE",
	                COALESCE("13_UNAVAILABLE",0) AS "13_UNAVAILABLE",
	                COALESCE("14_UNAVAILABLE",0) AS "14_UNAVAILABLE",
	                COALESCE("15_UNAVAILABLE",0) AS "15_UNAVAILABLE",
	                COALESCE("16_UNAVAILABLE",0) AS "16_UNAVAILABLE",
	                COALESCE("17_UNAVAILABLE",0) AS "17_UNAVAILABLE",
	                COALESCE("18_UNAVAILABLE",0) AS "18_UNAVAILABLE",
	                COALESCE("19_UNAVAILABLE",0) AS "19_UNAVAILABLE",
	                COALESCE("20_UNAVAILABLE",0) AS "20_UNAVAILABLE",
	                COALESCE("21_UNAVAILABLE",0) AS "21_UNAVAILABLE",
	                COALESCE("22_UNAVAILABLE",0) AS "22_UNAVAILABLE",
	                COALESCE("23_UNAVAILABLE",0) AS "23_UNAVAILABLE"
	            FROM HOURLY_UNAVAILABILITY
	            PIVOT (MAX(UNAVAILABLE) AS UNAVAILABLE FOR H IN (0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23))
	            )
		)' ;

		EXECUTE IMMEDIATE '
		CREATE TABLE HUMOREAU.CENTILES AS (
	        SELECT * FROM (
	            WITH ENRICHED_UNAVAILABILITY AS (
	                SELECT 
	                    /*+ PARALLEL(16) */
	                    DETAILS.MAC,
	                    DETAILS.HARDWARE_MODEL,
	                    "0_UNAVAILABLE","1_UNAVAILABLE","2_UNAVAILABLE","3_UNAVAILABLE","4_UNAVAILABLE","5_UNAVAILABLE","6_UNAVAILABLE",
	                    "7_UNAVAILABLE","8_UNAVAILABLE","9_UNAVAILABLE","10_UNAVAILABLE","11_UNAVAILABLE","12_UNAVAILABLE","13_UNAVAILABLE",
	                    "14_UNAVAILABLE","15_UNAVAILABLE","16_UNAVAILABLE","17_UNAVAILABLE","18_UNAVAILABLE","19_UNAVAILABLE","20_UNAVAILABLE",
	                    "21_UNAVAILABLE","22_UNAVAILABLE","23_UNAVAILABLE"
	                FROM HUMOREAU.UNAVAILABILITY_PCT U INNER JOIN HUMOREAU.EXTRA_INFOS DETAILS
	                    ON DETAILS.MAC = U.MAC
	            )
	            -- We try to find which frequency of unavailability is considered as standard 
	            -- (it splits the frequency into v_percentile_limit/100 in one group and 1-(v_percentile_limit/100) in the other)
	            SELECT 
	                /*+ PARALLEL(16) */
	                MAC,
	                HARDWARE_MODEL,
	                "0_UNAVAILABLE","1_UNAVAILABLE","2_UNAVAILABLE","3_UNAVAILABLE","4_UNAVAILABLE","5_UNAVAILABLE","6_UNAVAILABLE",
	                "7_UNAVAILABLE","8_UNAVAILABLE","9_UNAVAILABLE","10_UNAVAILABLE","11_UNAVAILABLE","12_UNAVAILABLE","13_UNAVAILABLE",
	                "14_UNAVAILABLE","15_UNAVAILABLE","16_UNAVAILABLE","17_UNAVAILABLE","18_UNAVAILABLE","19_UNAVAILABLE","20_UNAVAILABLE",
	                "21_UNAVAILABLE","22_UNAVAILABLE","23_UNAVAILABLE",
	                PERCENTILE_DISC(''' ||v_percentile_limit|| '''/100) WITHIN GROUP (ORDER BY "0_UNAVAILABLE" ASC) OVER (PARTITION BY HARDWARE_MODEL) AS "0_CUT",
	                PERCENTILE_DISC(''' || v_percentile_limit|| '''/100) WITHIN GROUP (ORDER BY "1_UNAVAILABLE" ASC) OVER (PARTITION BY HARDWARE_MODEL) AS "1_CUT",
	                PERCENTILE_DISC(''' || v_percentile_limit|| '''/100) WITHIN GROUP (ORDER BY "2_UNAVAILABLE" ASC) OVER (PARTITION BY HARDWARE_MODEL) AS "2_CUT",
	                PERCENTILE_DISC(''' || v_percentile_limit|| '''/100) WITHIN GROUP (ORDER BY "3_UNAVAILABLE" ASC) OVER (PARTITION BY HARDWARE_MODEL) AS "3_CUT",
	                PERCENTILE_DISC(''' || v_percentile_limit|| '''/100) WITHIN GROUP (ORDER BY "4_UNAVAILABLE" ASC) OVER (PARTITION BY HARDWARE_MODEL) AS "4_CUT",
	                PERCENTILE_DISC(''' || v_percentile_limit|| '''/100) WITHIN GROUP (ORDER BY "5_UNAVAILABLE" ASC) OVER (PARTITION BY HARDWARE_MODEL) AS "5_CUT",
	                PERCENTILE_DISC(''' || v_percentile_limit|| '''/100) WITHIN GROUP (ORDER BY "6_UNAVAILABLE" ASC) OVER (PARTITION BY HARDWARE_MODEL) AS "6_CUT",
	                PERCENTILE_DISC(''' || v_percentile_limit|| '''/100) WITHIN GROUP (ORDER BY "7_UNAVAILABLE" ASC) OVER (PARTITION BY HARDWARE_MODEL) AS "7_CUT",
	                PERCENTILE_DISC(''' || v_percentile_limit|| '''/100) WITHIN GROUP (ORDER BY "8_UNAVAILABLE" ASC) OVER (PARTITION BY HARDWARE_MODEL) AS "8_CUT",
	                PERCENTILE_DISC(''' || v_percentile_limit|| '''/100) WITHIN GROUP (ORDER BY "9_UNAVAILABLE" ASC) OVER (PARTITION BY HARDWARE_MODEL) AS "9_CUT",
	                PERCENTILE_DISC(''' || v_percentile_limit|| '''/100) WITHIN GROUP (ORDER BY "10_UNAVAILABLE" ASC) OVER (PARTITION BY HARDWARE_MODEL) AS "10_CUT",
	                PERCENTILE_DISC(''' || v_percentile_limit|| '''/100) WITHIN GROUP (ORDER BY "11_UNAVAILABLE" ASC) OVER (PARTITION BY HARDWARE_MODEL) AS "11_CUT",
	                PERCENTILE_DISC(''' || v_percentile_limit|| '''/100) WITHIN GROUP (ORDER BY "12_UNAVAILABLE" ASC) OVER (PARTITION BY HARDWARE_MODEL) AS "12_CUT",
	                PERCENTILE_DISC(''' || v_percentile_limit|| '''/100) WITHIN GROUP (ORDER BY "13_UNAVAILABLE" ASC) OVER (PARTITION BY HARDWARE_MODEL) AS "13_CUT",
	                PERCENTILE_DISC(''' || v_percentile_limit|| '''/100) WITHIN GROUP (ORDER BY "14_UNAVAILABLE" ASC) OVER (PARTITION BY HARDWARE_MODEL) AS "14_CUT",
	                PERCENTILE_DISC(''' || v_percentile_limit|| '''/100) WITHIN GROUP (ORDER BY "15_UNAVAILABLE" ASC) OVER (PARTITION BY HARDWARE_MODEL) AS "15_CUT",
	                PERCENTILE_DISC(''' || v_percentile_limit|| '''/100) WITHIN GROUP (ORDER BY "16_UNAVAILABLE" ASC) OVER (PARTITION BY HARDWARE_MODEL) AS "16_CUT",
	                PERCENTILE_DISC(''' || v_percentile_limit|| '''/100) WITHIN GROUP (ORDER BY "17_UNAVAILABLE" ASC) OVER (PARTITION BY HARDWARE_MODEL) AS "17_CUT",
	                PERCENTILE_DISC(''' || v_percentile_limit|| '''/100) WITHIN GROUP (ORDER BY "18_UNAVAILABLE" ASC) OVER (PARTITION BY HARDWARE_MODEL) AS "18_CUT",
	                PERCENTILE_DISC(''' || v_percentile_limit|| '''/100) WITHIN GROUP (ORDER BY "19_UNAVAILABLE" ASC) OVER (PARTITION BY HARDWARE_MODEL) AS "19_CUT",
	                PERCENTILE_DISC(''' || v_percentile_limit|| '''/100) WITHIN GROUP (ORDER BY "20_UNAVAILABLE" ASC) OVER (PARTITION BY HARDWARE_MODEL) AS "20_CUT",
	                PERCENTILE_DISC(''' || v_percentile_limit|| '''/100) WITHIN GROUP (ORDER BY "21_UNAVAILABLE" ASC) OVER (PARTITION BY HARDWARE_MODEL) AS "21_CUT",
	                PERCENTILE_DISC(''' || v_percentile_limit|| '''/100) WITHIN GROUP (ORDER BY "22_UNAVAILABLE" ASC) OVER (PARTITION BY HARDWARE_MODEL) AS "22_CUT",
	                PERCENTILE_DISC(''' || v_percentile_limit|| '''/100) WITHIN GROUP (ORDER BY "23_UNAVAILABLE" ASC) OVER (PARTITION BY HARDWARE_MODEL) AS "23_CUT"
	            FROM ENRICHED_UNAVAILABILITY
	            )
	    )' ;

	    EXECUTE IMMEDIATE '
		CREATE TABLE HUMOREAU.OUTLIERS AS (
	        -- We flag the CPEs that are outliers with respect to their own hardware_model, so that we can exclude those from our analysis 
	        SELECT 
	             MAC, 1 AS FLG_OUTLIER
	        FROM CENTILES
	        WHERE 
	            "0_UNAVAILABLE" >= "0_CUT" OR "1_UNAVAILABLE" >= "1_CUT" OR "2_UNAVAILABLE" >= "2_CUT" OR
	            "3_UNAVAILABLE" >= "3_CUT" OR "4_UNAVAILABLE" >= "4_CUT" OR "5_UNAVAILABLE" >= "5_CUT" OR
	            "6_UNAVAILABLE" >= "6_CUT" OR "7_UNAVAILABLE" >= "7_CUT" OR "8_UNAVAILABLE" >= "8_CUT" OR
	            "9_UNAVAILABLE" >= "9_CUT" OR "10_UNAVAILABLE" >= "10_CUT" OR "11_UNAVAILABLE" >= "11_CUT" OR
	            "12_UNAVAILABLE" >= "12_CUT" OR "13_UNAVAILABLE" >= "13_CUT" OR "14_UNAVAILABLE" >= "14_CUT" OR
	            "15_UNAVAILABLE" >= "15_CUT" OR "16_UNAVAILABLE" >= "16_CUT" OR "17_UNAVAILABLE" >= "17_CUT" OR
	            "18_UNAVAILABLE" >= "18_CUT" OR "19_UNAVAILABLE" >= "19_CUT" OR"20_UNAVAILABLE" >= "20_CUT" OR
	            "21_UNAVAILABLE" >= "21_CUT" OR "22_UNAVAILABLE" >= "22_CUT" OR "23_UNAVAILABLE" >= "23_CUT"
	    )' ;

	    EXECUTE IMMEDIATE ' 
		CREATE TABLE HUMOREAU.SAA_SVGP_ENRICHED as (
	        SELECT * FROM (
	            WITH FLAGGED_OFFLINE AS (
	                SELECT O.*, 1 AS "OFFLINE_FLG" 
	                FROM HUMOREAU.OFFLINE_CPE O
	            )
	            -- Enrich the saa data with this information
	            SELECT /*+ PARALLEL(16) */  
	                DETAILS.MAC,
	                DETAILS.HARDWARE_MODEL,
	                DETAILS.CLY_ACCOUNT_NUMBER ,
	                DETAILS.N_CPE AS N_CPE_BUILDING,
	                DETAILS.SERVICE_GROUP,
	                DETAILS.CMTS,
	                --
	                COALESCE(O.OFFLINE_FLG,0) AS OFFLINE_FLG,
	                --
	                SAA.ACCOUNT_NUMBER AS SAA_ACCOUNT_NUMBER,
	                SAA.HOUR_STAMP,
	                SAA.TXPOWER_UP, 
	                SAA.RXPOWER_UP, 
	                SAA.RXPOWER_DN, 
	                SAA.CER_DN, 
	                SAA.CER_UP, 
	                SAA.SNR_DN,
	                SAA.SNR_UP, 
	                SAA.PCT_TRAFFIC_DMH_UP, 
	                SAA.PCT_TRAFFIC_SDMH_UP
	            FROM 
	                HUMOREAU.EXTRA_INFOS DETAILS INNER JOIN SAA.CM_HOUR_HEALTH SAA
	                    ON DETAILS.MAC = SAA.MAC
	                    AND TRUNC(SAA.HOUR_STAMP) = ''' || TRUNC(day_to_consider) || '''
	                    AND DETAILS.MAC NOT IN (SELECT MAC FROM HUMOREAU.OUTLIERS)

	                -- We add the flag for offline
	                LEFT OUTER JOIN FLAGGED_OFFLINE O
	                    ON  SAA.MAC = O.MAC
	                        AND to_number(to_char(SAA.HOUR_STAMP,''HH24'')) = O.H
	                        AND to_number(to_char(SAA.HOUR_STAMP,''DD'')) = O.D
	            )
	    )' ;
		EXECUTE IMMEDIATE '
			CREATE TABLE HUMOREAU.SVG_AVG AS (
		        -- Compute for each service group the average, max and min of each measurement
		        -- NB: If there are no measurement in the service group for a given timestamp, then no max/range will be 
		        -- computed which is fine since anyway theree won''t be any CPE measurement to standardise.
		        SELECT  CMTS, 
		        SERVICE_GROUP, 
		        HOUR_STAMP, 
		        ----
		        AVG(TXPOWER_UP) AS AVG_TXPOWER_UP,
		        (MAX(TXPOWER_UP) - MIN(TXPOWER_UP)) AS RANGE_TXPOWER_UP,
		        --
		        AVG(RXPOWER_UP) AS AVG_RXPOWER_UP,
		        (MAX(RXPOWER_UP) - MIN(RXPOWER_UP)) AS RANGE_RXPOWER_UP,
		        --
		        AVG(RXPOWER_DN) AS AVG_RXPOWER_DN,
		        (MAX(RXPOWER_DN) - MIN(RXPOWER_DN)) AS RANGE_RXPOWER_DN,
		        ---
		        AVG(CER_UP) AS AVG_CER_UP,
		        (MAX(CER_UP) - MIN(CER_UP)) AS RANGE_CER_UP,
		        --
		        AVG(CER_DN) AS AVG_CER_DN,
		        (MAX(CER_DN) - MIN(CER_DN)) AS RANGE_CER_DN,
		        --
		        AVG(SNR_DN) AS AVG_SNR_DN,
		        (MAX(SNR_DN) - MIN(SNR_DN)) AS RANGE_SNR_DN,
		        --
		        AVG(SNR_UP) AS AVG_SNR_UP,
		        (MAX(SNR_UP) - MIN(SNR_UP)) AS RANGE_SNR_UP,
		        --
		        AVG(PCT_TRAFFIC_DMH_UP) AS AVG_PCT_TRAFFIC_DMH_UP,
		        (MAX(PCT_TRAFFIC_DMH_UP) - MIN(PCT_TRAFFIC_DMH_UP)) AS RANGE_PCT_TRAFFIC_DMH_UP,
		        --
		        AVG(PCT_TRAFFIC_SDMH_UP) AS AVG_PCT_TRAFFIC_SDMH_UP,
		        (MAX(PCT_TRAFFIC_SDMH_UP) - MIN(PCT_TRAFFIC_SDMH_UP)) AS RANGE_PCT_TRAFFIC_SDMH_UP
		        
		        FROM HUMOREAU.SAA_SVGP_ENRICHED
		        GROUP BY CMTS, SERVICE_GROUP, HOUR_STAMP
		    )' ;

	   	EXECUTE IMMEDIATE '
			CREATE TABLE HUMOREAU.STD_EXTRACTED_MES AS (
		        -- standardise the CPE measurement using  [x - mean(x)]/[max(x)-min(x)], 
		        -- where the aggregates are computed over the service group
		        SELECT  CPE.CMTS, 
		                CPE.SERVICE_GROUP, 
		                CPE.HOUR_STAMP, 
		                CPE.MAC, 
		                CPE.SAA_ACCOUNT_NUMBER, 
		                CPE.OFFLINE_FLG,
		                CPE.HARDWARE_MODEL,
		                CPE.CLY_ACCOUNT_NUMBER,
		                CPE.N_CPE_BUILDING,
		                -- (if denom is 0, then no variance at the service group level so the relative cpe measurement is 0)
		                CASE    WHEN SVGP.RANGE_TXPOWER_UP = 0 THEN 0 
		                        ELSE (CPE.TXPOWER_UP - SVGP.AVG_TXPOWER_UP)/SVGP.RANGE_TXPOWER_UP
		                        END AS TXPOWER_UP,
		                -- 
		                CASE    WHEN SVGP.RANGE_RXPOWER_UP = 0 THEN 0 
		                        ELSE (CPE.RXPOWER_UP- SVGP.AVG_RXPOWER_UP)/SVGP.RANGE_RXPOWER_UP
		                        END AS RXPOWER_UP,
		                --
		                CASE    WHEN SVGP.RANGE_RXPOWER_DN= 0 THEN 0 
		                        ELSE (CPE.RXPOWER_DN- SVGP.AVG_RXPOWER_DN)/SVGP.RANGE_RXPOWER_DN 
		                        END AS RXPOWER_DN,
		                --
		                CASE    WHEN SVGP.RANGE_CER_DN = 0 THEN 0 
		                        ELSE (CPE.CER_DN- SVGP.AVG_CER_DN)/SVGP.RANGE_CER_DN 
		                        END AS CER_DN,
		                --
		                CASE    WHEN SVGP.RANGE_CER_UP = 0 THEN 0 
		                        ELSE (CPE.CER_UP- SVGP.AVG_CER_UP)/SVGP.RANGE_CER_UP 
		                        END AS CER_UP,
		                --
		                CASE    WHEN SVGP.RANGE_SNR_DN = 0 THEN 0 
		                        ELSE (CPE.SNR_DN- SVGP.AVG_SNR_DN)/SVGP.RANGE_SNR_DN
		                        END AS SNR_DN,
		                --
		                CASE    WHEN SVGP.RANGE_SNR_UP = 0 THEN 0 
		                        ELSE (CPE.SNR_UP- SVGP.AVG_SNR_UP)/SVGP.RANGE_SNR_UP
		                        END AS SNR_UP,
		                --
		                CASE    WHEN SVGP.RANGE_PCT_TRAFFIC_DMH_UP = 0 THEN 0 
		                        ELSE (CPE.PCT_TRAFFIC_DMH_UP- SVGP.AVG_PCT_TRAFFIC_DMH_UP)/SVGP.RANGE_PCT_TRAFFIC_DMH_UP
		                        END AS PCT_TRAFFIC_DMH_UP,
		                --
		                CASE    WHEN SVGP.RANGE_PCT_TRAFFIC_SDMH_UP = 0 THEN 0 
		                        ELSE (CPE.PCT_TRAFFIC_SDMH_UP- SVGP.AVG_PCT_TRAFFIC_SDMH_UP)/SVGP.RANGE_PCT_TRAFFIC_SDMH_UP 
		                        END AS PCT_TRAFFIC_SDMH_UP
		                
		        FROM HUMOREAU.SVG_AVG SVGP, HUMOREAU.SAA_SVGP_ENRICHED CPE
		        WHERE SVGP.CMTS = CPE.CMTS AND SVGP.SERVICE_GROUP = CPE.SERVICE_GROUP AND SVGP.HOUR_STAMP = CPE.HOUR_STAMP
		    )' ;

		EXECUTE IMMEDIATE '
			CREATE TABLE HUMOREAU.DAY_0_6H_WINDOWS AS (
		        SELECT * FROM (
		            WITH STD_DAY_0 AS (
		                -- get only the data from day 0 and we had the time window indicator to which it belongs
		                SELECT
		                    CMTS, 
		                    SERVICE_GROUP, 
		                    MAC, 
		                    SAA_ACCOUNT_NUMBER, 
		                    OFFLINE_FLG,
		                    HARDWARE_MODEL,
		                    CLY_ACCOUNT_NUMBER,
		                    N_CPE_BUILDING,
		                    to_date(''' || TRUNC(day_to_consider) || ''',''DD/MM/YYYY HH24:MI:SS'') AS "DAY_0",

		                    TXPOWER_UP, RXPOWER_UP, RXPOWER_DN, CER_DN, 
		                    CER_UP, SNR_DN, SNR_UP, PCT_TRAFFIC_DMH_UP, PCT_TRAFFIC_SDMH_UP,
		                    CASE 
		                        WHEN to_number(to_char(T.HOUR_STAMP,''HH24'')) IN (23,22,21,20,19,18) THEN 0 -- those hours belongs to the most recent 6h window
		                        WHEN to_number(to_char(T.HOUR_STAMP,''HH24'')) IN (17,16,15,14,13,12) THEN 1
		                        WHEN to_number(to_char(T.HOUR_STAMP,''HH24'')) IN (11,10,9,8,7,6)     THEN 2
		                        WHEN to_number(to_char(T.HOUR_STAMP,''HH24'')) IN (5,4,3,2,1,0)       THEN 3 -- while these belong to the least recent
		                    END AS HOUR_WINDOW
		                FROM HUMOREAU.STD_EXTRACTED_MES T
		            )
		            -- We want to get the average of each measurement over 6h windows (23h to 17h then 17h to 11h ... ) for day_0
		            -- But we also count the number of missing measurement over each window.
		            SELECT 
		                CMTS, 
		                SERVICE_GROUP, 
		                MAC, 
		                SAA_ACCOUNT_NUMBER, 
		                HARDWARE_MODEL,
		                CLY_ACCOUNT_NUMBER,
		                N_CPE_BUILDING,
		                DAY_0,
		                --
		                HOUR_WINDOW, 
		                --
		                AVG(TXPOWER_UP) AS TXPOWER_UP,
		                AVG(RXPOWER_UP) AS RXPOWER_UP, 
		                AVG(RXPOWER_DN) AS RXPOWER_DN, 
		                AVG(CER_DN) AS CER_DN, 
		                AVG(CER_UP) AS CER_UP,
		                AVG(SNR_DN) AS SNR_DN,
		                AVG(SNR_UP) AS SNR_UP,
		                AVG(PCT_TRAFFIC_DMH_UP) AS PCT_TRAFFIC_DMH_UP, 
		                AVG(PCT_TRAFFIC_SDMH_UP) AS PCT_TRAFFIC_SDMH_UP,
		                -- then the missed values
		                100*(6 - COUNT(TXPOWER_UP))/6 AS MISS_TXPOWER_UP, 
		                100*(6 - COUNT(RXPOWER_UP))/6 AS MISS_RXPOWER_UP, 
		                100*(6 - COUNT(RXPOWER_DN))/6 AS MISS_RXPOWER_DN, 
		                100*(6 - COUNT(CER_DN))/6 AS MISS_CER_DN, 
		                100*(6 - COUNT(CER_UP))/6 AS MISS_CER_UP, 
		                100*(6 - COUNT(SNR_DN))/6 AS MISS_SNR_DN, 
		                100*(6 - COUNT(SNR_UP))/6 AS MISS_SNR_UP, 
		                100*(6 - COUNT(PCT_TRAFFIC_DMH_UP))/6 AS MISS_PCT_TRAFFIC_DMH_UP, 
		                100*(6 - COUNT(PCT_TRAFFIC_SDMH_UP))/6 AS MISS_PCT_TRAFFIC_SDMH_UP, 
		                -- and finally the unavailability percentage
		                100*SUM(OFFLINE_FLG)/6 AS OFFLINE_PCT    
		            FROM STD_DAY_0
		            GROUP BY CMTS, SERVICE_GROUP, MAC, SAA_ACCOUNT_NUMBER, HARDWARE_MODEL, CLY_ACCOUNT_NUMBER, N_CPE_BUILDING, HOUR_WINDOW, DAY_0
		        )
		    )' ;


	    EXECUTE IMMEDIATE '
		CREATE TABLE HUMOREAU.CPE_VECTOR AS (
	        SELECT *
	        FROM HUMOREAU.DAY_0_6H_WINDOWS 
	        PIVOT 
	        ( 
	          MAX(TXPOWER_UP) AS TXPOWER_UP, 
	          MAX(RXPOWER_UP) AS RXPOWER_UP, 
	          MAX(RXPOWER_DN) AS RXPOWER_DN, 
	          MAX(CER_DN) AS CER_DN, 
	          MAX(CER_UP) AS CER_UP, 
	          MAX(SNR_DN) AS SNR_DN,
	          MAX(SNR_UP) AS SNR_UP,
	          MAX(PCT_TRAFFIC_DMH_UP) AS PCT_TRAFFIC_DMH_UP, 
	          MAX(PCT_TRAFFIC_SDMH_UP) AS PCT_TRAFFIC_SDMH_UP,
	          -- and the offline percentage
	          MAX(OFFLINE_PCT) AS OFFLINE_PCT, 
	          -- then the missed values
	          MAX(MISS_TXPOWER_UP) AS MISS_TXPOWER_UP, 
	          MAX(MISS_RXPOWER_UP) AS MISS_RXPOWER_UP, 
	          MAX(MISS_RXPOWER_DN) AS MISS_RXPOWER_DN, 
	          MAX(MISS_CER_DN) AS MISS_CER_DN, 
	          MAX(MISS_CER_UP) AS MISS_CER_UP, 
	          MAX(MISS_SNR_DN) AS MISS_SNR_DN,
	          MAX(MISS_SNR_UP) AS MISS_SNR_UP,
	          MAX(MISS_PCT_TRAFFIC_DMH_UP) AS MISS_PCT_TRAFFIC_DMH_UP, 
	          MAX(MISS_PCT_TRAFFIC_SDMH_UP) AS MISS_PCT_TRAFFIC_SDMH_UP
	          
	          FOR HOUR_WINDOW IN (0,1,2,3)
	        )
	    )' ;

	    EXECUTE IMMEDIATE '
		CREATE TABLE HUMOREAU.VECTOR AS (
	        -- Finally we merge the tables from CPE and CMTS_UP/DN
	        SELECT 
	            CPE.DAY_0,
	            CPE.MAC,
	            ---
	            CPE.CLY_ACCOUNT_NUMBER,
	            CPE.SAA_ACCOUNT_NUMBER,
	            CPE.CMTS,
	            CPE.SERVICE_GROUP,
	            CPE.HARDWARE_MODEL,
	            CPE.N_CPE_BUILDING,
	            ---
	            ----- CPE
	            ---
	            "0_CER_DN",
	            "1_CER_DN",
	            "2_CER_DN",
	            "3_CER_DN",
	            --
	            "0_MISS_CER_DN",
	            "1_MISS_CER_DN",
	            "2_MISS_CER_DN",
	            "3_MISS_CER_DN",
	            -----------
	            "0_CER_UP",
	            "1_CER_UP",
	            "2_CER_UP",
	            "3_CER_UP",
	            --
	            "0_MISS_CER_UP",
	            "1_MISS_CER_UP",
	            "2_MISS_CER_UP",
	            "3_MISS_CER_UP",
	            -----------
	            "0_OFFLINE_PCT",
	            "1_OFFLINE_PCT",
	            "2_OFFLINE_PCT",
	            "3_OFFLINE_PCT",
	            -----------
	            "0_PCT_TRAFFIC_DMH_UP",
	            "1_PCT_TRAFFIC_DMH_UP",
	            "2_PCT_TRAFFIC_DMH_UP",
	            "3_PCT_TRAFFIC_DMH_UP",
	            --
	            "0_MISS_PCT_TRAFFIC_DMH_UP",
	            "1_MISS_PCT_TRAFFIC_DMH_UP",
	            "2_MISS_PCT_TRAFFIC_DMH_UP",
	            "3_MISS_PCT_TRAFFIC_DMH_UP",
	            -----------
	            "0_PCT_TRAFFIC_SDMH_UP",
	            "1_PCT_TRAFFIC_SDMH_UP",
	            "2_PCT_TRAFFIC_SDMH_UP",
	            "3_PCT_TRAFFIC_SDMH_UP",
	            --
	            "0_MISS_PCT_TRAFFIC_SDMH_UP",
	            "1_MISS_PCT_TRAFFIC_SDMH_UP",
	            "2_MISS_PCT_TRAFFIC_SDMH_UP",
	            "3_MISS_PCT_TRAFFIC_SDMH_UP",
	            -----------
	            "0_RXPOWER_DN",
	            "1_RXPOWER_DN",
	            "2_RXPOWER_DN",
	            "3_RXPOWER_DN",
	            --
	            "0_MISS_RXPOWER_DN",
	            "1_MISS_RXPOWER_DN",
	            "2_MISS_RXPOWER_DN",
	            "3_MISS_RXPOWER_DN",
	            -----------
	            "0_RXPOWER_UP",
	            "1_RXPOWER_UP",
	            "2_RXPOWER_UP",
	            "3_RXPOWER_UP",
	            --
	            "0_MISS_RXPOWER_UP",
	            "1_MISS_RXPOWER_UP",
	            "2_MISS_RXPOWER_UP",
	            "3_MISS_RXPOWER_UP",
	            -----------
	            "0_SNR_DN",
	            "1_SNR_DN",
	            "2_SNR_DN",
	            "3_SNR_DN",
	            --
	            "0_MISS_SNR_DN",
	            "1_MISS_SNR_DN",
	            "2_MISS_SNR_DN",
	            "3_MISS_SNR_DN",
	            -----------
	            "0_SNR_UP",
	            "1_SNR_UP",
	            "2_SNR_UP",
	            "3_SNR_UP",
	            --
	            "0_MISS_SNR_UP",
	            "1_MISS_SNR_UP",
	            "2_MISS_SNR_UP",
	            "3_MISS_SNR_UP",
	            -----------
	            "0_TXPOWER_UP",
	            "1_TXPOWER_UP",
	            "2_TXPOWER_UP",
	            "3_TXPOWER_UP",
	            --
	            "0_MISS_TXPOWER_UP",
	            "1_MISS_TXPOWER_UP",
	            "2_MISS_TXPOWER_UP",
	            "3_MISS_TXPOWER_UP",
	            -----------
	            ---
	            ----- CMTS_UP
	            ---
	            "0_CMTS_RX_UP",
	            "1_CMTS_RX_UP",
	            "2_CMTS_RX_UP",
	            "3_CMTS_RX_UP",
	            --
	            "0_CMTS_TX_UP",
	            "1_CMTS_TX_UP",
	            "2_CMTS_TX_UP",
	            "3_CMTS_TX_UP",
	            --
	            "0_CMTS_CER_UP",
	            "1_CMTS_CER_UP",
	            "2_CMTS_CER_UP",
	            "3_CMTS_CER_UP",
	            --
	            "0_CMTS_UTILIZATION_UP",
	            "1_CMTS_UTILIZATION_UP",
	            "2_CMTS_UTILIZATION_UP",
	            "3_CMTS_UTILIZATION_UP",
	            --
	            "0_CMTS_MS_UTILIZATION_UP",
	            "1_CMTS_MS_UTILIZATION_UP",
	            "2_CMTS_MS_UTILIZATION_UP",
	            "3_CMTS_MS_UTILIZATION_UP",
	            --
	            "0_CMTS_F_MS_UTILIZATION_UP",
	            "1_CMTS_F_MS_UTILIZATION_UP",
	            "2_CMTS_F_MS_UTILIZATION_UP",
	            "3_CMTS_F_MS_UTILIZATION_UP",
	            ---
	            ----- CMTS_DN
	            ---
	            "0_CMTS_RX_DN",
	            "1_CMTS_RX_DN",
	            "2_CMTS_RX_DN",
	            "3_CMTS_RX_DN",
	            --
	            "0_CMTS_SNR_DN",
	            "1_CMTS_SNR_DN",
	            "2_CMTS_SNR_DN",
	            "3_CMTS_SNR_DN",
	            --
	            "0_CMTS_CCER_DN",
	            "1_CMTS_CCER_DN",
	            "2_CMTS_CCER_DN",
	            "3_CMTS_CCER_DN",
	            --
	            "0_CMTS_CER_DN",
	            "1_CMTS_CER_DN",
	            "2_CMTS_CER_DN",
	            "3_CMTS_CER_DN",
	            --
	            "0_CMTS_UTILIZATION_DN",
	            "1_CMTS_UTILIZATION_DN",
	            "2_CMTS_UTILIZATION_DN",
	            "3_CMTS_UTILIZATION_DN"

	        FROM HUMOREAU.CPE_VECTOR CPE 

	        INNER JOIN HUMOREAU.CMTS_UP_VECTOR CMTS_UP
	            ON  CPE.CMTS = CMTS_UP.CMTS_NAME AND 
	                CPE.SERVICE_GROUP = CMTS_UP.SERVICE_GROUP AND
	                CPE.DAY_0 = CMTS_UP.DAY_0

	        INNER JOIN HUMOREAU.CMTS_DN_VECTOR CMTS_DN
	            ON CPE.CMTS = CMTS_DN.CMTS_NAME AND 
	                CPE.SERVICE_GROUP = CMTS_DN.SERVICE_GROUP AND
	                CPE.DAY_0 = CMTS_DN.DAY_0
	    )' ;

	    EXECUTE IMMEDIATE '
		CREATE TABLE HUMOREAU.DAILY_AVG_DAY_0 AS (
	        SELECT 
	            MAC,
	            DAY_0,
	            ---
	            CASE
	            WHEN COALESCE("0_CER_DN","1_CER_DN","2_CER_DN","3_CER_DN") IS NOT NULL THEN 
	                  (COALESCE("0_CER_DN",0) + COALESCE("1_CER_DN",0) + COALESCE("2_CER_DN",0) + COALESCE("3_CER_DN",0))/
	                  (CASE WHEN "0_CER_DN" IS NULL THEN 0 ELSE 1 END + 
	                   CASE WHEN "1_CER_DN" IS NULL THEN 0 ELSE 1 END + 
	                   CASE WHEN "2_CER_DN" IS NULL THEN 0 ELSE 1 END + 
	                   CASE WHEN "3_CER_DN" IS NULL THEN 0 ELSE 1 END 
	                   )
	            ELSE NULL
	            END AS CER_DN,
	            --
	            CASE
	            WHEN COALESCE("0_MISS_CER_DN","1_MISS_CER_DN","2_MISS_CER_DN","3_MISS_CER_DN") IS NOT NULL THEN 
	                  (COALESCE("0_MISS_CER_DN",0) + COALESCE("1_MISS_CER_DN",0) + COALESCE("2_MISS_CER_DN",0) + COALESCE("3_MISS_CER_DN",0))/
	                  (CASE WHEN "0_MISS_CER_DN" IS NULL THEN 0 ELSE 1 END + 
	                   CASE WHEN "1_MISS_CER_DN" IS NULL THEN 0 ELSE 1 END + 
	                   CASE WHEN "2_MISS_CER_DN" IS NULL THEN 0 ELSE 1 END + 
	                   CASE WHEN "3_MISS_CER_DN" IS NULL THEN 0 ELSE 1 END 
	                   )
	            ELSE NULL
	            END AS MISS_CER_DN,
	            -----------
	            CASE
	            WHEN COALESCE("0_CER_UP","1_CER_UP","2_CER_UP","3_CER_UP") IS NOT NULL THEN 
	                  (COALESCE("0_CER_UP",0) + COALESCE("1_CER_UP",0) + COALESCE("2_CER_UP",0) + COALESCE("3_CER_UP",0))/
	                  (CASE WHEN "0_CER_UP" IS NULL THEN 0 ELSE 1 END + 
	                   CASE WHEN "1_CER_UP" IS NULL THEN 0 ELSE 1 END + 
	                   CASE WHEN "2_CER_UP" IS NULL THEN 0 ELSE 1 END + 
	                   CASE WHEN "3_CER_UP" IS NULL THEN 0 ELSE 1 END 
	                   )
	            ELSE NULL
	            END AS CER_UP,
	            --
	            CASE
	            WHEN COALESCE("0_MISS_CER_UP","1_MISS_CER_UP","2_MISS_CER_UP","3_MISS_CER_UP") IS NOT NULL THEN 
	                  (COALESCE("0_MISS_CER_UP",0) + COALESCE("1_MISS_CER_UP",0) + COALESCE("2_MISS_CER_UP",0) + COALESCE("3_MISS_CER_UP",0))/
	                  (CASE WHEN "0_MISS_CER_UP" IS NULL THEN 0 ELSE 1 END + 
	                   CASE WHEN "1_MISS_CER_UP" IS NULL THEN 0 ELSE 1 END + 
	                   CASE WHEN "2_MISS_CER_UP" IS NULL THEN 0 ELSE 1 END + 
	                   CASE WHEN "3_MISS_CER_UP" IS NULL THEN 0 ELSE 1 END 
	                   )
	            ELSE NULL
	            END AS MISS_CER_UP,
	        -----------
	        CASE
	        WHEN COALESCE("0_OFFLINE_PCT","1_OFFLINE_PCT","2_OFFLINE_PCT","3_OFFLINE_PCT") IS NOT NULL THEN 
	              (COALESCE("0_OFFLINE_PCT",0) + COALESCE("1_OFFLINE_PCT",0) + COALESCE("2_OFFLINE_PCT",0) + COALESCE("3_OFFLINE_PCT",0))/
	              (CASE WHEN "0_OFFLINE_PCT" IS NULL THEN 0 ELSE 1 END + 
	               CASE WHEN "1_OFFLINE_PCT" IS NULL THEN 0 ELSE 1 END + 
	               CASE WHEN "2_OFFLINE_PCT" IS NULL THEN 0 ELSE 1 END + 
	               CASE WHEN "3_OFFLINE_PCT" IS NULL THEN 0 ELSE 1 END 
	               )
	        ELSE NULL
	        END AS OFFLINE_PCT,
	        -----------
	        CASE
	        WHEN COALESCE("0_PCT_TRAFFIC_DMH_UP","1_PCT_TRAFFIC_DMH_UP","2_PCT_TRAFFIC_DMH_UP","3_PCT_TRAFFIC_DMH_UP") IS NOT NULL THEN 
	              (COALESCE("0_PCT_TRAFFIC_DMH_UP",0) + COALESCE("1_PCT_TRAFFIC_DMH_UP",0) + COALESCE("2_PCT_TRAFFIC_DMH_UP",0) + COALESCE("3_PCT_TRAFFIC_DMH_UP",0))/
	              (CASE WHEN "0_PCT_TRAFFIC_DMH_UP" IS NULL THEN 0 ELSE 1 END + 
	               CASE WHEN "1_PCT_TRAFFIC_DMH_UP" IS NULL THEN 0 ELSE 1 END + 
	               CASE WHEN "2_PCT_TRAFFIC_DMH_UP" IS NULL THEN 0 ELSE 1 END + 
	               CASE WHEN "3_PCT_TRAFFIC_DMH_UP" IS NULL THEN 0 ELSE 1 END 
	               )
	        ELSE NULL
	        END AS PCT_TRAFFIC_DMH_UP,
	        --
	        CASE
	        WHEN COALESCE("0_MISS_PCT_TRAFFIC_DMH_UP","1_MISS_PCT_TRAFFIC_DMH_UP","2_MISS_PCT_TRAFFIC_DMH_UP","3_MISS_PCT_TRAFFIC_DMH_UP") IS NOT NULL THEN 
	              (COALESCE("0_MISS_PCT_TRAFFIC_DMH_UP",0) + COALESCE("1_MISS_PCT_TRAFFIC_DMH_UP",0) + COALESCE("2_MISS_PCT_TRAFFIC_DMH_UP",0) + COALESCE("3_MISS_PCT_TRAFFIC_DMH_UP",0))/
	              (CASE WHEN "0_MISS_PCT_TRAFFIC_DMH_UP" IS NULL THEN 0 ELSE 1 END + 
	               CASE WHEN "1_MISS_PCT_TRAFFIC_DMH_UP" IS NULL THEN 0 ELSE 1 END + 
	               CASE WHEN "2_MISS_PCT_TRAFFIC_DMH_UP" IS NULL THEN 0 ELSE 1 END + 
	               CASE WHEN "3_MISS_PCT_TRAFFIC_DMH_UP" IS NULL THEN 0 ELSE 1 END 
	               )
	        ELSE NULL
	        END AS MISS_PCT_TRAFFIC_DMH_UP,
	        -----------
	        CASE
	        WHEN COALESCE("0_PCT_TRAFFIC_SDMH_UP","1_PCT_TRAFFIC_SDMH_UP","2_PCT_TRAFFIC_SDMH_UP","3_PCT_TRAFFIC_SDMH_UP") IS NOT NULL THEN 
	              (COALESCE("0_PCT_TRAFFIC_SDMH_UP",0) + COALESCE("1_PCT_TRAFFIC_SDMH_UP",0) + COALESCE("2_PCT_TRAFFIC_SDMH_UP",0) + COALESCE("3_PCT_TRAFFIC_SDMH_UP",0))/
	              (CASE WHEN "0_PCT_TRAFFIC_SDMH_UP" IS NULL THEN 0 ELSE 1 END + 
	               CASE WHEN "1_PCT_TRAFFIC_SDMH_UP" IS NULL THEN 0 ELSE 1 END + 
	               CASE WHEN "2_PCT_TRAFFIC_SDMH_UP" IS NULL THEN 0 ELSE 1 END + 
	               CASE WHEN "3_PCT_TRAFFIC_SDMH_UP" IS NULL THEN 0 ELSE 1 END 
	               )
	        ELSE NULL
	        END AS PCT_TRAFFIC_SDMH_UP,
	        --
	        CASE
	        WHEN COALESCE("0_MISS_PCT_TRAFFIC_SDMH_UP","1_MISS_PCT_TRAFFIC_SDMH_UP","2_MISS_PCT_TRAFFIC_SDMH_UP","3_MISS_PCT_TRAFFIC_SDMH_UP") IS NOT NULL THEN 
	              (COALESCE("0_MISS_PCT_TRAFFIC_SDMH_UP",0) + COALESCE("1_MISS_PCT_TRAFFIC_SDMH_UP",0) + COALESCE("2_MISS_PCT_TRAFFIC_SDMH_UP",0) + COALESCE("3_MISS_PCT_TRAFFIC_SDMH_UP",0))/
	              (CASE WHEN "0_MISS_PCT_TRAFFIC_SDMH_UP" IS NULL THEN 0 ELSE 1 END + 
	               CASE WHEN "1_MISS_PCT_TRAFFIC_SDMH_UP" IS NULL THEN 0 ELSE 1 END + 
	               CASE WHEN "2_MISS_PCT_TRAFFIC_SDMH_UP" IS NULL THEN 0 ELSE 1 END + 
	               CASE WHEN "3_MISS_PCT_TRAFFIC_SDMH_UP" IS NULL THEN 0 ELSE 1 END 
	               )
	        ELSE NULL
	        END AS MISS_PCT_TRAFFIC_SDMH_UP,
	        -----------
	        CASE
	        WHEN COALESCE("0_RXPOWER_DN","1_RXPOWER_DN","2_RXPOWER_DN","3_RXPOWER_DN") IS NOT NULL THEN 
	              (COALESCE("0_RXPOWER_DN",0) + COALESCE("1_RXPOWER_DN",0) + COALESCE("2_RXPOWER_DN",0) + COALESCE("3_RXPOWER_DN",0))/
	              (CASE WHEN "0_RXPOWER_DN" IS NULL THEN 0 ELSE 1 END + 
	               CASE WHEN "1_RXPOWER_DN" IS NULL THEN 0 ELSE 1 END + 
	               CASE WHEN "2_RXPOWER_DN" IS NULL THEN 0 ELSE 1 END + 
	               CASE WHEN "3_RXPOWER_DN" IS NULL THEN 0 ELSE 1 END 
	               )
	        ELSE NULL
	        END AS RX_DN,
	        --
	        CASE
	        WHEN COALESCE("0_MISS_RXPOWER_DN","1_MISS_RXPOWER_DN","2_MISS_RXPOWER_DN","3_MISS_RXPOWER_DN") IS NOT NULL THEN 
	              (COALESCE("0_MISS_RXPOWER_DN",0) + COALESCE("1_MISS_RXPOWER_DN",0) + COALESCE("2_MISS_RXPOWER_DN",0) + COALESCE("3_MISS_RXPOWER_DN",0))/
	              (CASE WHEN "0_MISS_RXPOWER_DN" IS NULL THEN 0 ELSE 1 END + 
	               CASE WHEN "1_MISS_RXPOWER_DN" IS NULL THEN 0 ELSE 1 END + 
	               CASE WHEN "2_MISS_RXPOWER_DN" IS NULL THEN 0 ELSE 1 END + 
	               CASE WHEN "3_MISS_RXPOWER_DN" IS NULL THEN 0 ELSE 1 END 
	               )
	        ELSE NULL
	        END AS MISS_RX_DN,
	        -----------
	        CASE
	        WHEN COALESCE("0_RXPOWER_UP","1_RXPOWER_UP","2_RXPOWER_UP","3_RXPOWER_UP") IS NOT NULL THEN 
	              (COALESCE("0_RXPOWER_UP",0) + COALESCE("1_RXPOWER_UP",0) + COALESCE("2_RXPOWER_UP",0) + COALESCE("3_RXPOWER_UP",0))/
	              (CASE WHEN "0_RXPOWER_UP" IS NULL THEN 0 ELSE 1 END + 
	               CASE WHEN "1_RXPOWER_UP" IS NULL THEN 0 ELSE 1 END + 
	               CASE WHEN "2_RXPOWER_UP" IS NULL THEN 0 ELSE 1 END + 
	               CASE WHEN "3_RXPOWER_UP" IS NULL THEN 0 ELSE 1 END 
	               )
	        ELSE NULL
	        END AS RX_UP,
	        --
	        CASE
	        WHEN COALESCE("0_MISS_RXPOWER_UP","1_MISS_RXPOWER_UP","2_MISS_RXPOWER_UP","3_MISS_RXPOWER_UP") IS NOT NULL THEN 
	              (COALESCE("0_MISS_RXPOWER_UP",0) + COALESCE("1_MISS_RXPOWER_UP",0) + COALESCE("2_MISS_RXPOWER_UP",0) + COALESCE("3_MISS_RXPOWER_UP",0))/
	              (CASE WHEN "0_MISS_RXPOWER_UP" IS NULL THEN 0 ELSE 1 END + 
	               CASE WHEN "1_MISS_RXPOWER_UP" IS NULL THEN 0 ELSE 1 END + 
	               CASE WHEN "2_MISS_RXPOWER_UP" IS NULL THEN 0 ELSE 1 END + 
	               CASE WHEN "3_MISS_RXPOWER_UP" IS NULL THEN 0 ELSE 1 END 
	               )
	        ELSE NULL
	        END AS MISS_RX_UP,
	        -----------
	        CASE
	        WHEN COALESCE("0_SNR_DN","1_SNR_DN","2_SNR_DN","3_SNR_DN") IS NOT NULL THEN 
	              (COALESCE("0_SNR_DN",0) + COALESCE("1_SNR_DN",0) + COALESCE("2_SNR_DN",0) + COALESCE("3_SNR_DN",0))/
	              (CASE WHEN "0_SNR_DN" IS NULL THEN 0 ELSE 1 END + 
	               CASE WHEN "1_SNR_DN" IS NULL THEN 0 ELSE 1 END + 
	               CASE WHEN "2_SNR_DN" IS NULL THEN 0 ELSE 1 END + 
	               CASE WHEN "3_SNR_DN" IS NULL THEN 0 ELSE 1 END 
	               )
	        ELSE NULL
	        END AS SNR_DN,
	        --
	        CASE
	        WHEN COALESCE("0_MISS_SNR_DN","1_MISS_SNR_DN","2_MISS_SNR_DN","3_MISS_SNR_DN") IS NOT NULL THEN 
	              (COALESCE("0_MISS_SNR_DN",0) + COALESCE("1_MISS_SNR_DN",0) + COALESCE("2_MISS_SNR_DN",0) + COALESCE("3_MISS_SNR_DN",0))/
	              (CASE WHEN "0_MISS_SNR_DN" IS NULL THEN 0 ELSE 1 END + 
	               CASE WHEN "1_MISS_SNR_DN" IS NULL THEN 0 ELSE 1 END + 
	               CASE WHEN "2_MISS_SNR_DN" IS NULL THEN 0 ELSE 1 END + 
	               CASE WHEN "3_MISS_SNR_DN" IS NULL THEN 0 ELSE 1 END 
	               )
	        ELSE NULL
	        END AS MISS_SNR_DN,
	        -----------
	        CASE
	        WHEN COALESCE("0_SNR_UP","1_SNR_UP","2_SNR_UP","3_SNR_UP") IS NOT NULL THEN 
	              (COALESCE("0_SNR_UP",0) + COALESCE("1_SNR_UP",0) + COALESCE("2_SNR_UP",0) + COALESCE("3_SNR_UP",0))/
	              (CASE WHEN "0_SNR_UP" IS NULL THEN 0 ELSE 1 END + 
	               CASE WHEN "1_SNR_UP" IS NULL THEN 0 ELSE 1 END + 
	               CASE WHEN "2_SNR_UP" IS NULL THEN 0 ELSE 1 END + 
	               CASE WHEN "3_SNR_UP" IS NULL THEN 0 ELSE 1 END 
	               )
	        ELSE NULL
	        END AS SNR_UP,
	        --
	        CASE
	        WHEN COALESCE("0_MISS_SNR_UP","1_MISS_SNR_UP","2_MISS_SNR_UP","3_MISS_SNR_UP") IS NOT NULL THEN 
	              (COALESCE("0_MISS_SNR_UP",0) + COALESCE("1_MISS_SNR_UP",0) + COALESCE("2_MISS_SNR_UP",0) + COALESCE("3_MISS_SNR_UP",0))/
	              (CASE WHEN "0_MISS_SNR_UP" IS NULL THEN 0 ELSE 1 END + 
	               CASE WHEN "1_MISS_SNR_UP" IS NULL THEN 0 ELSE 1 END + 
	               CASE WHEN "2_MISS_SNR_UP" IS NULL THEN 0 ELSE 1 END + 
	               CASE WHEN "3_MISS_SNR_UP" IS NULL THEN 0 ELSE 1 END 
	               )
	        ELSE NULL
	        END AS MISS_SNR_UP,
	        -----------
	        CASE
	        WHEN COALESCE("0_TXPOWER_UP","1_TXPOWER_UP","2_TXPOWER_UP","3_TXPOWER_UP") IS NOT NULL THEN 
	              (COALESCE("0_TXPOWER_UP",0) + COALESCE("1_TXPOWER_UP",0) + COALESCE("2_TXPOWER_UP",0) + COALESCE("3_TXPOWER_UP",0))/
	              (CASE WHEN "0_TXPOWER_UP" IS NULL THEN 0 ELSE 1 END + 
	               CASE WHEN "1_TXPOWER_UP" IS NULL THEN 0 ELSE 1 END + 
	               CASE WHEN "2_TXPOWER_UP" IS NULL THEN 0 ELSE 1 END + 
	               CASE WHEN "3_TXPOWER_UP" IS NULL THEN 0 ELSE 1 END 
	               )
	        ELSE NULL
	        END AS TX_UP,
	        --
	        CASE
	        WHEN COALESCE("0_MISS_TXPOWER_UP","1_MISS_TXPOWER_UP","2_MISS_TXPOWER_UP","3_MISS_TXPOWER_UP") IS NOT NULL THEN 
	              (COALESCE("0_MISS_TXPOWER_UP",0) + COALESCE("1_MISS_TXPOWER_UP",0) + COALESCE("2_MISS_TXPOWER_UP",0) + COALESCE("3_MISS_TXPOWER_UP",0))/
	              (CASE WHEN "0_MISS_TXPOWER_UP" IS NULL THEN 0 ELSE 1 END + 
	               CASE WHEN "1_MISS_TXPOWER_UP" IS NULL THEN 0 ELSE 1 END + 
	               CASE WHEN "2_MISS_TXPOWER_UP" IS NULL THEN 0 ELSE 1 END + 
	               CASE WHEN "3_MISS_TXPOWER_UP" IS NULL THEN 0 ELSE 1 END 
	               )
	        ELSE NULL
	        END AS MISS_TX_UP,
	        -----------
	        ---
	        ----- CMTS_UP
	        ---
	        CASE
	        WHEN COALESCE("0_CMTS_RX_UP","1_CMTS_RX_UP","2_CMTS_RX_UP","3_CMTS_RX_UP") IS NOT NULL THEN 
	              (COALESCE("0_CMTS_RX_UP",0) + COALESCE("1_CMTS_RX_UP",0) + COALESCE("2_CMTS_RX_UP",0) + COALESCE("3_CMTS_RX_UP",0))/
	              (CASE WHEN "0_CMTS_RX_UP" IS NULL THEN 0 ELSE 1 END + 
	               CASE WHEN "1_CMTS_RX_UP" IS NULL THEN 0 ELSE 1 END + 
	               CASE WHEN "2_CMTS_RX_UP" IS NULL THEN 0 ELSE 1 END + 
	               CASE WHEN "3_CMTS_RX_UP" IS NULL THEN 0 ELSE 1 END 
	               )
	        ELSE NULL
	        END AS CMTS_RX_UP,
	        --
	        CASE
	        WHEN COALESCE("0_CMTS_TX_UP","1_CMTS_TX_UP","2_CMTS_TX_UP","3_CMTS_TX_UP") IS NOT NULL THEN 
	              (COALESCE("0_CMTS_TX_UP",0) + COALESCE("1_CMTS_TX_UP",0) + COALESCE("2_CMTS_TX_UP",0) + COALESCE("3_CMTS_TX_UP",0))/
	              (CASE WHEN "0_CMTS_TX_UP" IS NULL THEN 0 ELSE 1 END + 
	               CASE WHEN "1_CMTS_TX_UP" IS NULL THEN 0 ELSE 1 END + 
	               CASE WHEN "2_CMTS_TX_UP" IS NULL THEN 0 ELSE 1 END + 
	               CASE WHEN "3_CMTS_TX_UP" IS NULL THEN 0 ELSE 1 END 
	               )
	        ELSE NULL
	        END AS CMTS_TX_UP,
	        --
	        CASE
	        WHEN COALESCE("0_CMTS_CER_UP","1_CMTS_CER_UP","2_CMTS_CER_UP","3_CMTS_CER_UP") IS NOT NULL THEN 
	              (COALESCE("0_CMTS_CER_UP",0) + COALESCE("1_CMTS_CER_UP",0) + COALESCE("2_CMTS_CER_UP",0) + COALESCE("3_CMTS_CER_UP",0))/
	              (CASE WHEN "0_CMTS_CER_UP" IS NULL THEN 0 ELSE 1 END + 
	               CASE WHEN "1_CMTS_CER_UP" IS NULL THEN 0 ELSE 1 END + 
	               CASE WHEN "2_CMTS_CER_UP" IS NULL THEN 0 ELSE 1 END + 
	               CASE WHEN "3_CMTS_CER_UP" IS NULL THEN 0 ELSE 1 END 
	               )
	        ELSE NULL
	        END AS CMTS_CER_UP,
	        --
	        CASE
	        WHEN COALESCE("0_CMTS_UTILIZATION_UP","1_CMTS_UTILIZATION_UP","2_CMTS_UTILIZATION_UP","3_CMTS_UTILIZATION_UP") IS NOT NULL THEN 
	              (COALESCE("0_CMTS_UTILIZATION_UP",0) + COALESCE("1_CMTS_UTILIZATION_UP",0) + COALESCE("2_CMTS_UTILIZATION_UP",0) + COALESCE("3_CMTS_UTILIZATION_UP",0))/
	              (CASE WHEN "0_CMTS_UTILIZATION_UP" IS NULL THEN 0 ELSE 1 END + 
	               CASE WHEN "1_CMTS_UTILIZATION_UP" IS NULL THEN 0 ELSE 1 END + 
	               CASE WHEN "2_CMTS_UTILIZATION_UP" IS NULL THEN 0 ELSE 1 END + 
	               CASE WHEN "3_CMTS_UTILIZATION_UP" IS NULL THEN 0 ELSE 1 END 
	               )
	        ELSE NULL
	        END AS CMTS_UTILIZATION_UP,
	        --
	        CASE
	        WHEN COALESCE("0_CMTS_MS_UTILIZATION_UP","1_CMTS_MS_UTILIZATION_UP","2_CMTS_MS_UTILIZATION_UP","3_CMTS_MS_UTILIZATION_UP") IS NOT NULL THEN 
	              (COALESCE("0_CMTS_MS_UTILIZATION_UP",0) + COALESCE("1_CMTS_MS_UTILIZATION_UP",0) + COALESCE("2_CMTS_MS_UTILIZATION_UP",0) + COALESCE("3_CMTS_MS_UTILIZATION_UP",0))/
	              (CASE WHEN "0_CMTS_MS_UTILIZATION_UP" IS NULL THEN 0 ELSE 1 END + 
	               CASE WHEN "1_CMTS_MS_UTILIZATION_UP" IS NULL THEN 0 ELSE 1 END + 
	               CASE WHEN "2_CMTS_MS_UTILIZATION_UP" IS NULL THEN 0 ELSE 1 END + 
	               CASE WHEN "3_CMTS_MS_UTILIZATION_UP" IS NULL THEN 0 ELSE 1 END 
	               )
	        ELSE NULL
	        END AS CMTS_MS_UTILIZATION_UP,
	        --
	        CASE
	        WHEN COALESCE("0_CMTS_F_MS_UTILIZATION_UP","1_CMTS_F_MS_UTILIZATION_UP","2_CMTS_F_MS_UTILIZATION_UP","3_CMTS_F_MS_UTILIZATION_UP") IS NOT NULL THEN 
	              (COALESCE("0_CMTS_F_MS_UTILIZATION_UP",0) + COALESCE("1_CMTS_F_MS_UTILIZATION_UP",0) + COALESCE("2_CMTS_F_MS_UTILIZATION_UP",0) + COALESCE("3_CMTS_F_MS_UTILIZATION_UP",0))/
	              (CASE WHEN "0_CMTS_F_MS_UTILIZATION_UP" IS NULL THEN 0 ELSE 1 END + 
	               CASE WHEN "1_CMTS_F_MS_UTILIZATION_UP" IS NULL THEN 0 ELSE 1 END + 
	               CASE WHEN "2_CMTS_F_MS_UTILIZATION_UP" IS NULL THEN 0 ELSE 1 END + 
	               CASE WHEN "3_CMTS_F_MS_UTILIZATION_UP" IS NULL THEN 0 ELSE 1 END 
	               )
	        ELSE NULL
	        END AS CMTS_F_MS_UTILIZATION_UP,
	        ---
	        ----- CMTS_DN
	        ---
	        CASE
	        WHEN COALESCE("0_CMTS_RX_DN","1_CMTS_RX_DN","2_CMTS_RX_DN","3_CMTS_RX_DN") IS NOT NULL THEN 
	              (COALESCE("0_CMTS_RX_DN",0) + COALESCE("1_CMTS_RX_DN",0) + COALESCE("2_CMTS_RX_DN",0) + COALESCE("3_CMTS_RX_DN",0))/
	              (CASE WHEN "0_CMTS_RX_DN" IS NULL THEN 0 ELSE 1 END + 
	               CASE WHEN "1_CMTS_RX_DN" IS NULL THEN 0 ELSE 1 END + 
	               CASE WHEN "2_CMTS_RX_DN" IS NULL THEN 0 ELSE 1 END + 
	               CASE WHEN "3_CMTS_RX_DN" IS NULL THEN 0 ELSE 1 END 
	               )
	        ELSE NULL
	        END AS CMTS_RX_DN,
	        --
	        CASE
	        WHEN COALESCE("0_CMTS_SNR_DN","1_CMTS_SNR_DN","2_CMTS_SNR_DN","3_CMTS_SNR_DN") IS NOT NULL THEN 
	              (COALESCE("0_CMTS_SNR_DN",0) + COALESCE("1_CMTS_SNR_DN",0) + COALESCE("2_CMTS_SNR_DN",0) + COALESCE("3_CMTS_SNR_DN",0))/
	              (CASE WHEN "0_CMTS_SNR_DN" IS NULL THEN 0 ELSE 1 END + 
	               CASE WHEN "1_CMTS_SNR_DN" IS NULL THEN 0 ELSE 1 END + 
	               CASE WHEN "2_CMTS_SNR_DN" IS NULL THEN 0 ELSE 1 END + 
	               CASE WHEN "3_CMTS_SNR_DN" IS NULL THEN 0 ELSE 1 END 
	               )
	        ELSE NULL
	        END AS CMTS_SNR_DN,
	        --
	        CASE
	        WHEN COALESCE("0_CMTS_CCER_DN","1_CMTS_CCER_DN","2_CMTS_CCER_DN","3_CMTS_CCER_DN") IS NOT NULL THEN 
	              (COALESCE("0_CMTS_CCER_DN",0) + COALESCE("1_CMTS_CCER_DN",0) + COALESCE("2_CMTS_CCER_DN",0) + COALESCE("3_CMTS_CCER_DN",0))/
	              (CASE WHEN "0_CMTS_CCER_DN" IS NULL THEN 0 ELSE 1 END + 
	               CASE WHEN "1_CMTS_CCER_DN" IS NULL THEN 0 ELSE 1 END + 
	               CASE WHEN "2_CMTS_CCER_DN" IS NULL THEN 0 ELSE 1 END + 
	               CASE WHEN "3_CMTS_CCER_DN" IS NULL THEN 0 ELSE 1 END 
	               )
	        ELSE NULL
	        END AS CMTS_CCER_DN,
	        --
	        CASE
	        WHEN COALESCE("0_CMTS_CER_DN","1_CMTS_CER_DN","2_CMTS_CER_DN","3_CMTS_CER_DN") IS NOT NULL THEN 
	              (COALESCE("0_CMTS_CER_DN",0) + COALESCE("1_CMTS_CER_DN",0) + COALESCE("2_CMTS_CER_DN",0) + COALESCE("3_CMTS_CER_DN",0))/
	              (CASE WHEN "0_CMTS_CER_DN" IS NULL THEN 0 ELSE 1 END + 
	               CASE WHEN "1_CMTS_CER_DN" IS NULL THEN 0 ELSE 1 END + 
	               CASE WHEN "2_CMTS_CER_DN" IS NULL THEN 0 ELSE 1 END + 
	               CASE WHEN "3_CMTS_CER_DN" IS NULL THEN 0 ELSE 1 END 
	               )
	        ELSE NULL
	        END AS CMTS_CER_DN,
	        --
	        CASE
	        WHEN COALESCE("0_CMTS_UTILIZATION_DN","1_CMTS_UTILIZATION_DN","2_CMTS_UTILIZATION_DN","3_CMTS_UTILIZATION_DN") IS NOT NULL THEN 
	              (COALESCE("0_CMTS_UTILIZATION_DN",0) + COALESCE("1_CMTS_UTILIZATION_DN",0) + COALESCE("2_CMTS_UTILIZATION_DN",0) + COALESCE("3_CMTS_UTILIZATION_DN",0))/
	              (CASE WHEN "0_CMTS_UTILIZATION_DN" IS NULL THEN 0 ELSE 1 END + 
	               CASE WHEN "1_CMTS_UTILIZATION_DN" IS NULL THEN 0 ELSE 1 END + 
	               CASE WHEN "2_CMTS_UTILIZATION_DN" IS NULL THEN 0 ELSE 1 END + 
	               CASE WHEN "3_CMTS_UTILIZATION_DN" IS NULL THEN 0 ELSE 1 END 
	               )
	        ELSE NULL
	        END AS CMTS_UTILIZATION_DN
	        FROM HUMOREAU.VECTOR
	        WHERE TRUNC(DAY_0) = ''' || TRUNC(day_to_consider) || '''
	   	)';
	END INITIALIZE_STEP_NO_DIFFS;

	------------------------------------------------------------------------------------------------

	/*
	Creates the DAILY_AVG_DIFFS table without filling it

	@params:
	- day_to_consider: the date that we wish to intepret as day 0 for that operation
	*/
	PROCEDURE INITIALIZE_STEP_WITH_DIFFS(day_to_consider IN DATE) AS
	BEGIN
		EXECUTE IMMEDIATE '
				-- WE ADD THE CONDITION WHERE 1=0 TO JUST CREATE EMPTY TABLE
				CREATE TABLE HUMOREAU.DAILY_AVG_DIFFS AS (
			        SELECT * FROM (
			            WITH DAILY_MARKED AS (
			                SELECT 
			                    CASE WHEN TRUNC(DAY_0) = ''' || TRUNC(day_to_consider) || ''' THEN 0 ELSE 1 END AS D,
			                    MAC, 
			                    OFFLINE_PCT,
			                    ----- CPE
			                    CER_DN,
			                    MISS_CER_DN,
			                    CER_UP,
			                    MISS_CER_UP,
			                    PCT_TRAFFIC_DMH_UP,
			                    MISS_PCT_TRAFFIC_DMH_UP,
			                    PCT_TRAFFIC_SDMH_UP,
			                    MISS_PCT_TRAFFIC_SDMH_UP,
			                    RX_DN,
			                    MISS_RX_DN,
			                    RX_UP,
			                    MISS_RX_UP,
			                    SNR_DN,
			                    MISS_SNR_DN,
			                    SNR_UP,
			                    MISS_SNR_UP,
			                    TX_UP,
			                    MISS_TX_UP,
			                    ----- CMTS_UP
			                    CMTS_RX_UP,
			                    CMTS_TX_UP,
			                    CMTS_CER_UP,
			                    CMTS_UTILIZATION_UP,
			                    CMTS_MS_UTILIZATION_UP,
			                    CMTS_F_MS_UTILIZATION_UP,
			                    ----- CMTS_DN
			                    CMTS_RX_DN,
			                    CMTS_SNR_DN,
			                    CMTS_CCER_DN,
			                    CMTS_CER_DN,
			                    CMTS_UTILIZATION_DN
			                FROM HUMOREAU.DAILY_AVG_DAY_0
			            ), PIVOTED AS (
			                SELECT * 
			                FROM DAILY_MARKED
			                PIVOT(
			                    MAX(OFFLINE_PCT) AS OFFLINE_PCT,
			                    ----- CPE
			                    MAX(CER_DN) AS CER_DN,
			                    MAX(MISS_CER_DN) AS MISS_CER_DN,
			                    MAX(CER_UP) AS CER_UP,
			                    MAX(MISS_CER_UP) AS MISS_CER_UP,
			                    MAX(PCT_TRAFFIC_DMH_UP) AS PCT_TRAFFIC_DMH_UP,
			                    MAX(MISS_PCT_TRAFFIC_DMH_UP) AS MISS_PCT_TRAFFIC_DMH_UP,
			                    MAX(PCT_TRAFFIC_SDMH_UP) AS PCT_TRAFFIC_SDMH_UP,
			                    MAX(MISS_PCT_TRAFFIC_SDMH_UP) AS MISS_PCT_TRAFFIC_SDMH_UP,
			                    MAX(RX_DN) AS RX_DN,
			                    MAX(MISS_RX_DN) AS MISS_RX_DN,
			                    MAX(RX_UP) AS RX_UP,
			                    MAX(MISS_RX_UP) AS MISS_RX_UP,
			                    MAX(SNR_DN) AS SNR_DN,
			                    MAX(MISS_SNR_DN) AS MISS_SNR_DN,
			                    MAX(SNR_UP) AS SNR_UP,
			                    MAX(MISS_SNR_UP) AS MISS_SNR_UP,
			                    MAX(TX_UP) AS TX_UP,
			                    MAX(MISS_TX_UP) AS MISS_TX_UP,
			                    ----- CMTS_UP
			                    MAX(CMTS_RX_UP) AS CMTS_RX_UP,
			                    MAX(CMTS_TX_UP) AS CMTS_TX_UP,
			                    MAX(CMTS_CER_UP) AS CMTS_CER_UP,
			                    MAX(CMTS_UTILIZATION_UP) AS CMTS_UTILIZATION_UP,
			                    MAX(CMTS_MS_UTILIZATION_UP) AS CMTS_MS_UTILIZATION_UP,
			                    MAX(CMTS_F_MS_UTILIZATION_UP) AS CMTS_F_MS_UTILIZATION_UP,
			                    ----- CMTS_DN
			                    MAX(CMTS_RX_DN) AS CMTS_RX_DN,
			                    MAX(CMTS_SNR_DN) AS CMTS_SNR_DN,
			                    MAX(CMTS_CCER_DN) AS CMTS_CCER_DN,
			                    MAX(CMTS_CER_DN) AS CMTS_CER_DN,
			                    MAX(CMTS_UTILIZATION_DN) AS CMTS_UTILIZATION_DN
			                    FOR D IN (0,1)
			                    )
			            )
			            SELECT 
			                -- we can now compute the differences
			                MAC, 
			               to_date(''' || TRUNC(day_to_consider) || ''',''DD/MM/YYYY HH24:MI:SS'') AS DAY_0,
			                ---
			                "1_OFFLINE_PCT"                                             AS OFFLINE_PCT,
			                "0_CER_DN"                  - "1_CER_DN"                    AS CER_DN,
			                "0_MISS_CER_DN"                                             AS MISS_CER_DN,
			                "0_CER_UP"                  - "1_CER_UP"                    AS CER_UP,
			                "0_MISS_CER_UP"                                             AS MISS_CER_UP,
			                "0_PCT_TRAFFIC_DMH_UP"      - "1_PCT_TRAFFIC_DMH_UP"        AS PCT_TRAFFIC_DMH_UP,
			                "0_MISS_PCT_TRAFFIC_DMH_UP"                                 AS MISS_PCT_TRAFFIC_DMH_UP,
			                "0_PCT_TRAFFIC_SDMH_UP"     - "1_PCT_TRAFFIC_SDMH_UP"       AS PCT_TRAFFIC_SDMH_UP,
			                "0_MISS_PCT_TRAFFIC_SDMH_UP"                                AS MISS_PCT_TRAFFIC_SDMH_UP,
			                "0_RX_DN"                   - "1_RX_DN"                     AS RX_DN,
			                "0_MISS_RX_DN"                                              AS MISS_RX_DN,
			                "0_RX_UP"                   - "1_RX_UP"                     AS RX_UP,
			                "0_MISS_RX_UP"                                              AS MISS_RX_UP,
			                "0_SNR_DN"                  - "1_SNR_DN"                    AS SNR_DN,
			                "0_MISS_SNR_DN"                                             AS MISS_SNR_DN,
			                "0_SNR_UP"                  - "1_SNR_UP"                    AS SNR_UP,
			                "0_MISS_SNR_UP"                                             AS MISS_SNR_UP,
			                "0_TX_UP"                   - "1_TX_UP"                     AS TX_UP,
			                "0_MISS_TX_UP"                                              AS MISS_TX_UP,
			                "0_CMTS_RX_UP"              - "1_CMTS_RX_UP"                AS CMTS_RX_UP,
			                "0_CMTS_TX_UP"              - "1_CMTS_TX_UP"                AS CMTS_TX_UP,
			                "0_CMTS_CER_UP"             - "1_CMTS_CER_UP"               AS CMTS_CER_UP,
			                "0_CMTS_UTILIZATION_UP"     - "1_CMTS_UTILIZATION_UP"       AS CMTS_UTILIZATION_UP,
			                "0_CMTS_MS_UTILIZATION_UP"  - "1_CMTS_MS_UTILIZATION_UP"    AS CMTS_MS_UTILIZATION_UP,
			                "0_CMTS_F_MS_UTILIZATION_UP"- "1_CMTS_F_MS_UTILIZATION_UP"  AS CMTS_F_MS_UTILIZATION_UP,
			                "0_CMTS_RX_DN"              - "1_CMTS_RX_DN"                AS CMTS_RX_DN,
			                "0_CMTS_SNR_DN"             - "1_CMTS_SNR_DN"               AS CMTS_SNR_DN,
			                "0_CMTS_CCER_DN"            - "1_CMTS_CCER_DN"              AS CMTS_CCER_DN,
			                "0_CMTS_CER_DN"             - "1_CMTS_CER_DN"               AS CMTS_CER_DN,
			                "0_CMTS_UTILIZATION_DN"     - "1_CMTS_UTILIZATION_DN"       AS CMTS_UTILIZATION_DN
			            FROM PIVOTED
			    	) WHERE 1 = 0
				)';
	END;

	------------------------------------------------------------------------------------------------
	
	/*
	Creates all intermediary tables necessaery to collect VIA data. 

	@params:
	- day_to_consider: the date that we wish to intepret as day 0 for that operation
	*/
	PROCEDURE INITIALIZE_STEP_VIA(day_to_consider IN DATE) AS
	BEGIN
		EXECUTE IMMEDIATE '
		CREATE TABLE HUMOREAU.SUBSET_MILESTONES AS (
			-- We create the subset of milestones that concern day_1 and internet flows to reduce the complexity of our joins later on.
			SELECT * 
			FROM SCO.REP_VIA_MILESTONE_V 
			WHERE 	1 = 0 
					AND TRUNC(CALENDAR_DATE) = ''' || TRUNC(day_to_consider) ||'''
					AND UPPER(PROCESS_FLOW) = ''INTERNET''
		)';

		EXECUTE IMMEDIATE '
		CREATE TABLE HUMOREAU.FULL_FLOWS AS (
			-- We first find the successful events the day after our day_0
			SELECT
			    SESSION_ID, 
			    FLOW_ID,
                ''FULL'' AS TAG
			FROM HUMOREAU.SUBSET_MILESTONES 
			WHERE 	1=0 AND (CNT_INTERACT_SENT > 0 OR CNT_CASE_SENT > 0)
		)';
		
		EXECUTE IMMEDIATE '
			CREATE TABLE HUMOREAU.FULL_FLOWS_TAGGED AS (
		        -- tag the flows with a successful event
		        SELECT  T.TAG,
		                M.*
		        FROM HUMOREAU.SUBSET_MILESTONES M FULL OUTER JOIN HUMOREAU.FULL_FLOWS T 
		            ON M.SESSION_ID = T.SESSION_ID AND M.FLOW_ID = T.FLOW_ID
		        WHERE 1=0 
		    )';

		EXECUTE IMMEDIATE '
		CREATE TABLE HUMOREAU.FLAGGED_EVENT AS (
	        -- find the minimum event NO of these full flows (to identify the milestone of interest)
	        SELECT SESSION_ID, MIN(EVENT_NO) AS EVENT_NO_MILESTONE, 1 AS STARTING_MILESTONE
	        FROM HUMOREAU.FULL_FLOWS_TAGGED
	        WHERE 1=0 AND TAG = ''FULL''
	        GROUP BY SESSION_ID
	    )';

		EXECUTE IMMEDIATE '
			CREATE TABLE HUMOREAU.TAGGED_MILESTONE AS (
		        SELECT * FROM (
		            -- In the original table we set the "STARTING_MILESTONE" flow for the event 
		            -- that has the starting milestone (the lowest event_no of a full flow)
		            SELECT F.STARTING_MILESTONE, R.* 
		            FROM HUMOREAU.SUBSET_MILESTONES R FULL OUTER JOIN HUMOREAU.FLAGGED_EVENT F 
		                ON R.SESSION_ID = F.SESSION_ID AND R.EVENT_NO = F.EVENT_NO_MILESTONE
		            ORDER BY R.SESSION_ID, R.EVENT_NO
		            ) WHERE 1=0
			)';
		EXECUTE IMMEDIATE '
			CREATE TABLE HUMOREAU.MILESTONES AS (
				-- We can finally extract the milestones that correspond to each event_NO 
				-- and session_id and a process_flow.
				SELECT  START_TIME AS MILESTONE_START_T, 
				        EVENT_NO,
				        SESSION_ID, 
				        MILESTONE_NAME, 
				        PROCESS_FLOW
				FROM HUMOREAU.TAGGED_MILESTONE
				WHERE 1=0 AND STARTING_MILESTONE = 1 
			)';

		EXECUTE IMMEDIATE '
		CREATE TABLE HUMOREAU.VIA_DETAILS AS (
		    SELECT * FROM (
		    WITH SESSIONID_2_CASEID AS(
		        -- we find the case that corresponds to each session
		        SELECT /*+ PARALLEL(16) */ 
		            JOURNAL_ID AS SESSION_ID,
		            EMP_ID, 
		            CASE_ID 
		        FROM SCO.REP_VIA_SESSION_V
		    ), CUSTOMER_DETAILS AS (
		        -- Get the customer details of a particular case.
		        SELECT /*+ PARALLEL(16) */ 
		            ID AS CASE_ID, 
		            CUSTOMER_ID, 
		            CUST_FIRST_NAME, 
		            CUST_LAST_NAME, 
		            DURATION AS CASE_DURATION, 
		            START_TIME AS CASE_START_T 
		        FROM SCO.REP_VIA_CASE_V
		    )
		    -- And we can finally join all of it in one table.
		    select  
		    		/*+ PARALLEL(16) */ 
		    		S.CASE_ID,
		    		S.EMP_ID, 
		            CASE_START_T, 
		            M.SESSION_ID, 
		            M.PROCESS_FLOW, 
		            M.MILESTONE_NAME,
		            CUSTOMER_ID AS CLY_ACCOUNT_NUMBER,
                    to_date(''' || TRUNC(day_to_consider) || ''',''DD/MM/YYYY HH24:MI:SS'') AS "DAY_0"
		    from HUMOREAU.MILESTONES M 
		            INNER JOIN SESSIONID_2_CASEID S 
		                ON M.SESSION_ID = S.SESSION_ID
		            INNER JOIN CUSTOMER_DETAILS C 
		                ON C.CASE_ID = S.CASE_ID
		        ) WHERE 1=0
		)';

		EXECUTE IMMEDIATE '
		CREATE TABLE HUMOREAU.SUCCESS_FLAGS AS(
		    SELECT /*+ PARALLEL(16) */ 
		        INTERACTION_ID,
		        ACCOUNT_NUMBER,
		        CREATE_DATE,
		        EMP_ID,
		        FLG_FTR_0D_FLG,
		        FLG_FTR_7D_FLG
		    FROM SCO.REP_CLY_INTERACTION_V
		    WHERE 1=0 AND TRUNC(CREATE_DATE) = '''|| TRUNC(day_to_consider) ||'''
		)';

		EXECUTE IMMEDIATE '
			CREATE TABLE HUMOREAU.TENTATIVE_MATCH AS (
			    -- Could contain duplicates
			    SELECT  CLY_ACCOUNT_NUMBER,
			        INTERACTION_ID,
			        SESSION_ID,
			        CASE_START_T,
			        T2.CREATE_DATE AS INTERACTION_CREATE_DATE,
			        FLG_FTR_0D_FLG,
			        FLG_FTR_7D_FLG
			    FROM HUMOREAU.VIA_DETAILS T1 JOIN humoreau.SUCCESS_FLAGS T2
			        -- We join the two based on the assumption that it is very unlikely that the same 
			        -- employee handles the same account_number on the same day in VIA.
			        ON TRUNC(T1.CASE_START_T) = TRUNC(T2.CREATE_DATE) 
			            AND T1.CASE_START_T <= T2.CREATE_DATE
			            AND T1.CLY_ACCOUNT_NUMBER = T2.ACCOUNT_NUMBER
			            AND T1.EMP_ID = T2.EMP_ID
			            -- TO AVOID MATCHES WITH NULLS.
			            AND (   T1.EMP_ID IS NOT NULL 
			                    AND T2.EMP_ID IS NOT NULL 
			                    AND T1.CASE_START_T IS NOT NULL
			                    AND T2.CREATE_DATE IS NOT NULL 
			                    AND T1.CLY_ACCOUNT_NUMBER IS NOT NULL 
			                    AND T2.ACCOUNT_NUMBER IS NOT NULL )
			    WHERE 1=0
			)';
		EXECUTE IMMEDIATE '
			CREATE TABLE HUMOREAU.FLAGGED_SUCESSFUL_SESSIONS AS (
				-- to get rid of the duplicated we take the first match, as it is the one that is the 
				-- closest to gthe VIA date (we are quite tolerant to errors here because if we use 
				-- this to estimate whether an interaction was successful or not which anyway is an estimate by itself)
			    SELECT * FROM (
			        WITH NUMBERED_DUPLICATES AS ( 
			            SELECT T.*, ROW_NUMBER() OVER (PARTITION BY SESSION_ID ORDER BY INTERACTION_CREATE_DATE ASC) RN
			            FROM TENTATIVE_MATCH T
			        )
			        SELECT 
			            SESSION_ID,
			            FLG_FTR_0D_FLG,
			            FLG_FTR_7D_FLG
			        FROM NUMBERED_DUPLICATES
			        WHERE RN = 1
			    ) WHERE 1=0
			)';

		EXECUTE IMMEDIATE '
		CREATE TABLE HUMOREAU.VIA_MACS_NOT_CONFIRMED AS (
		    SELECT * FROM (
		    	WITH CLY_ID_2_MACS AS (
				    SELECT   
				     REPLACE(NODE_KEY,'':'') AS MAC,
				     CLY_ACCT_NUMBER as CLY_ACCOUNT_NUMBER
				    FROM DM_TOPO_CH.ETL_STG_NW_TOPO_NODES 
				    WHERE topo_node_type_id = 55
				)
				SELECT 
					CASE_ID,
					SESSION_ID,
					EMP_ID,
					V.CLY_ACCOUNT_NUMBER,
					MAC, 
		            CASE_START_T, 
		            PROCESS_FLOW, 
		            MILESTONE_NAME,
		            DAY_0
				FROM VIA_DETAILS V JOIN CLY_ID_2_MACS C 
					ON V.CLY_ACCOUNT_NUMBER = C.CLY_ACCOUNT_NUMBER
			) WHERE 1=0
		)';

		EXECUTE IMMEDIATE '
			CREATE TABLE HUMOREAU.FTR_FLAGGED_MACS AS (
				SELECT 
				    V.*,
				    FLG_FTR_7D_FLG,
				    FLG_FTR_0D_FLG
				FROM HUMOREAU.VIA_MACS_NOT_CONFIRMED V INNER JOIN FLAGGED_SUCESSFUL_SESSIONS S
				    ON V.SESSION_ID = S.SESSION_ID
				WHERE 1=0
			)';
		EXECUTE IMMEDIATE '
	 		CREATE TABLE HUMOREAU.VIA_MACS AS (
			    SELECT * FROM HUMOREAU.FTR_FLAGGED_MACS
			   	WHERE 1=0 AND FLG_FTR_7D_FLG = 1
			)';
	END;

	 /*
    Creates LOG_TABLE
    */
    PROCEDURE INIT_LOGTABLE AS 
    BEGIN
        EXECUTE IMMEDIATE 'CREATE TABLE HUMOREAU.LOG_TABLE
            (
            LOG_DATE        TIMESTAMP (6)   DEFAULT systimestamp,
            IS_ERROR        NUMBER,
            LOG_TEXT        VARCHAR2(4000),
            IS_DONE         NUMBER
            )';
    END INIT_LOGTABLE;
END CPE_FAIL_DETECTION_POC_INIT;
/