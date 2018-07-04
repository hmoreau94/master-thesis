/*
AUTHOR:
	Hugo Moreau - hugo.moreau@epfl.ch
	Msc in Communication Systems
	Minor in Management and Technological Entrepreneurship

DESCRIPTION:
	This package contains all the necessary procedure to be able to perform Data Collection for CPE Failure prediction. 
	It was designed for the database architecture of UPC Cablecom in Feb-Aug 2018 and the following description will
	make the assumption of such architecture. This particular set of procedure is supposed to run on DMP and will fill 
	in temprorary windows that shall be unloaded by CPE_FAIL_DETECTION_POC.Main_Proc in DMT (package described by package_DMT.sql).
	The package assumes that it is ran every day as it works with a rolling window mechanism.

ERROR_LOGGING:
	The Main_proc is using a Logging table with autonomous transactions so that even in the event when the job would fail 
	one can still check HUMOREAU.LOG_TABLE to understand what has happened.

INITIALIZATION
	This package will not compile in the absence of the intermediary tables it uses. Therefore in order to initialize 
	the space of the database one should run:
	1. CPE_FAIL_DETECTION_POC_INIT.initialize;
	2. CPE_FAIL_DETECTION_POC.init;
	3. CPE_FAILURE_ERROR_LOG.reset;

	Then intermediary tables will have been created and the rolling window mechanism initialized allowing the execution 
	of this package.

EXECUTION:
	BEGIN
	    HUMOREAU.CPE_FAIL_DETECTION_POC.MAIN_PROC;
	END;
*/


CREATE OR REPLACE PACKAGE CPE_FAIL_DETECTION_POC 
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
    v_max_date						DATE;
    n_rows							number;
    v_sql           				varchar2(100);
    error_message           		varchar2(100);
    num_errors                      NUMBER;


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

    -- Exception
    INVALID_DAY_0					EXCEPTION;
    EMPTY_INTERMEDIATE_TABLE		EXCEPTION;
    INVALID_STATE					EXCEPTION;
    FAILED_LAST_RUN					EXCEPTION;
    
    /*
    Then the procedures
    */
    -- UTILS
    PROCEDURE PRINT_EXEC_TIME(should_print IN BOOLEAN, start_time IN number, end_time IN number, prefix VARCHAR2);
    PROCEDURE PRINT_DEBUG_INFO(should_print IN BOOLEAN, TABLE_NAME IN VARCHAR2, cannot_be_empty IN BOOLEAN);
    PROCEDURE CLEAN_UP(complete IN BOOLEAN);
	PROCEDURE ERROR_HANDLING;
	PROCEDURE CHECK_LAST_RUN;
	PROCEDURE CHECK_STATE;
	PROCEDURE DELETE_ALL_TABLES;
	PROCEDURE INIT;
	PROCEDURE INITIALIZE_STEP_INSERTS(day_to_consider IN DATE);

    -- MAIN     
    PROCEDURE MAIN_PROC;
    
    -- CMTS-DN
    PROCEDURE CMTS_DN_EXTRACT(day_to_consider  IN DATE);
    PROCEDURE CMTS_DN_SIX_H_AVG(day_to_consider  IN DATE);
    PROCEDURE CMTS_DN_PIVOT_SIX_H(day_to_consider  IN DATE);
    PROCEDURE CMTS_DN_ADD_SVGP(day_to_consider  IN DATE);
    PROCEDURE CMTS_DN_BUILD_VECTOR(day_to_consider  IN DATE);
    
    -- CMTS-UP
    PROCEDURE CMTS_UP_EXTRACT(day_to_consider  IN DATE);
    PROCEDURE CMTS_UP_SIX_H_AVG(day_to_consider  IN DATE);
    PROCEDURE CMTS_UP_PIVOT_SIX_H(day_to_consider  IN DATE);
    PROCEDURE CMTS_UP_ADD_SVGP(day_to_consider  IN DATE);
    PROCEDURE CMTS_UP_BUILD_VECTOR(day_to_consider  IN DATE);
    
    -- CPE
    PROCEDURE STATIC_INFOS(day_to_consider  IN DATE);
    PROCEDURE OFFLINE_CPE(day_to_consider  IN DATE);
    PROCEDURE COMPUTE_UNAVAILABILITY_PCT(day_to_consider  IN DATE);
    PROCEDURE COMPUTE_CENTILES(day_to_consider  IN DATE);
    PROCEDURE DETECT_OUTLIERS(day_to_consider  IN DATE);
    PROCEDURE EXTRACT_ENRICH_SAA(day_to_consider  IN DATE);
    PROCEDURE COMPUTE_SVG_AVG(day_to_consider  IN DATE);
    PROCEDURE STANDARDISE_BY_SVG(day_to_consider  IN DATE);
    PROCEDURE CPE_SIX_H_AVG(day_to_consider  IN DATE);
    PROCEDURE CPE_PIVOT_SIX_H(day_to_consider  IN DATE);
    
    -- DUMP TO CREATE 5D VECTOR
    PROCEDURE VECTOR_1D(day_to_consider  IN DATE);
    PROCEDURE DAILY_AVG_DAY_0(day_to_consider  IN DATE);
    PROCEDURE DAILY_DIFFS(day_to_consider  IN DATE);

    -- VIA
    PROCEDURE SUBSET_MILESTONES;
    PROCEDURE DETECT_FULL_FLOWS;
    PROCEDURE TAG_FULL_FLOWS;
    PROCEDURE FLAG_EVENTS;
    PROCEDURE TAG_MILESTONES;
    PROCEDURE EXTRACT_MILESTONES;
    PROCEDURE EXTRACT_VIA_DETAILS;
    PROCEDURE TAG_SUCCESS;
    PROCEDURE TENTATIVE_MATCH;
    PROCEDURE FLAG_SUCCESSFUL_EVENTS;
    PROCEDURE GET_VIA_MACS;
    PROCEDURE FLAG_MACS;
    PROCEDURE BUILD_VIA_MACS;
END CPE_FAIL_DETECTION_POC;
/

CREATE OR REPLACE PACKAGE BODY CPE_FAIL_DETECTION_POC 
IS
	
	-- UTILS

	/*
	In order to print the execution time of a procedure

	@params:
	- should_print: whether to print or not
	- start_time, end_time: time stamps of beginning execution and ending execution
	- prefix: the messages that will prefix the execution time for improved human readability
	*/
	PROCEDURE PRINT_EXEC_TIME(should_print IN BOOLEAN, start_time IN number, end_time IN number, prefix VARCHAR2) AS
	BEGIN 
		if should_print then
			CPE_FAILURE_ERROR_LOG.put_line(RPAD(prefix,40)|| LPAD(to_char((end_time-start_time)/100),5) ||' seconds.',0,0);
		end if;
	END PRINT_EXEC_TIME;

	/*
	In order to print debug informations: the number of entries in intermediate tables. It will also raise 
	an exception in the case where a table is supposed to not be empty and actually is.

	@params:
	- should_print: whether to print or not
	- table_name: the table name for which we wish to get the info
	- cannot_be_empty: whether the table can be empty or not

	@raises:
	- EMPTY_INTERMEDIATE_TABLE: if the table is empty and cannot_be_empty is set to true
	*/
	PROCEDURE PRINT_DEBUG_INFO(should_print IN BOOLEAN, TABLE_NAME IN VARCHAR2, cannot_be_empty IN BOOLEAN) AS
	BEGIN 
		v_sql := 'select count(*) from ' || TABLE_NAME;
	    EXECUTE IMMEDIATE v_sql into DEBUG_SIZE;
		if should_print then
			CPE_FAILURE_ERROR_LOG.put_line(TABLE_NAME || ' has size '|| DEBUG_SIZE ||' entries.',0,0);
		end if;
		IF(DEBUG_SIZE = 0 and cannot_be_empty) THEN 
			RAISE EMPTY_INTERMEDIATE_TABLE;
		END IF;
	END PRINT_DEBUG_INFO;

	/*
	Handles errors by logging them in the log_table and rolling back.
	*/
    PROCEDURE ERROR_HANDLING AS
    BEGIN
        CPE_FAILURE_ERROR_LOG.PUT_LINE(error_message,1,0);
        ROLLBACK;
        RAISE_APPLICATION_ERROR(-20010,error_message);
    END;

    /*
	Because the DMT procedure is supposed to empty the log_table, if anything went wrong during the DMP or DMT 
	run last iteration we will not have an empty log_table and an exception will be raised
	*/
    PROCEDURE CHECK_LAST_RUN AS 
    BEGIN
    	select count(*) INTO n_rows from HUMOREAU.log_table;
	    if(n_rows > 0) then 
	    	RAISE FAILED_LAST_RUN;
	    end if;
    END;

    /*
    Initial check of the state of the tables and the parameters set in the package.
	(Each check is well explained in the code)

	@raise:
	- INVALID_DAY_0: if var_day_0 is not set to a compatible date
	- INVALID_STATE: the initialization of the 3 buffer tables is not correct
    */
    PROCEDURE CHECK_STATE AS 
    BEGIN
	    if(TRUNC(var_day_0) > TRUNC(sysdate-1) OR TRUNC(var_day_0) < TRUNC(sysdate-11)) then 
	        -- we first check that var_day_0 is included in SAA history
	        RAISE INVALID_DAY_0;
	    end if;

	    if(not(TRUNC(sysdate) >= TRUNC(var_via_day_0 + 9))) then 
	        -- the flag that tells us whether an interaction was finished is not yet defined.
	        RAISE INVALID_DAY_0;
	    end if;

	    SELECT /*+ Parallel(16) */ DISTINCT(DAY_0) INTO v_max_date FROM HUMOREAU.VECTOR;
	    IF(TRUNC(v_max_date) != TRUNC(VAR_DAY_0 - 1)) THEN
	    	-- Vector doesn't contain Day -1 and therefore the induction isn't well initialized
	    	RAISE INVALID_STATE;
	    END IF;

	    SELECT /*+ Parallel(16) */ MAX(DISTINCT(DAY_0)) INTO v_max_date FROM HUMOREAU.DAILY_AVG_DIFFS;
	    IF(TRUNC(v_max_date) != TRUNC(VAR_DAY_0 - 1)) THEN 
	    	-- Daily_avg_diffs doesn't contain Day -1 and therefore the induction isn't well initialized
	    	RAISE INVALID_STATE;
	    END IF;

	    SELECT /*+ Parallel(16) */ MAX(DISTINCT(DAY_0)) INTO v_max_date FROM HUMOREAU.DAILY_AVG_DAY_0;
	    IF(TRUNC(v_max_date) != TRUNC(VAR_DAY_0 - 1)) THEN 
	    	-- Daily_avg_day_0 doesn't contain Day -1 and therefore the induction isn't well initialized
	    	RAISE INVALID_STATE;
	    END IF;
    END;


    /*
	Truncates all the intermediary tables to keep the space used at a minimum.

	@params:
	- complete: set to True if we wish to truncate all the tables (including VIA), useful for the init phase
    */
	PROCEDURE CLEAN_UP(complete IN BOOLEAN) IS
	BEGIN
		v_start_time := dbms_utility.get_time;
		if save_space then
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
			if(complete) then 
				-- so that we can also clean for the initialization
				EXECUTE IMMEDIATE 'TRUNCATE TABLE HUMOREAU.SUBSET_MILESTONES DROP STORAGE';
				EXECUTE IMMEDIATE 'TRUNCATE TABLE HUMOREAU.FULL_FLOWS DROP STORAGE';
				EXECUTE IMMEDIATE 'TRUNCATE TABLE HUMOREAU.FULL_FLOWS_TAGGED DROP STORAGE';
				EXECUTE IMMEDIATE 'TRUNCATE TABLE HUMOREAU.FLAGGED_EVENT DROP STORAGE';
				EXECUTE IMMEDIATE 'TRUNCATE TABLE HUMOREAU.TAGGED_MILESTONE DROP STORAGE';
				EXECUTE IMMEDIATE 'TRUNCATE TABLE HUMOREAU.MILESTONES DROP STORAGE';
				EXECUTE IMMEDIATE 'TRUNCATE TABLE HUMOREAU.VIA_DETAILS DROP STORAGE';
				EXECUTE IMMEDIATE 'TRUNCATE TABLE HUMOREAU.SUCCESS_FLAGS DROP STORAGE';
				EXECUTE IMMEDIATE 'TRUNCATE TABLE HUMOREAU.TENTATIVE_MATCH DROP STORAGE';
				EXECUTE IMMEDIATE 'TRUNCATE TABLE HUMOREAU.FLAGGED_SUCESSFUL_SESSIONS DROP STORAGE';
				EXECUTE IMMEDIATE 'TRUNCATE TABLE HUMOREAU.VIA_MACS_NOT_CONFIRMED DROP STORAGE';
				EXECUTE IMMEDIATE 'TRUNCATE TABLE HUMOREAU.FTR_FLAGGED_MACS DROP STORAGE';
			end if;
	    end if; 	
		print_exec_time(print_perf,v_start_time,dbms_utility.get_time,'Cleaning up the temporary tables ');
	END CLEAN_UP;

	/*
	Delete all the tables used by the package
	*/
	PROCEDURE DELETE_ALL_TABLES IS 
	BEGIN
		v_start_time := dbms_utility.get_time;
		EXECUTE IMMEDIATE 'DROP TABLE HUMOREAU.DN_EXTRACTED';
		EXECUTE IMMEDIATE 'DROP TABLE HUMOREAU.DN_HOURLY';
		EXECUTE IMMEDIATE 'DROP TABLE HUMOREAU.CMTS_DN_HOURLY';
		EXECUTE IMMEDIATE 'DROP TABLE HUMOREAU.CMTS_DN';
		EXECUTE IMMEDIATE 'DROP TABLE HUMOREAU.CMTS_DN_VECTOR';
		EXECUTE IMMEDIATE 'DROP TABLE HUMOREAU.UP_EXTRACTED';
		EXECUTE IMMEDIATE 'DROP TABLE HUMOREAU.UP_HOURLY';
		EXECUTE IMMEDIATE 'DROP TABLE HUMOREAU.CMTS_UP_HOURLY';
		EXECUTE IMMEDIATE 'DROP TABLE HUMOREAU.CMTS_UP';
		EXECUTE IMMEDIATE 'DROP TABLE HUMOREAU.CMTS_UP_VECTOR';
		EXECUTE IMMEDIATE 'DROP TABLE HUMOREAU.EXTRA_INFOS';
		EXECUTE IMMEDIATE 'DROP TABLE HUMOREAU.OFFLINE_CPE';
		EXECUTE IMMEDIATE 'DROP TABLE HUMOREAU.UNAVAILABILITY_PCT';
		EXECUTE IMMEDIATE 'DROP TABLE HUMOREAU.CENTILES';
		EXECUTE IMMEDIATE 'DROP TABLE HUMOREAU.OUTLIERS';
		EXECUTE IMMEDIATE 'DROP TABLE HUMOREAU.SAA_SVGP_ENRICHED';
		EXECUTE IMMEDIATE 'DROP TABLE HUMOREAU.SVG_AVG';
		EXECUTE IMMEDIATE 'DROP TABLE HUMOREAU.STD_EXTRACTED_MES';
		EXECUTE IMMEDIATE 'DROP TABLE HUMOREAU.DAY_0_6H_WINDOWS'; 
		EXECUTE IMMEDIATE 'DROP TABLE HUMOREAU.CPE_VECTOR';
		EXECUTE IMMEDIATE 'DROP TABLE HUMOREAU.SUBSET_MILESTONES';
		EXECUTE IMMEDIATE 'DROP TABLE HUMOREAU.FULL_FLOWS';
		EXECUTE IMMEDIATE 'DROP TABLE HUMOREAU.FULL_FLOWS_TAGGED';
		EXECUTE IMMEDIATE 'DROP TABLE HUMOREAU.FLAGGED_EVENT';
		EXECUTE IMMEDIATE 'DROP TABLE HUMOREAU.TAGGED_MILESTONE';
		EXECUTE IMMEDIATE 'DROP TABLE HUMOREAU.MILESTONES';
		EXECUTE IMMEDIATE 'DROP TABLE HUMOREAU.VIA_DETAILS';
		EXECUTE IMMEDIATE 'DROP TABLE HUMOREAU.SUCCESS_FLAGS';
		EXECUTE IMMEDIATE 'DROP TABLE HUMOREAU.TENTATIVE_MATCH';
		EXECUTE IMMEDIATE 'DROP TABLE HUMOREAU.FLAGGED_SUCESSFUL_SESSIONS';
		EXECUTE IMMEDIATE 'DROP TABLE HUMOREAU.VIA_MACS_NOT_CONFIRMED';
		EXECUTE IMMEDIATE 'DROP TABLE HUMOREAU.FTR_FLAGGED_MACS';
		EXECUTE IMMEDIATE 'DROP TABLE HUMOREAU.VIA_MACS';
		-- And the 3 rolling window tables
		EXECUTE IMMEDIATE 'DROP TABLE HUMOREAU.DAILY_AVG_DIFFS';
		EXECUTE IMMEDIATE 'DROP TABLE HUMOREAU.DAILY_AVG_DAY_0';
		EXECUTE IMMEDIATE 'DROP TABLE HUMOREAU.VECTOR';
		-- and the logs
		EXECUTE IMMEDIATE 'DROP TABLE HUMOREAU.LOG_TABLE';
		print_exec_time(print_perf,v_start_time,dbms_utility.get_time,'Deleting all the tables ');
	END DELETE_ALL_TABLES;


	/*
	Prepares the state of buffer tables such that MAIN_PROC can be ran for Day 0. It will iteratively insert the correct 
	data in VECTOR DAILY_AVG_DAY_0 and DAILY_AVG_DIFFS
	*/
	PROCEDURE INIT AS
	BEGIN
		HUMOREAU.CPE_FAILURE_ERROR_LOG.put_line('[INITIALIZING] Will be ready to run MAIN_PROC for :' || to_char(var_day_0,'dd/mm/yyyy'),0,0);
		-- Then we keep inserting until the state is correct
		INITIALIZE_STEP_INSERTS(var_day_0 - 4);
		INITIALIZE_STEP_INSERTS(var_day_0 - 3);	
		INITIALIZE_STEP_INSERTS(var_day_0 - 2);	
		INITIALIZE_STEP_INSERTS(var_day_0 - 1);

		HUMOREAU.CPE_FAIL_DETECTION_POC.print_exec_time(print_perf,v_start_time_total,dbms_utility.get_time,'=========> TOTAL Running time ');
	    HUMOREAU.CPE_FAILURE_ERROR_LOG.put_line('Completed successfully !',0,1);
	END;

	/*
	Performs an insertion step to the buffer tables for a given date (it will fill in all the intermediary tables, fill in 
	the buffer tables and clean up the state)
	*/
	PROCEDURE INITIALIZE_STEP_INSERTS(day_to_consider IN DATE) AS
	BEGIN
		HUMOREAU.CPE_FAILURE_ERROR_LOG.put_line('Computing the vectors for day_0 = ' || to_char(day_to_consider,'dd/mm/yyyy'),0,0);

		-- CMTS-DN
		if(print_perf) then
	        HUMOREAU.CPE_FAILURE_ERROR_LOG.put_line('-----> CMTS Downstream',0,0);
	    end if;
		HUMOREAU.CPE_FAIL_DETECTION_POC.CMTS_DN_EXTRACT(day_to_consider);
	    HUMOREAU.CPE_FAIL_DETECTION_POC.CMTS_DN_SIX_H_AVG(day_to_consider);
	    HUMOREAU.CPE_FAIL_DETECTION_POC.CMTS_DN_PIVOT_SIX_H(day_to_consider);
	    HUMOREAU.CPE_FAIL_DETECTION_POC.CMTS_DN_ADD_SVGP(day_to_consider);
	    HUMOREAU.CPE_FAIL_DETECTION_POC.CMTS_DN_BUILD_VECTOR(day_to_consider);
	    v_end_time_dn := dbms_utility.get_time; 
	    HUMOREAU.CPE_FAIL_DETECTION_POC.print_exec_time(print_perf,v_start_time_total,v_end_time_dn,'==> CMTS-DN Total Running time ');
	    
	    -- CMTS-UP
	    if(print_perf) then
	        HUMOREAU.CPE_FAILURE_ERROR_LOG.put_line('-----> CMTS Upstream',0,0);
	    end if;
	    HUMOREAU.CPE_FAIL_DETECTION_POC.CMTS_UP_EXTRACT(day_to_consider);
	    HUMOREAU.CPE_FAIL_DETECTION_POC.CMTS_UP_SIX_H_AVG(day_to_consider);
	    HUMOREAU.CPE_FAIL_DETECTION_POC.CMTS_UP_PIVOT_SIX_H(day_to_consider);
	    HUMOREAU.CPE_FAIL_DETECTION_POC.CMTS_UP_ADD_SVGP(day_to_consider);
	    HUMOREAU.CPE_FAIL_DETECTION_POC.CMTS_UP_BUILD_VECTOR(day_to_consider);
	    v_end_time_up := dbms_utility.get_time; 
	    HUMOREAU.CPE_FAIL_DETECTION_POC.print_exec_time(print_perf,v_end_time_dn,v_end_time_up,'==> CMTS-UP Total Running time ');
	    
	    -- CPE
		if(print_perf) then
	        dbms_output.new_line;
	        dbms_output.put_line('-----> CPE');
	        HUMOREAU.CPE_FAILURE_ERROR_LOG.put_line('-----> CPE',0,0);
	    end if;
	    HUMOREAU.CPE_FAIL_DETECTION_POC.STATIC_INFOS(day_to_consider);
	    HUMOREAU.CPE_FAIL_DETECTION_POC.OFFLINE_CPE(day_to_consider);
	    HUMOREAU.CPE_FAIL_DETECTION_POC.COMPUTE_UNAVAILABILITY_PCT(day_to_consider);
	    HUMOREAU.CPE_FAIL_DETECTION_POC.COMPUTE_CENTILES(day_to_consider);
	    HUMOREAU.CPE_FAIL_DETECTION_POC.DETECT_OUTLIERS(day_to_consider);
	    HUMOREAU.CPE_FAIL_DETECTION_POC.EXTRACT_ENRICH_SAA(day_to_consider);
	    HUMOREAU.CPE_FAIL_DETECTION_POC.COMPUTE_SVG_AVG(day_to_consider);
	    HUMOREAU.CPE_FAIL_DETECTION_POC.STANDARDISE_BY_SVG(day_to_consider);
	    HUMOREAU.CPE_FAIL_DETECTION_POC.CPE_SIX_H_AVG(day_to_consider);
	    HUMOREAU.CPE_FAIL_DETECTION_POC.CPE_PIVOT_SIX_H(day_to_consider);
	    v_end_time_cpe := dbms_utility.get_time; 
	    HUMOREAU.CPE_FAIL_DETECTION_POC.print_exec_time(print_perf,v_end_time_up,v_end_time_cpe,'==> CPE Total Running time ');
	    
	    -- DUMP TO CREATE 5D VECTOR
	    if(print_perf) then
	        dbms_output.new_line;
	        HUMOREAU.CPE_FAILURE_ERROR_LOG.put_line('-----> BUILDING VECTOR',0,0);
	    end if;
	    HUMOREAU.CPE_FAIL_DETECTION_POC.VECTOR_1D(day_to_consider);
	    HUMOREAU.CPE_FAIL_DETECTION_POC.DAILY_AVG_DAY_0(day_to_consider);
	    HUMOREAU.CPE_FAIL_DETECTION_POC.DAILY_DIFFS(day_to_consider);
	    v_end_time_vector := dbms_utility.get_time; 
	    HUMOREAU.CPE_FAIL_DETECTION_POC.print_exec_time(print_perf,v_end_time_up,v_end_time_vector,'==> VECTOR BUILD Total Running time ');
	    HUMOREAU.CPE_FAIL_DETECTION_POC.CLEAN_UP(FALSE);
	    COMMIT;
	END;


	/*
	Allows to run the induction that fills in the buffer tables. 
	It calls all the sub-procedures to fill in the intermediary tables.
	*/
	PROCEDURE MAIN_PROC IS 
	BEGIN
		v_start_time_total := dbms_utility.get_time;
		
		CHECK_LAST_RUN;
		CHECK_STATE;

	    CPE_FAILURE_ERROR_LOG.put_line('Computing the vectors for day_0 = ' || to_char(var_day_0,'dd/mm/yyyy'),0,0);
	    if save_space then
	        CPE_FAILURE_ERROR_LOG.put_line('**Space Saving Activated**',0,0);
	    end if;

		-- CMTS-DN
		if(print_perf) then
	        CPE_FAILURE_ERROR_LOG.put_line('-----> CMTS Downstream',0,0);
	    end if;
		CMTS_DN_EXTRACT(var_day_0);
	    CMTS_DN_SIX_H_AVG(var_day_0);
	    CMTS_DN_PIVOT_SIX_H(var_day_0);
	    CMTS_DN_ADD_SVGP(var_day_0);
	    CMTS_DN_BUILD_VECTOR(var_day_0);
	    v_end_time_dn := dbms_utility.get_time; 
	    print_exec_time(print_perf,v_start_time_total,v_end_time_dn,'==> CMTS-DN Total Running time ');
	    
	    -- CMTS-UP
	    if(print_perf) then
	        CPE_FAILURE_ERROR_LOG.put_line('-----> CMTS Upstream',0,0);
	    end if;
	    CMTS_UP_EXTRACT(var_day_0);
	    CMTS_UP_SIX_H_AVG(var_day_0);
	    CMTS_UP_PIVOT_SIX_H(var_day_0);
	    CMTS_UP_ADD_SVGP(var_day_0);
	    CMTS_UP_BUILD_VECTOR(var_day_0);
	    v_end_time_up := dbms_utility.get_time; 
	    print_exec_time(print_perf,v_end_time_dn,v_end_time_up,'==> CMTS-UP Total Running time ');
	    
	    -- CPE
		if(print_perf) then
	        dbms_output.new_line;
	        dbms_output.put_line('-----> CPE');
	        CPE_FAILURE_ERROR_LOG.put_line('-----> CPE',0,0);
	    end if;
	    STATIC_INFOS(var_day_0);
	    OFFLINE_CPE(var_day_0);
	    COMPUTE_UNAVAILABILITY_PCT(var_day_0);
	    COMPUTE_CENTILES(var_day_0);
	    DETECT_OUTLIERS(var_day_0);
	    EXTRACT_ENRICH_SAA(var_day_0);
	    COMPUTE_SVG_AVG(var_day_0);
	    STANDARDISE_BY_SVG(var_day_0);
	    CPE_SIX_H_AVG(var_day_0);
	    CPE_PIVOT_SIX_H(var_day_0);
	    v_end_time_cpe := dbms_utility.get_time; 
	    print_exec_time(print_perf,v_end_time_up,v_end_time_cpe,'==> CPE Total Running time ');
	    
	    -- DUMP TO CREATE 5D VECTOR
	    if(print_perf) then
	        dbms_output.new_line;
	        CPE_FAILURE_ERROR_LOG.put_line('-----> BUILDING VECTOR',0,0);
	    end if;
	    VECTOR_1D(var_day_0);
	    DAILY_AVG_DAY_0(var_day_0);
	    DAILY_DIFFS(var_day_0);
	    v_end_time_vector := dbms_utility.get_time; 
	    print_exec_time(print_perf,v_end_time_up,v_end_time_vector,'==> VECTOR BUILD Total Running time ');

	    -- GETTING THE VIA MACS
	    if(print_perf) then
	        dbms_output.new_line;
	        CPE_FAILURE_ERROR_LOG.put_line('-----> VIA',0,0);
	    end if;
	    SUBSET_MILESTONES;
		DETECT_FULL_FLOWS;
		TAG_FULL_FLOWS;
		FLAG_EVENTS;
		TAG_MILESTONES;
		EXTRACT_MILESTONES;
		EXTRACT_VIA_DETAILS;
		TAG_SUCCESS;
		TENTATIVE_MATCH;
		FLAG_SUCCESSFUL_EVENTS;
		GET_VIA_MACS;
		FLAG_MACS;
		BUILD_VIA_MACS;
		print_exec_time(print_perf,v_end_time_vector,dbms_utility.get_time,'==> VIA Total Running time ');
		-- we can finaly delete all intermediary tables
		CLEAN_UP(TRUE);
	    COMMIT;
	    print_exec_time(print_perf,v_start_time_total,dbms_utility.get_time,'=========> TOTAL Running time ');
	    CPE_FAILURE_ERROR_LOG.put_line('Completed successfully !',0,1);

	EXCEPTION
		WHEN INVALID_DAY_0 THEN 
			error_message := 'DAY_0 chosen is not included in SAA history or invalid w.r.t VIA flags';
			ERROR_HANDLING;

		WHEN FAILED_LAST_RUN THEN
			error_message := 'Last run was unsuccessful';
			ERROR_HANDLING;

		WHEN INVALID_STATE THEN 
			error_message := 'The intermediate tables do not contain the expected dates';
			ERROR_HANDLING;

	    WHEN EMPTY_INTERMEDIATE_TABLE THEN 
	    	error_message := 'One of the intermediate table was empty (check previous entries for more details)';
	    	ERROR_HANDLING;

	    WHEN OTHERS THEN 
	    	error_message := 'Error code ' || SQLCODE || ': ' || SQLERRM;
	        ERROR_HANDLING;

	END MAIN_PROC;

	/*
	================================================================================================
	=========================================CMTS-DN================================================
	================================================================================================
	*/


	PROCEDURE CMTS_DN_EXTRACT(day_to_consider  IN DATE) IS 
	BEGIN
		v_start_time := dbms_utility.get_time; 
	    INSERT INTO HUMOREAU.DN_EXTRACTED (
	        -- Extracts the measurement from SAA for the CMTS Downstram, it will only consider data for the 
	        -- date that we are considering in order to limit the dataset for later computation
	        SELECT * FROM
	            (
	            WITH 
	            EXTRACTED AS(
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
	                WHERE  TRUNC(HOUR) = TRUNC(day_to_consider)
	                )
	            -- Transform the Downstream Name to extract only the Interface description and the time_stamp 
	            -- to only retain to which 6h window the entry corresponds to
	            SELECT  /*+ PARALLEL(16) */ 
	                    CMTS_NAME,
	                    REGEXP_SUBSTR(DOWNSTREAM_NAME,'(Downstream [[:alnum:]/]+)') AS IFC_DESCR, 
	                    CASE 
	                        WHEN to_number(to_char(HOUR,'HH24')) IN (23,22,21,20,19,18) THEN 0 -- those hours belongs to the most recent 6h window
	                        WHEN to_number(to_char(HOUR,'HH24')) IN (17,16,15,14,13,12) THEN 1
	                        WHEN to_number(to_char(HOUR,'HH24')) IN (11,10,9,8,7,6)     THEN 2
	                        WHEN to_number(to_char(HOUR,'HH24')) IN (5,4,3,2,1,0)       THEN 3 -- while these belong to the least recent
	                    END AS HOUR_WINDOW, 
	                    RXPOWER, 
	                    AVG_SNR,
	                    CCER, 
	                    CER, 
	                    UTILIZATION
	            FROM EXTRACTED 
	            )
	        );
	    	print_exec_time(print_perf,v_start_time,dbms_utility.get_time,'Updating DN_EXTRACTED took ');
		    PRINT_DEBUG_INFO(debug,'HUMOREAU.DN_EXTRACTED',TRUE);
	END CMTS_DN_EXTRACT;

	------------------------------------------------------------------------------------------------

	PROCEDURE CMTS_DN_SIX_H_AVG(day_to_consider  IN DATE) IS 
	BEGIN
		v_start_time := dbms_utility.get_time; 
	    INSERT INTO HUMOREAU.DN_HOURLY (
	        -- Average measurement over each 6 hour windows
	        SELECT * FROM(
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
	        );
	    print_exec_time(print_perf,v_start_time,dbms_utility.get_time,'Updating DN_HOURLY took ');
	    PRINT_DEBUG_INFO(debug,'HUMOREAU.DN_HOURLY',TRUE);
	END CMTS_DN_SIX_H_AVG;

	------------------------------------------------------------------------------------------------

	PROCEDURE CMTS_DN_PIVOT_SIX_H(day_to_consider  IN DATE) IS 
	BEGIN
		v_start_time := dbms_utility.get_time; 
	    INSERT INTO HUMOREAU.CMTS_DN_HOURLY (
	        -- Group for each MAC the average for each 6h window 
	        -- (each measurement X will be transformed into {0,1,2,3}_X)
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
	        );
	    print_exec_time(print_perf,v_start_time,dbms_utility.get_time,'Updating CMTS_DN_HOURLY took ');
	    PRINT_DEBUG_INFO(debug,'HUMOREAU.CMTS_DN_HOURLY',TRUE);	
	END CMTS_DN_PIVOT_SIX_H;

	------------------------------------------------------------------------------------------------
	
	PROCEDURE CMTS_DN_ADD_SVGP(day_to_consider  IN DATE) IS 
	BEGIN
		v_start_time := dbms_utility.get_time; 
	    INSERT INTO HUMOREAU.CMTS_DN (
	        -- Complement the information we have about each entry by adding to 
	        -- which service grouo the interface belongs to
	        SELECT * FROM(
	            WITH IFC2SVG AS (
	                select CMTS_NAME, SERVICE_GROUP_NAME AS SERVICE_GROUP, IFC_DESCR 
	                from DM_DIM.ETL_STG_TOPO_CMTS2NODE
	            )
	            SELECT 
	                IFC2SVG.SERVICE_GROUP,
	                TRUNC(day_to_consider) AS "DAY_0",
	                CMTS_DN.*
	            FROM HUMOREAU.CMTS_DN_HOURLY CMTS_DN INNER JOIN IFC2SVG
	                ON  CMTS_DN.CMTS_NAME = IFC2SVG.CMTS_NAME 
	                    AND CMTS_DN.IFC_DESCR = IFC2SVG.IFC_DESCR
	            )
	        );
	    print_exec_time(print_perf,v_start_time,dbms_utility.get_time,'Updating CMTS_DN took ');
	    PRINT_DEBUG_INFO(debug,'HUMOREAU.CMTS_DN',TRUE);
	END CMTS_DN_ADD_SVGP;

	------------------------------------------------------------------------------------------------
	
	PROCEDURE CMTS_DN_BUILD_VECTOR(day_to_consider  IN DATE) IS 
	BEGIN
		v_start_time := dbms_utility.get_time; 
	    INSERT INTO HUMOREAU.CMTS_DN_VECTOR (
	    	-- Computes the average of each 6h aggregates by service group and build the final 
	    	-- vector of information for CMTS downstream measurement
	        SELECT 
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
	        );
	    print_exec_time(print_perf,v_start_time,dbms_utility.get_time,'Updating CMTS_DN_VECTOR took ');
	    PRINT_DEBUG_INFO(debug,'HUMOREAU.CMTS_DN_VECTOR',TRUE);
	END CMTS_DN_BUILD_VECTOR;

	/*
	================================================================================================
	=========================================CMTS-UP================================================
	================================================================================================
	*/

	PROCEDURE CMTS_UP_EXTRACT(day_to_consider  IN DATE) IS 
	BEGIN
		v_start_time := dbms_utility.get_time; 
	    INSERT INTO HUMOREAU.UP_EXTRACTED (
	        -- Extracts the measurement from SAA for the CMTS Upstream, it will only consider data for the 
	        -- date that we are considering in order to limit the dataset for later computation 
	        SELECT * FROM (
	            WITH
	            EXTRACTED AS (
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
	            WHERE TRUNC(HOUR) = TRUNC(day_to_consider)
	            )
	            SELECT /*+ PARALLEL(16) */ 
	            	-- Converts some column: extracts the Interface description from the Upstream name and 
	            	-- converts the time stamp into the 6h window to which the entry belongs to
	                CMTS_NAME,
	                REGEXP_SUBSTR(UPSTREAM_NAME,'[[:alnum:] .]+/[0-9 .]+/[0-9 .]+$') AS IFC_DESCR, 
	                CASE 
	                    WHEN to_number(to_char(HOUR,'HH24')) IN (23,22,21,20,19,18) THEN 0 -- those hours belongs to the most recent 6h window
	                    WHEN to_number(to_char(HOUR,'HH24')) IN (17,16,15,14,13,12) THEN 1
	                    WHEN to_number(to_char(HOUR,'HH24')) IN (11,10,9,8,7,6)     THEN 2
	                    WHEN to_number(to_char(HOUR,'HH24')) IN (5,4,3,2,1,0)       THEN 3 -- while these belong to the least recent
	                END AS HOUR_WINDOW, 
	                RXPOWER, 
	                TXPOWER, 
	                CER, 
	                UTILIZATION, 
	                MS_UTILIZATION, 
	                FREECONT_MS_UTILIZATION
	            FROM EXTRACTED
	            )
	        );
	    print_exec_time(print_perf,v_start_time,dbms_utility.get_time,'Updating UP_EXTRACTED took ');
	    PRINT_DEBUG_INFO(debug,'HUMOREAU.UP_EXTRACTED',TRUE);
	END CMTS_UP_EXTRACT;

	------------------------------------------------------------------------------------------------

	PROCEDURE CMTS_UP_SIX_H_AVG(day_to_consider  IN DATE) IS 
	BEGIN
		v_start_time := dbms_utility.get_time; 
		INSERT INTO HUMOREAU.UP_HOURLY (
		-- Average measurement over each 6 hour windows	
        SELECT * FROM (
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
        );
	    print_exec_time(print_perf,v_start_time,dbms_utility.get_time,'Updating UP_HOURLY took ');
	   	PRINT_DEBUG_INFO(debug,'HUMOREAU.UP_HOURLY',TRUE);
	END CMTS_UP_SIX_H_AVG;

	------------------------------------------------------------------------------------------------

	PROCEDURE CMTS_UP_PIVOT_SIX_H(day_to_consider  IN DATE) IS 
	BEGIN
		v_start_time := dbms_utility.get_time; 
		INSERT INTO HUMOREAU.CMTS_UP_HOURLY (
			-- Group for each MAC the average for each 6h window 
	        -- (each measurement X will be transformed into {0,1,2,3}_X)
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
        );
	    print_exec_time(print_perf,v_start_time,dbms_utility.get_time,'Updating CMTS_UP_HOURLY took ');
	    PRINT_DEBUG_INFO(debug,'HUMOREAU.CMTS_UP_HOURLY',TRUE);
	END CMTS_UP_PIVOT_SIX_H;

	------------------------------------------------------------------------------------------------
	
	PROCEDURE CMTS_UP_ADD_SVGP(day_to_consider  IN DATE) IS 
	BEGIN
		v_start_time := dbms_utility.get_time;
		INSERT INTO HUMOREAU.CMTS_UP (
		-- Complement the information we have about each entry by adding to 
	    -- which service grouo the interface belongs to
	        SELECT * FROM(
	            WITH IFC2SVG AS (
	                select CMTS_NAME, SERVICE_GROUP_NAME AS SERVICE_GROUP, IFC_DESCR 
	                from DM_DIM.ETL_STG_TOPO_CMTS2NODE
	            )
	            SELECT 
	                IFC2SVG.SERVICE_GROUP,
	                TRUNC(day_to_consider) AS "DAY_0",
	                CMTS_UP.*
	            FROM HUMOREAU.CMTS_UP_HOURLY CMTS_UP INNER JOIN IFC2SVG
	                ON  CMTS_UP.CMTS_NAME = IFC2SVG.CMTS_NAME 
	                    AND CMTS_UP.IFC_DESCR = IFC2SVG.IFC_DESCR
	            )
	        );	
	    print_exec_time(print_perf,v_start_time,dbms_utility.get_time,'Updating CMTS_UP took ');
	    PRINT_DEBUG_INFO(debug,'HUMOREAU.CMTS_UP',TRUE);
	END CMTS_UP_ADD_SVGP;

	------------------------------------------------------------------------------------------------
	
	PROCEDURE CMTS_UP_BUILD_VECTOR(day_to_consider  IN DATE) IS 
	BEGIN
		v_start_time := dbms_utility.get_time; 
		INSERT INTO HUMOREAU.CMTS_UP_VECTOR (
        SELECT 
            -- Computes the average of each 6h aggregates by service group and build the final 
	    	-- vector of information for CMTS downstream measurement
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
        );
        print_exec_time(print_perf,v_start_time,dbms_utility.get_time,'Updating CMTS_UP_VECTOR took ');
	    PRINT_DEBUG_INFO(debug,'HUMOREAU.CMTS_UP_VECTOR',TRUE);
	END CMTS_UP_BUILD_VECTOR;


	/*
	================================================================================================
	============================================CPE=================================================
	================================================================================================
	*/


	PROCEDURE STATIC_INFOS(day_to_consider  IN DATE) IS 
	BEGIN
		v_start_time := dbms_utility.get_time; 
	    INSERT INTO HUMOREAU.EXTRA_INFOS (
	    	-- Collect what we call static information about each CPE
	        SELECT * FROM (
	            WITH HW_INFO AS (
	                -- First the Hardware model, clarify account id, and Building id
	                SELECT
	                    /*+ PARALLEL(16) */ 
	                    REPLACE(NODE_KEY,':') AS MAC, 
	                    SRC_NODE_MODEL AS HARDWARE_MODEL, 
	                    SRC_NODE_BUILDING_ID AS BUILDING_ID, 
	                    CLY_ACCT_NUMBER as CLY_ACCOUNT_NUMBER
	                FROM DM_TOPO_CH.ETL_STG_NW_TOPO_NODES 
	                WHERE topo_node_type_id = v_cpe_type
	            ),N_CPE_2_BUILDING AS (
	                -- compute the number of CPE in the same building
	                SELECT  BUILDING_ID, 
	                    COUNT(DISTINCT MAC) AS N_CPE
	                FROM HW_INFO 
	                GROUP BY BUILDING_ID
	            ), EXTENDED_VALIDITY AS (
	                -- Determine the service group to which each MAC belongs to (we need to get the maximum valid window)
	                SELECT /*+ PARALLEL(16) */ CMTS, SERVICE_GROUP, MAC, MIN(Modemloss_valid_from) AS V_FROM, MAX(modemloss_valid_to) AS V_TO
	                FROM CTSP_HIST.CTSP_HIST_MODEMLOSS_CH
	                GROUP BY  CMTS, SERVICE_GROUP, MAC
	            ), SVG_CMTS_INFO AS (
	            	-- modify the MAC adress to have the same format than SAA
	                SELECT /*+ PARALLEL(16) */ 
	                    CMTS, SERVICE_GROUP, UPPER(REPLACE(MAC,':')) AS MAC
	                FROM EXTENDED_VALIDITY 
	                WHERE TRUNC(V_FROM) <= TRUNC(day_to_consider) AND TRUNC(V_TO) >= TRUNC(day_to_consider)
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
	        	-- we wish to get rid of models that have deep sleep mode
	            'CONNECT BOX CH7465LG COMPAL',
	            -- OLD MODEMS
	            'UBEE EVM3236 (ED 3.0) - CPE',
	            'UBEE EVM3206 (ED 3.0) - CPE',
	            'WLAN MODEM TC7200 - CPE',
	            'WLAN MODEM EVW3226 - CPE',
	            'WLAN MODEM TC7200 V2 - CPE',
	            'WLAN MODEM TWG870 - CPE'
	            ) 
	        );
	    print_exec_time(print_perf,v_start_time,dbms_utility.get_time,'Updating EXTRA_INFOS took ');
	    PRINT_DEBUG_INFO(debug,'HUMOREAU.EXTRA_INFOS',TRUE);
	END STATIC_INFOS;

	------------------------------------------------------------------------------------------------

	PROCEDURE OFFLINE_CPE(day_to_consider  IN DATE) IS 
	BEGIN
		v_start_time := dbms_utility.get_time; 
		INSERT INTO HUMOREAU.OFFLINE_CPE (
		-- Determine all the hours over the past 10 days where MACs where offline
        SELECT * FROM (
            WITH SELECTED_MACS AS (
                SELECT /*+ PARALLEL(16) */ MAC, HOUR_STAMP
                FROM  SAA.CM_HOUR_HEALTH
                WHERE 
                    CM_STATUS != v_online_status
                    AND TRUNC(HOUR_STAMP) <= TRUNC(SYSDATE-1) -- To limit ourselves to days where we have FULL DAYS history
            )
            SELECT  /*+ PARALLEL(16) */ 
                    MAC,
                    to_number(to_char(HOUR_STAMP,'HH24')) H, 
                    to_number(to_char(HOUR_STAMP,'DD')) D
            FROM SELECTED_MACS
            )
        );
        print_exec_time(print_perf,v_start_time,dbms_utility.get_time,'Updating OFFLINE_CPE took ');
        
	    -- Determine the exact number of days for which we have history in SAA
	    SELECT /*+ PARALLEL(16) */(TRUNC(MAX(HOUR_STAMP)) - TRUNC(MIN(HOUR_STAMP)) + 1) INTO v_n_days_offline_history
	    FROM SAA.CM_HOUR_HEALTH
	    WHERE TRUNC(HOUR_STAMP) <= TRUNC(SYSDATE-1);
	    print_exec_time(print_perf,v_start_time,dbms_utility.get_time,'Updating OFFLINE_CPE took ');
	    PRINT_DEBUG_INFO(debug,'HUMOREAU.OFFLINE_CPE',TRUE);
	END OFFLINE_CPE;

	------------------------------------------------------------------------------------------------

	PROCEDURE COMPUTE_UNAVAILABILITY_PCT(day_to_consider  IN DATE) IS 
	BEGIN
	    INSERT INTO HUMOREAU.UNAVAILABILITY_PCT (
	    	-- For each CPE, hour, we compute the ratio of days of all the history we have where 
	    	-- the CPE was offline (computed as a percentage)
	        SELECT * FROM (
	            WITH HOURLY_UNAVAILABILITY AS (
	                SELECT /*+ PARALLEL(16) */ MAC, H, 100*COUNT(D)/(v_n_days_offline_history) UNAVAILABLE
	                FROM HUMOREAU.OFFLINE_CPE
	                GROUP BY MAC, H
	            )
	            -- Pivot the results to have one entry per cpe
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
	        );
	    print_exec_time(print_perf,v_start_time,dbms_utility.get_time,'Updating UNAVAILABILITY_PCT took ');
	    PRINT_DEBUG_INFO(debug,'HUMOREAU.UNAVAILABILITY_PCT',TRUE);
	END COMPUTE_UNAVAILABILITY_PCT;

	------------------------------------------------------------------------------------------------

	PROCEDURE COMPUTE_CENTILES(day_to_consider  IN DATE) IS 
	BEGIN
		v_start_time := dbms_utility.get_time; 
	    -- Find the standard hour unavailability per hardware model over 10 days of history.
	    INSERT INTO HUMOREAU.CENTILES (
	    	-- Compute for each hour the max percent of offline days that keeps v_percentile_lmit% of CPEs
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
	                PERCENTILE_DISC(v_percentile_limit/100) WITHIN GROUP (ORDER BY "0_UNAVAILABLE" ASC) OVER (PARTITION BY HARDWARE_MODEL) AS "0_CUT",
	                PERCENTILE_DISC(v_percentile_limit/100) WITHIN GROUP (ORDER BY "1_UNAVAILABLE" ASC) OVER (PARTITION BY HARDWARE_MODEL) AS "1_CUT",
	                PERCENTILE_DISC(v_percentile_limit/100) WITHIN GROUP (ORDER BY "2_UNAVAILABLE" ASC) OVER (PARTITION BY HARDWARE_MODEL) AS "2_CUT",
	                PERCENTILE_DISC(v_percentile_limit/100) WITHIN GROUP (ORDER BY "3_UNAVAILABLE" ASC) OVER (PARTITION BY HARDWARE_MODEL) AS "3_CUT",
	                PERCENTILE_DISC(v_percentile_limit/100) WITHIN GROUP (ORDER BY "4_UNAVAILABLE" ASC) OVER (PARTITION BY HARDWARE_MODEL) AS "4_CUT",
	                PERCENTILE_DISC(v_percentile_limit/100) WITHIN GROUP (ORDER BY "5_UNAVAILABLE" ASC) OVER (PARTITION BY HARDWARE_MODEL) AS "5_CUT",
	                PERCENTILE_DISC(v_percentile_limit/100) WITHIN GROUP (ORDER BY "6_UNAVAILABLE" ASC) OVER (PARTITION BY HARDWARE_MODEL) AS "6_CUT",
	                PERCENTILE_DISC(v_percentile_limit/100) WITHIN GROUP (ORDER BY "7_UNAVAILABLE" ASC) OVER (PARTITION BY HARDWARE_MODEL) AS "7_CUT",
	                PERCENTILE_DISC(v_percentile_limit/100) WITHIN GROUP (ORDER BY "8_UNAVAILABLE" ASC) OVER (PARTITION BY HARDWARE_MODEL) AS "8_CUT",
	                PERCENTILE_DISC(v_percentile_limit/100) WITHIN GROUP (ORDER BY "9_UNAVAILABLE" ASC) OVER (PARTITION BY HARDWARE_MODEL) AS "9_CUT",
	                PERCENTILE_DISC(v_percentile_limit/100) WITHIN GROUP (ORDER BY "10_UNAVAILABLE" ASC) OVER (PARTITION BY HARDWARE_MODEL) AS "10_CUT",
	                PERCENTILE_DISC(v_percentile_limit/100) WITHIN GROUP (ORDER BY "11_UNAVAILABLE" ASC) OVER (PARTITION BY HARDWARE_MODEL) AS "11_CUT",
	                PERCENTILE_DISC(v_percentile_limit/100) WITHIN GROUP (ORDER BY "12_UNAVAILABLE" ASC) OVER (PARTITION BY HARDWARE_MODEL) AS "12_CUT",
	                PERCENTILE_DISC(v_percentile_limit/100) WITHIN GROUP (ORDER BY "13_UNAVAILABLE" ASC) OVER (PARTITION BY HARDWARE_MODEL) AS "13_CUT",
	                PERCENTILE_DISC(v_percentile_limit/100) WITHIN GROUP (ORDER BY "14_UNAVAILABLE" ASC) OVER (PARTITION BY HARDWARE_MODEL) AS "14_CUT",
	                PERCENTILE_DISC(v_percentile_limit/100) WITHIN GROUP (ORDER BY "15_UNAVAILABLE" ASC) OVER (PARTITION BY HARDWARE_MODEL) AS "15_CUT",
	                PERCENTILE_DISC(v_percentile_limit/100) WITHIN GROUP (ORDER BY "16_UNAVAILABLE" ASC) OVER (PARTITION BY HARDWARE_MODEL) AS "16_CUT",
	                PERCENTILE_DISC(v_percentile_limit/100) WITHIN GROUP (ORDER BY "17_UNAVAILABLE" ASC) OVER (PARTITION BY HARDWARE_MODEL) AS "17_CUT",
	                PERCENTILE_DISC(v_percentile_limit/100) WITHIN GROUP (ORDER BY "18_UNAVAILABLE" ASC) OVER (PARTITION BY HARDWARE_MODEL) AS "18_CUT",
	                PERCENTILE_DISC(v_percentile_limit/100) WITHIN GROUP (ORDER BY "19_UNAVAILABLE" ASC) OVER (PARTITION BY HARDWARE_MODEL) AS "19_CUT",
	                PERCENTILE_DISC(v_percentile_limit/100) WITHIN GROUP (ORDER BY "20_UNAVAILABLE" ASC) OVER (PARTITION BY HARDWARE_MODEL) AS "20_CUT",
	                PERCENTILE_DISC(v_percentile_limit/100) WITHIN GROUP (ORDER BY "21_UNAVAILABLE" ASC) OVER (PARTITION BY HARDWARE_MODEL) AS "21_CUT",
	                PERCENTILE_DISC(v_percentile_limit/100) WITHIN GROUP (ORDER BY "22_UNAVAILABLE" ASC) OVER (PARTITION BY HARDWARE_MODEL) AS "22_CUT",
	                PERCENTILE_DISC(v_percentile_limit/100) WITHIN GROUP (ORDER BY "23_UNAVAILABLE" ASC) OVER (PARTITION BY HARDWARE_MODEL) AS "23_CUT"
	            FROM ENRICHED_UNAVAILABILITY
	            )
	        );
	    print_exec_time(print_perf,v_start_time,dbms_utility.get_time,'Updating CENTILES took ');
	    PRINT_DEBUG_INFO(debug,'HUMOREAU.CENTILES',TRUE);
	END COMPUTE_CENTILES;

	------------------------------------------------------------------------------------------------

	PROCEDURE DETECT_OUTLIERS(day_to_consider  IN DATE) IS 
	BEGIN
		v_start_time := dbms_utility.get_time; 
		INSERT INTO HUMOREAU.OUTLIERS (
	        -- Flag the CPEs that are outliers with respect to their own hardware_model, 
	        -- so that we can exclude those from our analysis 
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
	    );   
	    print_exec_time(print_perf,v_start_time,dbms_utility.get_time,'Updating OUTLIERS took ');
	    PRINT_DEBUG_INFO(debug,'HUMOREAU.OUTLIERS',TRUE);
	END DETECT_OUTLIERS;

	------------------------------------------------------------------------------------------------

	PROCEDURE EXTRACT_ENRICH_SAA(day_to_consider  IN DATE) IS 
	BEGIN
		v_start_time := dbms_utility.get_time; 
	    INSERT INTO HUMOREAU.SAA_SVGP_ENRICHED (
	    	-- Extracts measurements from SAA for day 0 and enriches it with 
	    	-- the static infos and add the offline information
	        SELECT * FROM (
	            WITH FLAGGED_OFFLINE AS (
	                SELECT O.*, 1 AS "OFFLINE_FLG" 
	                FROM HUMOREAU.OFFLINE_CPE O
	            )
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
	                    AND TRUNC(SAA.HOUR_STAMP) = TRUNC(day_to_consider)
	                    AND DETAILS.MAC NOT IN (SELECT MAC FROM HUMOREAU.OUTLIERS)

	                LEFT OUTER JOIN FLAGGED_OFFLINE O
	                    ON  SAA.MAC = O.MAC
	                        AND to_number(to_char(SAA.HOUR_STAMP,'HH24')) = O.H
	                        AND to_number(to_char(SAA.HOUR_STAMP,'DD')) = O.D
	            )
	        );
	    print_exec_time(print_perf,v_start_time,dbms_utility.get_time,'Updating SAA_SVGP_ENRICHED took ');
	    PRINT_DEBUG_INFO(debug,'HUMOREAU.SAA_SVGP_ENRICHED',TRUE);
	END EXTRACT_ENRICH_SAA;

	------------------------------------------------------------------------------------------------

	PROCEDURE COMPUTE_SVG_AVG(day_to_consider  IN DATE) IS 
	BEGIN
		v_start_time := dbms_utility.get_time; 
	    -- Get the average of values by service group because we want to normalize 
	    -- each CPE measurement w.r.t to its service group
	    INSERT INTO HUMOREAU.SVG_AVG (
	        -- Compute for each service group the average, max and min of each measurement
	        -- NB: If there are no measurement in the service group for a given timestamp, then no max/range will be 
	        -- computed which is fine since anyway theree won't be any CPE measurement to standardise.
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
	    );
	    print_exec_time(print_perf,v_start_time,dbms_utility.get_time,'Updating SVG_AVG took ');
		PRINT_DEBUG_INFO(debug,'HUMOREAU.SVG_AVG',TRUE);
	END COMPUTE_SVG_AVG;

	------------------------------------------------------------------------------------------------

	PROCEDURE STANDARDISE_BY_SVG(day_to_consider  IN DATE) IS 
	BEGIN
		v_start_time := dbms_utility.get_time; 
		INSERT INTO HUMOREAU.STD_EXTRACTED_MES (
	        -- Standardise the CPE measurement using  [x - mean(x)]/[max(x)-min(x)], 
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
	                -- (if the denominator is 0, then no variance at the service group level so the relative cpe measurement is 0)
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
	    );
	    print_exec_time(print_perf,v_start_time,dbms_utility.get_time,'Updating STD_EXTRACTED_MES took ');
	    PRINT_DEBUG_INFO(debug,'HUMOREAU.STD_EXTRACTED_MES',TRUE);
	END STANDARDISE_BY_SVG;

	------------------------------------------------------------------------------------------------

	PROCEDURE CPE_SIX_H_AVG(day_to_consider  IN DATE) IS 
	BEGIN
		v_start_time := dbms_utility.get_time;
	    INSERT INTO HUMOREAU.DAY_0_6H_WINDOWS (
	    	-- Average for each CPE measurements over 6h windows and also add the percentage 
	    	-- of missing values over the 6h windows for each measurement
	        SELECT * FROM (
	            WITH STD_DAY_0 AS (
	                SELECT
	                    CMTS, 
	                    SERVICE_GROUP, 
	                    MAC, 
	                    SAA_ACCOUNT_NUMBER, 
	                    OFFLINE_FLG,
	                    HARDWARE_MODEL,
	                    CLY_ACCOUNT_NUMBER,
	                    N_CPE_BUILDING,
	                    TRUNC(day_to_consider) AS "DAY_0",

	                    TXPOWER_UP, RXPOWER_UP, RXPOWER_DN, CER_DN, 
	                    CER_UP, SNR_DN, SNR_UP, PCT_TRAFFIC_DMH_UP, PCT_TRAFFIC_SDMH_UP,
	                    CASE 
	                        WHEN to_number(to_char(T.HOUR_STAMP,'HH24')) IN (23,22,21,20,19,18) THEN 0 -- those hours belongs to the most recent 6h window
	                        WHEN to_number(to_char(T.HOUR_STAMP,'HH24')) IN (17,16,15,14,13,12) THEN 1
	                        WHEN to_number(to_char(T.HOUR_STAMP,'HH24')) IN (11,10,9,8,7,6)     THEN 2
	                        WHEN to_number(to_char(T.HOUR_STAMP,'HH24')) IN (5,4,3,2,1,0)       THEN 3 -- while these belong to the least recent
	                    END AS HOUR_WINDOW
	                FROM HUMOREAU.STD_EXTRACTED_MES T
	            )
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
	    );
	    print_exec_time(print_perf,v_start_time,dbms_utility.get_time,'Updating DAY_0_6H_WINDOWS took ');
	    PRINT_DEBUG_INFO(debug,'HUMOREAU.DAY_0_6H_WINDOWS',TRUE);
	END CPE_SIX_H_AVG;

	------------------------------------------------------------------------------------------------

	PROCEDURE CPE_PIVOT_SIX_H(day_to_consider  IN DATE) IS 
	BEGIN
		v_start_time := dbms_utility.get_time; 
	    INSERT INTO HUMOREAU.CPE_VECTOR (
	    	-- We pivot those average to get one entry for each CPE
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
	    );
	    print_exec_time(print_perf,v_start_time,dbms_utility.get_time,'Updating CPE_VECTOR took ');
	    PRINT_DEBUG_INFO(debug,'HUMOREAU.CPE_VECTOR',TRUE);
	END CPE_PIVOT_SIX_H;

	/*
	================================================================================================
	============================================CREATE 1D VEC=======================================
	================================================================================================
	*/

	------------------------------------------------------------------------------------------------

	PROCEDURE VECTOR_1D(day_to_consider  IN DATE) IS 
	BEGIN
		v_start_time := dbms_utility.get_time;
     	-- we delete the old content as we have aggregated it daily (during the previous iteration) 
     	-- and put it in daily_aggregates
     	DELETE FROM HUMOREAU.VECTOR;
    	INSERT INTO HUMOREAU.VECTOR (
	        -- Merge the vectors for each CPE to have a single vector 
	        -- containing the CPE and CMTS UP/DN information consolidated
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
        );
	    print_exec_time(print_perf,v_start_time,dbms_utility.get_time,'Updating VECTOR took ');
	    PRINT_DEBUG_INFO(debug,'HUMOREAU.VECTOR',TRUE);
	END VECTOR_1D;

	-------------------------------------------------------------------------------

	PROCEDURE DAILY_AVG_DAY_0(day_to_consider  IN DATE) IS 
	BEGIN
		v_start_time := dbms_utility.get_time;
		-- we always want to keep to two distinct dates in the table, therefore we delete the oldest one.
		DELETE FROM HUMOREAU.DAILY_AVG_DAY_0 WHERE TRUNC(DAY_0) = trunc(day_to_consider-2);

        INSERT INTO HUMOREAU.DAILY_AVG_DAY_0 (
        	-- inserts for the current day_0 the average of each each mesurement over the day (only consider 
        	-- the average over non null entries as we have already included this information 
        	-- into the calculated MISS_ measurements)
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
	        WHERE TRUNC(DAY_0) = TRUNC(day_to_consider)
	    	);
	    print_exec_time(print_perf,v_start_time,dbms_utility.get_time,'Updating DAILY_AVG_DAY_0 took ');
	    PRINT_DEBUG_INFO(debug,'HUMOREAU.DAILY_AVG_DAY_0',TRUE);	
	END DAILY_AVG_DAY_0;

	------------------------------------------------------------------------------------------------

	PROCEDURE DAILY_DIFFS(day_to_consider  IN DATE) IS 
	BEGIN
		v_start_time := dbms_utility.get_time;
		-- We wish to always keep the last 5 days of daily average differences into the table therefore 
		-- we delete the eldest entry
		DELETE FROM HUMOREAU.DAILY_AVG_DIFFS WHERE TRUNC(DAY_0) = trunc(day_to_consider-5); 
	    INSERT INTO HUMOREAU.DAILY_AVG_DIFFS (
	    	-- insert into the table the difference between each measurement 
	    	-- aggregate from day 0 and the one from day-1
	        SELECT * FROM (
	            WITH DAILY_MARKED AS (
	                SELECT 
	                    CASE WHEN TRUNC(DAY_0) = TRUNC(day_to_consider) THEN 0 ELSE 1 END AS D,
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
	                TRUNC(day_to_consider) AS DAY_0,
	                ---
	                "0_OFFLINE_PCT"                                             AS OFFLINE_PCT,
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
	    	));	
		print_exec_time(print_perf,v_start_time,dbms_utility.get_time,'Updating DAILY_AVG_DIFFS took ');	
		PRINT_DEBUG_INFO(debug,'HUMOREAU.DAILY_AVG_DIFFS',TRUE);	
	END DAILY_DIFFS;

	/*
	================================================================================================
	============================================VIA=================================================
	================================================================================================
	*/
	
    PROCEDURE SUBSET_MILESTONES AS 
    BEGIN
    	v_start_time := dbms_utility.get_time;
	    INSERT INTO HUMOREAU.SUBSET_MILESTONES (
			-- We create the subset of milestones that concern day_1 and internet flows to reduce the 
			-- complexity of our joins later on.
			SELECT * 
			FROM SCO.REP_VIA_MILESTONE_V 
			WHERE TRUNC(CALENDAR_DATE) = TRUNC(var_via_day_0+1) 
					AND UPPER(PROCESS_FLOW) = 'INTERNET'
		);

    	print_exec_time(print_perf,v_start_time,dbms_utility.get_time,'Updating SUBSET_MILESTONES took ');
		PRINT_DEBUG_INFO(debug,'HUMOREAU.SUBSET_MILESTONES',TRUE);
    END;

    ------------------------------------------------------------------------------------------------

    PROCEDURE DETECT_FULL_FLOWS AS 
    BEGIN
    	v_start_time := dbms_utility.get_time;
	    INSERT INTO HUMOREAU.FULL_FLOWS (
			-- It can happend that CSR starts many flows for one session but doesn't finish them all 
			-- therefore we detect those that have been finished
			SELECT
			    SESSION_ID, 
			    FLOW_ID,
			    'FULL' AS TAG
			FROM HUMOREAU.SUBSET_MILESTONES 
			WHERE 	(CNT_INTERACT_SENT > 0 OR CNT_CASE_SENT > 0)
		);
    	print_exec_time(print_perf,v_start_time,dbms_utility.get_time,'Updating FULL_FLOWS took ');
		PRINT_DEBUG_INFO(debug,'HUMOREAU.FULL_FLOWS',FALSE);
    END;

    ------------------------------------------------------------------------------------------------
    
    PROCEDURE TAG_FULL_FLOWS AS 
    BEGIN	
    	v_start_time := dbms_utility.get_time;
		INSERT INTO HUMOREAU.FULL_FLOWS_TAGGED (
	        -- We tag the full flows in the original tables
	        SELECT  T.TAG,
	                M.*
	        FROM HUMOREAU.SUBSET_MILESTONES M FULL OUTER JOIN HUMOREAU.FULL_FLOWS T 
	            ON M.SESSION_ID = T.SESSION_ID AND M.FLOW_ID = T.FLOW_ID
	    );

    	print_exec_time(print_perf,v_start_time,dbms_utility.get_time,'Updating FULL_FLOWS_TAGGED took ');
		PRINT_DEBUG_INFO(debug,'HUMOREAU.FULL_FLOWS_TAGGED',FALSE);
    END;

    ------------------------------------------------------------------------------------------------
    
    PROCEDURE FLAG_EVENTS AS 
    BEGIN	
    	v_start_time := dbms_utility.get_time;
		INSERT INTO HUMOREAU.FLAGGED_EVENT (
	        -- For each session, we find the event with the minimum id as we us the milestone name 
	        -- of this event as a label for the problem expereinced by the customer
	        SELECT SESSION_ID, MIN(EVENT_NO) AS EVENT_NO_MILESTONE, 1 AS STARTING_MILESTONE
	        FROM HUMOREAU.FULL_FLOWS_TAGGED
	        WHERE TAG = 'FULL'
	        GROUP BY SESSION_ID
	    );
    	print_exec_time(print_perf,v_start_time,dbms_utility.get_time,'Updating FLAGGED_EVENT took ');
		PRINT_DEBUG_INFO(debug,'HUMOREAU.FLAGGED_EVENT',FALSE);
    END;

    ------------------------------------------------------------------------------------------------
    
    PROCEDURE TAG_MILESTONES AS 
    BEGIN	
    	v_start_time := dbms_utility.get_time;
		INSERT INTO HUMOREAU.TAGGED_MILESTONE (
	        SELECT * FROM (
	            -- Finally we can tag such starting milestone in the original table.
	            SELECT F.STARTING_MILESTONE, R.* 
	            FROM HUMOREAU.SUBSET_MILESTONES R FULL OUTER JOIN HUMOREAU.FLAGGED_EVENT F 
	                ON R.SESSION_ID = F.SESSION_ID AND R.EVENT_NO = F.EVENT_NO_MILESTONE
	            ORDER BY R.SESSION_ID, R.EVENT_NO
	            )
		    );
	    print_exec_time(print_perf,v_start_time,dbms_utility.get_time,'Updating TAGGED_MILESTONE took ');
		PRINT_DEBUG_INFO(debug,'HUMOREAU.TAGGED_MILESTONE',FALSE);
    END;

    ------------------------------------------------------------------------------------------------

    PROCEDURE EXTRACT_MILESTONES AS 
    BEGIN	
    	v_start_time := dbms_utility.get_time;
		INSERT INTO HUMOREAU.MILESTONES (
			-- Finally we extract the milestones of interest 
			SELECT  START_TIME AS MILESTONE_START_T, 
			        EVENT_NO,
			        SESSION_ID, 
			        MILESTONE_NAME, 
			        PROCESS_FLOW
			FROM HUMOREAU.TAGGED_MILESTONE
			WHERE STARTING_MILESTONE = 1 
		);
    	print_exec_time(print_perf,v_start_time,dbms_utility.get_time,'Updating MILESTONES took ');
		PRINT_DEBUG_INFO(debug,'HUMOREAU.MILESTONES',FALSE);
    END;

    ------------------------------------------------------------------------------------------------

    PROCEDURE EXTRACT_VIA_DETAILS AS 
    BEGIN	
    	v_start_time := dbms_utility.get_time;
		INSERT INTO HUMOREAU.VIA_DETAILS (
			-- we get a table that contains all the information that we wish to extract from VIA 
			-- for full flows along with the label extracted (Milestone name)
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
		            TRUNC(var_via_day_0) AS "DAY_0"    
		    from HUMOREAU.MILESTONES M 
		            INNER JOIN SESSIONID_2_CASEID S 
		                ON M.SESSION_ID = S.SESSION_ID
		            INNER JOIN CUSTOMER_DETAILS C 
		                ON C.CASE_ID = S.CASE_ID
		        )
		    ); 
    	print_exec_time(print_perf,v_start_time,dbms_utility.get_time,'Updating VIA_DETAILS took ');
		PRINT_DEBUG_INFO(debug,'HUMOREAU.VIA_DETAILS',FALSE);
    END;

    ------------------------------------------------------------------------------------------------

    PROCEDURE TAG_SUCCESS AS 
    BEGIN	
    	v_start_time := dbms_utility.get_time;
	    INSERT INTO HUMOREAU.SUCCESS_FLAGS (
	    	-- we also use clarify interactions to determine whether the VIA session correspond to a First 
	    	-- Time Resolution thanks to the FTR flag (it gives us an increased certainty that the label is 
	    	-- correct, because if the problem was solved it means that the flow was correct most probably)
		    SELECT /*+ PARALLEL(16) */ 
		        INTERACTION_ID,
		        ACCOUNT_NUMBER,
		        CREATE_DATE,
		        EMP_ID,
		        FLG_FTR_0D_FLG,
		        FLG_FTR_7D_FLG
		    FROM SCO.REP_CLY_INTERACTION_V
		    WHERE TRUNC(CREATE_DATE) = TRUNC(var_via_day_0+1)
		);
    	print_exec_time(print_perf,v_start_time,dbms_utility.get_time,'Updating SUCCESS_FLAGS took ');
		PRINT_DEBUG_INFO(debug,'HUMOREAU.SUCCESS_FLAGS',FALSE);
    END;

    ------------------------------------------------------------------------------------------------

    PROCEDURE TENTATIVE_MATCH AS 
    BEGIN	
    	v_start_time := dbms_utility.get_time;
	    INSERT INTO HUMOREAU.TENTATIVE_MATCH (
		    -- We try to match clarify interactions to VIA sessions. Their is no common key on which 
		    -- we could join and therefore the strategy is as follow:
		    -- 		1. VIA case started the same day than the CLY interaction
		    -- 		2. Because it is VIA that will create the CLY interaction, the timestamp from VIA 
		    -- needs to be not after the CLY one.
		    --		3. The account numbers match
		    -- 		4. The employee ID match
		    --		5. We make sure that such match are never done on NULL values.
		    -- (Could contain duplicates)
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
		);
    	print_exec_time(print_perf,v_start_time,dbms_utility.get_time,'Updating TENTATIVE_MATCH took ');
		PRINT_DEBUG_INFO(debug,'HUMOREAU.TENTATIVE_MATCH',FALSE);
    END;

    ------------------------------------------------------------------------------------------------

    PROCEDURE FLAG_SUCCESSFUL_EVENTS AS 
    BEGIN	
    	v_start_time := dbms_utility.get_time;
		INSERT INTO HUMOREAU.FLAGGED_SUCESSFUL_SESSIONS (
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
		    )
		);
    	print_exec_time(print_perf,v_start_time,dbms_utility.get_time,'Updating FLAGGED_SUCESSFUL_SESSIONS took ');
		PRINT_DEBUG_INFO(debug,'HUMOREAU.FLAGGED_SUCESSFUL_SESSIONS',FALSE);
    END;

    ------------------------------------------------------------------------------------------------

    PROCEDURE GET_VIA_MACS AS 
    BEGIN	
    	v_start_time := dbms_utility.get_time;
		INSERT INTO HUMOREAU.VIA_MACS_NOT_CONFIRMED (
			-- In VIA we do not store the MAC address of the device concerned by a given call and therefore we need also to determine it.
			-- We cannot know for sure therefore we flag all the devices that we know for the account that called.
		    SELECT * FROM (
		    	WITH CLY_ID_2_MACS AS (
				    SELECT   
				     REPLACE(NODE_KEY,':') AS MAC,
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
			)
		); 
    	print_exec_time(print_perf,v_start_time,dbms_utility.get_time,'Updating VIA_MACS_NOT_CONFIRMED took ');
		PRINT_DEBUG_INFO(debug,'HUMOREAU.VIA_MACS_NOT_CONFIRMED',FALSE);
    END;

    ------------------------------------------------------------------------------------------------

    PROCEDURE FLAG_MACS AS 
    BEGIN	
    	v_start_time := dbms_utility.get_time;
	    INSERT INTO HUMOREAU.FTR_FLAGGED_MACS (
	    	-- Add the FTR flags to the VIA MACS.
			SELECT 
			    V.*,
			    FLG_FTR_7D_FLG,
			    FLG_FTR_0D_FLG
			FROM HUMOREAU.VIA_MACS_NOT_CONFIRMED V INNER JOIN FLAGGED_SUCESSFUL_SESSIONS S
			    ON V.SESSION_ID = S.SESSION_ID
			);
    	print_exec_time(print_perf,v_start_time,dbms_utility.get_time,'Updating FTR_FLAGGED_MACS took ');
		PRINT_DEBUG_INFO(debug,'HUMOREAU.FTR_FLAGGED_MACS',FALSE);
    END;

    ------------------------------------------------------------------------------------------------

    PROCEDURE BUILD_VIA_MACS AS 
    BEGIN	
    	v_start_time := dbms_utility.get_time;
    	-- we delete the previous VIA MACs as they should have already been unloaded to DMT.
    	DELETE FROM HUMOREAU.VIA_MACS;
	    INSERT INTO HUMOREAU.VIA_MACS (
	    	-- we insert all the details of VIA calls corresponging to Frist time resolutions.
		    SELECT * FROM HUMOREAU.FTR_FLAGGED_MACS
		   	WHERE FLG_FTR_7D_FLG = 1
			);
    	print_exec_time(print_perf,v_start_time,dbms_utility.get_time,'Updating VIA_MACS took ');
		PRINT_DEBUG_INFO(debug,'HUMOREAU.VIA_MACS',FALSE);
    END;


END CPE_FAIL_DETECTION_POC;
/