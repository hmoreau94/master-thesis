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
    This package will not compile in the absence of the buffer tables it uses. Therefore in order to initialize 
    the space of the database one should run:
    1. CPE_FAIL_DETECTION_POC_INIT.initialize;
    3. CPE_FAILURE_ERROR_LOG.reset;

    The tables to dump the vectors and Via macs will then have been created.

EXECUTION:
    -- to dump the results from DMP into DMT
    BEGIN
        HUMOREAU.CPE_FAIL_DETECTION_POC.MAIN_PROC;
    END;

    -- to fill in the SAMPLED_VECTOR Table
    BEGIN
        HUMOREAU.CPE_FAIL_DETECTION_POC.SAMPLE(n);
    END;

HELP: Here are some queries to check the content of buffer tables in DMP and DMT but also of the log_table. 

    SELECT * FROM LOG_TABLE ORDER BY LOG_DATE ;

    select distinct(day_0) from HUMOREAU.vector@DMT2DMP;
    select distinct(day_0) from HUMOREAU.DAILY_AVG_DIFFS@DMT2DMP order by day_0; 
    select distinct(day_0) from HUMOREAU.DAILY_AVG_DAY_0@DMT2DMP order by day_0;
    select distinct(day_0) from HUMOREAU.VIA_MACS@DMT2DMP order by day_0;

    select distinct(day_0) from HUMOREAU.vector_five_days_II order by day_0;
    select distinct(day_0) from HUMOREAU.VIA_MACS order by day_0;
*/


CREATE OR REPLACE PACKAGE CPE_FAIL_DETECTION_POC 
IS
    -- we create the type used by the sequences to sample from the large table VECTOR_FIVE_DAYS_II
    TYPE t_numbers IS TABLE OF NUMBER;

    /*
    First we declare the necessary variables
    */
    var_day_0                       DATE := SYSDATE - 1; -- sets day 0 for which the data is computed at execution
    var_via_day_0                   DATE := var_day_0 - 8; -- the day for which we look for failing CPEs
    print_perf                      BOOLEAN     := TRUE; -- to print the execution time of procedure in log_table

    old_row_count                   NUMBER;
    new_row_count                   NUMBER;
    num_errors                      NUMBER;
    already_there                   NUMBER;
    count_vectors                   NUMBER;
    v_max_date                      DATE;
    error_message                   varchar2(100);
    v_start_time_total              number(10);

    -- Exceptions
    failed_update                   EXCEPTION;
    failed_dmp                      EXCEPTION;
    invalid_state                   EXCEPTION;
    vectors_already_here            EXCEPTION;

    PROCEDURE MAIN_PROC;
    PROCEDURE UPDATE_LOGTABLE;
    PROCEDURE UPDATE_VECTOR;
    PROCEDURE UPDATE_VIA;

    -- UTILS
    PROCEDURE DELETE_ALL_TABLES;
    PROCEDURE SAMPLE(n_sample IN number);
    PROCEDURE PRINT_INSERTION_COUNT(old_c IN number, new_c IN number, table_name IN VARCHAR2, must_increase_count IN BOOLEAN);
    PROCEDURE ERROR_HANDLING;
    PROCEDURE PRINT_EXEC_TIME(should_print IN BOOLEAN, start_time IN number, end_time IN number, prefix VARCHAR2);
    FUNCTION GENERATE_SERIES(P_START IN PLS_INTEGER, P_END IN PLS_INTEGER, P_STEP  IN PLS_INTEGER:=1) RETURN T_NUMBERS PIPELINED;

END CPE_FAIL_DETECTION_POC;
/
CREATE OR REPLACE PACKAGE BODY CPE_FAIL_DETECTION_POC IS 

    /*
    Generates a serie of integers 

    @params:
    - P_START,P_END: Bounds
    - P_STEP: the steps for generating the serie
    */
    FUNCTION GENERATE_SERIES(P_START IN PLS_INTEGER, P_END IN PLS_INTEGER, P_STEP  IN PLS_INTEGER := 1) 
       RETURN T_NUMBERS PIPELINED 
    AS 
        V_I                     PLS_INTEGER := CASE WHEN P_START IS NULL THEN 1 ELSE P_START END; 
        V_STEP                  PLS_INTEGER := CASE WHEN P_STEP IS NULL OR P_STEP = 0 THEN 1 ELSE P_STEP END; 
        V_TERMINATING_VALUE     PLS_INTEGER :=  P_START + TRUNC(ABS(P_START-P_END) / ABS(V_STEP) ) * V_STEP; 
    BEGIN 
        -- Check for invalid inputs 
        IF ( P_START > P_END AND SIGN(P_STEP) = 1 ) OR ( P_START < P_END AND SIGN(P_STEP) = -1 ) THEN RETURN; 
        END IF; 
        -- Generate integers 
        LOOP 
            PIPE ROW ( V_I ); 
            EXIT WHEN ( V_I = V_TERMINATING_VALUE ); 
            V_I := V_I + V_STEP; 
        END LOOP; 
        RETURN; 
    END GENERATE_SERIES;



    -- UTILS
    /*
    Deletes all tables necessary for this package.
    */
    PROCEDURE DELETE_ALL_TABLES AS
    BEGIN
        EXECUTE IMMEDIATE 'DROP TABLE HUMOREAU.VECTOR_FIVE_DAYS_II';
        EXECUTE IMMEDIATE 'DROP TABLE HUMOREAU.VIA_MACS';
        EXECUTE IMMEDIATE 'DROP TABLE HUMOREAU.SAMPLED_VECTORS';
        -- and the logs
        EXECUTE IMMEDIATE 'DROP TABLE HUMOREAU.LOG_TABLE';
    END DELETE_ALL_TABLES;

    /*
    For debug purposes will insert in the log table the number of rows inserted and checks that it makes sense. 

    @params:
    - old_c: old count of entries in the table
    - new_c: new count of entries in the table
    - table_name: the table name
    - must_increase_count: set to true if we expect the number of inserted rows to be strictly positive

    @raises:
    - failed_update: if we expect the insertion to yield a higher number of rows and it isn't the case.
    */
    PROCEDURE PRINT_INSERTION_COUNT(old_c IN number, new_c IN number, table_name IN VARCHAR2, must_increase_count IN BOOLEAN) AS
    BEGIN
        CPE_FAILURE_ERROR_LOG.PUT_LINE(new_c - old_c|| ' rows inserted in '|| table_name,0,0);
        if(old_row_count >= new_row_count and must_increase_count) THEN
            RAISE failed_update;
        end if;
    END;

    /*
    Logs errors and rolls back the state of the database.
    */
    PROCEDURE ERROR_HANDLING AS
    BEGIN
        CPE_FAILURE_ERROR_LOG.PUT_LINE(error_message,1,0);
        ROLLBACK;
        RAISE_APPLICATION_ERROR(-20010,error_message);
    END;

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
    Will insert in SAMPLED_VECTORS a random sample from VECTOR_FIVE_DAYS. (Because the sick CPEs are rare, we will 
    always take them all in our sample and subsample the big table of all CPEs)

    @params: 
    - n_sample : number of entries from VECTOR_FIVE_DAYS_II we wish to subsample (the final number of sample may be different 
    depending on the number of collision between the sample and sick CPEs)
    */
    PROCEDURE SAMPLE(n_sample IN number) AS 
    BEGIN
        DELETE FROM SAMPLED_VECTORS;

        -- We look at the last sequence number generated by the seq object
        SELECT last_number into count_vectors
           FROM all_sequences 
        WHERE sequence_owner = 'HUMOREAU' AND sequence_name = 'ISEQ$$_10352324';

        INSERT INTO SAMPLED_VECTORS (
            SELECT * FROM (
                WITH RANDOM_IDS AS (
                    -- WE CREATE A SERIES OF RANDOM IDS
                    SELECT ID FROM ( 
                        SELECT ROUND(DBMS_RANDOM.VALUE*count_vectors) AS ID 
                        FROM TABLE(GENERATE_SERIES(1,n_sample))
                        ) 
                    GROUP  BY ID
                ), SUB_SAMPLE_VFD AS (
                    -- USE THESE RANDOM IDS TO CREATE A RANDOM SAMPLE FROM VECTOR_FIVE_DAYS_II
                    SELECT V.*, '' AS MILESTONE_NAME 
                    FROM HUMOREAU.VECTOR_FIVE_DAYS_II V
                    WHERE SEQ_ID IN (select * from RANDOM_IDS)
                ), FAILING_VECS AS (
                    -- GET THE VECTORS FOR THE 'FAILING' CPEs
                    SELECT VEC.*, VIA.MILESTONE_NAME
                    FROM VIA_MACS VIA INNER JOIN VECTOR_FIVE_DAYS_II VEC
                        ON VEC.DAY_0 = VIA.DAY_0 AND VEC.MAC = VIA.MAC
                )
                -- WE GROUP ANYTHING INTO ONE TABLE
                SELECT * FROM SUB_SAMPLE_VFD 
                UNION
                SELECT * FROM FAILING_VECS
                )
            );
        COMMIT;
    END SAMPLE;


    /*
    Main procedure: performs the construction of the Vector of 5 days history and will then dump it to the DMT tables.
    It will delete as soon as this is done on DMP to use space as efficiently as possible.
    */
    PROCEDURE MAIN_PROC IS 
    BEGIN
        v_start_time_total := dbms_utility.get_time; 
        UPDATE_LOGTABLE;
        CPE_FAILURE_ERROR_LOG.put_line('Building the vectors for day_0 = ' || to_char(var_day_0,'dd/mm/yyyy'),0,0);
        UPDATE_VECTOR;
        UPDATE_VIA;
        COMMIT;
        print_exec_time(print_perf,v_start_time_total,dbms_utility.get_time,'=========> TOTAL Running time ');
        CPE_FAILURE_ERROR_LOG.put_line('Completed successfully !',0,1);
    EXCEPTION
        WHEN failed_update THEN 
            error_message := 'The update was unsuccessful!';
            ERROR_HANDLING;
        WHEN failed_dmp THEN
            error_message := 'DMP run has encountered errors';
        WHEN invalid_state THEN
            error_message := 'The content of DMP isnt complient with day_0';
            ERROR_HANDLING;
        WHEN vectors_already_here THEN
            error_message := 'VECTOR_FIVE_DAYS already contains the vectors for day_0 = ' || to_char(var_day_0,'dd/mm/yyyy');
            ERROR_HANDLING;
        WHEN OTHERS THEN 
            error_message := 'Error code ' || SQLCODE || ': ' || SQLERRM;
            ERROR_HANDLING;
    END MAIN_PROC;


    /*
    Checks that the DMP procedure has execuyed correctly and will dump the content 
    of the DMP logtable to DMT one before deleting its content. 
    */
    PROCEDURE UPDATE_LOGTABLE AS
    BEGIN
        DELETE from HUMOREAU.log_table;
        -- We first check that no problem occured when collecting data in DMP
        SELECT COUNT(*) INTO num_errors FROM HUMOREAU.LOG_TABLE@DMT2DMP WHERE TRUNC(LOG_DATE) = TRUNC(SYSDATE) AND IS_ERROR = 1;
        if(num_errors > 0) then 
            raise failed_dmp;
        end if;

        -- then we dump the table to DMT and erase its content in DMP
        SELECT COUNT(*) INTO old_row_count FROM HUMOREAU.LOG_TABLE;
        INSERT INTO HUMOREAU.LOG_TABLE ( SELECT * FROM HUMOREAU.LOG_TABLE@DMT2DMP );
        DELETE FROM HUMOREAU.LOG_TABLE@DMT2DMP;
        SELECT COUNT(*) INTO new_row_count FROM HUMOREAU.LOG_TABLE;
        PRINT_INSERTION_COUNT(old_row_count, new_row_count, 'LOG_TABLE',TRUE);
    END UPDATE_LOGTABLE;

    /*
    Check the validity of the state of DMP buffer tables before constructing the five day vectors and adding a sequence 
    number to each entry in order to be able to easily sample from the resulting big table
    */
    PROCEDURE UPDATE_VECTOR AS
    BEGIN
        /*
            Checking the validity of the data in DMP to build the vectors for day_0
        */
        SELECT /*+ Parallel(16) */ DISTINCT(DAY_0) INTO v_max_date FROM HUMOREAU.VECTOR@DMT2DMP;
        IF(TRUNC(v_max_date) != TRUNC(VAR_DAY_0)) THEN 
            RAISE INVALID_STATE;
        END IF;

        SELECT /*+ Parallel(16) */ MAX(DISTINCT(DAY_0)) INTO v_max_date FROM HUMOREAU.DAILY_AVG_DIFFS@DMT2DMP;
        IF(TRUNC(v_max_date) != TRUNC(VAR_DAY_0)) THEN 
            RAISE INVALID_STATE;
        END IF;

        SELECT /*+ Parallel(16) */ MAX(DISTINCT(DAY_0)) INTO v_max_date FROM HUMOREAU.DAILY_AVG_DAY_0@DMT2DMP;
        IF(TRUNC(v_max_date) != TRUNC(VAR_DAY_0)) THEN 
            RAISE INVALID_STATE;
        END IF;

        SELECT /*+ Parallel(16) */ COUNT(*) INTO already_there FROM HUMOREAU.VECTOR_FIVE_DAYS_II WHERE TRUNC(DAY_0) = TRUNC(v_max_date);
        if(already_there > 0) THEN 
            RAISE vectors_already_here;
        END IF;

        /*
            Performing the update
        */
        SELECT COUNT(*) INTO old_row_count FROM HUMOREAU.VECTOR_FIVE_DAYS_II;
        INSERT INTO HUMOREAU.VECTOR_FIVE_DAYS_II (
            DAY_0,
            MAC,
            CLY_ACCOUNT_NUMBER,
            SAA_ACCOUNT_NUMBER,
            CMTS,
            SERVICE_GROUP,
            HARDWARE_MODEL,
            N_CPE_BUILDING,
            OFFLINE_PCT_6H,
            OFFLINE_PCT_12H,
            OFFLINE_PCT_18H,
            OFFLINE_PCT_24H,
            OFFLINE_PCT,
            OFFLINE_PCT_1D,
            OFFLINE_PCT_2D,
            OFFLINE_PCT_3D,
            OFFLINE_PCT_4D,
            OFFLINE_PCT_5D,
            CER_DN_6H,
            CER_DN_12H,
            CER_DN_18H,
            CER_DN,
            CER_DN_1D,
            CER_DN_2D,
            CER_DN_3D,
            CER_DN_4D,
            CER_DN_5D,
            MISS_CER_DN_6H,
            MISS_CER_DN_12H,
            MISS_CER_DN_18H,
            MISS_CER_DN_24H,
            MISS_CER_DN,
            MISS_CER_DN_1D,
            MISS_CER_DN_2D,
            MISS_CER_DN_3D,
            MISS_CER_DN_4D,
            MISS_CER_DN_5D,
            CER_UP_6H,
            CER_UP_12H,
            CER_UP_18H,
            CER_UP,
            CER_UP_1D,
            CER_UP_2D,
            CER_UP_3D,
            CER_UP_4D,
            CER_UP_5D,
            MISS_CER_UP_6H,
            MISS_CER_UP_12H,
            MISS_CER_UP_18H,
            MISS_CER_UP_24H,
            MISS_CER_UP,
            MISS_CER_UP_1D,
            MISS_CER_UP_2D,
            MISS_CER_UP_3D,
            MISS_CER_UP_4D,
            MISS_CER_UP_5D,
            PCT_TRAFFIC_DMH_UP_6H,
            PCT_TRAFFIC_DMH_UP_12H,
            PCT_TRAFFIC_DMH_UP_18H,
            PCT_TRAFFIC_DMH_UP,
            PCT_TRAFFIC_DMH_UP_1D,
            PCT_TRAFFIC_DMH_UP_2D,
            PCT_TRAFFIC_DMH_UP_3D,
            PCT_TRAFFIC_DMH_UP_4D,
            PCT_TRAFFIC_DMH_UP_5D,
            MISS_PCT_TRAFFIC_DMH_UP_6H,
            MISS_PCT_TRAFFIC_DMH_UP_12H,
            MISS_PCT_TRAFFIC_DMH_UP_18H,
            MISS_PCT_TRAFFIC_DMH_UP_24H,
            MISS_PCT_TRAFFIC_DMH_UP,
            MISS_PCT_TRAFFIC_DMH_UP_1D,
            MISS_PCT_TRAFFIC_DMH_UP_2D,
            MISS_PCT_TRAFFIC_DMH_UP_3D,
            MISS_PCT_TRAFFIC_DMH_UP_4D,
            MISS_PCT_TRAFFIC_DMH_UP_5D,
            PCT_TRAFFIC_SDMH_UP_6H,
            PCT_TRAFFIC_SDMH_UP_12H,
            PCT_TRAFFIC_SDMH_UP_18H,
            PCT_TRAFFIC_SDMH_UP,
            PCT_TRAFFIC_SDMH_UP_1D,
            PCT_TRAFFIC_SDMH_UP_2D,
            PCT_TRAFFIC_SDMH_UP_3D,
            PCT_TRAFFIC_SDMH_UP_4D,
            PCT_TRAFFIC_SDMH_UP_5D,
            MISS_PCT_TRAFFIC_SDMH_UP_6H,
            MISS_PCT_TRAFFIC_SDMH_UP_12H,
            MISS_PCT_TRAFFIC_SDMH_UP_18H,
            MISS_PCT_TRAFFIC_SDMH_UP_24H,
            MISS_PCT_TRAFFIC_SDMH_UP,
            MISS_PCT_TRAFFIC_SDMH_UP_1D,
            MISS_PCT_TRAFFIC_SDMH_UP_2D,
            MISS_PCT_TRAFFIC_SDMH_UP_3D,
            MISS_PCT_TRAFFIC_SDMH_UP_4D,
            MISS_PCT_TRAFFIC_SDMH_UP_5D,
            RX_DN_6H,
            RX_DN_12H,
            RX_DN_18H,
            RX_DN,
            RX_DN_1D,
            RX_DN_2D,
            RX_DN_3D,
            RX_DN_4D,
            RX_DN_5D,
            MISS_RX_DN_6H,
            MISS_RX_DN_12H,
            MISS_RX_DN_18H,
            MISS_RX_DN_24H,
            MISS_RX_DN,
            MISS_RX_DN_1D,
            MISS_RX_DN_2D,
            MISS_RX_DN_3D,
            MISS_RX_DN_4D,
            MISS_RX_DN_5D,
            RX_UP_6H,
            RX_UP_12H,
            RX_UP_18H,
            RX_UP,
            RX_UP_1D,
            RX_UP_2D,
            RX_UP_3D,
            RX_UP_4D,
            RX_UP_5D,
            MISS_RX_UP_6H,
            MISS_RX_UP_12H,
            MISS_RX_UP_18H,
            MISS_RX_UP_24H,
            MISS_RX_UP,
            MISS_RX_UP_1D,
            MISS_RX_UP_2D,
            MISS_RX_UP_3D,
            MISS_RX_UP_4D,
            MISS_RX_UP_5D,
            SNR_DN_6H,
            SNR_DN_12H,
            SNR_DN_18H,
            SNR_DN,
            SNR_DN_1D,
            SNR_DN_2D,
            SNR_DN_3D,
            SNR_DN_4D,
            SNR_DN_5D,
            MISS_SNR_DN_6H,
            MISS_SNR_DN_12H,
            MISS_SNR_DN_18H,
            MISS_SNR_DN_24H,
            MISS_SNR_DN,
            MISS_SNR_DN_1D,
            MISS_SNR_DN_2D,
            MISS_SNR_DN_3D,
            MISS_SNR_DN_4D,
            MISS_SNR_DN_5D,
            SNR_UP_6H,
            SNR_UP_12H,
            SNR_UP_18H,
            SNR_UP,
            SNR_UP_1D,
            SNR_UP_2D,
            SNR_UP_3D,
            SNR_UP_4D,
            SNR_UP_5D,
            MISS_SNR_UP_6H,
            MISS_SNR_UP_12H,
            MISS_SNR_UP_18H,
            MISS_SNR_UP_24H,
            MISS_SNR_UP,
            MISS_SNR_UP_1D,
            MISS_SNR_UP_2D,
            MISS_SNR_UP_3D,
            MISS_SNR_UP_4D,
            MISS_SNR_UP_5D,
            TX_UP_6H,
            TX_UP_12H,
            TX_UP_18H,
            TX_UP,
            TX_UP_1D,
            TX_UP_2D,
            TX_UP_3D,
            TX_UP_4D,
            TX_UP_5D,
            MISS_TX_UP_6H,
            MISS_TX_UP_12H,
            MISS_TX_UP_18H,
            MISS_TX_UP_24H,
            MISS_TX_UP,
            MISS_TX_UP_1D,
            MISS_TX_UP_2D,
            MISS_TX_UP_3D,
            MISS_TX_UP_4D,
            MISS_TX_UP_5D,
            CMTS_RX_UP_6H,
            CMTS_RX_UP_12H,
            CMTS_RX_UP_18H,
            CMTS_RX_UP ,
            CMTS_RX_UP_1D ,
            CMTS_RX_UP_2D ,
            CMTS_RX_UP_3D ,
            CMTS_RX_UP_4D ,
            CMTS_RX_UP_5D ,
            CMTS_TX_UP_6H,
            CMTS_TX_UP_12H,
            CMTS_TX_UP_18H,
            CMTS_TX_UP ,
            CMTS_TX_UP_1D ,
            CMTS_TX_UP_2D ,
            CMTS_TX_UP_3D ,
            CMTS_TX_UP_4D ,
            CMTS_TX_UP_5D ,
            CMTS_CER_UP_6H,
            CMTS_CER_UP_12H,
            CMTS_CER_UP_18H,
            CMTS_CER_UP ,
            CMTS_CER_UP_1D ,
            CMTS_CER_UP_2D ,
            CMTS_CER_UP_3D ,
            CMTS_CER_UP_4D ,
            CMTS_CER_UP_5D ,
            CMTS_UTILIZATION_UP_6H,
            CMTS_UTILIZATION_UP_12H,
            CMTS_UTILIZATION_UP_18H,
            CMTS_UTILIZATION_UP ,
            CMTS_UTILIZATION_UP_1D ,
            CMTS_UTILIZATION_UP_2D ,
            CMTS_UTILIZATION_UP_3D ,
            CMTS_UTILIZATION_UP_4D ,
            CMTS_UTILIZATION_UP_5D ,
            CMTS_MS_UTILIZATION_UP_6H,
            CMTS_MS_UTILIZATION_UP_12H,
            CMTS_MS_UTILIZATION_UP_18H,
            CMTS_MS_UTILIZATION_UP ,
            CMTS_MS_UTILIZATION_UP_1D ,
            CMTS_MS_UTILIZATION_UP_2D ,
            CMTS_MS_UTILIZATION_UP_3D ,
            CMTS_MS_UTILIZATION_UP_4D ,
            CMTS_MS_UTILIZATION_UP_5D ,
            CMTS_F_MS_UTILIZATION_UP_6H,
            CMTS_F_MS_UTILIZATION_UP_12H,
            CMTS_F_MS_UTILIZATION_UP_18H,
            CMTS_F_MS_UTILIZATION_UP,
            CMTS_F_MS_UTILIZATION_UP_1D ,
            CMTS_F_MS_UTILIZATION_UP_2D ,
            CMTS_F_MS_UTILIZATION_UP_3D ,
            CMTS_F_MS_UTILIZATION_UP_4D ,
            CMTS_F_MS_UTILIZATION_UP_5D,
            CMTS_RX_DN_6H,
            CMTS_RX_DN_12H,
            CMTS_RX_DN_18H,
            CMTS_RX_DN,
            CMTS_RX_DN_1D,
            CMTS_RX_DN_2D,
            CMTS_RX_DN_3D,
            CMTS_RX_DN_4D,
            CMTS_RX_DN_5D,
            CMTS_SNR_DN_6H,
            CMTS_SNR_DN_12H,
            CMTS_SNR_DN_18H,
            CMTS_SNR_DN,
            CMTS_SNR_DN_1D,
            CMTS_SNR_DN_2D,
            CMTS_SNR_DN_3D,
            CMTS_SNR_DN_4D,
            CMTS_SNR_DN_5D,
            CMTS_CCER_DN_6H,
            CMTS_CCER_DN_12H,
            CMTS_CCER_DN_18H,
            CMTS_CCER_DN,
            CMTS_CCER_DN_1D,
            CMTS_CCER_DN_2D,
            CMTS_CCER_DN_3D,
            CMTS_CCER_DN_4D,
            CMTS_CCER_DN_5D,
            CMTS_CER_DN_6H,
            CMTS_CER_DN_12H,
            CMTS_CER_DN_18H,
            CMTS_CER_DN,
            CMTS_CER_DN_1D,
            CMTS_CER_DN_2D,
            CMTS_CER_DN_3D,
            CMTS_CER_DN_4D,
            CMTS_CER_DN_5D,
            CMTS_UTILIZATION_DN_6H,
            CMTS_UTILIZATION_DN_12H,
            CMTS_UTILIZATION_DN_18H,
            CMTS_UTILIZATION_DN,
            CMTS_UTILIZATION_DN_1D,
            CMTS_UTILIZATION_DN_2D,
            CMTS_UTILIZATION_DN_3D,
            CMTS_UTILIZATION_DN_4D,
            CMTS_UTILIZATION_DN_5D
            )
         (
            SELECT *
            FROM (
                WITH DAILY_MARKED AS (
                        SELECT 
                            MAC, 
                            CASE 
                                WHEN TRUNC(DAY_0,'DD') = TRUNC(var_day_0, 'DD') THEN 0
                                WHEN TRUNC(DAY_0,'DD') = TRUNC(var_day_0-1, 'DD') THEN 1
                                WHEN TRUNC(DAY_0,'DD') = TRUNC(var_day_0-2, 'DD') THEN 2
                                WHEN TRUNC(DAY_0,'DD') = TRUNC(var_day_0-3, 'DD') THEN 3
                                WHEN TRUNC(DAY_0,'DD') = TRUNC(var_day_0-4, 'DD') THEN 4
                            END AS D,
                            OFFLINE_PCT,
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
                            CMTS_RX_UP,
                            CMTS_TX_UP,
                            CMTS_CER_UP,
                            CMTS_UTILIZATION_UP,
                            CMTS_MS_UTILIZATION_UP,
                            CMTS_F_MS_UTILIZATION_UP,
                            CMTS_RX_DN,
                            CMTS_SNR_DN,
                            CMTS_CCER_DN,
                            CMTS_CER_DN,
                            CMTS_UTILIZATION_DN
                        FROM HUMOREAU.DAILY_AVG_DIFFS@DMT2DMP
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
                        FOR D IN (0,1,2,3,4)
                        )
                    ), REFACTORED AS (
                        SELECT
                        MAC,
                        "0_OFFLINE_PCT" AS OFFLINE_PCT_1D,
                        "1_OFFLINE_PCT" AS OFFLINE_PCT_2D,
                        "2_OFFLINE_PCT" AS OFFLINE_PCT_3D,
                        "3_OFFLINE_PCT" AS OFFLINE_PCT_4D,
                        "4_OFFLINE_PCT" AS OFFLINE_PCT_5D,
                        ---
                        "0_CER_DN" AS CER_DN_1D,
                        "1_CER_DN" AS CER_DN_2D,
                        "2_CER_DN" AS CER_DN_3D,
                        "3_CER_DN" AS CER_DN_4D,
                        "4_CER_DN" AS CER_DN_5D,
                        ---
                        "0_MISS_CER_DN" AS MISS_CER_DN_1D,
                        "1_MISS_CER_DN" AS MISS_CER_DN_2D,
                        "2_MISS_CER_DN" AS MISS_CER_DN_3D,
                        "3_MISS_CER_DN" AS MISS_CER_DN_4D,
                        "4_MISS_CER_DN" AS MISS_CER_DN_5D,
                        ---
                        "0_CER_UP" AS CER_UP_1D,
                        "1_CER_UP" AS CER_UP_2D,
                        "2_CER_UP" AS CER_UP_3D,
                        "3_CER_UP" AS CER_UP_4D,
                        "4_CER_UP" AS CER_UP_5D,
                        ---
                        "0_MISS_CER_UP" AS MISS_CER_UP_1D,
                        "1_MISS_CER_UP" AS MISS_CER_UP_2D,
                        "2_MISS_CER_UP" AS MISS_CER_UP_3D,
                        "3_MISS_CER_UP" AS MISS_CER_UP_4D,
                        "4_MISS_CER_UP" AS MISS_CER_UP_5D,
                        ---
                        "0_PCT_TRAFFIC_DMH_UP" AS PCT_TRAFFIC_DMH_UP_1D,
                        "1_PCT_TRAFFIC_DMH_UP" AS PCT_TRAFFIC_DMH_UP_2D,
                        "2_PCT_TRAFFIC_DMH_UP" AS PCT_TRAFFIC_DMH_UP_3D,
                        "3_PCT_TRAFFIC_DMH_UP" AS PCT_TRAFFIC_DMH_UP_4D,
                        "4_PCT_TRAFFIC_DMH_UP" AS PCT_TRAFFIC_DMH_UP_5D,
                        ---
                        "0_MISS_PCT_TRAFFIC_DMH_UP" AS MISS_PCT_TRAFFIC_DMH_UP_1D,
                        "1_MISS_PCT_TRAFFIC_DMH_UP" AS MISS_PCT_TRAFFIC_DMH_UP_2D,
                        "2_MISS_PCT_TRAFFIC_DMH_UP" AS MISS_PCT_TRAFFIC_DMH_UP_3D,
                        "3_MISS_PCT_TRAFFIC_DMH_UP" AS MISS_PCT_TRAFFIC_DMH_UP_4D,
                        "4_MISS_PCT_TRAFFIC_DMH_UP" AS MISS_PCT_TRAFFIC_DMH_UP_5D,
                        ---
                        "0_PCT_TRAFFIC_SDMH_UP" AS PCT_TRAFFIC_SDMH_UP_1D,
                        "1_PCT_TRAFFIC_SDMH_UP" AS PCT_TRAFFIC_SDMH_UP_2D,
                        "2_PCT_TRAFFIC_SDMH_UP" AS PCT_TRAFFIC_SDMH_UP_3D,
                        "3_PCT_TRAFFIC_SDMH_UP" AS PCT_TRAFFIC_SDMH_UP_4D,
                        "4_PCT_TRAFFIC_SDMH_UP" AS PCT_TRAFFIC_SDMH_UP_5D,
                        ---
                        "0_MISS_PCT_TRAFFIC_SDMH_UP" AS MISS_PCT_TRAFFIC_SDMH_UP_1D,
                        "1_MISS_PCT_TRAFFIC_SDMH_UP" AS MISS_PCT_TRAFFIC_SDMH_UP_2D,
                        "2_MISS_PCT_TRAFFIC_SDMH_UP" AS MISS_PCT_TRAFFIC_SDMH_UP_3D,
                        "3_MISS_PCT_TRAFFIC_SDMH_UP" AS MISS_PCT_TRAFFIC_SDMH_UP_4D,
                        "4_MISS_PCT_TRAFFIC_SDMH_UP" AS MISS_PCT_TRAFFIC_SDMH_UP_5D,
                        ---
                        "0_RX_DN" AS RX_DN_1D,
                        "1_RX_DN" AS RX_DN_2D,
                        "2_RX_DN" AS RX_DN_3D,
                        "3_RX_DN" AS RX_DN_4D,
                        "4_RX_DN" AS RX_DN_5D,
                        ---
                        "0_MISS_RX_DN" AS MISS_RX_DN_1D,
                        "1_MISS_RX_DN" AS MISS_RX_DN_2D,
                        "2_MISS_RX_DN" AS MISS_RX_DN_3D,
                        "3_MISS_RX_DN" AS MISS_RX_DN_4D,
                        "4_MISS_RX_DN" AS MISS_RX_DN_5D,
                        ---
                        "0_RX_UP" AS RX_UP_1D,
                        "1_RX_UP" AS RX_UP_2D,
                        "2_RX_UP" AS RX_UP_3D,
                        "3_RX_UP" AS RX_UP_4D,
                        "4_RX_UP" AS RX_UP_5D,
                        ---
                        "0_MISS_RX_UP" AS MISS_RX_UP_1D,
                        "1_MISS_RX_UP" AS MISS_RX_UP_2D,
                        "2_MISS_RX_UP" AS MISS_RX_UP_3D,
                        "3_MISS_RX_UP" AS MISS_RX_UP_4D,
                        "4_MISS_RX_UP" AS MISS_RX_UP_5D,
                        ---
                        "0_SNR_DN" AS SNR_DN_1D,
                        "1_SNR_DN" AS SNR_DN_2D,
                        "2_SNR_DN" AS SNR_DN_3D,
                        "3_SNR_DN" AS SNR_DN_4D,
                        "4_SNR_DN" AS SNR_DN_5D,
                        ---
                        "0_MISS_SNR_DN" AS MISS_SNR_DN_1D,
                        "1_MISS_SNR_DN" AS MISS_SNR_DN_2D,
                        "2_MISS_SNR_DN" AS MISS_SNR_DN_3D,
                        "3_MISS_SNR_DN" AS MISS_SNR_DN_4D,
                        "4_MISS_SNR_DN" AS MISS_SNR_DN_5D,
                        ---
                        "0_SNR_UP" AS SNR_UP_1D,
                        "1_SNR_UP" AS SNR_UP_2D,
                        "2_SNR_UP" AS SNR_UP_3D,
                        "3_SNR_UP" AS SNR_UP_4D,
                        "4_SNR_UP" AS SNR_UP_5D,
                        ---
                        "0_MISS_SNR_UP" AS MISS_SNR_UP_1D,
                        "1_MISS_SNR_UP" AS MISS_SNR_UP_2D,
                        "2_MISS_SNR_UP" AS MISS_SNR_UP_3D,
                        "3_MISS_SNR_UP" AS MISS_SNR_UP_4D,
                        "4_MISS_SNR_UP" AS MISS_SNR_UP_5D,
                        ---
                        "0_TX_UP" AS TX_UP_1D,
                        "1_TX_UP" AS TX_UP_2D,
                        "2_TX_UP" AS TX_UP_3D,
                        "3_TX_UP" AS TX_UP_4D,
                        "4_TX_UP" AS TX_UP_5D,
                        ---
                        "0_MISS_TX_UP" AS MISS_TX_UP_1D,
                        "1_MISS_TX_UP" AS MISS_TX_UP_2D,
                        "2_MISS_TX_UP" AS MISS_TX_UP_3D,
                        "3_MISS_TX_UP" AS MISS_TX_UP_4D,
                        "4_MISS_TX_UP" AS MISS_TX_UP_5D,
                        ---
                        "0_CMTS_RX_UP" AS CMTS_RX_UP_1D,
                        "1_CMTS_RX_UP" AS CMTS_RX_UP_2D,
                        "2_CMTS_RX_UP" AS CMTS_RX_UP_3D,
                        "3_CMTS_RX_UP" AS CMTS_RX_UP_4D,
                        "4_CMTS_RX_UP" AS CMTS_RX_UP_5D,
                        ---
                        "0_CMTS_TX_UP" AS CMTS_TX_UP_1D,
                        "1_CMTS_TX_UP" AS CMTS_TX_UP_2D,
                        "2_CMTS_TX_UP" AS CMTS_TX_UP_3D,
                        "3_CMTS_TX_UP" AS CMTS_TX_UP_4D,
                        "4_CMTS_TX_UP" AS CMTS_TX_UP_5D,
                        ---
                        "0_CMTS_CER_UP" AS CMTS_CER_UP_1D,
                        "1_CMTS_CER_UP" AS CMTS_CER_UP_2D,
                        "2_CMTS_CER_UP" AS CMTS_CER_UP_3D,
                        "3_CMTS_CER_UP" AS CMTS_CER_UP_4D,
                        "4_CMTS_CER_UP" AS CMTS_CER_UP_5D,
                        ---
                        "0_CMTS_UTILIZATION_UP" AS CMTS_UTILIZATION_UP_1D,
                        "1_CMTS_UTILIZATION_UP" AS CMTS_UTILIZATION_UP_2D,
                        "2_CMTS_UTILIZATION_UP" AS CMTS_UTILIZATION_UP_3D,
                        "3_CMTS_UTILIZATION_UP" AS CMTS_UTILIZATION_UP_4D,
                        "4_CMTS_UTILIZATION_UP" AS CMTS_UTILIZATION_UP_5D,
                        ---
                        "0_CMTS_MS_UTILIZATION_UP" AS CMTS_MS_UTILIZATION_UP_1D,
                        "1_CMTS_MS_UTILIZATION_UP" AS CMTS_MS_UTILIZATION_UP_2D,
                        "2_CMTS_MS_UTILIZATION_UP" AS CMTS_MS_UTILIZATION_UP_3D,
                        "3_CMTS_MS_UTILIZATION_UP" AS CMTS_MS_UTILIZATION_UP_4D,
                        "4_CMTS_MS_UTILIZATION_UP" AS CMTS_MS_UTILIZATION_UP_5D,
                        ---
                        "0_CMTS_F_MS_UTILIZATION_UP" AS CMTS_F_MS_UTILIZATION_UP_1D,
                        "1_CMTS_F_MS_UTILIZATION_UP" AS CMTS_F_MS_UTILIZATION_UP_2D,
                        "2_CMTS_F_MS_UTILIZATION_UP" AS CMTS_F_MS_UTILIZATION_UP_3D,
                        "3_CMTS_F_MS_UTILIZATION_UP" AS CMTS_F_MS_UTILIZATION_UP_4D,
                        "4_CMTS_F_MS_UTILIZATION_UP" AS CMTS_F_MS_UTILIZATION_UP_5D,
                        ---
                        "0_CMTS_RX_DN" AS CMTS_RX_DN_1D,
                        "1_CMTS_RX_DN" AS CMTS_RX_DN_2D,
                        "2_CMTS_RX_DN" AS CMTS_RX_DN_3D,
                        "3_CMTS_RX_DN" AS CMTS_RX_DN_4D,
                        "4_CMTS_RX_DN" AS CMTS_RX_DN_5D,
                        ---
                        "0_CMTS_SNR_DN" AS CMTS_SNR_DN_1D,
                        "1_CMTS_SNR_DN" AS CMTS_SNR_DN_2D,
                        "2_CMTS_SNR_DN" AS CMTS_SNR_DN_3D,
                        "3_CMTS_SNR_DN" AS CMTS_SNR_DN_4D,
                        "4_CMTS_SNR_DN" AS CMTS_SNR_DN_5D,
                        ---
                        "0_CMTS_CCER_DN" AS CMTS_CCER_DN_1D,
                        "1_CMTS_CCER_DN" AS CMTS_CCER_DN_2D,
                        "2_CMTS_CCER_DN" AS CMTS_CCER_DN_3D,
                        "3_CMTS_CCER_DN" AS CMTS_CCER_DN_4D,
                        "4_CMTS_CCER_DN" AS CMTS_CCER_DN_5D,
                        ---
                        "0_CMTS_CER_DN" AS CMTS_CER_DN_1D,
                        "1_CMTS_CER_DN" AS CMTS_CER_DN_2D,
                        "2_CMTS_CER_DN" AS CMTS_CER_DN_3D,
                        "3_CMTS_CER_DN" AS CMTS_CER_DN_4D,
                        "4_CMTS_CER_DN" AS CMTS_CER_DN_5D,
                        ---
                        "0_CMTS_UTILIZATION_DN" AS CMTS_UTILIZATION_DN_1D,
                        "1_CMTS_UTILIZATION_DN" AS CMTS_UTILIZATION_DN_2D,
                        "2_CMTS_UTILIZATION_DN" AS CMTS_UTILIZATION_DN_3D,
                        "3_CMTS_UTILIZATION_DN" AS CMTS_UTILIZATION_DN_4D,
                        "4_CMTS_UTILIZATION_DN" AS CMTS_UTILIZATION_DN_5D
                        FROM PIVOTED
                    )
                    -- now we can join everything
                    SELECT 
                        V.DAY_0,
                        V.MAC,
                        V.CLY_ACCOUNT_NUMBER,
                        V.SAA_ACCOUNT_NUMBER,
                        V.CMTS,
                        V.SERVICE_GROUP,
                        V.HARDWARE_MODEL,
                        V.N_CPE_BUILDING,
                        
                        "0_OFFLINE_PCT" as OFFLINE_PCT_6H,
                        "1_OFFLINE_PCT" as OFFLINE_PCT_12H,
                        "2_OFFLINE_PCT" as OFFLINE_PCT_18H,
                        "3_OFFLINE_PCT" as OFFLINE_PCT_24H,
                        OFFLINE_PCT,
                        OFFLINE_PCT_1D,
                        OFFLINE_PCT_2D,
                        OFFLINE_PCT_3D,
                        OFFLINE_PCT_4D,
                        OFFLINE_PCT_5D,
                        ----- CPE
                        "0_CER_DN" - "1_CER_DN" AS "CER_DN_6H",
                        "1_CER_DN" - "2_CER_DN" AS "CER_DN_12H",
                        "2_CER_DN" - "3_CER_DN" AS "CER_DN_18H",
                        CER_DN,
                        CER_DN_1D,
                        CER_DN_2D,
                        CER_DN_3D,
                        CER_DN_4D,
                        CER_DN_5D,
                        ----
                        "0_MISS_CER_DN" AS "MISS_CER_DN_6H",
                        "1_MISS_CER_DN" AS "MISS_CER_DN_12H",
                        "2_MISS_CER_DN" AS "MISS_CER_DN_18H",
                        "3_MISS_CER_DN" AS "MISS_CER_DN_24H",
                        MISS_CER_DN,
                        MISS_CER_DN_1D,
                        MISS_CER_DN_2D,
                        MISS_CER_DN_3D,
                        MISS_CER_DN_4D,
                        MISS_CER_DN_5D,
                        ----
                        "0_CER_UP" - "1_CER_UP" AS "CER_UP_6H",
                        "1_CER_UP" - "2_CER_UP" AS "CER_UP_12H",
                        "2_CER_UP" - "3_CER_UP" AS "CER_UP_18H",
                        CER_UP,
                        CER_UP_1D,
                        CER_UP_2D,
                        CER_UP_3D,
                        CER_UP_4D,
                        CER_UP_5D,
                        ----
                        "0_MISS_CER_UP" AS "MISS_CER_UP_6H",
                        "1_MISS_CER_UP" AS "MISS_CER_UP_12H",
                        "2_MISS_CER_UP" AS "MISS_CER_UP_18H",
                        "3_MISS_CER_UP" AS "MISS_CER_UP_24H",
                        MISS_CER_UP,
                        MISS_CER_UP_1D,
                        MISS_CER_UP_2D,
                        MISS_CER_UP_3D,
                        MISS_CER_UP_4D,
                        MISS_CER_UP_5D,
                        ----
                        "0_PCT_TRAFFIC_DMH_UP" - "1_PCT_TRAFFIC_DMH_UP" AS "PCT_TRAFFIC_DMH_UP_6H",
                        "1_PCT_TRAFFIC_DMH_UP" - "2_PCT_TRAFFIC_DMH_UP" AS "PCT_TRAFFIC_DMH_UP_12H",
                        "2_PCT_TRAFFIC_DMH_UP" - "3_PCT_TRAFFIC_DMH_UP" AS "PCT_TRAFFIC_DMH_UP_18H",
                        PCT_TRAFFIC_DMH_UP,
                        PCT_TRAFFIC_DMH_UP_1D,
                        PCT_TRAFFIC_DMH_UP_2D,
                        PCT_TRAFFIC_DMH_UP_3D,
                        PCT_TRAFFIC_DMH_UP_4D,
                        PCT_TRAFFIC_DMH_UP_5D,

                        ----
                        "0_MISS_PCT_TRAFFIC_DMH_UP" AS "MISS_PCT_TRAFFIC_DMH_UP_6H",
                        "1_MISS_PCT_TRAFFIC_DMH_UP" AS "MISS_PCT_TRAFFIC_DMH_UP_12H",
                        "2_MISS_PCT_TRAFFIC_DMH_UP" AS "MISS_PCT_TRAFFIC_DMH_UP_18H",
                        "3_MISS_PCT_TRAFFIC_DMH_UP" AS "MISS_PCT_TRAFFIC_DMH_UP_24H",
                        MISS_PCT_TRAFFIC_DMH_UP,
                        MISS_PCT_TRAFFIC_DMH_UP_1D,
                        MISS_PCT_TRAFFIC_DMH_UP_2D,
                        MISS_PCT_TRAFFIC_DMH_UP_3D,
                        MISS_PCT_TRAFFIC_DMH_UP_4D,
                        MISS_PCT_TRAFFIC_DMH_UP_5D,
                        ----
                        "0_PCT_TRAFFIC_SDMH_UP" - "1_PCT_TRAFFIC_SDMH_UP" AS "PCT_TRAFFIC_SDMH_UP_6H",
                        "1_PCT_TRAFFIC_SDMH_UP" - "2_PCT_TRAFFIC_SDMH_UP" AS "PCT_TRAFFIC_SDMH_UP_12H",
                        "2_PCT_TRAFFIC_SDMH_UP" - "3_PCT_TRAFFIC_SDMH_UP" AS "PCT_TRAFFIC_SDMH_UP_18H",
                        PCT_TRAFFIC_SDMH_UP,
                        PCT_TRAFFIC_SDMH_UP_1D,
                        PCT_TRAFFIC_SDMH_UP_2D,
                        PCT_TRAFFIC_SDMH_UP_3D,
                        PCT_TRAFFIC_SDMH_UP_4D,
                        PCT_TRAFFIC_SDMH_UP_5D,
                        ----
                        "0_MISS_PCT_TRAFFIC_SDMH_UP" AS "MISS_PCT_TRAFFIC_SDMH_UP_6H",
                        "1_MISS_PCT_TRAFFIC_SDMH_UP" AS "MISS_PCT_TRAFFIC_SDMH_UP_12H",
                        "2_MISS_PCT_TRAFFIC_SDMH_UP" AS "MISS_PCT_TRAFFIC_SDMH_UP_18H",
                        "3_MISS_PCT_TRAFFIC_SDMH_UP" AS "MISS_PCT_TRAFFIC_SDMH_UP_24H",
                        MISS_PCT_TRAFFIC_SDMH_UP,
                        MISS_PCT_TRAFFIC_SDMH_UP_1D,
                        MISS_PCT_TRAFFIC_SDMH_UP_2D,
                        MISS_PCT_TRAFFIC_SDMH_UP_3D,
                        MISS_PCT_TRAFFIC_SDMH_UP_4D,
                        MISS_PCT_TRAFFIC_SDMH_UP_5D,
                        ----
                        "0_RXPOWER_DN" - "1_RXPOWER_DN" AS "RX_DN_6H",
                        "1_RXPOWER_DN" - "2_RXPOWER_DN" AS "RX_DN_12H",
                        "2_RXPOWER_DN" - "3_RXPOWER_DN" AS "RX_DN_18H",
                        RX_DN,
                        RX_DN_1D,
                        RX_DN_2D,
                        RX_DN_3D,
                        RX_DN_4D,
                        RX_DN_5D,
                        ----
                        "0_MISS_RXPOWER_DN" AS "MISS_RX_DN_6H",
                        "1_MISS_RXPOWER_DN" AS "MISS_RX_DN_12H",
                        "2_MISS_RXPOWER_DN" AS "MISS_RX_DN_18H",
                        "3_MISS_RXPOWER_DN" AS "MISS_RX_DN_24H",
                        MISS_RX_DN,
                        MISS_RX_DN_1D,
                        MISS_RX_DN_2D,
                        MISS_RX_DN_3D,
                        MISS_RX_DN_4D,
                        MISS_RX_DN_5D,
                        ----
                        "0_RXPOWER_UP" - "1_RXPOWER_UP" AS "RX_UP_6H",
                        "1_RXPOWER_UP" - "2_RXPOWER_UP" AS "RX_UP_12H",
                        "2_RXPOWER_UP" - "3_RXPOWER_UP" AS "RX_UP_18H",
                        RX_UP,
                        RX_UP_1D,
                        RX_UP_2D,
                        RX_UP_3D,
                        RX_UP_4D,
                        RX_UP_5D,
                        ----
                        "0_MISS_RXPOWER_UP" AS "MISS_RX_UP_6H",
                        "1_MISS_RXPOWER_UP" AS "MISS_RX_UP_12H",
                        "2_MISS_RXPOWER_UP" AS "MISS_RX_UP_18H",
                        "3_MISS_RXPOWER_UP" AS "MISS_RX_UP_24H",
                        MISS_RX_UP,
                        MISS_RX_UP_1D,
                        MISS_RX_UP_2D,
                        MISS_RX_UP_3D,
                        MISS_RX_UP_4D,
                        MISS_RX_UP_5D,
                        ----
                        "0_SNR_DN" - "1_SNR_DN" AS "SNR_DN_6H",
                        "1_SNR_DN" - "2_SNR_DN" AS "SNR_DN_12H",
                        "2_SNR_DN" - "3_SNR_DN" AS "SNR_DN_18H",
                        SNR_DN,
                        SNR_DN_1D,
                        SNR_DN_2D,
                        SNR_DN_3D,
                        SNR_DN_4D,
                        SNR_DN_5D,
                        ----
                        "0_MISS_SNR_DN" AS "MISS_SNR_DN_6H",
                        "1_MISS_SNR_DN" AS "MISS_SNR_DN_12H",
                        "2_MISS_SNR_DN" AS "MISS_SNR_DN_18H",
                        "3_MISS_SNR_DN" AS "MISS_SNR_DN_24H",
                        MISS_SNR_DN,
                        MISS_SNR_DN_1D,
                        MISS_SNR_DN_2D,
                        MISS_SNR_DN_3D,
                        MISS_SNR_DN_4D,
                        MISS_SNR_DN_5D,
                        ----
                        "0_SNR_UP" - "1_SNR_UP" AS "SNR_UP_6H",
                        "1_SNR_UP" - "2_SNR_UP" AS "SNR_UP_12H",
                        "2_SNR_UP" - "3_SNR_UP" AS "SNR_UP_18H",
                        SNR_UP,
                        SNR_UP_1D,
                        SNR_UP_2D,
                        SNR_UP_3D,
                        SNR_UP_4D,
                        SNR_UP_5D,
                        ----
                        "0_MISS_SNR_UP" AS "MISS_SNR_UP_6H",
                        "1_MISS_SNR_UP" AS "MISS_SNR_UP_12H",
                        "2_MISS_SNR_UP" AS "MISS_SNR_UP_18H",
                        "3_MISS_SNR_UP" AS "MISS_SNR_UP_24H",
                        MISS_SNR_UP,
                        MISS_SNR_UP_1D,
                        MISS_SNR_UP_2D,
                        MISS_SNR_UP_3D,
                        MISS_SNR_UP_4D,
                        MISS_SNR_UP_5D,
                        ----
                        "0_TXPOWER_UP" - "1_TXPOWER_UP" AS "TX_UP_6H",
                        "1_TXPOWER_UP" - "2_TXPOWER_UP" AS "TX_UP_12H",
                        "2_TXPOWER_UP" - "3_TXPOWER_UP" AS "TX_UP_18H",
                        TX_UP,
                        TX_UP_1D,
                        TX_UP_2D,
                        TX_UP_3D,
                        TX_UP_4D,
                        TX_UP_5D,
                        ----
                        "0_MISS_TXPOWER_UP" AS "MISS_TX_UP_6H",
                        "1_MISS_TXPOWER_UP" AS "MISS_TX_UP_12H",
                        "2_MISS_TXPOWER_UP" AS "MISS_TX_UP_18H",
                        "3_MISS_TXPOWER_UP" AS "MISS_TX_UP_24H",
                        MISS_TX_UP,
                        MISS_TX_UP_1D,
                        MISS_TX_UP_2D,
                        MISS_TX_UP_3D,
                        MISS_TX_UP_4D,
                        MISS_TX_UP_5D,
                        ----
                        ----- CMTS_UP
                        "0_CMTS_RX_UP" - "1_CMTS_RX_UP" AS "CMTS_RX_UP_6H",
                        "1_CMTS_RX_UP" - "2_CMTS_RX_UP" AS "CMTS_RX_UP_12H",
                        "2_CMTS_RX_UP" - "3_CMTS_RX_UP" AS "CMTS_RX_UP_18H",
                        CMTS_RX_UP ,
                        CMTS_RX_UP_1D ,
                        CMTS_RX_UP_2D ,
                        CMTS_RX_UP_3D ,
                        CMTS_RX_UP_4D ,
                        CMTS_RX_UP_5D ,
                        ----
                        "0_CMTS_TX_UP" - "1_CMTS_TX_UP" AS "CMTS_TX_UP_6H",
                        "1_CMTS_TX_UP" - "2_CMTS_TX_UP" AS "CMTS_TX_UP_12H",
                        "2_CMTS_TX_UP" - "3_CMTS_TX_UP" AS "CMTS_TX_UP_18H",
                        CMTS_TX_UP ,
                        CMTS_TX_UP_1D ,
                        CMTS_TX_UP_2D ,
                        CMTS_TX_UP_3D ,
                        CMTS_TX_UP_4D ,
                        CMTS_TX_UP_5D ,
                        ----
                        "0_CMTS_CER_UP" - "1_CMTS_CER_UP" AS "CMTS_CER_UP_6H",
                        "1_CMTS_CER_UP" - "2_CMTS_CER_UP" AS "CMTS_CER_UP_12H",
                        "2_CMTS_CER_UP" - "3_CMTS_CER_UP" AS "CMTS_CER_UP_18H",
                        CMTS_CER_UP ,
                        CMTS_CER_UP_1D ,
                        CMTS_CER_UP_2D ,
                        CMTS_CER_UP_3D ,
                        CMTS_CER_UP_4D ,
                        CMTS_CER_UP_5D ,
                        ----
                        "0_CMTS_UTILIZATION_UP" - "1_CMTS_UTILIZATION_UP" AS "CMTS_UTILIZATION_UP_6H",
                        "1_CMTS_UTILIZATION_UP" - "2_CMTS_UTILIZATION_UP" AS "CMTS_UTILIZATION_UP_12H",
                        "2_CMTS_UTILIZATION_UP" - "3_CMTS_UTILIZATION_UP" AS "CMTS_UTILIZATION_UP_18H",
                        CMTS_UTILIZATION_UP ,
                        CMTS_UTILIZATION_UP_1D ,
                        CMTS_UTILIZATION_UP_2D ,
                        CMTS_UTILIZATION_UP_3D ,
                        CMTS_UTILIZATION_UP_4D ,
                        CMTS_UTILIZATION_UP_5D ,
                        ----
                        "0_CMTS_MS_UTILIZATION_UP" - "1_CMTS_MS_UTILIZATION_UP" AS "CMTS_MS_UTILIZATION_UP_6H",
                        "1_CMTS_MS_UTILIZATION_UP" - "2_CMTS_MS_UTILIZATION_UP" AS "CMTS_MS_UTILIZATION_UP_12H",
                        "2_CMTS_MS_UTILIZATION_UP" - "3_CMTS_MS_UTILIZATION_UP" AS "CMTS_MS_UTILIZATION_UP_18H",
                        CMTS_MS_UTILIZATION_UP ,
                        CMTS_MS_UTILIZATION_UP_1D ,
                        CMTS_MS_UTILIZATION_UP_2D ,
                        CMTS_MS_UTILIZATION_UP_3D ,
                        CMTS_MS_UTILIZATION_UP_4D ,
                        CMTS_MS_UTILIZATION_UP_5D ,
                        ----
                        "0_CMTS_F_MS_UTILIZATION_UP" - "1_CMTS_F_MS_UTILIZATION_UP" AS "CMTS_F_MS_UTILIZATION_UP_6H",
                        "1_CMTS_F_MS_UTILIZATION_UP" - "2_CMTS_F_MS_UTILIZATION_UP" AS "CMTS_F_MS_UTILIZATION_UP_12H",
                        "2_CMTS_F_MS_UTILIZATION_UP" - "3_CMTS_F_MS_UTILIZATION_UP" AS "CMTS_F_MS_UTILIZATION_UP_18H",
                        CMTS_F_MS_UTILIZATION_UP,
                        CMTS_F_MS_UTILIZATION_UP_1D ,
                        CMTS_F_MS_UTILIZATION_UP_2D ,
                        CMTS_F_MS_UTILIZATION_UP_3D ,
                        CMTS_F_MS_UTILIZATION_UP_4D ,
                        CMTS_F_MS_UTILIZATION_UP_5D,
                        ----
                        ----- CMTS_DN
                        "0_CMTS_RX_DN" - "1_CMTS_RX_DN" AS "CMTS_RX_DN_6H",
                        "1_CMTS_RX_DN" - "2_CMTS_RX_DN" AS "CMTS_RX_DN_12H",
                        "2_CMTS_RX_DN" - "3_CMTS_RX_DN" AS "CMTS_RX_DN_18H",
                        CMTS_RX_DN,
                        CMTS_RX_DN_1D,
                        CMTS_RX_DN_2D,
                        CMTS_RX_DN_3D,
                        CMTS_RX_DN_4D,
                        CMTS_RX_DN_5D,
                        ----
                        "0_CMTS_SNR_DN" - "1_CMTS_SNR_DN" AS "CMTS_SNR_DN_6H",
                        "1_CMTS_SNR_DN" - "2_CMTS_SNR_DN" AS "CMTS_SNR_DN_12H",
                        "2_CMTS_SNR_DN" - "3_CMTS_SNR_DN" AS "CMTS_SNR_DN_18H",
                        CMTS_SNR_DN,
                        CMTS_SNR_DN_1D,
                        CMTS_SNR_DN_2D,
                        CMTS_SNR_DN_3D,
                        CMTS_SNR_DN_4D,
                        CMTS_SNR_DN_5D,
                        ----
                        "0_CMTS_CCER_DN" - "1_CMTS_CCER_DN" AS "CMTS_CCER_DN_6H",
                        "1_CMTS_CCER_DN" - "2_CMTS_CCER_DN" AS "CMTS_CCER_DN_12H",
                        "2_CMTS_CCER_DN" - "3_CMTS_CCER_DN" AS "CMTS_CCER_DN_18H",
                        CMTS_CCER_DN,
                        CMTS_CCER_DN_1D,
                        CMTS_CCER_DN_2D,
                        CMTS_CCER_DN_3D,
                        CMTS_CCER_DN_4D,
                        CMTS_CCER_DN_5D,
                        ----
                        "0_CMTS_CER_DN" - "1_CMTS_CER_DN" AS "CMTS_CER_DN_6H",
                        "1_CMTS_CER_DN" - "2_CMTS_CER_DN" AS "CMTS_CER_DN_12H",
                        "2_CMTS_CER_DN" - "3_CMTS_CER_DN" AS "CMTS_CER_DN_18H",
                        CMTS_CER_DN,
                        CMTS_CER_DN_1D,
                        CMTS_CER_DN_2D,
                        CMTS_CER_DN_3D,
                        CMTS_CER_DN_4D,
                        CMTS_CER_DN_5D,
                        ----
                        "0_CMTS_UTILIZATION_DN" - "1_CMTS_UTILIZATION_DN" AS "CMTS_UTILIZATION_DN_6H",
                        "1_CMTS_UTILIZATION_DN" - "2_CMTS_UTILIZATION_DN" AS "CMTS_UTILIZATION_DN_12H",
                        "2_CMTS_UTILIZATION_DN" - "3_CMTS_UTILIZATION_DN" AS "CMTS_UTILIZATION_DN_18H",
                        CMTS_UTILIZATION_DN,
                        CMTS_UTILIZATION_DN_1D,
                        CMTS_UTILIZATION_DN_2D,
                        CMTS_UTILIZATION_DN_3D,
                        CMTS_UTILIZATION_DN_4D,
                        CMTS_UTILIZATION_DN_5D
                    FROM HUMOREAU.VECTOR@DMT2DMP V 
                        INNER JOIN HUMOREAU.DAILY_AVG_DAY_0@DMT2DMP DAILY
                            ON TRUNC(DAILY.DAY_0) = TRUNC(var_day_0)
                            AND TRUNC(V.DAY_0) = TRUNC(var_day_0)
                            and V.MAC = DAILY.MAC
                        INNER JOIN HUMOREAU.REFACTORED R
                            ON V.MAC = R.MAC
                )
            );
        SELECT COUNT(*) INTO new_row_count FROM HUMOREAU.VECTOR_FIVE_DAYS_II;
        PRINT_INSERTION_COUNT(old_row_count, new_row_count, 'VECTOR_FIVE_DAYS_II',True);
    END UPDATE_VECTOR;

    /*
    Dumps the content of VIA_MACs from DMP into DMT before deleting the content on DMP. 
    */
    PROCEDURE UPDATE_VIA AS 
    BEGIN
        SELECT /*+ Parallel(16) */ MAX(DISTINCT(DAY_0)) INTO v_max_date FROM HUMOREAU.VIA_MACS@DMT2DMP;
        if(TRUNC(v_max_date) != TRUNC(var_via_day_0)) THEN 
            RAISE INVALID_STATE;
        END IF;

        SELECT COUNT(*) INTO old_row_count FROM HUMOREAU.VIA_MACS;
        insert into VIA_MACS (select * from HUMOREAU.VIA_MACS@DMT2DMP);
        SELECT COUNT(*) INTO new_row_count FROM HUMOREAU.VIA_MACS;
        PRINT_INSERTION_COUNT(old_row_count, new_row_count, 'VIA_MACS',False);
    END UPDATE_VIA;

END CPE_FAIL_DETECTION_POC;
/
