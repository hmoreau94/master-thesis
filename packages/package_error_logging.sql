/*
AUTHOR:
	Hugo Moreau - hugo.moreau@epfl.ch
	Msc in Communication Systems
	Minor in Management and Technological Entrepreneurship

DESCRIPTION:
	This package contains the procedures that are used in order to 
	log with autonomous transactions events. It should be declared 
	both on the test and production datamarts.

INITIALIZATION:
	CREATE TABLE HUMOREAU.LOG_TABLE(
		LOG_TEXT VARCHAR2(4000),
		IS_ERROR NUMBER,
		IS_DONE NUMBER
	);
*/


CREATE OR REPLACE PACKAGE CPE_FAILURE_ERROR_LOG IS 
	PROCEDURE PUT_LINE(v_text VARCHAR2, error NUMBER, finished NUMBER);
	PROCEDURE RESET;
END CPE_FAILURE_ERROR_LOG;
/

CREATE OR REPLACE PACKAGE BODY CPE_FAILURE_ERROR_LOG IS
	
	/*
	To create a new entry in the logtable

	@params:
	- v_text: the log text
	- error: should be 1 when the entry corresponds to an error
	- finished: should be set to 1 when the main procedure that we are logging is done.
	*/
	PROCEDURE PUT_LINE(v_text VARCHAR2, error NUMBER, finished NUMBER) IS 
	PRAGMA AUTONOMOUS_TRANSACTION;
		/* error should be set to 1 when we are logging an error */
	BEGIN 
		INSERT INTO HUMOREAU.LOG_TABLE(LOG_TEXT,IS_ERROR,IS_DONE) VALUES(SUBSTR(V_TEXT,1,4000),error,finished);
		COMMIT;
	EXCEPTION
	    WHEN OTHERS THEN RAISE_APPLICATION_ERROR(-20001,SQLERRM);
	END PUT_LINE;

	/*
	In order to reset the content of the log_table.
	*/
	PROCEDURE RESET IS
	BEGIN
	    DELETE FROM HUMOREAU.LOG_TABLE;
	    COMMIT;
	END RESET;
END CPE_FAILURE_ERROR_LOG;
/