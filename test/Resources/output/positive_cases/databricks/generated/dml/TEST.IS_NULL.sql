CREATE WIDGET TEXT processing_date DEFAULT '';

-- COMMAND --


-- COMMAND --

%run test sample

-- COMMAND --
sample cmd;



-- COMMAND --

-- Below query is generated from : mapping_test_file_is_null.csv
INSERT OVERWRITE TEST.IS_NULL
SELECT
	IF(SRC_TBL1.SRC_COL1 IS NULL, 0, 1) AS TGT_COL1
FROM SRC_DB1.SRC_TBL1;