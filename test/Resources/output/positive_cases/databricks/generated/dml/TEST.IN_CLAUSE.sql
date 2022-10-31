CREATE WIDGET TEXT processing_date DEFAULT '';

-- COMMAND --

-- Below query is generated from : mapping_test_file_in_clause.csv
INSERT OVERWRITE TEST.IN_CLAUSE
SELECT
	IF(SRC_TBL1.SRC_COL1 IN ('A', 'C'), 0, 1) AS TGT_COL1
FROM SRC_DB1.SRC_TBL1;