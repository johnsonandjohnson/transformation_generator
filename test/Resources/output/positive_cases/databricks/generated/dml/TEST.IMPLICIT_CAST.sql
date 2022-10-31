CREATE WIDGET TEXT processing_date DEFAULT '';

-- COMMAND --

-- Below query is generated from : mapping_test_file_implicit_cast.csv
INSERT OVERWRITE TEST.IMPLICIT_CAST
SELECT
	CAST(SRC_TBL1.SRC_COL1 AS INT) AS TGT_COL1
FROM SRC_DB1.SRC_TBL1;