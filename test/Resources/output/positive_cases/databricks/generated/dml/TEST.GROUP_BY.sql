CREATE WIDGET TEXT processing_date DEFAULT '';

-- COMMAND --

-- Below query is generated from : mapping_test_file_group_by.csv
INSERT OVERWRITE TEST.GROUP_BY
SELECT
	SRC_TBL1.SRC_COL1 AS TGT_COL1
FROM SRC_DB1.SRC_TBL1
GROUP BY SRC_COL1;