CREATE WIDGET TEXT processing_date DEFAULT '';

-- COMMAND --

sample cmd;


-- COMMAND --

%run test sample

-- COMMAND --
sample cmd;



-- COMMAND --

-- Below query is generated from : mapping_test_file_where_clause.csv
INSERT OVERWRITE TEST.WHERE_CLAUSE
SELECT
	SRC_TBL1.SRC_COL1 AS TGT_COL1,
	SRC_TBL1.SRC_COL2 AS TGT_COL2,
	SRC_TBL1.SRC_COL3 AS TGT_COL3
FROM SRC_DB1.SRC_TBL1
WHERE SRC_TBL1.SRC_COL1 = 'A' AND SRC_TBL1.SRC_COL2 <> 'ABC' AND SRC_TBL1.SRC_COL3 NOT IN ('ABC', 'DEF');