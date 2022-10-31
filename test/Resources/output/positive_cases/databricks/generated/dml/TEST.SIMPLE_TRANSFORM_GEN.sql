CREATE WIDGET TEXT processing_date DEFAULT '';

-- COMMAND --

sample cmd;


-- COMMAND --

%run test sample

-- COMMAND --


-- COMMAND --

-- Below query is generated from : mapping_test_file_simple_transform_gen.csv
INSERT OVERWRITE TEST.SIMPLE_TRANSFORM_GEN
SELECT
	SRC_TBL1.SRC_COL1 AS TGT_COL1,
	SRC_TBL1.SRC_COL2 AS TGT_COL2
FROM SRC_DB1.SRC_TBL1;