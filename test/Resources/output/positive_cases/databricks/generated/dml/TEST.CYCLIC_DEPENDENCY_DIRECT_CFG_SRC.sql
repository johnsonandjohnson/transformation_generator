CREATE WIDGET TEXT processing_date DEFAULT '';

-- COMMAND --

-- Below query is generated from : mapping_test_file_cycle_direct_cfg_src.csv
INSERT OVERWRITE TEST.CYCLIC_DEPENDENCY_DIRECT_CFG_SRC
SELECT
	SRC_TBL1.SRC_COL1 AS TGT_COL1	-- comment for TGT_COL1,
	SRC_TBL1.SRC_COL2 AS TGT_COL2	-- comment for TGT_COL2
FROM SRC_DB1.SRC_TBL1;