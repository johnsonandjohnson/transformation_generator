CREATE WIDGET TEXT processing_date DEFAULT '';

-- COMMAND --

DROP TABLE IF EXISTS TEST.CYCLIC_DEPENDENCY_INDIRECT_CFG_3;

CREATE TABLE TEST.CYCLIC_DEPENDENCY_INDIRECT_CFG_3
(
	TGT_COL1 STRING,
	TGT_COL2 INTEGER
)

STORED AS PARQUET
LOCATION '/mnt/dct/chdp/test/cyclic_dependency_indirect_cfg_3';