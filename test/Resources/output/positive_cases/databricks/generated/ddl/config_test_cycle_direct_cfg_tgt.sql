CREATE WIDGET TEXT processing_date DEFAULT '';

-- COMMAND --

DROP TABLE IF EXISTS TEST.CYCLIC_DEPENDENCY_DIRECT_CFG_TGT;

CREATE TABLE TEST.CYCLIC_DEPENDENCY_DIRECT_CFG_TGT
(
	TGT_COL1 STRING,
	TGT_COL2 INTEGER
)

STORED AS PARQUET
LOCATION '/mnt/dct/chdp/test/cyclic_dependency_direct_cfg_tgt';