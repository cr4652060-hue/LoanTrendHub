CREATE TABLE IF NOT EXISTS fact_trend (
                                          biz_date TEXT NOT NULL,
                                          scope TEXT NOT NULL,
                                          branch TEXT NOT NULL,
                                          metric TEXT NOT NULL,
                                          value REAL,
                                          source_file TEXT,
                                          created_at TEXT DEFAULT (datetime('now')),
    PRIMARY KEY (biz_date, scope, branch, metric)
    );

CREATE TABLE IF NOT EXISTS metric_def (
                                          metric TEXT PRIMARY KEY,
                                          name TEXT NOT NULL,
                                          unit TEXT NOT NULL,
                                          kind TEXT NOT NULL,
                                          base_metric TEXT
);

INSERT OR IGNORE INTO metric_def(metric, name, unit, kind, base_metric) VALUES
('CNT_TOTAL', '户数', '户', 'LEVEL', NULL),
('BAL_TOTAL', '余额', '万元', 'LEVEL', NULL),
('DOD_CNT', '较上日-户数', '户', 'DELTA', 'CNT_TOTAL'),
('DOD_BAL', '较上日-余额', '万元', 'DELTA', 'BAL_TOTAL'),
('MOM_CNT', '较上月-户数', '户', 'DELTA', 'CNT_TOTAL'),
('MOM_BAL', '较上月-余额', '万元', 'DELTA', 'BAL_TOTAL'),
('BOY_CNT', '较年初-户数', '户', 'DELTA', 'CNT_TOTAL'),
('BOY_BAL', '较年初-余额', '万元', 'DELTA', 'BAL_TOTAL'),
('Y2M_CNT', '增量较同期-户数', '户', 'DELTA', 'CNT_TOTAL'),
('Y2M_BAL', '增量较同期-余额', '万元', 'DELTA', 'BAL_TOTAL'),
('GR_CNT', '增幅-户数', '%', 'RATE', 'CNT_TOTAL'),
('GR_BAL', '增幅-余额', '%', 'RATE', 'BAL_TOTAL'),
('CALC_DOD_RATE_CNT', '系统日环比-户数', '%', 'RATE', 'CNT_TOTAL'),
('CALC_DOD_RATE_BAL', '系统日环比-余额', '%', 'RATE', 'BAL_TOTAL'),
('CALC_MTD_RATE_CNT', '系统月初以来-户数', '%', 'RATE', 'CNT_TOTAL'),
('CALC_MTD_RATE_BAL', '系统月初以来-余额', '%', 'RATE', 'BAL_TOTAL'),
('CALC_YTD_RATE_CNT', '系统年初以来-户数', '%', 'RATE', 'CNT_TOTAL'),
('CALC_YTD_RATE_BAL', '系统年初以来-余额', '%', 'RATE', 'BAL_TOTAL');