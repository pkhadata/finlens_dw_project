# FinLens Data Warehouse & ETL Pipeline

ETL pipeline chuyển dữ liệu FinLens từ PostgreSQL sang Snowflake qua các lớp Bronze, Silver và Gold. Pipeline có validation ở các bước chính và có thể chạy lại nhiều lần.

## 1. Overview

**Source:** PostgreSQL  
**Target:** Snowflake  
**ETL:** Python + SQL  
**Database:** `FINLENS_DW`  
**Warehouse:** `FINLENS_WH`

```text
PostgreSQL
    ↓
BRONZE
    ↓
SILVER
    ↓
GOLD
    ↓
Analytics
```

`OPS.ETL_RUN_LOG` lưu lịch sử chạy pipeline.

## 2. Source Data

Schema nguồn: `finlens`

| Table | Rows | Nội dung |
|---|---:|---|
| `symbols` | 1,650 | Thông tin mã chứng khoán |
| `sectors` | 125 | Phân loại ngành ICB |
| `stock_ohlcv_daily` | 81 | OHLCV cổ phiếu |
| `index_ohlcv_daily` | 996 | OHLCV chỉ số |
| `corporate_bonds` | 2,045 | Thông tin trái phiếu |

Natural keys:

| Table | Key |
|---|---|
| `symbols` | `symbol` |
| `sectors` | `icb` |
| `stock_ohlcv_daily` | `(symbol, date)` |
| `index_ohlcv_daily` | `(symbol, date)` |
| `corporate_bonds` | `bond_code` |

> Đây là snapshot dữ liệu được cung cấp cho project, không phải toàn bộ lịch sử thị trường.

## 3. Warehouse

### BRONZE

Lưu snapshot từ PostgreSQL và thêm `_BATCH_ID`, `_LOADED_AT`, `_SOURCE_SYSTEM`.

```text
SYMBOLS
SECTORS
STOCK_OHLCV_DAILY
INDEX_OHLCV_DAILY
CORPORATE_BONDS
```

### SILVER

Dữ liệu được chuẩn hóa và đưa về mô hình dimension/fact:

```text
DIM_DATE
DIM_SYMBOL
DIM_SECTOR

FACT_STOCK_DAILY
FACT_INDEX_DAILY
FACT_CORPORATE_BOND
```

Một số metric được tính tại đây:

- `DAILY_RETURN_PCT`
- `INTRADAY_RANGE_PCT`
- `DAYS_TO_MATURITY`
- `YEARS_TO_MATURITY`
- `OUTSTANDING_RATIO`

### GOLD

Các bảng phục vụ phân tích:

```text
STOCK_PERFORMANCE
SECTOR_PERFORMANCE
MARKET_PERFORMANCE
BOND_ANALYSIS
```

### OPS

```text
ETL_RUN_LOG
```

Lưu `RUN_ID`, thời gian chạy, trạng thái và số dòng đã nạp.

## 4. ETL Pipeline

Chạy toàn bộ pipeline:

```bash
python scripts/run_pipeline.py
```

Có 5 bước:

```text
[1/5] BRONZE INGESTION
[2/5] BRONZE VALIDATION
[3/5] SILVER BUILD
[4/5] GOLD BUILD
[5/5] END-TO-END VALIDATION
```

### Bronze Ingestion

Đọc dữ liệu từ PostgreSQL, đưa vào bảng staging rồi cập nhật Bronze bằng `INSERT OVERWRITE`. Nếu source table rỗng, snapshot hiện tại không bị xóa.

### Bronze Validation

Kiểm tra row count, duplicate natural key, null/blank key và metadata.

### Silver Build

Tạo dimensions, facts và các metric cần thiết từ Bronze.

### Gold Build

Tạo 4 bảng Gold phục vụ phân tích.

### End-to-End Validation

Kiểm tra row count giữa các layer, grain, key, `DIM_DATE` và một số logic dữ liệu tài chính như OHLC, volume và bond metrics.

Nếu validation thất bại, pipeline dừng.

## 5. Test Results

Full pipeline đã chạy thành công nhiều lần.

```text
[1/5] BRONZE INGESTION       [PASS]
[2/5] BRONZE VALIDATION      [PASS]
[3/5] SILVER BUILD           [PASS]
[4/5] GOLD BUILD             [PASS]
[5/5] END-TO-END VALIDATION  [PASS]

PIPELINE SUCCESS
```

Row counts chính:

| Table | Rows |
|---|---:|
| `SILVER.DIM_DATE` | 366 |
| `SILVER.DIM_SYMBOL` | 1,650 |
| `SILVER.DIM_SECTOR` | 125 |
| `SILVER.FACT_STOCK_DAILY` | 81 |
| `SILVER.FACT_INDEX_DAILY` | 996 |
| `SILVER.FACT_CORPORATE_BOND` | 2,045 |
| `GOLD.STOCK_PERFORMANCE` | 81 |
| `GOLD.SECTOR_PERFORMANCE` | 81 |
| `GOLD.MARKET_PERFORMANCE` | 249 |
| `GOLD.BOND_ANALYSIS` | 2,045 |

Pipeline đã được chạy lại sau lần chạy đầu và tiếp tục PASS, không phát sinh duplicate snapshot.

## 6. Analytics

Các query nằm trong `sql/analytics/`:

```text
01_stock_analysis.sql
02_sector_analysis.sql
03_market_analysis.sql
04_bond_analysis.sql
05_phase5_validation.sql
```

Một số nội dung phân tích:

- cổ phiếu tăng/giảm mạnh
- hiệu suất và volatility
- hiệu suất theo ngành
- xu hướng thị trường
- volume
- kỳ hạn trái phiếu

## 7. Run

Requirements:

- Python 3.10+
- PostgreSQL source
- Snowflake account
- các biến kết nối trong `.env`

Cài dependencies:

```bash
python -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt
```

Kiểm tra kết nối:

```bash
python scripts/test_connection.py
```

Chạy pipeline:

```bash
python scripts/run_pipeline.py
```

Sau đó có thể chạy các file SQL trong `sql/analytics/` trên Snowflake Snowsight.

## 8. Demo

Demo ngắn:

1. Giới thiệu luồng `PostgreSQL → Bronze → Silver → Gold`.
2. Chạy `python scripts/run_pipeline.py`.
3. Cho xem 5 bước `[PASS]`.
4. Mở Snowflake và xem dữ liệu trong `BRONZE`, `SILVER`, `GOLD`.
5. Mở `OPS.ETL_RUN_LOG` để xem lịch sử chạy.
6. Chạy một query phân tích về stock/market và một query về bond.
7. Chạy lại pipeline để kiểm tra khả năng re-run.

## 9. Project Structure

```text
finlens_dw_project/
├── README.md
├── requirements.txt
├── .env
├── reports/
│   └── source_profile.csv
├── scripts/
│   ├── build_gold.py
│   ├── build_silver.py
│   ├── ingest_to_snowflake.py
│   ├── profile_source.py
│   ├── run_pipeline.py
│   └── test_connection.py
└── sql/
    ├── 01_snowflake_init.sql
    ├── 23_create_silver.sql
    └── analytics/
        ├── 01_stock_analysis.sql
        ├── 02_sector_analysis.sql
        ├── 03_market_analysis.sql
        ├── 04_bond_analysis.sql
        └── 05_phase5_validation.sql
```

## 10. Limitations

- Hiện tại pipeline sử dụng full snapshot, chưa phải incremental/CDC.
- Orchestration được thực hiện bằng Python.
- Chưa có scheduler tự động.
- Dataset hiện tại có phạm vi nhỏ, đặc biệt là dữ liệu stock OHLCV.

Nếu phát triển tiếp, có thể bổ sung incremental loading, CDC và scheduler/orchestrator.
