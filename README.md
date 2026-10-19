# FinLens Data Warehouse — Snowflake ETL Demo

Project demo Data Engineer: ingest dữ liệu từ database nguồn có schema `finlens` lên Snowflake, triển khai Bronze → Silver → Gold, data quality checks và truy vấn phân tích.

> **Lưu ý quan trọng:** hiện mới có mô tả 5 bảng, chưa có DDL/cột thực tế. Bước profiling sẽ đọc tên cột và kiểu dữ liệu thật. SQL Silver/Gold bên dưới dùng tên cột giả định phổ biến (`symbol`, `date`, `open`, `close`,...) và cần chỉnh mapping nếu nguồn khác tên.

## 1. Scope & mục tiêu

Nguồn dữ liệu:
- `symbols`: mã chứng khoán/quỹ, sàn, tên công ty, ngành.
- `sectors`: phân ngành ICB.
- `stock_ohlcv_daily`: giá cổ phiếu/quỹ theo ngày.
- `index_ohlcv_daily`: giá chỉ số theo ngày.
- `corporate_bonds`: trái phiếu doanh nghiệp còn lưu hành.

Đầu ra:
- Snowflake database `FINLENS_DW`, schemas `BRONZE`, `SILVER`, `GOLD`, `OPS`.
- Bronze lưu snapshot nguồn và metadata nạp (`_ETL_BATCH_ID`, `_ETL_LOADED_AT_UTC`).
- Silver chuẩn hóa và deduplicate theo business key.
- Gold star schema phục vụ phân tích hiệu suất cổ phiếu, ngành, chỉ số và kỳ hạn trái phiếu.
- `OPS.ETL_RUN_LOG` theo dõi số dòng, trạng thái và lỗi mỗi bảng.

## 2. Sơ đồ kiến trúc

```text
FinLens source DB (schema finlens)
            |
            | Python + SQLAlchemy (Extract)
            v
Snowflake FINLENS_DW.BRONZE (append-only snapshots)
            |
            | Snowflake SQL: standardize / deduplicate / validate
            v
Snowflake FINLENS_DW.SILVER (clean tables)
            |
            | Star-schema modeling / business metrics
            v
Snowflake FINLENS_DW.GOLD
  dim_date, dim_symbol, dim_sector
  fact_stock_daily, fact_index_daily, fact_bond_current
            |
            v
SQL / Power BI / market analysis
```

## 3. Điều kiện cần

- Quyền đọc database nguồn.
- Tài khoản Snowflake trial hoặc tài khoản có quyền tạo database/schema/warehouse/table.
- Python 3.10+.
- Kết nối mạng từ máy chạy Python tới cả database nguồn và Snowflake.

Snowflake trial được cấp tối đa 30 ngày **hoặc** tới khi dùng hết free usage balance. Warehouse compute và storage đều tiêu thụ hạn mức; kiểm tra usage thường xuyên và không để warehouse chạy khi không dùng.

## 4. Thiết lập

### 4.1. Cài Python packages

```bash
python -m venv .venv
# Windows PowerShell
.venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

### 4.2. Cấu hình kết nối

Copy `.env.example` thành `.env` rồi sửa thông tin kết nối.

Ví dụ MySQL:

```dotenv
SOURCE_DATABASE_URL=mysql+pymysql://USER:PASSWORD@HOST:3306/your_database
SOURCE_SCHEMA=finlens
SOURCE_TABLES=symbols,sectors,stock_ohlcv_daily,index_ohlcv_daily,corporate_bonds
```

Ví dụ PostgreSQL:

```dotenv
SOURCE_DATABASE_URL=postgresql+psycopg://USER:PASSWORD@HOST:5432/your_database
SOURCE_SCHEMA=finlens
```

Nếu schema `finlens` chính là database trong MySQL, vẫn dùng `SOURCE_SCHEMA=finlens` và đảm bảo user có quyền đọc schema/database đó. Hãy URL-encode ký tự đặc biệt trong username/password.

Snowflake:

```dotenv
SNOWFLAKE_ACCOUNT=your_account_identifier
SNOWFLAKE_USER=your_username
SNOWFLAKE_PASSWORD=your_password
SNOWFLAKE_WAREHOUSE=FINLENS_WH
SNOWFLAKE_DATABASE=FINLENS_DW
SNOWFLAKE_ROLE=SYSADMIN
```

Không commit file `.env` hoặc thông tin đăng nhập lên GitHub.

### 4.3. Khởi tạo Snowflake

Mở Snowflake Snowsight → Worksheets, chạy `sql/00_init.sql` bằng role có quyền tạo warehouse/database/schema. Nếu tổ chức không cho phép role `SYSADMIN`, dùng role được cấp và chỉnh `SNOWFLAKE_ROLE`.

### 4.4. Profiling schema nguồn (bắt buộc trước transform)

```bash
python scripts/profile_source.py
```

Script in danh sách bảng/cột/kiểu dữ liệu/số dòng, đồng thời xuất `reports/source_profile.csv`. Kiểm tra tên cột thật, khóa định danh, null và duplicate trước khi chạy SQL ở mục 6.

### 4.5. Nạp snapshot vào Bronze

```bash
python scripts/ingest_to_snowflake.py
```

Script nạp lần lượt 5 bảng, bổ sung `_ETL_BATCH_ID` và `_ETL_LOADED_AT_UTC`, ghi trạng thái vào `FINLENS_DW.OPS.ETL_RUN_LOG`. Mỗi lần chạy sẽ append snapshot mới vào Bronze; Silver dùng timestamp nạp mới nhất để deduplicate.

> Với bảng OHLCV lớn, bản demo này nạp full snapshot để dễ chứng minh end-to-end. Sau khi chạy được lần đầu, nên đổi sang incremental extraction theo ngày (và reload một cửa sổ gần đây để bắt các điều chỉnh dữ liệu) nhằm giảm thời gian/credit.

## 5. Bronze: kiểm tra kết quả nạp

Chạy `sql/01_profile_snowflake.sql`. Kiểm tra bảng audit:

```sql
SELECT *
FROM FINLENS_DW.OPS.ETL_RUN_LOG
ORDER BY STARTED_AT DESC;
```

Kiểm tra row count từng bảng Bronze:

```sql
SELECT 'SYMBOLS' AS TABLE_NAME, COUNT(*) AS ROWS FROM FINLENS_DW.BRONZE.SYMBOLS
UNION ALL SELECT 'SECTORS', COUNT(*) FROM FINLENS_DW.BRONZE.SECTORS
UNION ALL SELECT 'STOCK_OHLCV_DAILY', COUNT(*) FROM FINLENS_DW.BRONZE.STOCK_OHLCV_DAILY
UNION ALL SELECT 'INDEX_OHLCV_DAILY', COUNT(*) FROM FINLENS_DW.BRONZE.INDEX_OHLCV_DAILY
UNION ALL SELECT 'CORPORATE_BONDS', COUNT(*) FROM FINLENS_DW.BRONZE.CORPORATE_BONDS;
```

## 6. Silver/Gold transformation

Mở `sql/02_silver_gold_template.sql` và đối chiếu với `reports/source_profile.csv` trước khi chạy. Những cột cần kiểm tra tối thiểu:

| Bảng | Cột canonical được giả định trong template |
|---|---|
| `symbols` | `symbol`, `exchange`, `company_name`, `sector_code` |
| `sectors` | `sector_code`, `sector_name`, `sector_level`, `parent_sector_code` |
| `stock_ohlcv_daily` | `symbol`, `date`, `open`, `high`, `low`, `close`, `volume` |
| `index_ohlcv_daily` | `symbol`, `date`, `open`, `high`, `low`, `close`, `volume` |
| `corporate_bonds` | `bond_code`, `issuer`, `coupon_rate`, `maturity_date`, `outstanding_amount`, `bond_type` |

Nếu nguồn dùng `ticker` thay `symbol`, `trade_date` thay `date`, `issuer_name` thay `issuer`, v.v. hãy đổi các biểu thức `SELECT` ở Silver sang tên nguồn thật, đồng thời chuẩn hóa output về canonical names này. Nếu bonds không có `bond_code`, chọn natural key đáng tin cậy sau khi profiling — không tự tạo một mã giả nếu không đảm bảo duy nhất.

Sau khi cập nhật mapping và chạy SQL, dùng `sql/03_gold_analytics.sql` để kiểm tra mô hình Gold và minh họa phân tích.

## 7. Star schema & grain

| Model | Grain (một dòng đại diện cho) | Key gợi ý |
|---|---|---|
| `GOLD.DIM_DATE` | một ngày lịch | `DATE_KEY` |
| `GOLD.DIM_SECTOR` | một ngành ICB | `SECTOR_CODE` |
| `GOLD.DIM_SYMBOL` | một mã chứng khoán | `SYMBOL` |
| `GOLD.FACT_STOCK_DAILY` | một mã × một ngày giao dịch | (`SYMBOL`, `TRADE_DATE`) |
| `GOLD.FACT_INDEX_DAILY` | một chỉ số × một ngày | (`INDEX_SYMBOL`, `TRADE_DATE`) |
| `GOLD.FACT_BOND_CURRENT` | một lô trái phiếu hiện lưu hành | `BOND_CODE` hoặc natural key đã kiểm chứng |

Fact OHLCV chứa open/high/low/close/volume, daily return và khoảng dao động trong ngày. `FACT_BOND_CURRENT` là ảnh chụp trạng thái hiện tại nếu nguồn chỉ giữ danh sách trái phiếu còn lưu hành; không nên gọi đây là lịch sử biến động dư nợ nếu không có snapshot theo thời gian.

## 8. Data quality checks bắt buộc

1. **Uniqueness:** không trùng (`symbol`, `date`) trong stock/index OHLCV sau Silver.
2. **OHLC logic:** `low <= open <= high`, `low <= close <= high`.
3. **Non-negative:** `volume >= 0`; dư nợ trái phiếu không âm.
4. **Completeness:** mã, ngày, close không null với OHLCV; ngày đáo hạn và tổ chức phát hành có mặt với trái phiếu nếu nguồn yêu cầu.
5. **Referential integrity:** `symbols.sector_code` phải match `sectors.sector_code` hoặc được đánh dấu `UNKNOWN/UNMAPPED`.
6. **Freshness:** timestamp nạp mới nhất và ngày giao dịch mới nhất nằm trong kỳ vọng nghiệp vụ.
7. **Reconciliation:** so sánh row count source vs batch loaded và ghi lỗi trong audit log.

## 9. Hướng phân tích (data mining)

- Top mã theo lợi suất 20/60 ngày; so sánh với VN-Index/VN30.
- Biến động 20/60 phiên và nhóm mã có độ biến động cao.
- Giá trị giao dịch ước tính (`close × volume`) theo ngày, mã, ngành.
- Hiệu suất và độ biến động trung bình theo ngành ICB.
- Phân tích xu hướng giá với MA20/MA50 (cần xác định cách xử lý dữ liệu thiếu phiên).
- Danh sách trái phiếu đáo hạn trong 30/90/180 ngày; tổng dư nợ theo tổ chức phát hành và loại trái phiếu.

FinLens biểu diễn giá cổ phiếu/chỉ số bằng **nghìn đồng**. Khi nhân `close × volume`, kết quả ở nghìn đồng; chia 1.000 để có **triệu đồng**. Đây là giá trị giao dịch xấp xỉ theo giá đóng cửa, không phải tổng giá trị khớp lệnh thực tế trong cả phiên.

## 10. Data lifecycle sau khi demo chạy

1. **Extract:** đọc nguồn theo bảng (về sau incremental theo date/watermark).
2. **Load:** append snapshot vào Bronze, gán batch id và load time.
3. **Transform:** làm sạch/deduplicate Silver; dựng fact/dimension Gold.
4. **Validate:** chạy checks trước khi publish dữ liệu Gold.
5. **Serve:** SQL Worksheet/Power BI dùng Gold, tránh query trực tiếp Bronze.
6. **Observe:** xem `OPS.ETL_RUN_LOG`, row count, freshness và lỗi.
7. **Recover:** sửa lỗi nguồn/mapping, chạy lại batch; giữ raw history để điều tra.
8. **Optimize:** incremental load và warehouse auto-suspend; trial Snowflake không phải free forever.

## 11. Trình tự demo/thuyết trình

1. Chạy profiling nguồn, trình bày kiểu dữ liệu và chất lượng dữ liệu.
2. Chạy init SQL, giải thích 4 schema.
3. Chạy ingestion Python; chứng minh batch id, row counts, audit status.
4. Chạy Silver và giải thích deduplication/validation.
5. Dựng star schema Gold và giải thích grain/keys.
6. Chạy 3–5 truy vấn phân tích trong `sql/03_gold_analytics.sql`.
7. Chỉ ra data lifecycle: batch tiếp theo cập nhật Bronze, Silver chọn snapshot mới nhất, Gold được rebuild, audit log ghi nhận lần chạy.

## 12. Giới hạn hiện tại

- Chưa có DDL thực tế nên mapping trong SQL transform là giả định và cần được đối chiếu.
- Ingestion hiện append full snapshots, phù hợp prototype hơn production.
- Script chưa có scheduler/alert tự động; có thể bổ sung GitHub Actions, Airflow hoặc Snowflake Tasks sau khi bản thủ công chạy ổn.
- Cần kiểm tra điều khoản sử dụng/redistribution của nguồn dữ liệu trước khi chia sẻ dataset hoặc dashboard ra ngoài.
