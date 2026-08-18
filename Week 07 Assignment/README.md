# Python Data Pipeline Engineering — Lab Assignment

ETL Pipeline สำหรับข้อมูลยอดขาย Omnichannel Retail: Extract → Transform (Data Quality)
→ Load เข้า Star Schema (SQLite) แบบ **Idempotent** และรองรับ **Incremental Loading**

## 1. วิธีติดตั้ง

ต้องการ Python 3.10+ กับไลบรารี:

```bash
pip install pandas openpyxl
```

ไฟล์ dataset ต้นฉบับ (`Python_Data_Pipeline_Lab_Dataset__1_.xlsx`) ต้องอยู่ในโฟลเดอร์ `data/`

โครงสร้างโฟลเดอร์:

```
project/
├── pipeline.py
├── data/
│   └── Python_Data_Pipeline_Lab_Dataset__1_.xlsx
├── output/
│   ├── retail_dw.db
│   ├── quarantine.csv
│   └── pipeline_run_log.csv
└── README.md
```

## 2. วิธีรัน

```bash
python3 pipeline.py
```

สคริปต์จะลบ `output/retail_dw.db` เดิม (ถ้ามี) แล้วรันสาธิตให้เห็นครบ 4 รอบตามที่โจทย์กำหนด:

1. **ROUND 1** — โหลด `batch_1` ครั้งแรก
2. **ROUND 2** — โหลด `batch_1` ซ้ำ → พิสูจน์ Idempotency (จำนวนแถวใน `fact_sales` ต้องไม่เพิ่ม)
3. **ROUND 3** — โหลด `batch_2` (ข้อมูลใหม่)
4. **ROUND 4** — โหลด `batch_3` (ข้อมูลใหม่)

ผลลัพธ์จริงจากการรัน (ตัวเลขอาจแตกต่างเล็กน้อยหากรันบนไฟล์ dataset คนละชุด):

| Round | Batch | rows_read | rows_valid | rows_rejected | rows_loaded | fact_sales rows (สะสม) |
| --- | --- | --- | --- | --- | --- | --- |
| 1 | 1 | 420 | 389 | 31 | 389 | 389 |
| 2 | 1 (ซ้ำ) | 420 | 389 | 31 | **0** | **389 (ไม่เพิ่ม)** |
| 3 | 2 | 424 | 386 | 35 | 386 | 774 |
| 4 | 3 | 424 | 386 | 35 | 385 | 1159 |

หมายเหตุ: batch_2 และ batch_3 แต่ละ batch มี `order_id` ซ้ำภายในตัวเอง 6 แถว
(ยุบเหลือ 3 คู่หลัง deduplicate) ตัวเลข `rows_read` จึงมากกว่า `rows_valid + rows_rejected`
อยู่ 3 แถวเสมอ (ดูสูตรคำนวณในหัวข้อที่ 4)

## 3. Star Schema

```
dim_customer(customer_key PK, customer_id UNIQUE, customer_name, province, segment)
dim_product (product_key PK, product_id UNIQUE, product_name, category)
dim_date    (date_key PK, full_date UNIQUE, day, month, quarter, year)

fact_sales (
  fact_key PK,
  order_id UNIQUE,               -- 1 แถว = 1 order_id ที่ผ่านการตรวจสอบแล้ว (grain)
  date_key FK -> dim_date,
  customer_key FK -> dim_customer,
  product_key FK -> dim_product,
  quantity, unit_price, discount_pct,
  gross_amount, net_amount,
  payment_method, sales_channel, updated_at
)
```

**Grain ของ fact_sales**: หนึ่งแถวต่อหนึ่ง `order_id` ที่ผ่านการตรวจสอบคุณภาพข้อมูลแล้ว
(dataset นี้ 1 order = 1 รายการสินค้า จึงเทียบเท่า "หนึ่งรายการขายสินค้าต่อ order_id")

ตารางเสริม:
- `quarantine_records` — เก็บทุกแถวที่ไม่ผ่านตรวจสอบ พร้อม `reason_code` และ `source_batch`
  (export เป็น `quarantine.csv` ทุกครั้งที่รัน)
- `pipeline_run_log` — บันทึกทุกรอบการรัน (`batch`, `started_at`, `ended_at`, `rows_read`,
  `rows_valid`, `rows_loaded`, `rows_rejected`, `status`) ทำหน้าที่เป็น watermark/audit log
  (export เป็น `pipeline_run_log.csv`)

## 4. กติกาการทำความสะอาดข้อมูล (Data Quality Rules)

| ปัญหาที่เจอ | วิธีจัดการ |
| --- | --- |
| `unit_price` เป็นข้อความ เช่น `"THB 979.4"` | ตัดคำ `THB` และ comma ออกก่อนแปลงเป็นตัวเลข (`errors="coerce"`) |
| `quantity` เป็น string ("`five`" เป็นต้น) | `pd.to_numeric(errors="coerce")` → ถ้าแปลงไม่ได้ = ค่าว่าง → quarantine |
| `discount_pct` เกิน 100 (เช่น 120) | ตรวจช่วง 0-100 → นอกช่วง = quarantine (`INVALID_DISCOUNT`) |
| `payment_method` case ไม่ตรง (`"credit card"` vs `"Credit Card"`) | Normalize ด้วย `.title()` + จับกรณีพิเศษ `PromptPay` |
| `sales_channel` มีค่า `"E-Commerce"` | Map เป็น `"Online"` ตาม data dictionary |
| `customer_id` / `product_id` ไม่มีใน Dimension | ตรวจสอบ Referential Integrity → quarantine (`CUSTOMER_NOT_FOUND` / `PRODUCT_NOT_FOUND`) |
| `order_id` ซ้ำ | Deduplicate โดยเก็บแถวที่ `updated_at` ล่าสุด (`drop_duplicates(keep="last")` หลัง sort) |
| ค่าว่าง (`customer_id`, `unit_price`, `order_datetime`) | ตรวจ `isna()` → quarantine พร้อม reason ที่เจาะจง |

**สูตรที่ใช้สำหรับ KPI ในแต่ละรอบ**: `rows_read = rows_valid + rows_rejected + rows_deduplicated_out`
(ตัวเลข duplicate ที่ถูกลบออกก่อนตรวจสอบ ไม่นับใน `rows_rejected` เพราะไม่มี reason_code
เชิงคุณภาพข้อมูล แต่ถูกนับแยกใน KPI `rows_duplicated` ของ `run_pipeline()`)

## 5. Idempotency & Incremental Loading

- `fact_sales.order_id` เป็น `UNIQUE` — การ Insert ใช้ `INSERT ... ON CONFLICT(order_id) DO UPDATE`
  พร้อมเงื่อนไข `WHERE excluded.updated_at > fact_sales.updated_at` จึงจะอัปเดตแถวเดิมเมื่อ
  ข้อมูลใหม่กว่าเท่านั้น — รันซ้ำด้วยข้อมูลเดิมจึงไม่มีการเปลี่ยนแปลงใดๆ ในตาราง (rows_loaded = 0)
- `dim_customer` / `dim_product` / `dim_date` ใช้ `INSERT ... ON CONFLICT DO UPDATE` /
  `INSERT OR IGNORE` เช่นกัน ป้องกันข้อมูลซ้ำเมื่อรันหลายรอบ
- `pipeline_run_log` บันทึกทุกรอบการรันของทุก batch ทำหน้าที่เป็นทั้ง audit trail และ
  watermark สำหรับตรวจสอบว่า batch ไหนรันสำเร็จไปแล้ว

## 6. Error Handling

`run_pipeline()` ครอบการประมวลผลแต่ละ batch ด้วย `try/except`: ถ้า batch ใดล้มเหลว
(เช่น อ่านไฟล์ไม่ได้) จะบันทึกสถานะ `FAILED` ลง `pipeline_run_log` และข้ามไป batch ถัดไป
โดยไม่ลบหรือกระทบข้อมูลที่โหลดสำเร็จไปแล้วใน batch ก่อนหน้า ส่วนระดับแถว หากแถวใดไม่ผ่าน
กฎคุณภาพข้อมูล จะถูกแยกไป `quarantine_records` พร้อม `reason_code` แทนที่จะทำให้ทั้ง batch ล้มเหลว

## 7. Reflection

เหตุใด Availability จึงมักสำคัญกว่า Strictness ใน Production Pipeline:

ใน Production ข้อมูลจากหลายช่องทางย่อมมีความไม่สมบูรณ์อยู่เสมอ ถ้า Pipeline ออกแบบ
แบบ Strict คือหยุดทำงานทั้งระบบทันทีที่เจอแถวผิดปกติเพียงแถวเดียว ผลกระทบจะลามไปถึง
รายงานยอดขายทั้งหมดที่ฝ่ายธุรกิจต้องใช้ตัดสินใจรายวัน ทำให้เกิดความเสียหายที่ใหญ่กว่า
การมีข้อมูลไม่ครบบางส่วน การออกแบบแบบ "Quarantine" ที่แยกเฉพาะแถวที่มีปัญหาออกไป
พร้อมบันทึกเหตุผล ทำให้ระบบยังคง Available และส่งมอบข้อมูลที่ถูกต้องส่วนใหญ่ได้ตรงเวลา
ในขณะที่ทีมสามารถไปตรวจสอบและแก้ไขแถวที่มีปัญหาแยกต่างหากภายหลังได้ นอกจากนี้
Availability ที่สูงยังทำให้ Pipeline รองรับ Incremental Load ได้อย่างต่อเนื่อง
โดยไม่ต้อง re-run ทั้งระบบใหม่ทุกครั้งที่เจอข้อมูลเสีย ซึ่งจะสิ้นเปลืองทรัพยากรและเวลา
มากกว่าการปล่อยให้ระบบทำงานต่อไปพร้อม Data Quality Report ที่โปร่งใส
