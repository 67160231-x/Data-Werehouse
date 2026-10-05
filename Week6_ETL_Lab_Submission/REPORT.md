# ETL Lab Report

Student ID: 67160231
Name: Puwish Pimchat

## 1. Data Quality Problems Found
- Customers: customer_id ซ้ำ 2 รายการ, province เขียนไม่เป็นมาตรฐาน (ไทย/อังกฤษ/ตัวย่อ/ตัวพิมพ์), province หาย 1, email หาย 1
- Products: JSON ซ้อนกัน (category.name, pricing.price), category เป็น null 1 รายการ, price เป็น string มี comma 1 รายการ
- Orders: order_id ซ้ำ 3 รายการ, order_date 4 รูปแบบ + parse ไม่ได้ 1 รายการ, status ตัวพิมพ์ไม่สม่ำเสมอ, qty ติดลบ 1, unit_price ติดลบ 1, discount_pct เกิน 100 1 รายการ, อ้างอิง customer/product ที่ไม่มีจริง 2 รายการ (แต่สถานะ cancelled อยู่แล้ว)

## 2. Cleaning / Transformation Rules
- Customers: ลบ id ซ้ำ (เก็บแถวแรก), map province ด้วย lookup table → ไม่พบ/ว่าง = "Unknown", เติม email ว่างด้วย placeholder
- Products: flatten JSON, แปลง price string→number, category ว่าง = "Unknown"
- Orders: ลบ order_id ซ้ำ, parse date ทีละรูปแบบจนกว่าจะสำเร็จ, status เป็นตัวพิมพ์เล็ก, reject qty/price/discount ผิดเงื่อนไข หรือ date parse ไม่ได้
- Merge: เก็บเฉพาะ paid/completed, join customers+products, ไม่พบ = reject
- คำนวณ: gross = qty×unit_price, discount = gross×discount_pct/100, sales_amount = gross-discount

## 3. Rejected Records
จำนวน: 7 รายการ

เหตุผลหลัก: duplicate_order_id (3), invalid_qty (1), invalid_unit_price (1), invalid_discount_pct (1), invalid_order_date (1)

## 4. ETL Validation
- Valid transformed rows: 100
- Warehouse rows: 100
- Duplicate order_id: 0
- Source total sales: 192,074.66
- Warehouse total sales: 192,074.66
- Validation status: PASS

## 5. Idempotency Test
จำนวน fact_sales หลัง run ครั้งที่ 1: 100

จำนวน fact_sales หลัง run ครั้งที่ 2: 100

อธิบายผล: ไม่เพิ่ม เพราะ order_id เป็น PRIMARY KEY และใช้ INSERT OR IGNORE ตอน load ทำให้ข้าม record ที่มีอยู่แล้ว
