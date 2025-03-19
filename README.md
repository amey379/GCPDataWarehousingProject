# 🚀 Boston 311 Service Request | GCP Data Warehousing Project

## 📌 Project Overview
The **Boston 311 Service Request System** provides a centralized platform for handling **non-emergency city service requests**. This project focuses on **processing historical and real-time 311 request data** to enable **advanced analytics and reporting**. 

Using **Google Cloud Platform (GCP)**, we built an **end-to-end ETL pipeline** to process, clean, and store the data efficiently for **structured analysis in BigQuery**.

## ⚡ Key Features
✅ **Built an ETL pipeline** with **Apache Beam & Google Dataflow** for **distributed data processing**  
✅ **Stored structured data** in **BigQuery** for analytics & reporting  
✅ **Optimized query performance** with **partitioning & clustering in BigQuery**  
✅ **Used Parquet & Avro formats** for **efficient data storage**  
✅ **Developed Power BI dashboards** for insights  

---

## 📊 Architecture & Data Flow

1️⃣ **Bronze Layer (Raw Data Storage)**
- Stores raw **Boston 311 service request data** in **Google Cloud Storage (GCS)**
- Extracts raw CSV/Parquet data and loads it into **Apache Beam (Dataflow)** for transformation

2️⃣ **Silver Layer (Cleaned Data)**
- Applies **data cleaning, deduplication, and standardization**
- Stores **cleaned & enriched data** in **Parquet format** on GCS

3️⃣ **Gold Layer (Data Warehouse)**
- Loads structured data into **BigQuery Fact & Dimension Tables**
- Optimizes performance with **partitioning & clustering**
- Enables **fast analytical queries** for reporting

---

![image](https://github.com/user-attachments/assets/5a30c269-d9f8-4012-905f-52705fedf71f)

![image](https://github.com/user-attachments/assets/ad3dafa6-3cbb-4df6-8383-3ddff656e4c9)

![image](https://github.com/user-attachments/assets/6712a613-14a8-4ac0-bd5a-0b0d38e49b72)

![image](https://github.com/user-attachments/assets/ec4347bf-e304-47dc-8a60-0f2e4de06e3f)

![image](https://github.com/user-attachments/assets/e20754e1-8c0f-4413-984e-353f53be8e22)

![image](https://github.com/user-attachments/assets/2c922397-9d15-4a8f-bdbb-91db86145cdb)
