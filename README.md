# Kafka_Ecom
# 🛒 Real-Time E-Commerce Data Pipeline

![Python](https://img.shields.io/badge/Python-3776AB?style=for-the-badge&logo=python&logoColor=white)
![Apache Kafka](https://img.shields.io/badge/Apache_Kafka-231F20?style=for-the-badge&logo=apache-kafka&logoColor=white)
![Snowflake](https://img.shields.io/badge/Snowflake-29B5E8?style=for-the-badge&logo=snowflake&logoColor=white)
![Streamlit](https://img.shields.io/badge/Streamlit-FF4B4B?style=for-the-badge&logo=streamlit&logoColor=white)

## 📌 Project Overview
This repository contains my graduation project for the **Digital Egypt Pioneers Initiative (DEPI) - Data Engineering Track**. 

The project is an **End-to-End Real-Time Data Pipeline** designed to simulate, ingest, process, and visualize e-commerce traffic on the fly. It is built to handle live data streams, enforce data quality, and provide instant analytical insights using modern data engineering tools and practices.

## 🏗️ Architecture & Workflow
The pipeline follows a robust event-driven architecture combined with the **Medallion Architecture** pattern for data warehousing:

1. **Ingestion (Producer):** Custom Python scripts generate simulated live e-commerce events (e.g., purchases, clicks, user sessions) and push them to **Apache Kafka** topics.
2. **Streaming (Consumer):** Kafka consumes the streaming data and continuously loads it into the data warehouse.
3. **Data Warehouse (Snowflake):** The data flows through three distinct layers to ensure quality and structure:
   * 🥉 **Bronze Layer:** Stores raw, unvalidated streaming events.
   * 🥈 **Silver Layer:** Cleans, filters, and validates the data (e.g., removing nulls, standardizing formats).
   * 🥇 **Gold Layer:** Aggregates and structures the data into business-ready tables for analytics.
4. **Visualization (Streamlit):** A live dashboard connects to the Gold layer to visualize key business metrics, revenue trends, and user activities in real-time.

## 🛠️ Tech Stack
* **Language:** Python
* **Message Broker / Streaming:** Apache Kafka
* **Cloud Data Warehouse:** Snowflake
* **Data Visualization:** Streamlit

## 🚀 How to Run the Project

### Prerequisites
* Python 3.8+ installed.
* Apache Kafka environment set up (Local or Cloud).
* Snowflake account with appropriate credentials.

### Installation Steps
1. **Clone the repository:**
```bash
   git clone [https://github.com/MohamedMigo/Kafka_Ecom.git](https://github.com/MohamedMigo/Kafka_Ecom.git)
   cd Kafka_Ecom
