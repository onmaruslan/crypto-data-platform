# 🚀 Real-Time Crypto Data Platform

Production-like Data Engineering project.

## 📌 Overview

This project implements an end-to-end ELT pipeline:

- Extract crypto trade data from public API
- Store raw data in AWS S3 (data lake)
- Orchestrate workflows using Apache Airflow
- Load data into PostgreSQL (staging layer)
- Transform data using ELT approach
- Export processed data to Parquet format

## 🏗 Architecture

Crypto API  
↓  
S3 (Raw Data Lake)  
↓  
Airflow DAG  
↓  
PostgreSQL (Staging)  
↓  
Analytics Tables  

## 🛠 Tech Stack

- Python
- PostgreSQL
- Apache Airflow
- AWS S3
- Parquet (PyArrow)
- Docker
- GitHub Actions (CI/CD)