# ETL Pipeline  planet42 Assessment

## 🚀 About the Project

This project implements an **ETL pipeline** that:
- **Extracts** transaction data from an API.
- **Transforms** the data (cleaning, enrichment, and aggregation).
- **Loads** the processed data into a MySQL database.
- Uses **Apache Airflow** for orchestration.
- Runs inside **Docker** using `docker-compose`.

## 🛠 Built With

- **Python**
- **Apache Airflow**
- **MySQL**
- **Docker & Docker Compose**

## 📦 Folder Structure

```
project_root/
│── dags/
│   ├── etl_dag.py  # Airflow DAG for scheduling ETL pipeline
│── config/
│   ├── requirements.txt  # Python dependencies
│── docker/
│   ├── Dockerfile  # Container setup
│   ├── docker-compose.yml  # Service orchestration
│── docs/
│   ├── README.md  
|   ├── Write Up.docx  
│── sql/
│   ├── analytics_queries.sql  # SQL queries for insights
```

## 🚀 Getting Started

### Prerequisites
Ensure you have the following installed:
- Docker & Docker Compose
- Python 3.9+

### Installation

1. Clone the repo:
   ```bash
   git clone https://github.com/tshipamadi2025/Planet42-Assessment.git
   cd Planet42-Assessment
   ```

2. Build and start services:
   ```bash
   docker-compose up --build
   ```

3. Access services:
   - **Airflow UI:** http://localhost:8080 (User: `airflow`, Password: `airflow`)
   - **MySQL:** `localhost:3306`, User: `root`, Password: `root`

## 📊 Running the Pipeline

1. Open **Airflow UI** (`http://localhost:8080`)
2. Enable & trigger the `etl_dag` DAG.
3. Verify data in MySQL:
   ```bash
   docker exec -it mysql_db mysql -u root -p
   # Enter password: root
   USE planet42_db;
   SELECT * FROM transactions LIMIT 10;
   ```

## 📈 Analytics Queries

### Total transactions per product category
```sql
SELECT product_category, COUNT(*) AS total_transactions FROM transactions GROUP BY product_category;
```

### Top 5 accounts by total transaction value
```sql
SELECT customer_id, SUM(transaction_amount) AS total_spent FROM transactions GROUP BY customer_id ORDER BY total_spent DESC LIMIT 5;
```

### Monthly spend trends over the past year
```sql
SELECT DATE_FORMAT(transaction_date, '%Y-%m') AS month, SUM(transaction_amount) AS total_spent FROM transactions WHERE transaction_date >= DATE_SUB(CURDATE(), INTERVAL 12 MONTH) GROUP BY month ORDER BY month;
```

## 🙌 Acknowledgments
- Based on [othneildrew's Best README Template](https://github.com/othneildrew/Best-README-Template).


