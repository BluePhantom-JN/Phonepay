# 📊 PhonePe Pulse Data Pipeline with MySQL

This project is designed to extract JSON data from PhonePe Pulse data repository and insert it into a structured **MySQL** database. It uses **Python**, **Pandas**, and **MySQL Connector** to perform **ETL (Extract, Transform, Load)** operations across multiple datasets such as **aggregated transactions**, **user data**, **insurance**, **map-based metrics**, and **top district-wise records**.
## 🏗️ Project Structure

```bash
project-root/
│
├── pulse/
│   └── data/
│       ├── aggregated/
│       ├── map/
│       └── top/
├── main.py  ← Your current script
└── README.md
```

## 🔧 Requirements

* Python 3.7+
* MySQL Server
* Required Python libraries:

  ```bash
  pip install pandas mysql-connector-python
  ```

---

## ⚙️ Setup Instructions

### 🔌 Step 1: Configure MySQL Connection

In your script:

```python
connection = mysql.connector.connect(
    host='localhost',
    user='root',
    password='root',
    database='phonepay'
)
```

Ensure the database `phonepay` exists before running the script:

```sql
CREATE DATABASE phonepay;
```

---

## 📁 Dataset Modules

### 1. Aggregated Data

#### ✅ `aggregated/transaction`

* Transaction type
* Count
* Amount

#### ✅ `aggregated/user`

* Brand-wise device usage
* Registered users & app opens

#### ✅ `aggregated/insurance`

* Insurance transaction count and amount

---

### 2. Map-Based Data

#### ✅ `map/insurance`

* State-wise insurance data with coordinates

#### ✅ `map/transaction`

* District-level transaction count and amount

#### ✅ `map/user`

* District-level user registrations and app opens

---

### 3. Top Data

#### ✅ `top/insurance`

* Top districts by insurance metrics

---

## 🧠 Code Functionality

### 🔍 Extract Function

Reads nested directory data using:

```python
def extract_transaction_data(path):
    ...
```

### 🧾 Dataframe to SQL

Converts Pandas DataFrame to tuple list:

```python
def get_list_values(values):
    return [tuple(x) for x in values.to_numpy()]
```

### 📥 SQL Table Creation & Insertion

Each dataset includes:

* Table creation with primary keys
* JSON parsing
* Dataframe creation
* `INSERT IGNORE` to prevent duplication

---

## 🏁 How to Run

1. Update all file paths according to your system
2. Ensure MySQL server is running
3. Run the script:

   ```bash
   python main.py
   ```

---

## 🔐 Primary Keys & Integrity

Each table uses a **composite primary key** for unique entries (e.g., `state + year + quarter + type`) to maintain data integrity and prevent duplicates.

---

## 📌 Notes

* You can use `INSERT IGNORE` for idempotent insertions
* Each `.json` file is parsed only if it contains valid data
* Percentages are formatted as string values with `%`

---
## 📜 License
This project is for educational use. Attribution required if reused in public platforms.
---
