# 📊 PhonePe Pulse Data Pipeline with MySQL

This project extracts, transforms, and loads data from the **[PhonePe Pulse GitHub Repository](https://github.com/PhonePe/pulse)** into a **MySQL** database using Python. It covers **aggregated**, **map-based**, and **top** datasets related to transactions, users, and insurance across India.

---

## 🧬 Clone the Dataset

To begin, clone the official PhonePe Pulse data repository:

```bash
git clone https://github.com/PhonePe/pulse.git
```

This will download the full JSON dataset used by the script.

---

## 🏗️ Project Structure

```bash
project-root/
│
├── pulse/                        ← Cloned GitHub repo
│   └── data/
│       ├── aggregated/
│       ├── map/
│       └── top/
├── demo.py                       ← Python ETL script
└── README.md
```

---

## 🔧 Requirements

* **Python 3.7+**
* **MySQL Server**
* Required Python libraries:

  ```bash
  pip install pandas mysql-connector-python
  ```
---

## ⚙️ Setup Instructions

### 🔌 Step 1: Configure MySQL Connection

```python
connection = mysql.connector.connect(
    host='localhost',
    user='root',
    password='root',
    database='phonepay'
)
```

Make sure the database exists:

```sql
CREATE DATABASE phonepay;
```

---

## 📁 Dataset Modules

### 1. Aggregated Data

| Dataset Path             | Data Included                                   |
| ------------------------ | ----------------------------------------------- |
| `aggregated/transaction` | Transaction type, count, amount                 |
| `aggregated/user`        | Device brand usage, app opens, registered users |
| `aggregated/insurance`   | Insurance count and value                       |

---

### 2. Map-Based Data

| Dataset Path      | Data Included                     |
| ----------------- | --------------------------------- |
| `map/insurance`   | Insurance data with lat/lng       |
| `map/transaction` | District-wise transaction metrics |
| `map/user`        | District-wise user data           |

---

### 3. Top Data

| Dataset Path    | Data Included                      |
| --------------- | ---------------------------------- |
| `top/insurance` | Top districts by insurance metrics |

---

## 🧠 Code Functionality

### 🔍 Data Extraction

```python
def extract_transaction_data(path):
    ...
```

Walks through the JSON files and yields structured records.

### 🔄 Data Transformation

Data is transformed into Pandas DataFrames, then converted to SQL tuples:

```python
def get_list_values(values):
    return [tuple(x) for x in values.to_numpy()]
```

### 🗃️ Table Creation & Data Insertion

* Tables are created with **primary keys**
* Data is inserted using `INSERT IGNORE` to prevent duplicates

---

## 🏁 Run the Project

1. Clone the PhonePe Pulse repo
2. Configure MySQL
3. Update file paths in `main.py`
4. Run:

   ```bash
   python main.py
   ```

---

## 📌 Data Integrity

Each table uses composite primary keys like `(state, year, quarter, type)` to ensure unique records.

---

## 📜 License

For educational purposes. Please credit if reused or published.

---
