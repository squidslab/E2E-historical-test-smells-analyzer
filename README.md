# 🔍 E2E Historical Test Smells Analyzer

![Python](https://img.shields.io/badge/Python-3.8+-blue)
![Status](https://img.shields.io/badge/Status-Prototype-orange)
![Research](https://img.shields.io/badge/Purpose-Research-green)

⚠️ **Disclaimer**

This project **is not a final version of the tool**.

The codebase may contain:

* experimental implementations
* incomplete features
* potential bugs
* future changes and improvements

---

# 📄 Research Goal

The goal of this project is to support **empirical software engineering research** by analyzing how **End-to-End (E2E) test smells evolve over time in software repositories**.

Specifically, this tool aims to:

- detect **End-to-End test smells** in E2E test suites
- analyze their **historical evolution across commits**
- collect structured data useful for **empirical studies**
- store the results in **structured databases for further analysis**

# 📌 Project Overview

The system combines **static analysis** and **repository mining**.

Two main components are used:

1. **E2E Test Smell Detection**

A detector identifies smells inside test files.

2. **Historical Repository Mining**

The commit history of each repository is analyzed to understand **when smells appear and evolve**.

The final output is stored in **SQLite databases**.

---

# 📌 Architecture

The analysis pipeline follows these steps:

```
Repository Dataset
        │
        │
        ▼
E2E Test Smell Detector
(static analysis)
        │
        │
        ▼
CSV Files
(typescript_analysis.csv / javascript_analysis.csv)
        │
        │
        ▼
Historical Analyzer
(PyDriller commit mining)
        │
        │
        ▼
SQLite Databases
(historical_smellsJS.db / historical_smellsTS.db)
```

---

# 📌 Project Structure

```
project-root
│
├── history_smells-analyzerJS.py
├── history_smells-analyzerTS.py
├── requirements.txt
│
└── e2e-test-smell-analyzer/
```

---

# 🧪 Requirements

Before running the project ensure the following tools are installed:

* Python 3.8+
* pip
* Git
* Node.js (required by the smell detector)

---

# Setup Instructions

## 1. Download the E2E Test Smell Detector

Download the repository **e2e-test-smell-analyzer** as a `.zip` file.

Then:

1. Extract the archive
2. Copy the folder into this project directory
3. Open a terminal and move inside the folder

```
cd e2e-test-smell-analyzer
```

---

# 2. Configure the Detector

Follow the instructions contained in the **README of the detector** in order to install its dependencies and configure the environment.

---

# 3. Generate Analysis Files

Once configured, run the detector to generate the following files:

```
typescript_analysis.csv
javascript_analysis.csv
```

These files contain the **detected E2E test smells**.

---

# 4. Install Python Dependencies

Move to the **root directory of this project** and run:

```
pip install -r requirements.txt
```

---

# 5. Run the Historical Analysis

Execute the following scripts:

```
python history_smells-analyzerJS.py
python history_smells-analyzerTS.py
```

These scripts will analyze the commit history of the repositories and collect information about the evolution of the detected test smells.

---

# 🔀 Output

The system generates two SQLite databases:

* **historical_smellsJS.db**
* **historical_smellsTS.db**

The databases include information such as:

* repository name
* test file path
* framework
* commit SHA
* commit date
* commit author
* commit message
* smell type
* nearest and earliest future release date
* nearest and earliest future release version
* class name
* method name
* line number

These datasets can later be used for **empirical analysis or statistical studies**.

---

# ✴️​ Environment Variables (Optional)

The tool supports several environment variables to control the analysis.

| Variable             | Description                                       |
| -------------------- | ------------------------------------------------- |
| `E2E_TEST_MODE`      | Enables test mode with a reduced dataset          |
| `E2E_TEST_MAX_REPOS` | Maximum number of repositories to analyze         |
| `E2E_NUM_WORKERS`    | Number of parallel workers                        |
| `E2E_MAX_COMMITS`    | Maximum number of commits analyzed per repository |
| `E2E_DB_PATH`        | Path/name of the output database                  |
| `E2E_CLONE_ROOT`     | Directory where repositories are cloned           |

---

# ▶️​ Example Execution (PowerShell)

```
$env:E2E_TEST_MODE='1'
$env:E2E_TEST_MAX_REPOS='1'
$env:E2E_NUM_WORKERS='2'

python history_smells-analyzer.py
```

This configuration:

* enables **test mode**
* analyzes **only one repository**
* uses **two parallel workers**

---
