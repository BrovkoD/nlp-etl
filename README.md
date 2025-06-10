# NLP-ETL Pipeline for News and Telegram Analysis

## Table of Contents
1.  [Project Overview](#1-project-overview)
2.  [Technology Stack](#2-technology-stack)
3.  [Project Structure](#3-project-structure)
4.  [Setup and Running the Project](#4-setup-and-running-the-project)
    * [Prerequisites](#prerequisites)
    * [Configuration](#configuration)
    * [Building and Starting Services](#building-and-starting-services)
    * [Executing One-Time Initialization Scripts](#executing-one-time-initialization-scripts)
    * [Accessing Airflow UI](#accessing-airflow-ui)
    * [Applying .env Changes](#applying-env-changes)
5.  [DAGs Overview](#5-dags-overview)
6.  [Troubleshooting](#6-troubleshooting)

## 1. Project Overview

This project implements an Extract, Transform, Load (ETL) pipeline with a focus on Natural Language Processing (NLP). It's designed to process textual data from two distinct sources: a CNN news dataset and the Telegram channel "Труха⚡️Україна". The pipeline utilizes advanced language models for summarization and manipulation detection, with Apache Airflow serving as the orchestrator for all data workflows, and MySQL as the persistent data store.

**Key Features:**
* **Diverse Data Ingestion:** Extracts data from a CNN news dataset and live messages from the "Truxa Ukraine" Telegram Channel.
* **NLP-Powered Summarization:** Summarizes news articles using a T5-small language model.
* **Manipulation Detection:** Identifies tokens indicating manipulation within Telegram messages using a BERT-based model.
* **Structured Data Storage:** All processed data, including summaries and manipulation detection results, are stored in a MySQL database.
* **Robust Workflow Orchestration:** Apache Airflow manages task scheduling, dependencies, execution, and monitoring, ensuring the reliability and efficiency of the ETL processes.

## 2. Technology Stack

* **Python:** The core programming language for custom scripts, Airflow DAGs, and NLP model integration.
* **Apache Airflow (2.8.1-python3.10):** Workflow management platform for scheduling, executing, and monitoring the data pipelines.
* **MySQL (8):** Relational database used for storing both raw and processed data.
* **T5-small Model:** A smaller, efficient Transformer model for text-to-text tasks, specifically used for summarization within the `llm` service.
* **BERT Model:** A powerful Transformer model for natural language understanding, employed for manipulation detection. Assumed to be hosted separately or integrated into the `llm` service.
* **Docker:** Containerization platform used to package all services and their dependencies into isolated environments.
* **Docker Compose (v3.8):** A tool for defining and running multi-container Docker applications, simplifying the setup and management of the project's services.
* **Uvicorn / FastAPI:** (Within `llm` service) For serving the NLP models as an API.

## 3. Project Structure

```
nlp-etl/
├── .env                     # Environment variables for service configuration (e.g., database credentials, Airflow admin user)
├── docker-compose.yml       # Defines and orchestrates all Docker services
├── airflow/                 # Airflow-related configurations and files
│   ├── dags/                # Airflow DAG definitions
│   │   ├── telegram_dag.py
│   │   └── text_summary_dag.py
│   ├── scripts/             # Initialization scripts and their own Dockerfile/requirements
│   │   ├── mysql_init.py    # Script for initial MySQL table/schema setup
│   │   ├── telegram_init.py # Script for initial Telegram channel setup or data fetch
│   ├── Dockerfile           # This is the Dockerfile for the Airflow webserver/scheduler services
│   └── requirements.txt     # Python dependencies for Airflow core functionality and DAGs
└── llm/                     # LLM service files
    ├── api.py               # FastAPI application to expose LLM functionalities
    ├── Dockerfile           # Dockerfile for the LLM service
    └── requirements.txt     # Python dependencies for LLM models (e.g., transformers)
```

## 4. Setup and Running the Project

### Prerequisites

* **Docker Desktop:** Ensure Docker Desktop (which includes Docker Engine and Docker Compose) is installed and running on your system.
    * [Download Docker Desktop](https://www.docker.com/products/docker-desktop/)

### Configuration

1.  **Create/Configure `.env` file:**
    Create a file named `.env` in the root of your `nlp-etl` directory. This file will hold all necessary environment variables for your services.

    ```dotenv
    MYSQL_USER=
    MYSQL_PASSWORD=
    MYSQL_HOST=mysql:3306
    MYSQL_DB=nlp_etl
    MYSQL_ROOT_PASSWORD=
    
    AIRFLOW_USERNAME=
    AIRFLOW_FIRSTNAME=
    AIRFLOW_LASTNAME=
    AIRFLOW_ROLE=Admin
    AIRFLOW_EMAIL=
    AIRFLOW_PASSWORD=
    
    AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=
    AIRFLOW__CORE__EXECUTOR=LocalExecutor
    AIRFLOW__WEBSERVER__SECRET_KEY=
    AIRFLOW__LOGGING__REMOTE_LOGGING=False
    AIRFLOW__WEBSERVER__SERVE_LOGS=True
    
    TELEGRAM_API_ID=
    TELEGRAM_API_HASH=
    TELEGRAM_CHANNEL=@trukha_ukrainaa
    
    MANIPULATION_API_URL=
    SUMMARY_API_URL=http://llm:8000/predict
    ```
    **Fill values** with your actual desired credentials and database names.

### Building and Starting Services

1.  **Navigate to Project Root:**
    Open your terminal and change directory to the `nlp-etl` directory where your `docker-compose.yml` file is located.

    ```bash
    cd /path/to/your/nlp-etl
    ```

2.  **Build Images and Start Containers:**
    This command will build all necessary Docker images for your services (Airflow, LLM) and start all containers in detached mode.

    ```bash
    docker compose up --build
    ```
    * `--build`: Forces Docker Compose to rebuild images for services that have a `build` context (like `airflow-webserver`, `airflow-scheduler`, `llm`). This is crucial if you make changes to your Dockerfiles or application code.

3.  **Verify Services Status:**
    To check if all services are running correctly:

    ```bash
    docker ps
    ```
    You should see `mysql`, `airflow-webserver`, `airflow-scheduler`, and `llm` containers listed with `Status: Up`.

### Executing One-Time Initialization Scripts

1.  **Get Airflow Scheduler Container ID/Name:**
    It's often good practice to run one-off tasks from the scheduler container.

    ```bash
    docker ps
    # Note down the CONTAINER ID or NAME of your 'nlp-etl-airflow-scheduler-1' service (e.g., 'e3e44a335663')
    ```
    Let's assume the ID is `<airflow_scheduler_container_id>`.

2.  **Run `telegram_init.py`:**
    This script is intended to fetch initial Telegram messages or perform setup.

    ```bash
    docker exec -it <airflow_scheduler_container_id> python /scripts/telegram_init.py
    ```

3.  **Run `mysql_init.py`:**
    This script is intended to set up your MySQL database schema, tables, or initial data.

    ```bash
    docker exec -it <airflow_scheduler_container_id> python /scripts/mysql_init.py
    ```

### Accessing Airflow UI

Once your services are up and running, you can access the Airflow Web UI:

* Open your web browser and go to: `http://localhost:8080`
* Log in using the `AIRFLOW_USERNAME` and `AIRFLOW_PASSWORD` you defined in your `.env` file.

## 5. DAGs Overview

The project includes two primary Airflow DAGs located in `airflow/dags/`:

1.  **CNN News ETL DAG (`text_summary_dag.py`):**
    * **Extract:** CNN News Dataset (available in MySQL after running the initialization script).
    * **Transform:** Calls the `llm` service to summarize news articles using the `t5-small` model.
    * **Load:** Saves the summarized content back into MySQL.

2.  **Telegram Channel ETL DAG (`telegram_dag.py`):**
    * **Extract:** Fetches the last message from the Telegram Channel "Труха⚡️Україна".
    * **Transform:** Calls the `llm` service (or another endpoint) to detect tokens indicative of manipulation using a BERT-based model (hosted separately).
    * **Load:** Saves the detection results (e.g., flagged tokens, manipulation scores) into MySQL.

## 6. Troubleshooting

* **"Bad Request - The CSRF session token is missing" in Airflow UI:**
    * Clear your browser's cache and cookies.
    * Try an Incognito/Private window.
