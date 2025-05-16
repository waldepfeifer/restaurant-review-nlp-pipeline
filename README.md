<img width="1366" alt="image" src="https://github.com/user-attachments/assets/8a6ab53f-92d9-48f9-b519-0079ec4cf5ef" />

# Restaurant Review NLP Pipeline

## 📖 Project Overview

This project provides an end-to-end data pipeline for managing customer reviews from a restaurant chain. It consists of three scripts to handle:

- Ingestion of raw CSV reviews  
- NLP-based categorization using spaCy  
- Retrieval of processed reviews by date  

The system uses DuckDB for fast local storage and spaCy for natural language processing (lemmatization and entity recognition). It's built to be modular, efficient, and scalable across restaurants or input sources.

## 🎯 Objectives

- Ingest raw customer reviews into a centralized database  
- Automatically categorize each review as FOOD, SERVICE, or GENERAL  
- Enhance each review with:
  - Number of lemmas  
  - Character count  
- Enable filtered export of reviews from a given date  

## 🛠 Technologies Used

- Python 3.x  
- DuckDB  
- Pandas  
- spaCy  
- JSON  

## 📂 Project Structure

restaurant-review-nlp-pipeline/  
├── review_ingestion.py         – Ingests raw CSV reviews into raw_messages table  
├── review_processing.py        – Processes reviews using NLP and stores results in proc_messages  
├── review_retrieval.py         – Outputs JSON of processed reviews filtered by date  
├── requirements.txt            – Python dependencies  
├── reviews_sample.csv          – Sample input file  
└── messages.json               – Output JSON from retrieval script  

## 🚀 How It Works

### 1️⃣ Review Ingestion

Script: review_ingestion.py  
Usage:  
python3 review_ingestion.py reviews_sample.csv  

- Loads CSV with columns: timestamp, uuid, message  
- Creates DuckDB database sup-san-reviews.ddb if not present  
- Appends non-duplicate reviews to the raw_messages table  

### 2️⃣ NLP-Based Review Processing

Script: review_processing.py  
Usage:  
python3 review_processing.py  

- Uses spaCy to tokenize and lemmatize messages  
- Detects if a message is about:
  - FOOD (lemmas: sandwich, bread, meat, cheese, ham, omelette, food, meal)  
  - SERVICE (lemmas: waiter, service, table or MONEY entities)  
  - GENERAL (if neither applies)  
- Outputs to proc_messages with:
  - uuid, timestamp, message, category, num_lemm, num_char  
- Logs processed uuids in proc_log to avoid duplicates  

### 3️⃣ Review Retrieval (Filtered JSON Export)

Script: review_retrieval.py  
Usage:  
python3 review_retrieval.py YYYY-MM-DD  

- Fetches all processed reviews with timestamp >= input date  
- Outputs a file named messages.json with this structure:

num: 3  
messages:  
  - uuid: "123e4567-e89b-12d3-a456-426614174000"  
    timestamp: "2024-05-01T14:32:00"  
    message: "The sandwich was fresh and amazing!"  
    category: "FOOD"  
    num_lemm: 7  
    num_char: 43  
  - ...  

## 📊 Data Tables Summary

raw_messages:  
- timestamp  
- uuid  
- message  

proc_messages:  
- timestamp  
- uuid  
- message  
- category  
- num_lemm  
- num_char  

proc_log:  
- uuid  
- proc_time  

## 📦 Requirements

Install dependencies using:  
pip install -r requirements.txt  

Also run:  
python -m spacy download en_core_web_sm  

## ⚠ Notes

- sup-san-reviews.ddb is created automatically in the current directory  
- spaCy is used for tokenization, lemmatization, and entity detection  
- Category assignment logic is rule-based  
- Messages are processed only once using a control table  

## 📜 License

This project is licensed under the MIT License. See LICENSE file for details.
