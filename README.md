# WeatherDataPipline
# 🌦️ Automated Weather Data Pipeline

This project is an **automated data pipeline** that collects, processes, and stores **daily weather data** for multiple Egyptian cities. It utilizes the **WeatherAPI**, **Google Sheets**, and **Google BigQuery** to streamline the flow of data from API to cloud storage using **Python**.

## 🚀 Features

- ⛅ **Daily** weather data extraction for 19 Egyptian cities using WeatherAPI
- 🧹 Cleans and transforms JSON data into structured tabular format
- 📊 Appends daily records to Google Sheets with deduplication
- ☁️ Pushes the last 30 days of data into BigQuery for scalable analytics
- 🔄 Automatically manages dataset and table creation in BigQuery
- 🔐 Secure access via environment variables and service account credentials

---

## 🛠️ Tech Stack

- **Python**
- **WeatherAPI**
- **Pandas**
- **gspread** & **Google Sheets API**
- **Google BigQuery**
- **Google Cloud SDK**
- **OAuth2 / Service Account Authentication**

---

## 📁 Project Structure

