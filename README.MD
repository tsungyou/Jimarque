# 📈 Jimarque — TWSE/TPEX Data Pipeline

> 從 **TWSE、TPEX** 等網站抓取市場/個股指標，清洗後批量寫入 **PostgreSQL**。支援 **重試/退避/隨機抖動**、**UA/代理輪換**、以及 **每日排程**。

[![Python](https://img.shields.io/badge/Python-3.12+-3776AB.svg?logo=python&logoColor=white)](#requirements)
[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-14+-336791.svg?logo=postgresql&logoColor=white)](#database-schema)
[![License](https://img.shields.io/badge/License-MIT-black.svg)](#license)

---

## 目錄
- [概覽](#概覽)
- [資料來源](#資料來源)
- [系統架構](#系統架構)
- [快速開始](#快速開始)
- [設定](#設定)
- [資料庫結構](#資料庫結構)
- [常用腳本與指令](#常用腳本與指令)
- [排程](#排程)
- [除錯指南](#除錯指南)
- [最佳化建議](#最佳化建議)
- [專案結構（建議）](#專案結構建議)
- [License](#license)

---

## 概覽
- 🔁 **IP/UA 輪換**：定時或每 N 次切身份；403/429/5xx 自動退避並換身分  
- 🧪 **型別清洗**：`"" / "-"` → `NULL`；數字字串去逗號再轉 `Decimal/int`  
- 🧮 **批次寫入**：`psycopg2.extras.execute_values`（或 `COPY`）  
- 🧱 **UPSERT/去重**：用 `(da, code)` 或複合鍵避免重覆  
- 🛡️ **TLS 相容**（可選）：對特定站點放寬 OpenSSL 嚴格鏈檢核（仍驗憑證）

---

## 資料來源
- **TWSE**：`/rwd/zh/afterTrading/BWIBBU_d?date=YYYYMMDD&selectType=ALL&response=json`（殖利率/本益比/淨值比/收盤價…）
- **TPEX**：`/www/zh-tw/afterTrading/PERatio?date=YYYY/MM/DD&response=json`（上櫃 PER 指標）
- （可選）**TWSE OpenAPI**：`/v1/exchangeReport/BWIBBU_d` / `BWIBBU_ALL`

> 請遵守各網站 ToS、流量限制與 robots 規範。

---

## 系統架構
```mermaid
flowchart LR
  A[Scheduler/CLI] --> B[Fetcher (requests.Session)]
  B -->|UA/Proxy/Retry/Backoff| C[Parser & Normalizer]
  C --> D[Batch Writer]
  D -->|execute_values / COPY| E[(PostgreSQL)]
  E --> F[Analytics/BI/Backtests]
