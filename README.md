# Threaded API Miner
A multithreaded tool for mining data from APIs with configurable endpoints.

## Features
- Multithreaded API requests
- Configurable endpoints
- Retry logic with rotating proxies
- Progress tracking
- Data storage to disk or database

## Setup
1. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

2. Configure .env with required variables.
3. Run the tool:
   ```bash
   python main.py
   ```

## Project Structure
- `api_client/` API client logic and endpoint configurations.
- `utils/` Utility modules for logging, retries, and workers.
- `main.py` Entry point for the tool.