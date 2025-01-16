
## Threaded API Miner

A multithreaded tool for mining data from APIs with configurable endpoints.

---

## Features
- Multithreaded API requests
- Configurable endpoints
- Retry logic with rotating proxies
- Progress tracking
- Data storage to disk or database

---

## Setup

### 1. Install Dependencies
Install the required Python dependencies:
```bash
pip install -r requirements.txt
```

### 2. Configure Environment Variables
Create a `.env` file in the root directory and configure the required variables:
```env
SCRAPOXY_USER=<your_scrapoxy_user>
SCRAPOXY_TOKEN=<your_scrapoxy_token>
SCRAPOXY_PORT=<your_scrapoxy_port>
SCRAPOXY_URL=<your_scrapoxy_url>
SCRAPOXY_CRT=<optional_certificate_path>
DEFAULT_ENDPOINT=locations
DUCKDB_TOKEN=<your_duckdb_token>
```

### 3. Install and Set Up Scrapoxy
[Scrapoxy](https://github.com/scrapoxy/scrapoxy) is recommended for proxy management. Follow these steps to install and configure Scrapoxy:
For latest instructions visit their [docs](https://scrapoxy.io/intro/scrapoxy).

1. Install Docker if it is not already installed:
   ```bash
   brew install --cask docker
   ```

2. Pull the Scrapoxy Docker image:
   ```bash
   docker pull scrapoxy/scrapoxy
   ```

3. Run the Scrapoxy container:
   ```bash
   docker run -d -p 8888:8888 -p 8890:8890 -v ./scrapoxy:/cfg -e AUTH_LOCAL_USERNAME=admin -e AUTH_LOCAL_PASSWORD=password -e BACKEND_JWT_SECRET=secret1 -e FRONTEND_JWT_SECRET=secret2 -e STORAGE_FILE_FILENAME=/cfg/scrapoxy.json scrapoxy/scrapoxy
   ```

4. Access the Scrapoxy dashboard:
   Open your browser and navigate to `http://localhost:8888`. Use your Scrapoxy credentials to log in.

5. Configure your cloud provider using Scrapoxy's docs.

### 4. Run the Tool
Run the tool with the desired endpoint:
```bash
python main.py --endpoint locations
```

---

## Project Structure
- `api_client/`: API client logic and endpoint configurations.
- `utils/`: Utility modules for logging, retries, and workers.
- `main.py`: Entry point for the tool.

---

## Notes
- Ensure that Scrapoxy is running and properly configured before starting the tool.
- You can customize the number of threads, batch size, and maximum records by modifying the global variables in `main.py`.

---

## Troubleshooting
- If you encounter issues with proxies, verify that Scrapoxy is running and the `.env` file is correctly configured.
- For database-related issues, ensure that the DuckDB connection string is valid and the database file is accessible.