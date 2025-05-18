# TAIFEX DAILY

This project is designed to back up historical data from the Taiwan Futures Exchange (TAIFEX).

## 🌐 Web Application

- The data backed up by this project is visualized and accessible via the TAIFEX Web frontend:  
  [http://zane.myftp.org/](http://zane.myftp.org/)

## 📦 Features

- Automated download and backup of TAIFEX data.
- Supports Google Drive upload for cloud backup.
- Integrates with other TAIFEX projects (see below).
- SQLite3 database for local historical data storage.

## 🚀 Getting Started

### 1. Install Dependencies

```bash
pip install -r requirements.txt
```
Or, install `PyDrive` and `wget` directly:
```bash
pip install PyDrive wget
```

### 2. Google Drive API Setup

This procedure requires several steps:

1. Go to [Google Cloud Console](https://console.cloud.google.com/).
2. Create a new project.
3. Enable the "Google Drive API" for your project.
4. Create credentials for a desktop application and download the `client_secrets.json` file.
5. Place `client_secrets.json` in the `devices/` folder.

**Note:**  
On first use, run any script that performs a Google Drive upload in an environment with a UI/browser. This will open a browser window for OAuth 2.0 authentication. Upon successful authentication, a `mycreds.txt` file will be created in the `devices/` folder for future access.

```
devices/
├── client_secrets.json   # Downloaded from Google Cloud Console
├── mycreds.txt           # Generated after completing OAuth 2.0 authentication
```

### 3. Database Files

- The project uses `sqlite3` for database storage.
- The database files (`FCT_DB.db`, `II_DB.db`) are **not provided by default**.  
  If you require the database files, please contact: luke360351@gmail.com

- The database schema is referenced in `creat_tb_for_db`.  
  An initialization script will be provided in the future.

### 4. Example Usage

```bash
# Example: Download data for 2019/01/01 to 2019/01/02
./mining_rpt.py -d 20190101-20190102

# Example: Export TX futures data in 300-minute intervals (equivalent to one trading day(5-hours); day-level data)
./mining_rpt.py -e TX 300 -d 20190101

```

### 5. Automation Example (crontab)

Automate the backup by adding to your crontab (example):

```
30 15,20 * * 1-4 ./git/workspace/fex_daily.sh
30 15,20 * * 5   ./git/workspace/fex_daily.sh 4
```

## 🛠️ Related Projects

- [taifex_web](https://github.com/luketseng/taifex_web): Web frontend for chip analysis and data visualization.
- [taifex_infra](https://github.com/luketseng/taifex_infra): Infrastructure setup for containerized automation and testing.

## 📃 License

Personal, non-commercial use only.  
For commercial usage or database files, please contact: luke360351@gmail.com

---

If you encounter any issues or have suggestions, feel free to open an Issue or contact the maintainer.
