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

Or, install `PyDrive2` and `wget` directly:

```bash
pip install PyDrive2 wget
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

# Export by session from FCT_DB.db. Trading days are represented by the regular day-session date.
./mining_rpt.py -e TX 1 -d 20260828 --session day    # TX_20260828
./mining_rpt.py -e TX 1 -d 20260828 --session night  # TX_20260828_night
./mining_rpt.py -e TX 1 -d 20260828 --session full   # TX_20260828_full

# The wrapper script accepts the same session selector.
./fex_daily.sh 4 --session full

# FUT_TX.json and FUT_MTX.json are generated only for the day session.
# Night and full exports produce CSV files only.

```

### 5. Automation Example (crontab)

Automate the backup by adding to your crontab (example):

```
30 15,20 * * 1-4 ./git/workspace/fex_daily.sh
30 15,20 * * 5   ./git/workspace/fex_daily.sh 4
```

### Web data exports and ownership

Data fetching, SQLite processing, JSON publication, and Python regression tests
belong to `taifex_daily`. The `taifex_web` repository owns the published JSON,
HTML/CSS/JavaScript, and frontend tests.

The existing jobs have separate responsibilities; no additional export job is needed:

```cron
05 15,19 * * * ~/git/taifex_daily/mining_rpt.py --upload-recover > ~/git/taifex_daily/log.txt 2>&1
00 17,20 * * 1-5 cd ~/git/taifex_web/web_json && ~/git/taifex_daily/get_data.py --log-level DEBUG > ~/git/taifex_daily/log2.txt 2>&1
```

`mining_rpt.py` handles futures reports and day-session candle JSON.
`get_data.py` updates institutional and spot data, runs strategies, and exports
`foreign_positions.json` through `lib.foreign_positions`. Its JSON destination
is the working directory, so keep the `cd` in the second job. These are job
examples for the environment where the scripts and databases are installed.

For a manual position-only export, run from this repository:

```bash
python3 -m lib.foreign_positions ./II_DB.db --date 2026/10/16 --output ../taifex_web/web_json/foreign_positions.json
```

The output path is explicit to avoid publishing into the wrong working directory.
This uses the same exporter as `get_data.py`: it reads SQLite without modifying
the database, preserves other dates, replaces the requested date atomically,
and leaves the output unchanged for incomplete source data.

Keep `tests/test_foreign_positions.py` and `tests/test_json_exports.py` in this
repository and commit them with the exporter and pipeline changes. Run them with:

```bash
python3 -m unittest discover -s tests -p 'test_foreign_positions.py'
python3 -m unittest discover -s tests -p 'test_json_exports.py'
```

## 🛠️ Related Projects

- [taifex_web](https://github.com/luketseng/taifex_web): Web frontend for chip analysis and data visualization.
- [taifex_infra](https://github.com/luketseng/taifex_infra): Infrastructure setup for containerized automation and testing.

## 📃 License

Personal, non-commercial use only.
For commercial usage or database files, please contact: luke360351@gmail.com

---

If you encounter any issues or have suggestions, feel free to open an Issue or contact the maintainer.
