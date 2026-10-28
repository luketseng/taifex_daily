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
- Tick-to-minute OHLCV conversion that preserves every trade, including identical records.
- Atomic replacement of supplied minute keys, with rollback on write failure.
- Independent raw-trade validation of minute counts and OHLCV.

## 🚀 Getting Started

### 1. Install Dependencies

Install the Python dependencies used by report parsing, Drive uploads, and
institutional-data fetching:

```bash
python3 -m pip install numpy PyDrive2 selenium chardet
```

Report downloads also use the system `wget` executable. Institutional-data
fetching requires Chrome/Chromium and a compatible WebDriver in the deployment.

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

A new report missing from Drive during its first upload is logged at INFO level.
Missing required objects and duplicate lookup results still produce warnings.

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

# Aggregate 300 source minute rows; missing minutes are not synthesized.
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

### 5. Automation examples

Install jobs in the deployment's crontab using the Asia/Taipei timezone.
`taifex_cron.txt` is a local deployment file and is not tracked by Git.

```cron
05 15,19 * * * ~/git/taifex_daily/mining_rpt.py --upload-recover > ~/git/taifex_daily/log.txt 2>&1
00 17,20 * * 1-5 cd ~/git/taifex_web/web_json && ~/git/taifex_daily/get_data.py --log-level DEBUG > ~/git/taifex_daily/log2.txt 2>&1
30 15,20 * * 1-4 ~/git/taifex_daily/fex_daily.sh
30 15,20 * * 1-4 ~/git/taifex_daily/fex_daily.sh -s full
30 15,20 * * 5 ~/git/taifex_daily/fex_daily.sh 4
30 15,20 * * 5 ~/git/taifex_daily/fex_daily.sh 4 -s full
```

### Web data exports and ownership

Data fetching, SQLite processing, JSON publication, and Python regression tests
belong to `taifex_daily`. The `taifex_web` repository owns the published JSON,
HTML/CSS/JavaScript, and frontend tests.

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

### Data integrity and validation

Raw ticks with identical fields remain separate trades. Minute candles use
end-of-minute labels, omit zero-trade minutes, and retain exact closing trades
in the final session minute. Standard TX/MTX night plus day sessions have
1,140 possible minute slots; the expected candle count subtracts slots with
no raw trades. The independent validator also checks every OHLCV value.

Reimporting replaces only supplied `(Date, Time)` candle keys in one transaction.
Absent keys remain untouched; existing historical data is not automatically
repaired by installing the code changes. Multi-minute exports group source
rows by count, rather than resampling to fixed wall-clock intervals.

See [Data integrity repairs and remaining historical scope](docs/data-integrity-repair.md)
for root causes, validation rules, the October 2026 cleanup/backfill results,
and unresolved history. Local recovery backups under `.db-cleanup-backups/`
are excluded from version control.

Run the regression groups in an environment with the project dependencies:

```bash
python3 -m unittest discover -s tests -p 'test_candle_storage.py'
python3 -m unittest discover -s tests -p 'test_tick_candles.py'
python3 -m unittest discover -s tests -p 'test_gdrive_logging.py'
python3 -m unittest discover -s tests -p 'test_json_exports.py'
python3 -m unittest discover -s tests -p 'test_foreign_positions.py'
```

These checks use temporary databases and mocked Drive operations.

## 🛠️ Related Projects

- [taifex_web](https://github.com/luketseng/taifex_web): Web frontend for chip analysis and data visualization.
- [taifex_infra](https://github.com/luketseng/taifex_infra): Infrastructure setup for containerized automation and testing.

## 📃 License

Personal, non-commercial use only.
For commercial usage or database files, please contact: luke360351@gmail.com

---

If you encounter any issues or have suggestions, feel free to open an Issue or contact the maintainer.
