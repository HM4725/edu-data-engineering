## 1) Installation
### 1-1. Install WSL (optional for Windows)
```powershell
wsl --install
```
After the installation, it needs to reboot.

### 1-2. Install and activate python-venv
#### Install python-venv
```bash
sudo apt update
sudo apt upgrade
sudo apt install python3-pip python3-venv
```
#### Activate the virtual environment
```bash
python3 -m venv .venv
. .venv/bin/activate
```

### 1-3. Install Airflow
#### Set Airflow home
```bash
export AIRFLOW_HOME=$(pwd)
```
#### Download Airflow
```bash
sh install.sh
```
Reference: https://airflow.apache.org/docs/apache-airflow/stable/start.html


## 2) Start Airflow
### 2-1. Run Airflow Standalone
```bash
airflow standalone
```