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
sudo apt install python3-pip python3-venv pkg-config default-libmysqlclient-dev
```
#### Activate the virtual environment
```bash
python3 -m venv .venv
. .venv/bin/activate
```

### 1-3. Install Airflow
#### Download Airflow
```bash
sh install.sh
```
Reference: https://airflow.apache.org/docs/apache-airflow/stable/start.html
#### Run bash script
```bash
source ~/.bashrc
```
#### Install additional packages for python
```bash
pip install apache-airflow-providers-mysql
```

## 2) Start Airflow
### 2-1. Run Airflow Standalone
```bash
airflow standalone
```