# 安裝 3.12（macOS / Homebrew）
brew install python@3.12

# 建立新的 venv
/opt/homebrew/bin/python3.12 -m venv .env312
source .env312/bin/activate

# 安裝依賴
pip install -r requirements.txt

# 執行你的腳本
python tpex_update.py