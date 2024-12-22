This project is a simple algorithmic system designed to generate buy/sell signals for the cryptocurrency market based on specific technical indicators. The signals are sent via Telegram notifications.

Features:
Technical Indicators:
RSI (Relative Strength Index)
MACD (Moving Average Convergence Divergence)
Bollinger Bands
Volume
Condition-Based Signal Generation:
Combines multiple indicators to generate buy/sell signals under predefined conditions.
Telegram Notifications:
Sends real-time alerts to Telegram when signal conditions are met.
API Integration:
Fetches live price data using the Binance API.
Folder Structure:
bash
Kodu kopyala
/crypto-signal-bot
|-- /data                   # Logs for historical and live data
|-- /indicators             # Technical indicator calculations
|-- /signals                # Buy/Sell signal generation
|-- /notifications          # Telegram integration
|-- /config                 # Configuration files and parameters
|-- main.py                 # Main file to run the system
Usage Instructions:

Install Dependencies:
bash
pip install -r requirements.txt
Set Up Your API Keys and Telegram Token:
Configure the config/settings.py file with your Binance API keys and Telegram token.
Run the Application:
bash

python main.py
Notes:
This phase does not include artificial intelligence. Signals are purely based on technical indicators.
More advanced algorithms and optimizations will be introduced in Phase 2.
