import pandas as pd

def check_rsi_condition(rsi, threshold=30):
    """RSI koşulunu kontrol eder."""
    return rsi < threshold

def check_inverse_fisher_rsi_condition(inverse_fisher_rsi, threshold=-0.8):
    """Inverse Fisher RSI koşulunu kontrol eder."""
    return inverse_fisher_rsi < threshold

def check_macd_condition(macd, macd_signal):
    """MACD koşulunu kontrol eder."""
    return macd > macd_signal

def check_bollinger_condition(close, lower_band, upper_band):
    """Bollinger Bands koşullarını kontrol eder."""
    buy_signal = close < lower_band
    sell_signal = close > upper_band
    return buy_signal, sell_signal

def check_fibonacci_condition(close, fibonacci_levels):
    """Fibonacci seviyeleri için koşulları kontrol eder."""
    buy_signal = close > fibonacci_levels['level_382']
    sell_signal = close < fibonacci_levels['level_618']
    return buy_signal, sell_signal
