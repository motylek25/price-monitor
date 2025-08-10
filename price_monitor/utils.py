import re
import unicodedata

def parse_price(text: str) -> float:
    """Извлекает цену из текста с учетом разных форматов"""
    if text is None or not isinstance(text, str):
        return None
        
    # Нормализация Unicode и удаление специальных пробелов
    s = unicodedata.normalize("NFKC", text)
    s = s.replace("\u202f", " ").replace("\xa0", " ")
    
    # Поиск числовых значений (включая разделители тысяч)
    matches = re.findall(r"[\d\s,\.]+", s)
    if not matches:
        return None
        
    # Берем первое найденное число и преобразуем
    num_str = matches[0].replace(" ", "").replace(",", ".")
    try:
        return float(num_str)
    except ValueError:
        return None

def normalize_name(name: str) -> str:
    """Нормализует название продукта для сравнения"""
    if not isinstance(name, str):
        return ""
        
    # Приведение к нижнему регистру и удаление спецсимволов
    s = name.lower()
    s = re.sub(r"[^a-zа-я0-9\s]", " ", s)  # Удаляем все не-буквы/цифры
    s = re.sub(r"\s+", " ", s).strip()      # Убираем лишние пробелы
    return s