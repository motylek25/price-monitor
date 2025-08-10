import pandas as pd
from rapidfuzz import fuzz, process
from price_monitor.utils import normalize_name

def match_competitors_to_catalog(
    scraped: pd.DataFrame, 
    catalog: pd.DataFrame, 
    cfg: dict
) -> pd.DataFrame:
    """Сопоставляет товары конкурентов с внутренним каталогом"""
    if scraped.empty or catalog.empty:
        return pd.DataFrame()
    
    # Предварительная обработка
    scraped = scraped.copy()
    catalog = catalog.copy()
    
    # Нормализация названий
    scraped["norm_name"] = scraped["name"].apply(normalize_name)
    catalog["norm_name"] = catalog["name"].apply(normalize_name)
    
    # Подготовка параметров
    threshold = cfg.get("match_threshold", 75)
    brand_boost = cfg.get("brand_boost", 10)
    
    # Создаем словарь для быстрого поиска
    catalog_dict = catalog.set_index("sku")[["norm_name", "brand"]].to_dict("index")
    
    # Функция для поиска наилучшего соответствия
    def find_best_match(row):
        query = row["norm_name"]
        
        # Поиск по всему каталогу
        best_sku = None
        best_score = 0
        
        for sku, data in catalog_dict.items():
            target = data["norm_name"]
            score = fuzz.WRatio(query, target)
            
            # Бонус за совпадение бренда
            if data["brand"] and data["brand"].lower() in query:
                score += brand_boost
                
            if score > best_score and score >= threshold:
                best_score = score
                best_sku = sku
                
        return best_sku, best_score if best_sku else None
    
    # Применяем сопоставление
    scraped[["matched_sku", "match_score"]] = scraped.apply(
        lambda x: find_best_match(x), 
        axis=1, 
        result_type="expand"
    )
    
    # Фильтруем совпадения
    matched = scraped.dropna(subset=["matched_sku"])
    
    # Форматируем результат
    result = matched.rename(columns={
        "site": "source_site",
        "name": "comp_name",
        "price": "comp_price",
        "url": "comp_url",
        "matched_sku": "sku",
    })[["source_site", "comp_name", "comp_price", "comp_url", "sku", "match_score"]]
    
    return result