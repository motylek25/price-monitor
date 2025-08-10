import time
import re
from urllib.parse import urljoin
import requests
from bs4 import BeautifulSoup
from price_monitor.utils import parse_price

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120 Safari/537.36",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection": "keep-alive"
}

def scrape_bs4(site_cfg: dict) -> list:
    """Парсинг статических сайтов с помощью BeautifulSoup"""
    base_url = site_cfg.get("base_url", "")
    list_urls = site_cfg["list_urls"]
    selectors = site_cfg["selectors"]
    price_regex = re.compile(site_cfg.get("price_regex", r"[\d\s,.]+"))
    
    all_products = []
    
    for url in list_urls:
        try:
            # Загрузка страницы
            response = requests.get(url, headers=HEADERS, timeout=30)
            response.raise_for_status()
            
            # Парсинг HTML
            soup = BeautifulSoup(response.text, "lxml")
            
            # Поиск товарных карточек
            product_cards = soup.select(selectors["item"])
            
            for card in product_cards:
                try:
                    # Извлечение данных
                    name_elem = card.select_one(selectors["name"])
                    price_elem = card.select_one(selectors["price"])
                    url_elem = card.select_one(selectors["url"])
                    
                    if not all([name_elem, price_elem, url_elem]):
                        continue
                    
                    name = name_elem.get_text(strip=True)
                    price_text = price_elem.get_text(strip=True)
                    
                    # Извлечение цены
                    price_match = price_regex.search(price_text)
                    price_value = parse_price(price_match.group(0)) if price_match else None
                    
                    if not price_value:
                        continue
                    
                    # Формирование полного URL
                    product_url = url_elem.get(selectors.get("attr_url", "href"))
                    full_url = urljoin(base_url, product_url) if product_url else url
                    
                    # Сохранение результата
                    all_products.append({
                        "site": site_cfg["name"],
                        "name": name,
                        "price": price_value,
                        "url": full_url
                    })
                    
                except Exception as e:
                    print(f"  Ошибка обработки карточки: {str(e)}")
            
            # Задержка между запросами
            time.sleep(1.5)
            
        except Exception as e:
            print(f"  Ошибка загрузки страницы {url}: {str(e)}")
    
    return all_products