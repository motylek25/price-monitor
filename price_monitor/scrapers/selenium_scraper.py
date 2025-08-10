import time
import re
from urllib.parse import urljoin
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from bs4 import BeautifulSoup
from price_monitor.utils import parse_price

def get_driver():
    """Создает и настраивает экземпляр драйвера Chrome"""
    options = Options()
    options.add_argument("--headless=new")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--window-size=1920,1080")
    options.add_argument("--disable-gpu")
    options.add_argument("--disable-extensions")
    options.add_argument("--disable-infobars")
    
    # Настройки для обхода блокировок
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    options.add_experimental_option('useAutomationExtension', False)
    
    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=options)
    
    # Маскировка под обычный браузер
    driver.execute_cdp_cmd(
        "Network.setUserAgentOverride", 
        {"userAgent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"}
    )
    driver.execute_cdp_cmd(
        "Page.addScriptToEvaluateOnNewDocument", 
        {"source": "Object.defineProperty(navigator, 'webdriver', {get: () => undefined})"}
    )
    
    return driver

def scrape_selenium(site_cfg: dict) -> list:
    """Парсинг динамических сайтов с помощью Selenium"""
    base_url = site_cfg.get("base_url", "")
    list_urls = site_cfg["list_urls"]
    selectors = site_cfg["selectors"]
    price_regex = re.compile(site_cfg.get("price_regex", r"[\d\s,.]+"))
    need_scroll = site_cfg.get("scroll", False)
    
    driver = get_driver()
    all_products = []
    
    try:
        for url in list_urls:
            try:
                # Загрузка страницы
                driver.get(url)
                
                # Ожидание загрузки контента
                wait = WebDriverWait(driver, 15)
                wait.until(EC.presence_of_element_located(
                    (By.CSS_SELECTOR, selectors.get("wait_for", selectors["item"]))
                ))
                
                # Прокрутка страницы при необходимости
                if need_scroll:
                    last_height = driver.execute_script("return document.body.scrollHeight")
                    while True:
                        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                        time.sleep(1.5)
                        new_height = driver.execute_script("return document.body.scrollHeight")
                        if new_height == last_height:
                            break
                        last_height = new_height
                
                # Парсинг HTML
                soup = BeautifulSoup(driver.page_source, "lxml")
                
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
                time.sleep(2)
                
            except Exception as e:
                print(f"  Ошибка загрузки страницы {url}: {str(e)}")
    
    finally:
        driver.quit()
    
    return all_products