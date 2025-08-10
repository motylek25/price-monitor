import re
from urllib.parse import urljoin
from scrapy.crawler import CrawlerProcess
from scrapy import Spider
from scrapy.http import Request, HtmlResponse
from price_monitor.utils import parse_price

def scrape_with_scrapy(site_cfg: dict) -> list:
    """Запуск Scrapy паука для сбора данных"""
    results = []
    base_url = site_cfg.get("base_url", "")
    selectors = site_cfg["selectors"]
    price_regex = re.compile(site_cfg.get("price_regex", r"[\d\s,.]+"))
    follow_links = site_cfg.get("follow_links", False)

    class GenericSpider(Spider):
        name = f"generic_{site_cfg['name']}"
        custom_settings = {
            "ROBOTSTXT_OBEY": True,
            "DOWNLOAD_DELAY": 1.5,
            "CONCURRENT_REQUESTS": 1,
            "USER_AGENT": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "LOG_LEVEL": "ERROR",
            "HTTPCACHE_ENABLED": True,
            "RETRY_TIMES": 2
        }

        def start_requests(self):
            for url in site_cfg["list_urls"]:
                yield Request(url, callback=self.parse)

        def parse(self, response):
            # Обработка карточек товаров
            for card in response.css(selectors["item"]):
                name = card.css(selectors["name"]).get()
                price = card.css(selectors["price"]).get()
                url = card.css(selectors["url"]).get()
                
                if not all([name, price, url]):
                    continue
                
                # Очистка названия
                name_clean = re.sub(r"\s+", " ", name).strip()
                
                # Извлечение цены
                price_match = price_regex.search(price)
                price_value = parse_price(price_match.group(0)) if price_match else None
                
                if not price_value:
                    continue
                
                # Формирование полного URL
                full_url = urljoin(base_url, url)
                
                # Сохранение результата
                results.append({
                    "site": site_cfg["name"],
                    "name": name_clean,
                    "price": price_value,
                    "url": full_url
                })
                
                # Переход на страницу товара при необходимости
                if follow_links:
                    yield response.follow(
                        full_url, 
                        callback=self.parse_product,
                        meta={"product": {"name": name_clean, "price": price_value}}
                    )
            
            # Пагинация (если нужно)
            if follow_links:
                next_page = response.css("a.next::attr(href)").get()
                if next_page:
                    yield response.follow(next_page, callback=self.parse)
        
        def parse_product(self, response):
            """Дополнительный парсинг страницы товара"""
            # Здесь можно добавить логику для извлечения дополнительных данных
            pass

    # Запуск паука
    process = CrawlerProcess()
    process.crawl(GenericSpider)
    process.start()
    
    return results