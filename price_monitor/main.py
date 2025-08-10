import argparse
from pathlib import Path
import pandas as pd
import yaml
import os

from price_monitor.scrapers.bs4_scraper import scrape_bs4
from price_monitor.scrapers.selenium_scraper import scrape_selenium
from price_monitor.scrapers.scrapy_runner import scrape_with_scrapy
from price_monitor.matching import match_competitors_to_catalog
from price_monitor.compare import build_price_comparison
from price_monitor.recommend import build_recommendations

# Определяем корневую директорию проекта
ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / "out"
CFG = ROOT / "config"
DATA = ROOT / "data"

def load_yaml(path):
    """Загрузка YAML-конфигурации"""
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)

def ensure_dirs():
    """Создаем выходные директории при необходимости"""
    OUT.mkdir(exist_ok=True, parents=True)

def cmd_scrape(args):
    """Команда сбора данных с сайтов конкурентов"""
    print("="*50)
    print("Запуск сбора цен конкурентов...")
    print("="*50)
    
    ensure_dirs()
    cfg = load_yaml(CFG / "sites.yaml")
    all_rows = []
    
    for site in cfg["sites"]:
        t = site["type"].lower()
        print(f"\n[ПАРСИНГ] {site['name']} ({t})")
        try:
            if t == "bs4":
                rows = scrape_bs4(site)
            elif t == "selenium":
                rows = scrape_selenium(site)
            elif t == "scrapy":
                rows = scrape_with_scrapy(site)
            else:
                print(f"  ⚠️ Неизвестный тип парсера: {t}")
                rows = []
            all_rows.extend(rows)
            print(f"  ✅ Найдено позиций: {len(rows)}")
        except Exception as e:
            print(f"  ❌ Ошибка при парсинге {site['name']}: {str(e)}")

    # Сохраняем результаты
    df = pd.DataFrame(all_rows)
    if not df.empty:
        df["price"] = pd.to_numeric(df["price"], errors="coerce")
        df = df.dropna(subset=["price"])
        output_path = OUT / "scraped_prices.csv"
        df.to_csv(output_path, index=False, encoding="utf-8")
        print(f"\nСохранено {len(df)} строк в {output_path}")
    else:
        print("\n⚠️ Не собрано ни одной цены!")
    return df

def cmd_analyze(args):
    """Команда анализа и сопоставления цен"""
    print("\n" + "="*50)
    print("Анализ и сопоставление цен...")
    print("="*50)
    
    ensure_dirs()
    
    # Загрузка данных
    catalog_path = DATA / "internal_catalog.csv"
    scraped_path = OUT / "scraped_prices.csv"
    
    if not scraped_path.exists():
        print("❌ Файл с ценами конкурентов не найден. Сначала выполните парсинг.")
        return
        
    catalog = pd.read_csv(catalog_path)
    scraped = pd.read_csv(scraped_path)
    pricing_cfg = load_yaml(CFG / "pricing.yaml")
    
    # Сопоставление данных
    matched = match_competitors_to_catalog(scraped, catalog, pricing_cfg)
    if not matched.empty:
        matched_path = OUT / "matched.csv"
        matched.to_csv(matched_path, index=False, encoding="utf-8")
        print(f"Сопоставлено {len(matched)} позиций. Сохранено в {matched_path}")
    else:
        print("⚠️ Не удалось сопоставить ни одной позиции")
        return
        
    # Сравнение цен
    comp = build_price_comparison(matched, catalog)
    if not comp.empty:
        comp_path = OUT / "comparison.csv"
        comp.to_csv(comp_path, index=False, encoding="utf-8")
        print(f"Сравнение цен сохранено в {comp_path}")
        
        # Вывод сводки
        print("\nСводка по позициям:")
        for _, row in comp.iterrows():
            status = "✅ Выгодно" if row["position"] == "cheapest" else "⚠️ Выше рынка"
            print(f"{row['name']} (SKU: {row['sku']}):")
            print(f"  Наша цена: {row['current_price']}₽ | Мин. конкурент: {row['min_comp_price']}₽")
            print(f"  Позиция: {status} | Конкурентов: {row['competitors']}")
    else:
        print("⚠️ Не удалось сравнить цены")

def cmd_recommend(args):
    """Команда генерации рекомендаций по ценам"""
    print("\n" + "="*50)
    print("Генерация рекомендаций по ценам...")
    print("="*50)
    
    ensure_dirs()
    comp_path = OUT / "comparison.csv"
    
    if not comp_path.exists():
        print("❌ Файл сравнения цен не найден. Сначала выполните анализ.")
        return
        
    # Загрузка данных
    catalog = pd.read_csv(DATA / "internal_catalog.csv")
    comparison = pd.read_csv(comp_path)
    pricing_cfg = load_yaml(CFG / "pricing.yaml")
    
    # Генерация рекомендаций
    recs = build_recommendations(comparison, catalog, pricing_cfg)
    if not recs.empty:
        recs_path = OUT / "recommendations.csv"
        recs.to_csv(recs_path, index=False, encoding="utf-8")
        
        # Вывод рекомендаций
        print("\nРекомендации по ценам:")
        for _, row in recs.iterrows():
            action_icon = "⬇️ Снизить" if row["action"] == "decrease" else "⬆️ Повысить" if row["action"] == "increase" else "🔄 Оставить"
            print(f"{row['sku']}: {action_icon} с {row['current_price']}₽ до {row['recommended_price']}₽")
            print(f"  Причина: {row['reason']}")
        
        print(f"\nПолные рекомендации сохранены в {recs_path}")
    else:
        print("⚠️ Не удалось сгенерировать рекомендации")

def cmd_run_all(args):
    """Выполнить все этапы последовательно"""
    cmd_scrape(args)
    cmd_analyze(args)
    cmd_recommend(args)

def main():
    """Главная функция для обработки команд"""
    parser = argparse.ArgumentParser(
        description="Система мониторинга цен конкурентов",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    subparsers = parser.add_subparsers(title="Команды", dest="command", required=True)

    # Парсинг
    scrape_parser = subparsers.add_parser("scrape", help="Собрать цены конкурентов")
    scrape_parser.set_defaults(func=cmd_scrape)

    # Анализ
    analyze_parser = subparsers.add_parser("analyze", help="Сопоставить и сравнить цены")
    analyze_parser.set_defaults(func=cmd_analyze)

    # Рекомендации
    recommend_parser = subparsers.add_parser("recommend", help="Сгенерировать рекомендации по ценам")
    recommend_parser.set_defaults(func=cmd_recommend)

    # Все этапы
    all_parser = subparsers.add_parser("run-all", help="Выполнить все этапы последовательно")
    all_parser.set_defaults(func=cmd_run_all)

    args = parser.parse_args()
    args.func(args)

if __name__ == "__main__":
    main()