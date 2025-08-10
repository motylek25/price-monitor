import pandas as pd

def build_price_comparison(
    matched: pd.DataFrame, 
    catalog: pd.DataFrame
) -> pd.DataFrame:
    """Сравнивает цены конкурентов с нашими ценами"""
    if matched.empty:
        return pd.DataFrame()
    
    # Группируем данные по SKU
    grouped = matched.groupby("sku").agg(
        min_comp_price=("comp_price", "min"),
        max_comp_price=("comp_price", "max"),
        avg_comp_price=("comp_price", "mean"),
        competitors=("source_site", "nunique"),
        comp_products=("comp_name", lambda x: list(x)[:3])  # Примеры товаров
    ).reset_index()
    
    # Объединяем с каталогом
    result = pd.merge(
        grouped, 
        catalog, 
        on="sku", 
        how="inner"
    )
    
    # Рассчитываем разницу
    result["price_difference"] = result["current_price"] - result["min_comp_price"]
    result["price_position"] = result.apply(
        lambda x: "cheapest" if x["current_price"] <= x["min_comp_price"] else "above_min",
        axis=1
    )
    
    # Форматируем результат
    return result[[
        "sku", "name", "brand", "category", "cost", "current_price",
        "min_comp_price", "max_comp_price", "avg_comp_price", "price_difference",
        "price_position", "competitors", "comp_products"
    ]]