import pandas as pd
import numpy as np

def build_recommendations(
    comparison: pd.DataFrame, 
    catalog: pd.DataFrame, 
    cfg: dict
) -> pd.DataFrame:
    """Генерирует рекомендации по корректировке цен"""
    if comparison.empty:
        return pd.DataFrame()
    
    # Параметры стратегии
    min_margin = cfg.get("min_margin_percent", 10) / 100
    undercut_delta = cfg.get("undercut_delta", 1.0)
    raise_delta = cfg.get("raise_delta", 0.5)
    tolerance = cfg.get("tolerance_percent", 1.5) / 100
    round_step = cfg.get("round_to", 1.0)
    
    recommendations = []
    
    for _, row in comparison.iterrows():
        our_price = row["current_price"]
        min_comp = row["min_comp_price"]
        cost = row["cost"]
        
        # Рассчитываем минимально допустимую цену
        min_allowed = cost * (1 + min_margin)
        
        # Инициализация
        action = "keep"
        reason = "Цена оптимальна"
        new_price = our_price
        
        # Логика рекомендаций
        if min_comp:
            # Расчет отклонения
            deviation = (our_price - min_comp) / min_comp
            
            if our_price > min_comp:
                # Мы дороже конкурентов
                candidate = max(min_comp - undercut_delta, min_allowed)
                
                if candidate < our_price:
                    action = "decrease"
                    reason = f"Цена выше минимальной конкурентной на {deviation:.1%}"
                    new_price = candidate
                else:
                    action = "keep"
                    reason = "Снижение ограничено минимальной маржой"
            elif abs(deviation) <= tolerance:
                # В пределах допустимого отклонения
                action = "keep"
                reason = f"Цена в пределах допустимого отклонения (±{tolerance*100:.1f}%)"
            else:
                # Мы дешевле конкурентов
                candidate = min(min_comp - raise_delta, min_allowed)
                
                if candidate > our_price and candidate > min_allowed:
                    action = "increase"
                    reason = f"Цена значительно ниже рынка (разница: {min_comp - our_price:.2f}₽)"
                    new_price = candidate
                else:
                    action = "keep"
                    reason = "Повышение нецелесообразно или ограничено маржой"
        else:
            # Нет данных конкурентов
            if our_price < min_allowed:
                action = "increase"
                reason = "Цена ниже минимально допустимой"
                new_price = min_allowed
            else:
                action = "keep"
                reason = "Нет данных конкурентов для сравнения"
        
        # Округление цены
        if round_step > 0:
            new_price = round(new_price / round_step) * round_step
        
        recommendations.append({
            "sku": row["sku"],
            "name": row["name"],
            "current_price": our_price,
            "min_comp_price": min_comp,
            "recommended_price": new_price,
            "action": action,
            "reason": reason,
            "min_allowed_price": min_allowed
        })
    
    return pd.DataFrame(recommendations)