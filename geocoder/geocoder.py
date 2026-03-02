"""
geocode_dataframe.py
--------------------
Функция geocode_df добавляет к датафрейму столбцы latitude, longitude и status
с помощью геокодера Nominatim (OpenStreetMap).

Требования:
    pip install geopy tqdm pandas

Ограничения Nominatim:
    - max 1 запрос в секунду с одного IP (публичный сервер nominatim.openstreetmap.org)
    - обязательный уникальный user_agent
    - см. https://operations.osmfoundation.org/policies/nominatim/
"""

import time
import pandas as pd
from geopy.geocoders import Nominatim
from geopy.extra.rate_limiter import RateLimiter
from geopy.exc import GeocoderServiceError, GeocoderTimedOut
from tqdm import tqdm


def geocode_df(
    df: pd.DataFrame,
    address: str | None = None,
    city: str | None = "city",
    region: str | None = None,
    country: str | None = None,
    user_agent: str = "geocode_df_app",
    min_delay_seconds: float = 1.1,
    max_retries: int = 3,
    timeout: int = 10,
    language: str = "ru",
) -> pd.DataFrame:
    """
    Геокодирует строки датафрейма с помощью Nominatim и добавляет столбцы:
        - latitude   (float | None)
        - longitude  (float | None)
        - status     (str)  — 'ok', 'not_found', 'error: <сообщение>'

    Параметры
    ----------
    df : pd.DataFrame
        Исходный датафрейм.
    address : str | None
        Имя столбца с улицей / адресом (необязательно).
    city : str | None
        Имя столбца с городом (по умолчанию 'city').
    region : str | None
        Имя столбца с регионом / областью (необязательно).
    country : str | None
        Имя столбца с названием страны (по умолчанию 'country').
    user_agent : str
        Уникальный идентификатор приложения для Nominatim.
        Измените на название своего проекта!
    min_delay_seconds : float
        Минимальная задержка между запросами (сек). Для публичного
        Nominatim рекомендуется >= 1.0. По умолчанию 1.1.
    max_retries : int
        Число повторных попыток при ошибке сервиса.
    timeout : int
        Таймаут одного запроса (сек).
    language : str
        Предпочитаемый язык ответа ('ru', 'en', ...).

    Возвращает
    ----------
    pd.DataFrame
        Копия датафрейма с добавленными столбцами latitude, longitude, status.
    """
    # --- Проверка маппинга ---
    column_map = {
        "street": address,
        "city": city,
        "state": region,
        "country": country,
    }
    # Убираем None-значения — Nominatim их игнорирует
    active_map = {k: v for k, v in column_map.items() if v is not None}

    if not active_map:
        raise ValueError(
            "Необходимо задать хотя бы один столбец: address, city, region или country."
        )

    missing = [col for col in active_map.values() if col not in df.columns]
    if missing:
        raise ValueError(f"Столбцы не найдены в датафрейме: {missing}")

    # --- Инициализация геокодера ---
    geolocator = Nominatim(user_agent=user_agent, timeout=timeout)

    geocode = RateLimiter(
        geolocator.geocode,
        min_delay_seconds=min_delay_seconds,
        max_retries=max_retries,
        error_wait_seconds=2.0,
        swallow_exceptions=True,   # при ошибке вернёт None вместо исключения
        return_value_on_exception=None,
    )

    # --- Вспомогательная функция для одной строки ---
    def _geocode_row(row: pd.Series):
        query = {nom_key: row[df_col] for nom_key, df_col in active_map.items()
                 if pd.notna(row[df_col]) and str(row[df_col]).strip() != ""}

        if not query:
            return None, None, "empty_query"

        try:
            location = geocode(query, language=language)
        except (GeocoderServiceError, GeocoderTimedOut) as exc:
            return None, None, f"error: {exc}"
        except Exception as exc:
            return None, None, f"error: {exc}"

        if location is None:
            return None, None, "not_found"

        return location.latitude, location.longitude, "ok"

    # --- Прогресс-бар и применение ---
    tqdm.pandas(desc="Геокодирование")

    result_df = df.copy()
    results = result_df.progress_apply(_geocode_row, axis=1, result_type="expand")
    results.columns = ["latitude", "longitude", "status"]

    result_df["latitude"] = results["latitude"]
    result_df["longitude"] = results["longitude"]
    result_df["status"] = results["status"]

    # --- Краткая статистика ---
    total = len(result_df)
    ok = (result_df["status"] == "ok").sum()
    not_found = (result_df["status"] == "not_found").sum()
    errors = total - ok - not_found
    print(
        f"\nГеокодирование завершено: {ok}/{total} успешно, "
        f"{not_found} не найдено, {errors} ошибок."
    )

    return result_df


# ---------------------------------------------------------------------------
# Пример использования
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    sample = pd.DataFrame(
        {
            "city": ["Москва", "Санкт-Петербург", "Новосибирск", "НесуществующийГород123"],
            "country": ["Россия", "Россия", "Россия", "Россия"],
        }
    )

    result = geocode_df(
        sample,
        city="city",
        country="country",
    )

    print(result[["city", "country", "latitude", "longitude", "status"]])
