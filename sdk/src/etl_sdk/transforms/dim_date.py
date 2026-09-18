"""日期维生成: 纯函数(无 IO),供 build_dw 幂等 upsert 进 dim_date。

节假日口径: 只收录日期完全确定的法定节假日;农历节日(清明/端午/中秋)与调休安排
每年不同,须按国务院当年通知维护 CHINA_HOLIDAYS(生产可替换为节假日库,如 chinesecalendar)。
"""

from __future__ import annotations

from datetime import date, timedelta
from typing import Any

CHINA_HOLIDAYS: dict[date, str] = {
    date(2026, 1, 1): "元旦",
    date(2026, 5, 1): "劳动节",
    date(2026, 10, 1): "国庆节",
    date(2026, 10, 2): "国庆节",
    date(2026, 10, 3): "国庆节",
}


def generate_date_rows(start: date, end: date) -> list[dict[str, Any]]:
    """生成 [start, end] 闭区间每天的维度行(date_key=YYYYMMDD,周一=1)。

    重复生成相同日期结果一致,配合 upsert 幂等;已存在行的节假日字段也会被刷新。
    """
    rows: list[dict[str, Any]] = []
    d = start
    while d <= end:
        iso = d.isocalendar()
        holiday = CHINA_HOLIDAYS.get(d)
        rows.append(
            {
                "date_key": int(d.strftime("%Y%m%d")),
                "stat_date": d,
                "year_num": d.year,
                "quarter_num": (d.month - 1) // 3 + 1,
                "month_num": d.month,
                "day_num": d.day,
                "week_of_year": iso.week,
                "day_of_week": d.isoweekday(),
                "is_weekend": 1 if d.isoweekday() >= 6 else 0,
                "is_holiday": 1 if holiday else 0,
                "holiday_name": holiday,
            }
        )
        d += timedelta(days=1)
    return rows
