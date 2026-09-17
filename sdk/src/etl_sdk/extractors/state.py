"""水位线状态: 两阶段提交 —— 抽取/装载全部成功后才推进,失败不动、重跑幂等。"""

from __future__ import annotations

from datetime import datetime

import sqlalchemy as sa
from structlog import get_logger

logger = get_logger(__name__)

WATERMARK_TABLE = "etl_meta.etl_watermark"


class WatermarkState:
    def __init__(self, engine: sa.Engine) -> None:
        self.engine = engine

    def read(self, table_name: str, shop_id: int, platform: str) -> datetime | None:
        with self.engine.connect() as conn:
            return conn.execute(
                sa.text(
                    f"SELECT watermark_value FROM {WATERMARK_TABLE} "
                    "WHERE table_name = :t AND shop_id = :s AND platform = :p"
                ),
                {"t": table_name, "s": shop_id, "p": platform},
            ).scalar_one_or_none()

    def advance(self, table_name: str, shop_id: int, platform: str, value: datetime) -> bool:
        """推进水位(条件更新: 只前进不后退,防并发/乱序回退)。返回是否实际推进。

        实现为 UPDATE(then INSERT): 先尝试把水位提升到 value(条件 watermark_value < value),
        rowcount=0 时可能是"行不存在"或"值已更大",查存在性区分两种情况。
        """
        params = {"t": table_name, "s": shop_id, "p": platform, "v": value}
        with self.engine.begin() as conn:
            result = conn.execute(
                sa.text(
                    f"UPDATE {WATERMARK_TABLE} SET watermark_value = :v "
                    "WHERE table_name = :t AND shop_id = :s AND platform = :p AND watermark_value < :v"
                ),
                params,
            )
            if result.rowcount:
                advanced = True
            else:
                exists = conn.execute(
                    sa.text(
                        f"SELECT 1 FROM {WATERMARK_TABLE} WHERE table_name = :t AND shop_id = :s AND platform = :p"
                    ),
                    params,
                ).scalar_one_or_none()
                if exists is None:
                    conn.execute(
                        sa.text(
                            f"INSERT INTO {WATERMARK_TABLE} "
                            "(table_name, shop_id, platform, watermark_value, watermark_type) "
                            "VALUES (:t, :s, :p, :v, 'updated_at')"
                        ),
                        params,
                    )
                    advanced = True
                else:
                    advanced = False  # 值已更大或相同,不回退
        logger.info(
            "watermark.advance",
            table=table_name,
            shop_id=shop_id,
            value=value.isoformat(),
            advanced=advanced,
        )
        return advanced
