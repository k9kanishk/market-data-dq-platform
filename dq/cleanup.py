from __future__ import annotations

from sqlmodel import text

from .db import init_db, session


def dedupe_observations() -> int:
    init_db()
    with session() as s:
        result = s.exec(
            text(
                """
                DELETE FROM observation
                WHERE id NOT IN (
                    SELECT MAX(id)
                    FROM observation
                    GROUP BY risk_factor_id, source_id, obs_date
                );
                """
            )
        )
        s.commit()
        return int(getattr(result, "rowcount", 0) or 0)
