"""Election-year-specific turn mapping derived from official pleito identifiers."""

PLEITO_TURN = {3220: 1, 3221: 2}


def turn_for_pleito(pleito_id: int | str | None) -> int | None:
    try:
        value = int(pleito_id) if pleito_id is not None else None
    except (TypeError, ValueError):
        return None
    return PLEITO_TURN.get(value)


def folder_for_pleito(pleito_id: int | str | None) -> str:
    turn = turn_for_pleito(pleito_id)
    if turn is not None:
        return f"turno-{turn}"
    return f"pleito-{pleito_id}" if pleito_id is not None else "turno-desconhecido"
