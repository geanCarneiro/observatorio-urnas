#!/usr/bin/env python3
"""Create a privacy-reduced summary from one collected section's log."""

from __future__ import annotations

import argparse
import json
import zipfile
from collections import Counter, defaultdict
from datetime import datetime
from pathlib import Path

EVENTS = {
    "Eleitor foi habilitado": "habilitacao",
    "O voto do eleitor foi computado": "voto_computado",
    "Voto confirmado para [Governador]": "voto_confirmado_governador",
    "Voto confirmado para [Presidente]": "voto_confirmado_presidente",
    "Inicio do Encerramento": "inicio_encerramento",
    "Operador selecionou: Encerrar votação": "comando_encerramento",
    "Procedimento de encerramento confirmado": "encerramento_confirmado",
}
CUTS = ("17:00:00", "17:15:00", "17:30:00", "17:45:00", "18:00:00", "18:30:00", "19:00:00")


def analyze(log_path: Path) -> dict:
    counts = Counter()
    counts_by_date = defaultdict(Counter)
    by_minute = Counter()
    after_cut = defaultdict(Counter)
    dates = set()
    records = 0
    with zipfile.ZipFile(log_path) as archive:
        names = archive.namelist()
        if "logd.dat" not in names:
            raise ValueError("ZIP has no logd.dat member")
        info = archive.getinfo("logd.dat")
        if info.file_size > 32 * 1024 * 1024:
            raise ValueError("logd.dat exceeds the 32 MiB safety limit")
        if archive.testzip() is not None:
            raise ValueError("ZIP CRC validation failed")
        for raw in archive.read("logd.dat").decode("latin1").splitlines():
            fields = raw.split("\t")
            if len(fields) < 5:
                continue
            try:
                timestamp = datetime.strptime(fields[0], "%d/%m/%Y %H:%M:%S")
            except ValueError:
                continue
            event = EVENTS.get(fields[4])
            if not event:
                continue
            records += 1
            dates.add(timestamp.strftime("%Y-%m-%d"))
            counts[event] += 1
            date_key = timestamp.strftime("%Y-%m-%d")
            counts_by_date[date_key][event] += 1
            if event in ("habilitacao", "voto_computado", "voto_confirmado_governador", "voto_confirmado_presidente"):
                by_minute[timestamp.strftime("%Y-%m-%dT%H:%M")] += 1
                for cut in CUTS:
                    if timestamp.strftime("%H:%M:%S") >= cut:
                        after_cut[date_key][f"{event}_apos_{cut[:5].replace(':', 'h')}"] += 1
    return {
        "source": "TSE logd.dat",
        "timestamp_interpretation": "horario registrado no log; fuso/deriva ainda nao validados",
        "dates_with_selected_events": sorted(dates),
        "selected_event_records": records,
        "event_counts_all_dates": dict(sorted(counts.items())),
        "event_counts_by_date": {d: dict(sorted(c.items())) for d, c in sorted(counts_by_date.items())},
        "event_counts_after_clock_cut_by_date": {d: dict(sorted(c.items())) for d, c in sorted(after_cut.items())},
        "events_per_minute": dict(sorted(by_minute.items())),
        "included_event_types": EVENTS,
        "excluded_fields": ["identificadores de eleitor/mesario", "biometria", "dados individuais", "eventos fora da lista permitida"],
        "limitations": ["assinatura JWS nao verificada", "relogio/fuso nao validados", "contagens nao demonstram causalidade ou irregularidade"],
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--log", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()
    summary = analyze(args.log)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(summary, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(json.dumps({"output": str(args.output), "selected_event_records": summary["selected_event_records"], "dates": summary["dates_with_selected_events"]}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
