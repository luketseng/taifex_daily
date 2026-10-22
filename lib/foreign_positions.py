"""Incrementally export dated foreign TX/TXO open interest for the web dashboard."""
import json
import os
import sqlite3
import tempfile
from datetime import datetime
from pathlib import Path


def export_foreign_positions(database, target_date, output_path):
    """Merge one complete trading day; return False when source rows are missing."""
    day = datetime.strptime(target_date, '%Y/%m/%d')
    output_path = Path(output_path)
    positions = {}
    with sqlite3.connect(Path(database).resolve().as_uri() + '?mode=ro', uri=True) as connection:
        rows = connection.execute(
            "SELECT 'TX', OI_B_Contract, OI_S_Contract FROM II_Fut "
            "WHERE Date=? AND Fut='TX' AND Institutional='FOR' "
            "UNION ALL SELECT 'TXO ' || PC, OI_B_Contract, OI_S_Contract FROM II_OP "
            "WHERE Date=? AND OP='TXO' AND Institutional='FOR' AND PC IN ('CALL','PUT')",
            (target_date, target_date),
        ).fetchall()
    if not rows:
        return False
    for product, buy, sell in rows:
        if product in positions:
            raise ValueError(f'Duplicate position row: {target_date} {product}')
        if not isinstance(buy, int) or not isinstance(sell, int) or buy < 0 or sell < 0:
            raise ValueError(f'Invalid position counts: {target_date} {product}')
        positions[product] = {'buy': buy, 'sell': sell}
    if set(positions) != {'TX', 'TXO CALL', 'TXO PUT'}:
        return False

    # Refuse to replace unreadable history with an empty record set.
    payload = json.loads(output_path.read_text(encoding='utf-8')) if output_path.exists() else {'dates': {}}
    if not isinstance(payload, dict) or not isinstance(payload.get('dates'), dict):
        raise ValueError('Invalid foreign position history structure')
    payload['dates'][day.strftime('%Y-%m-%d')] = positions
    payload['dates'] = dict(sorted(payload['dates'].items()))

    # Publish a complete file atomically with permissions readable by Apache.
    temporary_path = None
    try:
        with tempfile.NamedTemporaryFile(mode='w', encoding='utf-8', dir=output_path.parent,
                                         prefix='.foreign_positions-', suffix='.tmp', delete=False) as temporary:
            temporary_path = Path(temporary.name)
            json.dump(payload, temporary, ensure_ascii=False, separators=(',', ':'))
            temporary.write('\n')
            temporary.flush()
            os.fsync(temporary.fileno())
        os.chmod(temporary_path, 0o644)
        os.replace(temporary_path, output_path)
    finally:
        if temporary_path is not None and temporary_path.exists():
            temporary_path.unlink()
    return True


def main():
    """Export a day manually without fetching data or running strategies."""
    import argparse
    from datetime import date

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('database', type=Path)
    parser.add_argument('--date', default=date.today().strftime('%Y/%m/%d'),
                        help='Trading date: YYYY/MM/DD')
    parser.add_argument('--output', type=Path, required=True,
                        help='Destination foreign_positions.json path')
    args = parser.parse_args()
    if not export_foreign_positions(args.database, args.date, args.output):
        parser.exit(1, f'Incomplete positions for {args.date}; output preserved\n')
    print(f'Exported {args.date} to {args.output}; other trading dates preserved')


if __name__ == '__main__':
    main()
