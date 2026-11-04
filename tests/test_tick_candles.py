"""Real tick conversion must retain every trade and omit only empty minutes."""
from datetime import datetime, timedelta
import logging
from pathlib import Path
import sys
import tempfile
import unittest
import numpy as np

sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from lib.data_processing import DataProcessing
from tests.candle_reference import validate_session_candles, read_report_ticks


def tick(stamp, price=100, quantity=2):
    return [stamp.strftime('%Y%m%d'),'TX','202611',stamp.strftime('%H%M%S'),str(price),str(quantity),'-','-']


class TickCandleTests(unittest.TestCase):
    def setUp(self):
        self.processor=DataProcessing(logger=logging.getLogger(__name__))

    def convert(self, rows):
        return self.processor.process_ticks_to_candles(np.array(rows))

    def test_boundary_trade_and_final_single_trade_are_retained(self):
        rows=[tick(datetime(2026,10,27,15,0,10)),tick(datetime(2026,10,27,15,1,10),110)]
        self.assertEqual(self.convert(rows),[
            ('2026/10/27','15:01:00',100,100,100,100,1),
            ('2026/10/27','15:02:00',110,110,110,110,1)])

    def test_identical_ticks_are_distinct_trades(self):
        row=tick(datetime(2026,10,27,15,17,44),46108)
        self.assertEqual(self.convert([row,row]),[('2026/10/27','15:18:00',46108,46108,46108,46108,2)])

    def test_boundary_first_trade_contributes_open_high_and_volume(self):
        rows=[tick(datetime(2026,10,27,15,0,10)),tick(datetime(2026,10,27,15,1,10),110),tick(datetime(2026,10,27,15,1,20),101)]
        self.assertEqual(self.convert(rows)[-1],('2026/10/27','15:02:00',110,110,101,101,2))

    def test_empty_minutes_not_filled_and_first_time_not_assumed(self):
        rows=[tick(datetime(2026,10,27,16,3,10)),tick(datetime(2026,10,27,16,8,10))]
        self.assertEqual([r[1] for r in self.convert(rows)],['16:04:00','16:09:00'])

    def test_midnight_rolls_calendar_date(self):
        rows=[tick(datetime(2026,10,27,23,59,10)),tick(datetime(2026,10,28,0,0,10))]
        self.assertEqual([r[:2] for r in self.convert(rows)],[('2026/10/28','00:00:00'),('2026/10/28','00:01:00')])

    def test_closing_exact_tick_belongs_to_final_minute_even_after_gap(self):
        for hour,minute in ((5,0),(13,45)):
            close=datetime(2026,10,28,hour,minute)
            rows=[tick(close-timedelta(minutes=3),99),tick(close,101)]
            self.assertEqual(self.convert(rows)[-1],('2026/10/28',close.strftime('%H:%M:%S'),101,101,101,101,1))
            rows=[tick(close-timedelta(seconds=10),99),tick(close,101)]
            self.assertEqual(self.convert(rows)[-1],('2026/10/28',close.strftime('%H:%M:%S'),99,101,99,101,2))

    def test_full_sessions_equal_1140_minus_zero_trade_minutes_across_holiday(self):
        # Night calendar dates and the regular trading date can skip holidays.
        stamps=([datetime(2026,10,22,15)+timedelta(minutes=i,seconds=10) for i in range(840)]
                +[datetime(2026,10,27,8,45)+timedelta(minutes=i,seconds=10) for i in range(300)])
        for missing in (set(),{0,20,539,540,839,840,1139}):
            rows=[tick(stamp) for i,stamp in enumerate(stamps) if i not in missing]
            # An extra identical trade increases volume, never minute count.
            rows.insert(2,rows[1])
            result=self.convert(rows)
            self.assertEqual(len(result),1140-len(missing))
            self.assertEqual(len({r[:2] for r in result}),len(result))
            self.assertEqual(sum(r[6] for r in result),len(rows))
            audit=validate_session_candles(rows,result,'2026/10/22','2026/10/27')
            self.assertEqual(audit['zero_trade_minutes'],len(missing))

    def test_independent_validation_rejects_wrong_volume_even_with_right_count(self):
        row=tick(datetime(2026,10,27,15,17,44),46108)
        actual=self.convert([row,row])
        wrong=[(*actual[0][:6],1)]
        with self.assertRaisesRegex(ValueError,'OHLCV'):
            validate_session_candles([row,row],wrong,'2026/10/27','2026/10/28')
        with self.assertRaisesRegex(ValueError,'Expected 1 candles'):
            validate_session_candles([row,row],[],'2026/10/27','2026/10/28')

    def test_unsorted_ticks_are_rejected_without_silent_data_loss(self):
        with self.assertRaisesRegex(ValueError,'chronological'):
            self.convert([tick(datetime(2026,10,27,16)),tick(datetime(2026,10,27,15))])

    def test_empty_ticks(self):
        self.assertEqual(self.convert([]),[])

    def test_reference_reader_preserves_duplicates_and_selects_contract_month(self):
        with tempfile.TemporaryDirectory() as directory:
            path=Path(directory)/'sample.rpt'
            current='20261012,TX     ,202610     ,151744,100,2,-,-,\n'
            following='20261012,TX     ,202611     ,151744,200,2,-,-,\n'
            spread='20261012,TX     ,202610/202611,151744,300,2,-,-,\n'
            path.write_text(current+current+following+spread)
            rows=read_report_ticks(path,'TX','2026_10_12')
            self.assertEqual(len(rows),2)
            self.assertEqual(rows[0],rows[1])
            self.assertEqual(rows[0][2],'202610')
            path.write_text(following+following+spread)
            rows=read_report_ticks(path,'TX','2026_10_28')
            self.assertEqual(len(rows),2)
            self.assertEqual(rows[0][2],'202611')


if __name__=='__main__':
    unittest.main()
