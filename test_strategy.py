import unittest
import strategy as strat


class TestStrategy(unittest.TestCase):

    def test_short_grid_prices(self):
        """Test SHORT grid prices (orders placed above entry in ascending order)."""
        start_price = 100.0
        distance_pct = 40.0
        orders_qty = 4
        prices = strat.compute_grid_prices(start_price, distance_pct, orders_qty, "short")

        self.assertEqual(len(prices), 4)
        self.assertAlmostEqual(prices[0], 100.0, places=4)
        self.assertAlmostEqual(prices[3], 140.0, places=4)
        # Prices should be strictly increasing
        self.assertTrue(prices[0] < prices[1] < prices[2] < prices[3])

    def test_long_grid_prices(self):
        """
        Test LONG grid prices calculation:
        Reference short grid is calculated upward first, percentage gaps stored,
        and then applied downward sequentially from market entry price.
        """
        start_price = 100.0
        distance_pct = 40.0
        orders_qty = 4

        # Short reference grid:
        # p0 = 100
        # p1 = 110.5263
        # p2 = 123.5294
        # p3 = 140.0
        # gap1 = (110.5263 - 100) / 100 = 0.105263
        # gap2 = (123.5294 - 110.5263) / 110.5263 = 0.117647
        # gap3 = (140 - 123.5294) / 123.5294 = 0.133333

        long_prices = strat.compute_grid_prices(start_price, distance_pct, orders_qty, "long")

        self.assertEqual(len(long_prices), 4)
        self.assertAlmostEqual(long_prices[0], 100.0, places=4)
        # Check downward mirroring
        gap1 = (110.52631578947368 - 100.0) / 100.0
        gap2 = (123.52941176470588 - 110.52631578947368) / 110.52631578947368
        gap3 = (140.0 - 123.52941176470588) / 123.52941176470588

        self.assertAlmostEqual(long_prices[1], 100.0 * (1 - gap1), places=4)
        self.assertAlmostEqual(long_prices[2], long_prices[1] * (1 - gap2), places=4)
        self.assertAlmostEqual(long_prices[3], long_prices[2] * (1 - gap3), places=4)

        # Prices should be strictly decreasing
        self.assertTrue(long_prices[0] > long_prices[1] > long_prices[2] > long_prices[3])

    def test_compute_tp_price(self):
        """Test take-profit price calculation for LONG and SHORT."""
        avg_entry = 100.0
        tp_pct = 4.0

        tp_long = strat.compute_tp_price(avg_entry, tp_pct, "long")
        self.assertAlmostEqual(tp_long, 104.0, places=4)

        tp_short = strat.compute_tp_price(avg_entry, tp_pct, "short")
        self.assertAlmostEqual(tp_short, 96.0, places=4)

    def test_tp_distance_pct(self):
        """Test reversal distance calculation to TP."""
        # LONG: limit filled at 90.0, TP target at 104.0 -> distance % = (104 - 90)/90 * 100 = 15.5555%
        dist_long = strat.tp_distance_pct(90.0, 104.0, "long")
        self.assertAlmostEqual(dist_long, 15.555555, places=4)

        # SHORT: limit filled at 110.0, TP target at 96.0 -> distance % = (110 - 96)/110 * 100 = 12.7272%
        dist_short = strat.tp_distance_pct(110.0, 96.0, "short")
        self.assertAlmostEqual(dist_short, 12.727272, places=4)

    def test_expected_profit_usdt(self):
        """Test expected PnL calculation across multiple filled orders."""
        # LONG: order1 (100, 1.0), order2 (90, 2.0). TP at 104.0
        # PnL1 = (104 - 100) * 1.0 = 4.0
        # PnL2 = (104 - 90) * 2.0 = 28.0
        # Total PnL = 32.0
        filled_long = [(100.0, 1.0), (90.0, 2.0)]
        pnl_long = strat.expected_profit_usdt(filled_long, 104.0, "long")
        self.assertAlmostEqual(pnl_long, 32.0, places=4)

        # SHORT: order1 (100, 1.0), order2 (110, 2.0). TP at 96.0
        # PnL1 = (100 - 96) * 1.0 = 4.0
        # PnL2 = (110 - 96) * 2.0 = 28.0
        # Total PnL = 32.0
        filled_short = [(100.0, 1.0), (110.0, 2.0)]
        pnl_short = strat.expected_profit_usdt(filled_short, 96.0, "short")
        self.assertAlmostEqual(pnl_short, 32.0, places=4)


if __name__ == "__main__":
    unittest.main()
