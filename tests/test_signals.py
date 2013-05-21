from mock import patch, MagicMock
from fakecouch import FakeCouchDb
from . import TestBase


class TestSignals(TestBase):
    @classmethod
    def setUpClass(cls):
        super(TestSignals, cls).setUpClass()

        cls.db = FakeCouchDb()
        cls.p1 = patch('ctable.util.get_db', return_value=cls.db)
        cls.p1.start()

        from ctable import signals
        cls.signals = signals

    def test_process_fluff_diff_not_in_list(self):
        diff = dict(doc_type='MockIndicators')

        with patch('ctable.signals.ctable', MagicMock()) as mock:
            self.signals.process_fluff_diff(self, diff)
            self.assertFalse(mock.called)


    @patch('ctable.util.settings', FLUFF_PILLOWS_TO_SQL=['MockIndicators'])
    def test_process_fluff_diff_in_settings_list(self, list):
        diff = dict(doc_type='MockIndicators')

        with patch('ctable.signals.ctable', MagicMock()) as mock:
            self.signals.process_fluff_diff(self, diff)
            mock.process_fluff_diff.assert_called_once_with(diff)

    def test_process_fluff_diff_in_db_list(self):
        diff = dict(doc_type='MockIndicators')
        self.db.mock_docs['FLUFF_PILLOWS_TO_SQL'] = {'enabled_pillows': ['MockIndicators']}

        with patch('ctable.signals.ctable', MagicMock()) as mock:
            self.signals.process_fluff_diff(self, diff)
            mock.process_fluff_diff.assert_called_once_with(diff)
