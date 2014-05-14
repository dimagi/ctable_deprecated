from mock import patch, MagicMock
from ctable.tests import TestBase
from fluff.signals import BACKEND_COUCH


class TestSignals(TestBase):
    @classmethod
    def setUpClass(cls):
        super(TestSignals, cls).setUpClass()

        cls.p1 = patch('ctable.util.get_db', return_value=cls.db)
        cls.p1.start()

        from ctable import signals
        cls.signals = signals

    def setUp(self):
        self.db.reset()

    def test_process_fluff_diff_not_in_list(self):
        diff = dict(doc_type='MockIndicators')

        with patch('ctable.util.get_extractor', return_value=MagicMock()) as mock:
            self.signals.process_fluff_diff(self, diff)
            self.assertFalse(mock().called)


    @patch('ctable.util.settings', FLUFF_PILLOW_TYPES_TO_SQL={'MockIndicators': 'SQL'})
    @patch('ctable.util.get_extractor')
    def test_process_fluff_diff_in_settings_list(self, mock_extractor, list):
        diff = dict(doc_type='MockIndicators')

        self.signals.process_fluff_diff(self, diff=diff, backend=BACKEND_COUCH)

        mock_extractor().process_fluff_diff.assert_called_once_with(diff, 'SQL')

    @patch('ctable.util.get_extractor')
    def test_process_fluff_diff_in_db_list(self, mock_extractor):
        diff = dict(doc_type='MockIndicators')
        self.db.mock_docs['FLUFF_PILLOW_TYPES_TO_SQL'] = {'enabled_pillows': {'MockIndicators': 'SQL'}}

        self.signals.process_fluff_diff(self, diff=diff, backend=BACKEND_COUCH)
        mock_extractor().process_fluff_diff.assert_called_once_with(diff, 'SQL')
