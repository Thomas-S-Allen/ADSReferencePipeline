import io
import os
import sys
import unittest
from contextlib import redirect_stderr
from datetime import datetime
from unittest.mock import patch

project_home = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../../'))
if project_home not in sys.path:
    sys.path.insert(0, project_home)

import run


class TestRunResolveTimeDelay(unittest.TestCase):

    def test_resolve_default_time_delay_is_disabled(self):
        subdir = ['/tmp/input/A/file1.raw', '/tmp/input/A/file2.raw']

        with patch.object(run, 'get_date', return_value=datetime(2024, 1, 1)), \
             patch.object(run, 'get_source_filenames', return_value=[subdir]), \
             patch.object(run, 'process_files') as mock_process_files, \
             patch.object(run.time, 'sleep') as mock_sleep, \
             patch.object(run.logger, 'info'), \
             patch.object(run.processed_log, 'info'):
            result = run.main(['RESOLVE', '-p', '/tmp/input', '-e', '*.raw'])

        self.assertEqual(result, 0)
        mock_process_files.assert_called_once_with(subdir)
        mock_sleep.assert_not_called()

    def test_resolve_explicit_time_delay_enables_throttling(self):
        subdir = [
            '/tmp/input/A/file1.raw',
            '/tmp/input/A/file2.raw',
            '/tmp/input/A/file3.raw',
            '/tmp/input/A/file4.raw',
        ]

        with patch.object(run, 'get_date', return_value=datetime(2024, 1, 1)), \
             patch.object(run, 'get_source_filenames', return_value=[subdir]), \
             patch.object(run, 'process_files') as mock_process_files, \
             patch.object(run.time, 'sleep') as mock_sleep, \
             patch.object(run.logger, 'info'), \
             patch.object(run.processed_log, 'info'):
            result = run.main(['RESOLVE', '-p', '/tmp/input', '-e', '*.raw', '-t', '2'])

        self.assertEqual(result, 0)
        mock_process_files.assert_called_once_with(subdir)
        mock_sleep.assert_called_once_with(2.0)

    def test_resolve_explicit_zero_time_delay_disables_throttling(self):
        subdir = ['/tmp/input/A/file1.raw']

        with patch.object(run, 'get_date', return_value=datetime(2024, 1, 1)), \
             patch.object(run, 'get_source_filenames', return_value=[subdir]), \
             patch.object(run, 'process_files') as mock_process_files, \
             patch.object(run.time, 'sleep') as mock_sleep, \
             patch.object(run.logger, 'info'), \
             patch.object(run.processed_log, 'info'):
            result = run.main(['RESOLVE', '-p', '/tmp/input', '-e', '*.raw', '-t', '0'])

        self.assertEqual(result, 0)
        mock_process_files.assert_called_once_with(subdir)
        mock_sleep.assert_not_called()

    def test_resolve_rejects_negative_time_delay(self):
        stderr = io.StringIO()

        with redirect_stderr(stderr), self.assertRaises(SystemExit) as exc:
            run.main(['RESOLVE', '-p', '/tmp/input', '-e', '*.raw', '-t', '-5'])

        self.assertEqual(exc.exception.code, 2)
        self.assertIn('time_delay must be greater than or equal to 0.', stderr.getvalue())


if __name__ == '__main__':
    unittest.main()
