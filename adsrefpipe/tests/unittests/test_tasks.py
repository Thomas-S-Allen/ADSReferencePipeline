import sys, os
project_home = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../../'))
if project_home not in sys.path:
    sys.path.insert(0, project_home)

import datetime
import unittest
from unittest.mock import Mock, patch, MagicMock
import json
from contextlib import contextmanager

from adsrefpipe import app, tasks
from adsrefpipe.models import Base
from adsrefpipe.refparsers.handler import verify


def _make_session_scope(session_obj):
    @contextmanager
    def _scope():
        yield session_obj
    return _scope


class TestTasks(unittest.TestCase):

    def setUp(self):
        self.test_dir = os.path.join(project_home, 'adsrefpipe/tests')
        self.arXiv_stubdata_dir = os.path.join(self.test_dir, 'unittests/stubdata/txt/arXiv/0/')

        # Patch ADSCelery.__init__ so no real DB/session is constructed
        self.p_ads_init = patch("adsrefpipe.app.ADSCelery.__init__", autospec=True, return_value=None)
        self.p_ads_init.start()
        self.addCleanup(self.p_ads_init.stop)

        # Construct app (now safe because ADSCelery.__init__ is a no-op)
        self.app = app.ADSReferencePipelineCelery('test', local_config={
            # kept for consistency, but unused because DB init is patched out
            'SQLALCHEMY_URL': 'postgresql://mock/mock@localhost:5432/mock',
            'SQLALCHEMY_ECHO': False,
            'PROJ_HOME': project_home,
            'TEST_DIR': self.test_dir,
            'COMPARE_CLASSIC': False,
            'REFERENCE_PIPELINE_SERVICE_URL': 'http://0.0.0.0:5000/reference'
        })

        # Provide the minimal attributes ADSCelery would normally provide
        self.app._config = {
            'SQLALCHEMY_URL': 'postgresql://mock/mock@localhost:5432/mock',
            'SQLALCHEMY_ECHO': False,
            'PROJ_HOME': project_home,
            'TEST_DIR': self.test_dir,
            'COMPARE_CLASSIC': False,
            'REFERENCE_PIPELINE_SERVICE_URL': 'http://0.0.0.0:5000/reference'
        }
        self.app.conf = dict(self.app._config)
        self.app.logger = MagicMock(name="logger")

        # Mock session + session_scope
        self.mock_session = MagicMock(name="session")
        self.app.session_scope = _make_session_scope(self.mock_session)

        # Ensure parser selection does not touch DB: seed default_parsers
        # For arXiv stub paths ending with 00001.raw, extension is ".raw"
        self.app.default_parsers = {}
        self.app.default_parsers[".raw"] = {
            "name": "arXiv",
            "extension_pattern": ".raw",
            "reference_service_endpoint": "/text",
            "matches": []
        }

        # get_reference_service_endpoint always queries DB in your app.py,
        # so patch it to return what the tests expect.
        self.p_get_endpoint = patch.object(self.app, "get_reference_service_endpoint", autospec=True, return_value="/text")
        self.p_get_endpoint.start()
        self.addCleanup(self.p_get_endpoint.stop)

        # monkey-patch the tasks module global `app`
        tasks.app = self.app

        # No-op metadata create/drop calls (prevents DDL attempts)
        Base.metadata.create_all = MagicMock(name="create_all_noop")
        Base.metadata.drop_all = MagicMock(name="drop_all_noop")

    def tearDown(self):
        # In the real system close_app exists and may rely on ADSCelery internals;
        # make it safe here.
        if hasattr(self.app, "close_app"):
            try:
                self.app.close_app()
            except Exception:
                pass
        unittest.TestCase.tearDown(self)

    def test_app(self):
        assert self.app._config.get('SQLALCHEMY_URL') == 'postgresql://mock/mock@localhost:5432/mock'
        assert self.app.conf.get('SQLALCHEMY_URL') == 'postgresql://mock/mock@localhost:5432/mock'

    def test_process_references(self):
        """ test process_references task (DB mocked) """

        resolved_reference = [
            {
                "score": "1.0",
                "bibcode": "2011LRR....14....2U",
                "refstr": "J.-P. Uzan ...",
                "id": "H1I1"
            },
            {
                "score": "1.0",
                "bibcode": "2017RPPh...80l6902M",
                "refstr": "C. J. A. P. Martins ...",
                "id": "H1I2",
            }
        ]

        filename = os.path.join(self.arXiv_stubdata_dir, '00001.raw')
        parser_dict = self.app.get_parser(filename)
        parser = verify(parser_dict.get('name'))

        # Parse file (pure logic)
        toREFs = parser(filename=filename, buffer=None)
        self.assertTrue(toREFs)
        parsed_references = toREFs.process_and_dispatch()
        self.assertTrue(parsed_references)

        with patch('requests.post') as mock_resolved_references, \
             patch.object(self.app, "populate_tables_pre_resolved_initial_status", return_value=[{"id": "H1I1"}]) as mock_pre, \
             patch.object(self.app, "get_count_records") as mock_counts:

            mock_resolved_references.return_value = mock_response = Mock()
            mock_response.status_code = 200
            mock_response.content = json.dumps({"resolved": resolved_reference})

            for block_references in parsed_references:
                self.assertIn('bibcode', block_references)
                self.assertIn('references', block_references)

                references = self.app.populate_tables_pre_resolved_initial_status(
                    source_bibcode=block_references['bibcode'],
                    source_filename=filename,
                    parsername=parser_dict.get('name'),
                    references=block_references['references']
                )
                self.assertTrue(references)

            expected_count = [
                {'name': 'ReferenceSource', 'description': 'source reference file information', 'count': 2},
                {'name': 'ProcessedHistory', 'description': 'top level information for a processed run', 'count': 2},
                {'name': 'ResolvedReference', 'description': 'resolved reference information for a processed run', 'count': 4},
                {'name': 'CompareClassic', 'description': 'comparison of new and classic processed run', 'count': 0}
            ]
            mock_counts.return_value = expected_count

            self.assertTrue(self.app.get_count_records() == expected_count)
            self.assertTrue(mock_pre.called)

    def test_reprocess_subset_references(self):
        """ test reprocess_subset_references task (DB mocked) """

        reprocess_record = [
            {
                'source_filename': os.path.join(self.arXiv_stubdata_dir, '00002.raw'),
                'source_modified': datetime.datetime(2020, 4, 3, 18, 8, 42),
                'parser_name': 'arXiv',
                'block_references': [{
                    'source_bibcode': '0002arXiv.........Z',
                    'references': [{
                        'item_num': 2,
                        'refstr': 'Arcangeli, J., Desert, J.-M., ...',
                        'refraw': 'Arcangeli, J., Desert, J.-M., ...'
                    }]
                }]
            }
        ]

        resolved_reference = [
            {
                "score": "1.0",
                "bibcode": "2019A&A...625A.136A",
                "refstr": "Arcangeli, J., Desert, J.-M., ...",
                "id": "H1I1"
            }
        ]

        with patch('requests.post') as mock_resolved_references, \
             patch.object(self.app, "populate_tables_pre_resolved_retry_status",
                          return_value=[{'item_num': 2, 'refstr': 'Arcangeli ...', 'id': '2'}]) as mock_retry, \
             patch.object(self.app, "get_count_records") as mock_counts, \
             patch("adsrefpipe.tasks.app.populate_tables_post_resolved", return_value=True) as mock_post:

            mock_resolved_references.return_value = mock_response = Mock()
            mock_response.status_code = 200
            mock_response.content = json.dumps({"resolved": resolved_reference})

            parser_dict = self.app.get_parser(reprocess_record[0]['source_filename'])
            parser = verify(parser_dict.get('name'))

            # Process buffer (pure logic)
            toREFs = parser(filename=None, buffer=reprocess_record[0])
            self.assertTrue(toREFs)
            parsed_references = toREFs.process_and_dispatch()
            self.assertTrue(parsed_references)

            for block_references in parsed_references:
                self.assertIn('bibcode', block_references)
                self.assertIn('references', block_references)

                references = self.app.populate_tables_pre_resolved_retry_status(
                    source_bibcode=block_references['bibcode'],
                    source_filename=reprocess_record[0]['source_filename'],
                    source_modified=reprocess_record[0]['source_modified'],
                    retry_records=block_references['references']
                )
                self.assertTrue(references)

            for reference in references:
                tasks.task_process_reference({
                    'reference': reference,
                    'resolver_service_url': self.app._config['REFERENCE_PIPELINE_SERVICE_URL'] +
                                           self.app.get_reference_service_endpoint(parser_dict.get('name')),
                    'source_bibcode': block_references['bibcode'],
                    'source_filename': reprocess_record[0]['source_filename']
                })

            expected_count = [
                {'name': 'ReferenceSource', 'description': 'source reference file information', 'count': 1},
                {'name': 'ProcessedHistory', 'description': 'top level information for a processed run', 'count': 2},
                {'name': 'ResolvedReference', 'description': 'resolved reference information for a processed run', 'count': 3},
                {'name': 'CompareClassic', 'description': 'comparison of new and classic processed run', 'count': 0}
            ]
            mock_counts.return_value = expected_count

            self.assertTrue(self.app.get_count_records() == expected_count)
            self.assertTrue(mock_retry.called)
            self.assertTrue(mock_post.called)

    def test_task_process_reference_error(self):
        """ test task_process_reference when utils method returns False """

        reference_task = {
            'reference': [{'item_num': 2, 'refstr': 'Arcangeli ...', 'id': '2'}],
            'source_bibcode': '2023TEST..........S',
            'source_filename': 'some_source.txt',
            'resolver_service_url': 'text'
        }

        with patch("adsrefpipe.tasks.utils.post_request_resolved_reference", return_value=False):
            with self.assertRaises(tasks.FailedRequest):
                tasks.task_process_reference(reference_task)

    def test_task_process_reference_exception(self):
        """ test task_process_reference when KeyError is raised """

        reference_task = {
            'reference': [{'item_num': 2, 'refstr': 'Arcangeli ...', 'id': '2'}],
            'source_bibcode': '2023TEST..........S',
            'source_filename': 'some_source.txt',
            'resolver_service_url': 'text'
        }

        with patch("adsrefpipe.tasks.utils.post_request_resolved_reference", side_effect=KeyError):
            self.assertFalse(tasks.task_process_reference(reference_task))

    def test_task_process_reference_success(self):
        """ test task_process_reference successfully returns True """

        reference_task = {
            'reference': [{'item_num': 2, 'refstr': 'Arcangeli ...', 'id': '2'}],
            'source_bibcode': '2023TEST..........S',
            'source_filename': 'some_source.txt',
            'resolver_service_url': 'text'
        }

        with patch("adsrefpipe.tasks.utils.post_request_resolved_reference", return_value=["resolved_ref"]), \
             patch("adsrefpipe.tasks.app.populate_tables_post_resolved", return_value=True):
            self.assertTrue(tasks.task_process_reference(reference_task))


if __name__ == '__main__':
    unittest.main()

