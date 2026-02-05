import sys, os
project_home = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../../'))
if project_home not in sys.path:
    sys.path.insert(0, project_home)

import datetime
import unittest
from unittest.mock import Mock, patch
import json

from adsrefpipe import app, tasks, utils
from adsrefpipe.models import Base, Action, Parser, ReferenceSource, ProcessedHistory, ResolvedReference, CompareClassic
from adsrefpipe.refparsers.handler import verify
from adsrefpipe.tests.unittests.stubdata.dbdata import actions_records, parsers_records


class TestTasks(unittest.TestCase):

    postgresql_url_dict = {
        'port': 5432,
        'host': '127.0.0.1',
        'user': 'postgres',
        'database': 'postgres'
    }
    postgresql_url = 'postgresql://{user}:{user}@{host}:{port}/{database}' \
        .format(user=postgresql_url_dict['user'],
                host=postgresql_url_dict['host'],
                port=postgresql_url_dict['port'],
                database=postgresql_url_dict['database']
                )

    def setUp(self):
        self.test_dir = os.path.join(project_home, 'adsrefpipe/tests')
        self.arXiv_stubdata_dir = os.path.join(self.test_dir, 'unittests/stubdata/txt/arXiv/0/')

        # Patch ADSCelery.__init__ so no real DB/session is constructed
        self.p_ads_init = patch("adsrefpipe.app.ADSCelery.__init__", autospec=True, return_value=None)
        self.p_ads_init.start()
        self.addCleanup(self.p_ads_init.stop)

        # Construct app (now safe because ADSCelery.__init__ is a no-op)
        self.app = app.ADSReferencePipelineCelery('test', local_config={
            'SQLALCHEMY_URL': self.postgresql_url,
            'SQLALCHEMY_ECHO': False,
            'PROJ_HOME': project_home,
            'TEST_DIR': self.test_dir,
            'COMPARE_CLASSIC': False,
            'REFERENCE_PIPELINE_SERVICE_URL': 'http://0.0.0.0:5000/reference'
        })
        tasks.app = self.app # monkey-patch the app object
        Base.metadata.bind = self.app._session.get_bind()
        Base.metadata.create_all()
        self.add_stub_data()

    def tearDown(self):
        unittest.TestCase.tearDown(self)
        Base.metadata.drop_all()
        self.app.close_app()

    def test_app(self):
        assert self.app._config.get('SQLALCHEMY_URL') == self.postgresql_url
        assert self.app.conf.get('SQLALCHEMY_URL') == self.postgresql_url

    def add_stub_data(self):
        """ Add stub data """
        self.arXiv_stubdata_dir = os.path.join(self.test_dir, 'unittests/stubdata/txt/arXiv/0/')

        reference_source = [
            ('0002arXiv.........Z',os.path.join(self.arXiv_stubdata_dir,'00002.raw'),'00002.raw.result','arXiv'),
        ]

        processed_history = [
            ('2020-04-03 18:08:42', '2020-05-11 11:13:53', '67', '61'),
        ]

        resolved_reference = [
            [
                ('Alsubai, K. A., Parley, N. R., Bramich, D. M., et al. 2011, MNRAS, 417, 709.','2011MNRAS.417..709A',1.0),
                ('Arcangeli, J., Desert, J.-M., Parmentier, V., et al. 2019, A&A, 625, A136   ','2019A&A...625A.136A',1.0)
            ],
        ]

        with self.app.session_scope() as session:
            session.query(Action).delete()
            session.query(Parser).delete()
            session.bulk_save_objects(actions_records)
            session.bulk_save_objects(parsers_records)
            session.commit()

            for i, (a_reference,a_history) in enumerate(zip(reference_source,processed_history)):
                    reference_record = ReferenceSource(bibcode=a_reference[0],
                                                 source_filename=a_reference[1],
                                                 resolved_filename=a_reference[2],
                                                 parser_name=a_reference[3])
                    bibcode, source_filename = self.app.insert_reference_source_record(session, reference_record)
                    self.assertTrue(bibcode == a_reference[0])
                    self.assertTrue(source_filename == a_reference[1])

                    history_record = ProcessedHistory(bibcode=bibcode,
                                             source_filename=source_filename,
                                             source_modified=a_history[0],
                                             status=Action().get_status_new(),
                                             date=a_history[1],
                                             total_ref=a_history[2])
                    history_id = self.app.insert_history_record(session, history_record)
                    self.assertTrue(history_id != -1)

                    resolved_records = []
                    for j, service in enumerate(resolved_reference[i]):
                        resolved_record = ResolvedReference(history_id=history_id,
                                                   item_num=j+1,
                                                   reference_str=service[0],
                                                   bibcode=service[1],
                                                   score=service[2],
                                                   reference_raw=service[0])
                        resolved_records.append(resolved_record)
                    success = self.app.insert_resolved_reference_records(session, resolved_records)
                    self.assertTrue(success == True)
                    session.commit()

    def test_process_references(self):
        """ test process_references task """

        resolved_reference = [
            {
                "score": "1.0",
                "bibcode": "2011LRR....14....2U",
                "refstr": "J.-P. Uzan, Varying constants, gravitation and cosmology, Living Rev. Rel. 14 (2011) 2, [1009.5514]. ",
                "id": "H1I1"
            },
            {
                "score": "1.0",
                "bibcode": "2017RPPh...80l6902M",
                "refstr": "C. J. A. P. Martins, The status of varying constants: A review of the physics, searches and implications, 1709.02923.",
                "id": "H1I2",
            }
        ]

        with patch('requests.post') as mock_resolved_references:
            mock_resolved_references.return_value = mock_response = Mock()
            mock_response.status_code = 200
            mock_response.content = json.dumps({"resolved": resolved_reference})
            filename = os.path.join(self.arXiv_stubdata_dir,'00001.raw')
            parser_dict = self.app.get_parser(filename)
            parser = verify(parser_dict.get('name'))
            # now process the source file
            toREFs = parser(filename=filename, buffer=None)
            self.assertTrue(toREFs)
            parsed_references = toREFs.process_and_dispatch()
            self.assertTrue(parsed_references)
            for block_references in parsed_references:
                self.assertTrue('bibcode' in block_references)
                self.assertTrue('references' in block_references)
                references = self.app.populate_tables_pre_resolved_initial_status(source_bibcode=block_references['bibcode'],
                                                                                  source_filename=filename,
                                                                                  parsername=parser_dict.get('name'),
                                                                                  references=block_references['references'])
                self.assertTrue(references)
            expected_count = [{'name': 'ReferenceSource', 'description': 'source reference file information', 'count': 2},
                              {'name': 'ProcessedHistory', 'description': 'top level information for a processed run', 'count': 2},
                              {'name': 'ResolvedReference', 'description': 'resolved reference information for a processed run', 'count': 4},
                              {'name': 'CompareClassic', 'description': 'comparison of new and classic processed run', 'count': 0}]
            self.assertTrue(self.app.get_count_records() == expected_count)

    def test_reprocess_subset_references(self):
        """ test reprocess_subset_references task """

        reprocess_record = [
            {
                'source_filename': os.path.join(self.arXiv_stubdata_dir,'00002.raw'),
                'source_modified': datetime.datetime(2020, 4, 3, 18, 8, 42),
                'parser_name': 'arXiv',
                'block_references': [{
                    'source_bibcode': '0002arXiv.........Z',
                    'references': [{'item_num': 2,
                                'refstr': 'Arcangeli, J., Desert, J.-M., Parmentier, V., et al. 2019, A&A, 625, A136   ',
                                'refraw': 'Arcangeli, J., Desert, J.-M., Parmentier, V., et al. 2019, A&A, 625, A136   '}]
            }]
        }]
        resolved_reference = [
            {
                "score": "1.0",
                "bibcode": "2019A&A...625A.136A",
                "refstr": "Arcangeli, J., Desert, J.-M., Parmentier, V., et al. 2019, A&A, 625, A136   ",
                "id": "H1I1"
            }
        ]
        with patch('requests.post') as mock_resolved_references:
            mock_resolved_references.return_value = mock_response = Mock()
            mock_response.status_code = 200
            mock_response.content = json.dumps({"resolved": resolved_reference})
            parser_dict = self.app.get_parser(reprocess_record[0]['source_filename'])
            parser = verify(parser_dict.get('name'))
            # now process the buffer
            toREFs = parser(filename=None, buffer=reprocess_record[0])
            self.assertTrue(toREFs)
            parsed_references = toREFs.process_and_dispatch()
            self.assertTrue(parsed_references)
            for block_references in parsed_references:
                self.assertTrue('bibcode' in block_references)
                self.assertTrue('references' in block_references)
                references = self.app.populate_tables_pre_resolved_retry_status(source_bibcode=block_references['bibcode'],
                                                                                source_filename=reprocess_record[0]['source_filename'],
                                                                                source_modified=reprocess_record[0]['source_modified'],
                                                                                retry_records=block_references['references'])
                self.assertTrue(references)
            for reference in references:
                tasks.task_process_reference({'reference': reference,
                                              'resolver_service_url': self.app._config['REFERENCE_PIPELINE_SERVICE_URL'] +
                                                                      self.app.get_reference_service_endpoint(parser_dict.get('name')),
                                              'source_bibcode': block_references['bibcode'],
                                              'source_filename':reprocess_record[0]['source_filename']})
            expected_count = [{'name': 'ReferenceSource', 'description': 'source reference file information', 'count': 1},
                              {'name': 'ProcessedHistory', 'description': 'top level information for a processed run', 'count': 2},
                              {'name': 'ResolvedReference', 'description': 'resolved reference information for a processed run', 'count': 3},
                              {'name': 'CompareClassic', 'description': 'comparison of new and classic processed run', 'count': 0}]
            self.assertTrue(self.app.get_count_records() == expected_count)

    def test_task_process_reference_error(self):
        """ test task_process_reference when utils method returns False """

        reference_task = {
            'reference': [{'item_num': 2,
                           'refstr': 'Arcangeli, J., Desert, J.-M., Parmentier, V., et al. 2019, A&A, 625, A136   ',
                           'id': '2'}],
            'source_bibcode': '2023TEST..........S',
            'source_filename': 'some_source.txt',
            'resolver_service_url': 'text'
        }

        # mock post_request_resolved_reference to return false to trigger FailedRequest
        with patch("adsrefpipe.tasks.utils.post_request_resolved_reference", return_value=False):
            with self.assertRaises(tasks.FailedRequest):
                tasks.task_process_reference(reference_task)

    def test_task_process_reference_exception(self):
        """ test task_process_reference when KeyError is raised """

        reference_task = {
            'reference': [{'item_num': 2,
                           'refstr': 'Arcangeli, J., Desert, J.-M., Parmentier, V., et al. 2019, A&A, 625, A136   ',
                           'id': '2'}],
            'source_bibcode': '2023TEST..........S',
            'source_filename': 'some_source.txt',
            'resolver_service_url': 'text'
        }

        # mock post_request_resolved_reference to raise KeyError
        with patch("adsrefpipe.tasks.utils.post_request_resolved_reference", side_effect=KeyError):
            self.assertFalse(tasks.task_process_reference(reference_task))

    def test_task_process_reference_success(self):
        """ test task_process_reference successfully returns True """

        reference_task = {
            'reference': [{'item_num': 2,
                           'refstr': 'Arcangeli, J., Desert, J.-M., Parmentier, V., et al. 2019, A&A, 625, A136   ',
                           'id': '2'}],
            'source_bibcode': '2023TEST..........S',
            'source_filename': 'some_source.txt',
            'resolver_service_url': 'text'
        }

        # Mock post_request_resolved_reference to return a valid resolved reference
        with patch("adsrefpipe.tasks.utils.post_request_resolved_reference", return_value=["resolved_ref"]), \
                patch("adsrefpipe.tasks.app.populate_tables_post_resolved", return_value=True):
            self.assertTrue(tasks.task_process_reference(reference_task))


if __name__ == '__main__':
    unittest.main()
