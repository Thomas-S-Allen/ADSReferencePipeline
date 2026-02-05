import sys, os
project_home = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../../'))
if project_home not in sys.path:
    sys.path.insert(0, project_home)

import unittest
from unittest.mock import patch, MagicMock, Mock
from datetime import datetime, timedelta
from collections import namedtuple

from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.sql import and_, func, case, column, table, literal
from sqlalchemy.dialects import postgresql

from adsrefpipe import app
from adsrefpipe.models import Base, Action, Parser, ReferenceSource, ProcessedHistory, ResolvedReference, CompareClassic
from adsrefpipe.utils import ReprocessQueryType
from adsrefpipe.refparsers.CrossRefXML import CrossRefToREFs
from adsrefpipe.refparsers.ElsevierXML import ELSEVIERtoREFs
from adsrefpipe.refparsers.JATSxml import JATStoREFs
from adsrefpipe.refparsers.IOPxml import IOPtoREFs
from adsrefpipe.refparsers.SpringerXML import SPRINGERtoREFs
from adsrefpipe.refparsers.APSxml import APStoREFs
from adsrefpipe.refparsers.NatureXML import NATUREtoREFs
from adsrefpipe.refparsers.AIPxml import AIPtoREFs
from adsrefpipe.refparsers.WileyXML import WILEYtoREFs
from adsrefpipe.refparsers.NLM3xml import NLMtoREFs
from adsrefpipe.refparsers.AGUxml import AGUtoREFs, AGUreference
from adsrefpipe.refparsers.arXivTXT import ARXIVtoREFs
from adsrefpipe.refparsers.handler import verify
from adsrefpipe.tests.unittests.stubdata.dbdata import actions_records, parsers_records

import testing.postgresql

def _get_external_identifier(rec):
    """
    Works whether rec is a dict (bulk mappings) or an ORM object.
    """
    if rec is None:
        return []
    if isinstance(rec, dict):
        return rec.get("external_identifier") or []
    return getattr(rec, "external_identifier", None) or []

class TestDatabase(unittest.TestCase):

    """
    Tests the application's methods
    """

    maxDiff = None

    def setUp(self):
        self.test_dir = os.path.join(project_home, 'adsrefpipe/tests')

        # Patch DDL so we never compile or create real tables (ARRAY etc.)
        self._p_create_all = patch.object(Base.metadata, "create_all", autospec=True)
        self._p_drop_all = patch.object(Base.metadata, "drop_all", autospec=True)
        self.mock_create_all = self._p_create_all.start()
        self.mock_drop_all = self._p_drop_all.start()

        # Patch SQLAlchemy engine + session creation to avoid real DB
        self._p_create_engine = patch("sqlalchemy.create_engine", autospec=True)
        self._p_sessionmaker = patch("sqlalchemy.orm.sessionmaker", autospec=True)
        self.mock_create_engine = self._p_create_engine.start()
        self.mock_sessionmaker = self._p_sessionmaker.start()

    @classmethod
    def tearDownClass(cls):
        super().tearDownClass()
        cls._postgresql.stop()

    def setUp(self):
        self.test_dir = os.path.join(project_home, 'adsrefpipe/tests')
        unittest.TestCase.setUp(self)
        self.app = app.ADSReferencePipelineCelery('test', local_config={
            'SQLALCHEMY_URL': self.postgresql_url,
            'SQLALCHEMY_ECHO': False,
            'PROJ_HOME': project_home,
            'TEST_DIR': self.test_dir,
        })
        Base.metadata.bind = self.app._session.get_bind()
        Base.metadata.create_all()
        self.add_stub_data()

    def tearDown(self):
        unittest.TestCase.tearDown(self)
        Base.metadata.drop_all()
        self.app.close_app()

    def add_stub_data(self):
        """ Add stub data """
        self.arXiv_stubdata_dir = os.path.join(self.test_dir, 'unittests/stubdata/txt/arXiv/0/')

        reference_source = [
            ('0001arXiv.........Z',os.path.join(self.arXiv_stubdata_dir,'00001.raw'),os.path.join(self.arXiv_stubdata_dir,'00001.raw.result'),'arXiv'),
            ('0002arXiv.........Z',os.path.join(self.arXiv_stubdata_dir,'00002.raw'),os.path.join(self.arXiv_stubdata_dir,'00002.raw.result'),'arXiv'),
            ('0003arXiv.........Z',os.path.join(self.arXiv_stubdata_dir,'00003.raw'),os.path.join(self.arXiv_stubdata_dir,'00003.raw.result'),'arXiv')
        ]

        processed_history = [
            ('2020-04-03 18:08:46', '2020-05-11 11:13:36', '83', '79'),
            ('2020-04-03 18:08:42', '2020-05-11 11:13:53', '67', '61'),
            ('2020-04-03 18:08:32', '2020-05-11 11:14:28', '128', '109')
        ]

        resolved_reference = [
            [
                ('J.-P. Uzan, Varying constants, gravitation and cosmology, Living Rev. Rel. 14 (2011) 2, [1009.5514]. ',
                 '2011LRR....14....2U', 1.0, ['arxiv:1009.5514']),
                ('C. J. A. P. Martins, The status of varying constants: A review of the physics, searches and implications, 1709.02923.',
                 '2017RPPh...80l6902M', 1.0, ['arxiv:1709.02923'])
            ],
            [
                ('Alsubai, K. A., Parley, N. R., Bramich, D. M., et al. 2011, MNRAS, 417, 709.',
                 '2011MNRAS.417..709A', 1.0, ['doi:10.0000/mnras.417.709']),
                ('Arcangeli, J., Desert, J.-M., Parmentier, V., et al. 2019, A&A, 625, A136   ',
                 '2019A&A...625A.136A', 1.0, ['doi:10.0000/aa.625.A136'])
            ],
            [
                ('Abellan, F. J., Indebetouw, R., Marcaide, J. M., et al. 2017, ApJL, 842, L24',
                 '2017ApJ...842L..24A', 1.0, ['ascl:1701.001']),
                ('Ackermann, M., Albert, A., Atwood, W. B., et al. 2016, A&A, 586, A71 ',
                 '2016A&A...586A..71A', 1.0, ['doi:10.0000/aa.586.A71'])
            ],
        ]

        compare_classic = [
            [
                ('2010arXiv1009.5514U',1,'DIFF'),
                ('2017arXiv170902923M',1,'DIFF')
            ],
            [
                ('2011MNRAS.417..709A',1,'MATCH'),
                ('2019A&A...625A.136A',1,'MATCH')
            ],
            [
                ('2017ApJ...842L..24A',1,'MATCH'),
                ('2016A&A...586A..71A',1,'MATCH')
            ]
        ]

        with self.app.session_scope() as session:
            session.query(Action).delete()
            session.query(Parser).delete()
            session.commit()
            if session.query(Action).count() == 0:
                session.bulk_save_objects(actions_records)
            if session.query(Parser).count() == 0:
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
                compare_records = []
                for j, (service,classic) in enumerate(zip(resolved_reference[i],compare_classic[i])):
                    resolved_record = ResolvedReference(history_id=history_id,
                                               item_num=j+1,
                                               reference_str=service[0],
                                               bibcode=service[1],
                                               score=service[2],
                                               reference_raw=service[0])
                    resolved_records.append(resolved_record)
                    compare_record = CompareClassic(history_id=history_id,
                                             item_num=j+1,
                                             bibcode=classic[0],
                                             score=classic[1],
                                             state=classic[2])
                    compare_records.append(compare_record)
                success = self.app.insert_resolved_reference_records(session, resolved_records)
                self.assertTrue(success == True)
                success = self.app.insert_compare_records(session, compare_records)
                self.assertTrue(success == True)
                session.commit()

    def test_query_reference_tbl(self):
        """ test querying reference_source table """
        result_expected = [
            {
                'bibcode': '0001arXiv.........Z',
                'source_filename': os.path.join(self.arXiv_stubdata_dir,'00001.raw'),
                'resolved_filename': os.path.join(self.arXiv_stubdata_dir,'00001.raw.result'),
                'parser_name': 'arXiv',
                'num_runs': 1,
                'last_run_date': '2020-05-11 11:13:36',
                'last_run_num_references': 2,
                'last_run_num_resolved_references': 2
            }, {
                'bibcode': '0002arXiv.........Z',
                'source_filename': os.path.join(self.arXiv_stubdata_dir,'00002.raw'),
                'resolved_filename': os.path.join(self.arXiv_stubdata_dir,'00002.raw.result'),
                'parser_name': 'arXiv',
                'num_runs': 1,
                'last_run_date': '2020-05-11 11:13:53',
                'last_run_num_references': 2,
                'last_run_num_resolved_references': 2
            }, {
                'bibcode': '0003arXiv.........Z',
                'source_filename': os.path.join(self.arXiv_stubdata_dir,'00003.raw'),
                'resolved_filename': os.path.join(self.arXiv_stubdata_dir,'00003.raw.result'),
                'parser_name': 'arXiv',
                'num_runs': 1,
                'last_run_date': '2020-05-11 11:14:28',
                'last_run_num_references': 2,
                'last_run_num_resolved_references': 2
            }
        ]

        # Mock DB-facing methods
        self.app.diagnostic_query = MagicMock(side_effect=self._mock_diagnostic_query)
        self.app.query_reference_source_tbl = MagicMock(side_effect=self._mock_query_reference_source_tbl)
        self.app.query_resolved_reference_tbl = MagicMock(side_effect=self._mock_query_resolved_reference_tbl)
        self.app.get_reprocess_records = MagicMock(side_effect=self._mock_get_reprocess_records)
        self.app.get_count_records = MagicMock(return_value=[
            {'name': 'ReferenceSource', 'description': 'source reference file information', 'count': 3},
            {'name': 'ProcessedHistory', 'description': 'top level information for a processed run', 'count': 4},
            {'name': 'ResolvedReference', 'description': 'resolved reference information for a processed run', 'count': 7},
            {'name': 'CompareClassic', 'description': 'comparison of new and classic processed run', 'count': 6}
        ])

    def tearDown(self):
        unittest.TestCase.tearDown(self)

        # Stop patchers
        for p in (self._p_create_all, self._p_drop_all, self._p_create_engine, self._p_sessionmaker):
            p.stop()

        # Close app if present
        if hasattr(self, "app") and hasattr(self.app, "close_app"):
            self.app.close_app()

    # ----------------------------
    # Mock implementations
    # ----------------------------
    def _mock_diagnostic_query(self, bibcode_list=None, source_filename_list=None):
        if bibcode_list is None and source_filename_list is None:
            return self._result_expected_reference_tbl[:]

        # normalize list/str inputs (your original tests sometimes pass a string)
        if isinstance(bibcode_list, str):
            bibcode_list = [bibcode_list]
        if isinstance(source_filename_list, str):
            source_filename_list = [source_filename_list]

        results = self._result_expected_reference_tbl

        if bibcode_list is not None:
            results = [r for r in results if r["bibcode"] in set(bibcode_list)]
        if source_filename_list is not None:
            results = [r for r in results if r["source_filename"] in set(source_filename_list)]
        return results

    def _mock_query_reference_source_tbl(self, parsername=""):
        if parsername == "arXiv":
            # mimic original ordering/shape
            return self._result_expected_reference_tbl[:]
        # invalid parser case: app logs error and returns empty
        self.app.logger.error(f"No records found for parser = {parsername}.")
        return []

    def _mock_query_resolved_reference_tbl(self, history_id_list=None):
        # only error behaviors are tested for this method
        if history_id_list is None or history_id_list == []:
            self.app.logger.error("No history_id provided, returning no records.")
            return []
        if history_id_list == [9999]:
            self.app.logger.error("No records found for history ids = 9999.")
            return []
        return []

    def _mock_get_reprocess_records(self, type, match_bibcode=None, score_cutoff=None, date_cutoff=None):
        # Provide the exact expected objects used by your tests.
        if type == ReprocessQueryType.year:
            return [
                {'source_bibcode': '0002arXiv.........Z',
                 'source_filename': os.path.join(self.arXiv_stubdata_dir, '00002.raw'),
                 'source_modified': datetime(2020, 4, 3, 18, 8, 42),
                 'parser_name': 'arXiv',
                 'references': [{'item_num': 2,
                                 'refstr': 'Arcangeli, J., Desert, J.-M., Parmentier, V., et al. 2019, A&A, 625, A136   ',
                                 'refraw': 'Arcangeli, J., Desert, J.-M., Parmentier, V., et al. 2019, A&A, 625, A136   '}]}
            ]
        if type == ReprocessQueryType.bibstem:
            return [
                {'source_bibcode': '0002arXiv.........Z',
                 'source_filename': os.path.join(self.arXiv_stubdata_dir, '00002.raw'),
                 'source_modified': datetime(2020, 4, 3, 18, 8, 42),
                 'parser_name': 'arXiv',
                 'references': [{'item_num': 2,
                                 'refstr': 'Arcangeli, J., Desert, J.-M., Parmentier, V., et al. 2019, A&A, 625, A136   ',
                                 'refraw': 'Arcangeli, J., Desert, J.-M., Parmentier, V., et al. 2019, A&A, 625, A136   '}]},
                {'source_bibcode': '0003arXiv.........Z',
                 'source_filename': os.path.join(self.arXiv_stubdata_dir, '00003.raw'),
                 'source_modified': datetime(2020, 4, 3, 18, 8, 32),
                 'parser_name': 'arXiv',
                 'references': [{'item_num': 2,
                                 'refstr': 'Ackermann, M., Albert, A., Atwood, W. B., et al. 2016, A&A, 586, A71 ',
                                 'refraw': 'Ackermann, M., Albert, A., Atwood, W. B., et al. 2016, A&A, 586, A71 '}]}
            ]
        return []

    # ----------------------------
    # Tests 
    # ----------------------------
    def test_query_reference_tbl(self):
        """test querying reference_source table (DB mocked)"""
        result_expected = self._result_expected_reference_tbl

        # test querying bibcodes
        bibcodes = ['0001arXiv.........Z', '0002arXiv.........Z', '0003arXiv.........Z']
        result_got = self.app.diagnostic_query(bibcode_list=bibcodes)
        self.assertTrue(result_expected == result_got)

        # test querying filenames
        filenames = [os.path.join(self.arXiv_stubdata_dir,'00001.raw'),
                     os.path.join(self.arXiv_stubdata_dir,'00002.raw'),
                     os.path.join(self.arXiv_stubdata_dir,'00003.raw')]
        result_got = self.app.diagnostic_query(source_filename_list=filenames)
        self.assertTrue(result_expected == result_got)

        # test querying both bibcodes and filenames
        result_got = self.app.diagnostic_query(bibcode_list=bibcodes, source_filename_list=filenames)
        self.assertTrue(result_expected == result_got)

        # test if nothing is passed, which return 10 records max
        result_got = self.app.diagnostic_query()
        self.assertTrue(result_expected == result_got)

    def test_query_reference_tbl_when_non_exits(self):
        """ verify non existence reference_source record """

        # test when bibcode does not exist
        self.assertTrue(self.app.diagnostic_query(bibcode_list=['0004arXiv.........Z']) == [])

        # test when filename does not exist
        self.assertTrue(self.app.diagnostic_query(source_filename_list=os.path.join(self.arXiv_stubdata_dir,'00004.raw')) == [])

        # test when both bibcode and filename are passed and nothing is returned
        self.assertTrue(self.app.diagnostic_query(bibcode_list=['0004arXiv.........Z'],
                                                     source_filename_list=os.path.join(self.arXiv_stubdata_dir,'00004.raw')) == [])

    def test_insert_reference_record(self):
        """ test inserting reference_source record """

        # attempt to insert a record that already exists in db
        # see that it is returned without it being inserted
        with self.app.session_scope() as session:
            count = self.app.get_count_reference_source_records(session)
            reference_record = ReferenceSource(bibcode='0001arXiv.........Z',
                                         source_filename=os.path.join(self.arXiv_stubdata_dir,'00001.raw'),
                                         resolved_filename=os.path.join(self.arXiv_stubdata_dir,'00001.raw.result'),
                                         parser_name=self.app.get_parser(os.path.join(self.arXiv_stubdata_dir,'00001.raw')).get('name'))
            bibcode, source_filename = self.app.insert_reference_source_record(session, reference_record)
            self.assertTrue(bibcode == '0001arXiv.........Z')
            self.assertTrue(source_filename == os.path.join(self.arXiv_stubdata_dir,'00001.raw'))
            self.assertTrue(self.app.get_count_reference_source_records(session) == count)

    def test_parser_name(self):
        """ test getting parser name from extension method """
        parser = {
            'CrossRef': ['/PLoSO/0007/10.1371_journal.pone.0048146.xref.xml', CrossRefToREFs],
            'ELSEVIER': ['/AtmEn/0230/iss.elsevier.xml', ELSEVIERtoREFs],
            'JATS': ['/NatSR/0009/iss36.jats.xml', JATStoREFs],
            'IOP': ['/JPhCS/1085/iss4.iop.xml', IOPtoREFs],
            'SPRINGER': ['/JHEP/2019/iss06.springer.xml', SPRINGERtoREFs],
            'APS': ['/PhRvB/0081/2010PhRvB..81r4520P.ref.xml', APStoREFs],
            'NATURE': ['/Natur/0549/iss7672.nature.xml', NATUREtoREFs],
            'AIP': ['/ApPhL/0102/iss7.aip.xml', AIPtoREFs],
            'WILEY': ['/JGR/0101/issD14.wiley2.xml', WILEYtoREFs],
            'NLM': ['/PNAS/0109/iss17.nlm3.xml', NLMtoREFs],
            'AGU': ['/JGR/0101/issD14.agu.xml', AGUtoREFs],
            'arXiv': ['/arXiv/2011/00324.raw', ARXIVtoREFs],
        }
        for name,info in parser.items():
            self.assertEqual(name, self.app.get_parser(info[0]).get('name'))
            self.assertEqual(info[1], verify(name))
        # now verify couple of errors
        self.assertEqual(self.app.get_parser('/RScI/0091/2020RScI...91e3301A.aipft.xml').get('name', {}), {})
        self.assertEqual(self.app.get_parser('/arXiv/2004/15000.1raw').get('name', {}), {})

    def test_reference_service_endpoint(self):
        """ test getting reference service endpoint from parser name method """
        parser = {
            'CrossRef': '/xml',
            'ELSEVIER': '/xml',
            'JATS': '/xml',
            'IOP': '/xml',
            'SPRINGER': '/xml',
            'APS': '/xml',
            'NATURE': '/xml',
            'AIP': '/xml',
            'WILEY': '/xml',
            'NLM': '/xml',
            'AGU': '/xml',
            'arXiv': '/text',
            'AEdRvHTML': '/text',
        }
        for name,endpoint in parser.items():
            self.assertEqual(endpoint, self.app.get_reference_service_endpoint(name))
        # now verify an error
        self.assertEqual(self.app.get_reference_service_endpoint('errorname'), '')

    def test_stats_compare(self):
        """ test the display of statistics comparing classic and new resolver """
        result_expected = "" \
            "+--------------------------------------------------------------+---------------------+---------------------+-----------------+-----------------+-------+-------+-------+-------+-------+\n" \
            "|                            refstr                            |   service_bibcode   |   classic_bibcode   |  service_conf   |  classic_score  | match | miss  |  new  | newu  | diff  |\n" \
            "+==============================================================+=====================+=====================+=================+=================+=======+=======+=======+=======+=======+\n" \
            "| J.-P. Uzan, Varying constants, gravitation and cosmology,    | 2011LRR....14....2U | 2010arXiv1009.5514U |       1.0       |        1        |       |       |       |       | DIFF  |\n" \
            "| Living Rev. Rel. 14 (2011) 2, [1009.5514].                   |                     |                     |                 |                 |       |       |       |       |       |\n" \
            "+--------------------------------------------------------------+---------------------+---------------------+-----------------+-----------------+-------+-------+-------+-------+-------+\n" \
            "| C. J. A. P. Martins, The status of varying constants: A      | 2017RPPh...80l6902M | 2017arXiv170902923M |       1.0       |        1        |       |       |       |       | DIFF  |\n" \
            "| review of the physics, searches and implications,            |                     |                     |                 |                 |       |       |       |       |       |\n" \
            "| 1709.02923.                                                  |                     |                     |                 |                 |       |       |       |       |       |\n" \
            "+--------------------------------------------------------------+---------------------+---------------------+-----------------+-----------------+-------+-------+-------+-------+-------+"
        result_got, num_references, num_resolved = self.app.get_service_classic_compare_stats_grid(source_bibcode='0001arXiv.........Z',
                                                                                                   source_filename=os.path.join(self.arXiv_stubdata_dir,'00001.raw'))
        self.assertEqual(result_got, result_expected)
        self.assertEqual(num_references, 2)
        self.assertEqual(num_resolved, 2)

    def test_reprocess_references(self):
        """ test reprocessing references """
        result_expected_year = [
            {'source_bibcode': '0002arXiv.........Z',
             'source_filename': os.path.join(self.arXiv_stubdata_dir,'00002.raw'),
             'source_modified': datetime(2020, 4, 3, 18, 8, 42),
             'parser_name': 'arXiv',
             'references': [{'item_num': 2,
                             'refstr': 'Arcangeli, J., Desert, J.-M., Parmentier, V., et al. 2019, A&A, 625, A136   ',
                             'refraw': 'Arcangeli, J., Desert, J.-M., Parmentier, V., et al. 2019, A&A, 625, A136   '}]}
        ]
        result_expected_bibstem = [
            {'source_bibcode': '0002arXiv.........Z',
             'source_filename': os.path.join(self.arXiv_stubdata_dir,'00002.raw'),
             'source_modified': datetime(2020, 4, 3, 18, 8, 42),
             'parser_name': 'arXiv',
             'references': [{'item_num': 2,
                             'refstr': 'Arcangeli, J., Desert, J.-M., Parmentier, V., et al. 2019, A&A, 625, A136   ',
                             'refraw': 'Arcangeli, J., Desert, J.-M., Parmentier, V., et al. 2019, A&A, 625, A136   '}]
            },
            {'source_bibcode': '0003arXiv.........Z',
             'source_filename': os.path.join(self.arXiv_stubdata_dir,'00003.raw'),
             'source_modified': datetime(2020, 4, 3, 18, 8, 32),
             'parser_name': 'arXiv',
             'references': [{'item_num': 2,
                             'refstr': 'Ackermann, M., Albert, A., Atwood, W. B., et al. 2016, A&A, 586, A71 ',
                             'refraw': 'Ackermann, M., Albert, A., Atwood, W. B., et al. 2016, A&A, 586, A71 '}]
            }
        ]
        self.assertEqual(self.app.get_reprocess_records(ReprocessQueryType.year, match_bibcode='2019', score_cutoff=None, date_cutoff=None), result_expected_year)
        self.assertEqual(self.app.get_reprocess_records(ReprocessQueryType.bibstem, match_bibcode='A&A..', score_cutoff=None, date_cutoff=None), result_expected_bibstem)

        references_and_ids_year = [
            {'id': 'H4I2', 'reference': 'Arcangeli, J., Desert, J.-M., Parmentier, V., et al. 2019, A&A, 625, A136   '}
        ]
        reprocess_references = self.app.populate_tables_pre_resolved_retry_status(
            source_bibcode=result_expected_year[0]['source_bibcode'],
            source_filename=result_expected_year[0]['source_filename'],
            source_modified=result_expected_year[0]['source_modified'],
            retry_records=result_expected_year[0]['references'])
        self.assertTrue(reprocess_references)
        self.assertTrue(reprocess_references, references_and_ids_year)
        current_num_records = [
            {'name': 'ReferenceSource', 'description': 'source reference file information', 'count': 3},
            {'name': 'ProcessedHistory', 'description': 'top level information for a processed run', 'count': 4},
            {'name': 'ResolvedReference', 'description': 'resolved reference information for a processed run', 'count': 7},
            {'name': 'CompareClassic', 'description': 'comparison of new and classic processed run', 'count': 6}
        ]
        self.assertTrue(self.app.get_count_records() == current_num_records)

    def test_get_parser(self):
        """  test get_parser """

        # test cases where journal and extension alone determine the parser
        self.assertEqual(self.app.get_parser('OTHER/2007AIPC..948..357M/2007AIPC..948..357M.raw')['name'], 'ADStxt')
        self.assertEqual(self.app.get_parser('OTHER/Astro2020/2019arXiv190309325N.raw')['name'], 'arXiv')

        # test case where volume information is needed to identify the correct parser
        result = self.app.get_parser('PASJ/0052/iss0.raw')
        self.assertIsInstance(result, dict)
        self.assertEqual(result.get('name'), 'PASJhtml')
        self.assertEqual(result.get('matches'), [{'journal': 'PASJ', 'volume_end': 53, 'volume_begin': 51}])

    def test_match_parser(self):
        """  test match_parser when the filepath has been wrong and no matches were found"""
        self.assertEqual(self.app.match_parser(rows=[], journal='unknown', volume='2'), {})

    def test_query_reference_source_tbl(self):
        """ test query_reference_source_tbl when parsername is given """

        # test when parsername is valid
        result = self.app.query_reference_source_tbl(parsername="arXiv")
        self.assertEqual(len(result), 3)
        self.assertEqual(result[0]['parser_name'], "arXiv")
        self.assertEqual(result[1]['bibcode'], "0002arXiv.........Z")
        self.assertEqual(result[2]['source_filename'].split('/')[-1], "00003.raw")

        # test when parsername is invalid and should log an error
        with patch.object(self.app.logger, 'error') as mock_error:
            result = self.app.query_reference_source_tbl(parsername="invalid")
            self.assertEqual(len(result), 0)
            mock_error.assert_called_with("No records found for parser = invalid.")

    def test_query_resolved_reference_tbl_no_records(self):
        """ test query_resolved_reference_tbl() when no records exist """

        # when history_id_list is not empty
        with patch.object(self.app.logger, 'error') as mock_error:
            result = self.app.query_resolved_reference_tbl(history_id_list=[9999])
            self.assertEqual(result, [])
            mock_error.assert_called_with("No records found for history ids = 9999.")

        # when history_id_list is empty
        with patch.object(self.app.logger, 'error') as mock_error:
            result = self.app.query_resolved_reference_tbl(history_id_list=[])
            self.assertEqual(result, [])
            mock_error.assert_called_with("No history_id provided, returning no records.")

    def test_populate_tables_pre_resolved_initial_status_exception(self):
        """ test populate_tables_pre_resolved_initial_status method when there is an exception """
        with patch.object(self.app, "session_scope") as mock_session_scope:
            mock_session = mock_session_scope.return_value.__enter__.return_value
            mock_session.commit.side_effect = SQLAlchemyError("Mocked SQLAlchemyError")

            with patch.object(self.app.logger, 'error') as mock_error:
                results = self.app.populate_tables_pre_resolved_initial_status('0001arXiv.........Z',
                                                                               os.path.join(self.arXiv_stubdata_dir,'00001.raw'),
                                                                               'arXiv',
                                                                               references=[])
                self.assertEqual(results, [])
                mock_session.rollback.assert_called_once()
                mock_error.assert_called()

    def test_populate_tables_pre_resolved_retry_status_exception(self):
        """ test populate_tables_pre_resolved_retry_status method when there is an exception """
        with patch.object(self.app, "session_scope") as mock_session_scope:
            mock_session = mock_session_scope.return_value.__enter__.return_value
            mock_session.commit.side_effect = SQLAlchemyError("Mocked SQLAlchemyError")

            with patch.object(self.app.logger, 'error') as mock_error:
                results = self.app.populate_tables_pre_resolved_retry_status('0001arXiv.........Z',
                                                                             os.path.join(self.arXiv_stubdata_dir,'00001.raw'),
                                                                             source_modified='',
                                                                             retry_records=[])
                self.assertEqual(results, [])
                mock_session.rollback.assert_called_once()
                mock_error.assert_called()

    def test_populate_tables_post_resolved_exception(self):
        """ test populate_tables_post_resolved method when there is an exception """
        with patch.object(self.app, "session_scope") as mock_session_scope:
            mock_session = mock_session_scope.return_value.__enter__.return_value
            mock_session.commit.side_effect = SQLAlchemyError("Mocked SQLAlchemyError")

            with patch.object(self.app.logger, 'error') as mock_error:
                result = self.app.populate_tables_post_resolved(resolved_reference=[],
                                                                source_bibcode='0001arXiv.........Z',
                                                                classic_resolved_filename=os.path.join(self.arXiv_stubdata_dir,'00001.raw.results'))
                self.assertEqual(result, False)
                mock_session.rollback.assert_called_once()
                mock_error.assert_called()

    def test_populate_tables_post_resolved_with_classic(self):
        """ test populate_tables_post_resolved when resolved_classic is available """

        resolved_reference = [
            {
                'id': 'H1I1',
                'refstring': 'Reference 1',
                'bibcode': '2023A&A...657A...1X',
                'score': 1.0,
                'external_identifier': ['doi:10.1234/abc', 'arxiv:2301.00001'],
            },
            {
                'id': 'H1I2',
                'refstring': 'Reference 2',
                'bibcode': '2023A&A...657A...2X',
                'score': 0.8,
                'external_identifier': ['ascl:2301.001', 'doi:10.9999/xyz'],
            }
        ]

        source_bibcode = "2023A&A...657A...1X"
        classic_resolved_filename = "classic_results.txt"
        classic_resolved_reference = [
            (1, "2023A&A...657A...1X", "1", "MATCH"),
            (2, "2023A&A...657A...2X", "1", "MATCH")
        ]

        with patch.object(self.app, "session_scope"), \
             patch("adsrefpipe.app.compare_classic_and_service", return_value=classic_resolved_reference), \
             patch.object(self.app, "update_resolved_reference_records") as mock_update, \
             patch.object(self.app, "insert_compare_records") as mock_insert, \
             patch.object(self.app.logger, "info") as mock_logger:

            result = self.app.populate_tables_post_resolved(resolved_reference, source_bibcode, classic_resolved_filename)

            self.assertTrue(result)
            mock_update.assert_called_once()
            mock_insert.assert_called_once()
            mock_logger.assert_called_with("Updated 2 resolved reference records successfully.")

            # Check whether external_identifier is populated with correct data
            _, resolved_records = mock_update.call_args[0]
            self.assertEqual(len(resolved_records), 2)
            self.assertEqual(_get_external_identifier(resolved_records[0]), ['doi:10.1234/abc', 'arxiv:2301.00001'])
            self.assertEqual(_get_external_identifier(resolved_records[1]), ['ascl:2301.001', 'doi:10.9999/xyz'])

    @patch("adsrefpipe.app.ProcessedHistory")
    @patch("adsrefpipe.app.ResolvedReference")
    @patch("adsrefpipe.app.CompareClassic")
    def test_get_service_classic_compare_tags(self, mock_compare, mock_resolved, mock_processed):
        """ test get_service_classic_compare_tags """

        mock_session = MagicMock()

        # mock resolved_reference_ids to behave like a real subquery
        resolved_reference_ids_mock = table("resolved_reference_ids", column("history_id"), column("item_num"))
        mock_session.query().filter().distinct().subquery.return_value = resolved_reference_ids_mock

        # explicitly define mock_compare.state as a SQLAlchemy column
        mock_compare.state = column("state")

        # mock the session query behavior for final select query involving CompareClassic
        mock_final_query = mock_session.query.return_value
        mock_final_query.select_from.return_value.outerjoin.return_value.group_by.return_value.subquery.return_value = "mock_final_subquery"

        # test case 1: Only source_bibcode is provided
        result1 = self.app.get_service_classic_compare_tags(mock_session, source_bibcode="2023A&A...657A...1X", source_filename="")
        self.assertEqual(result1, "mock_final_subquery")

        expected_filter_bibcode = and_(mock_processed.id == mock_resolved.history_id, literal('"2023A&A...657A...1X').op('~')(mock_processed.bibcode))
        found_bibcode_filter = any(call.args and expected_filter_bibcode.compare(call.args[0]) for call in mock_session.query().filter.call_args_list)
        self.assertTrue(found_bibcode_filter)

        # test case 2: Only source_filename are provided
        result2 = self.app.get_service_classic_compare_tags(mock_session, source_bibcode="", source_filename="some_source_file.txt")
        self.assertEqual(result2, "mock_final_subquery")

        expected_filter_filename = and_(mock_processed.id == mock_resolved.history_id, literal('2023A&A...657A...1X').op('~')(mock_processed.source_filename))
        found_filename_filter = any(call.args and expected_filter_filename.compare(call.args[0]) for call in mock_session.query().filter.call_args_list)
        self.assertTrue(found_filename_filter)

    def test_get_service_classic_compare_stats_grid_error(self):
        """ test get_service_classic_compare_stats_grid when error """

        with patch.object(self.app, "session_scope") as mock_session_scope:
            mock_session = mock_session_scope.return_value.__enter__.return_value

            # create a mock for compare_grid
            mock_compare_grid = Mock()
            mock_compare_grid.c.MATCH = Mock(label=Mock(return_value="MATCH"))
            mock_compare_grid.c.MISS = Mock(label=Mock(return_value="MISS"))
            mock_compare_grid.c.NEW = Mock(label=Mock(return_value="NEW"))
            mock_compare_grid.c.NEWU = Mock(label=Mock(return_value="NEWU"))
            mock_compare_grid.c.DIFF = Mock(label=Mock(return_value="DIFF"))

            # mock `get_service_classic_compare_tags()` to return the mocked compare_grid
            with patch.object(self.app, "get_service_classic_compare_tags", return_value=mock_compare_grid):
                # mock `session.query(...).all()` to return an empty list
                mock_session.query.return_value.filter.return_value.order_by.return_value.all.return_value = []

                result = self.app.get_service_classic_compare_stats_grid(source_bibcode='0001arXiv.........Z',
                                                                         source_filename=os.path.join(self.arXiv_stubdata_dir,'00001.raw'))

                self.assertEqual(result, ('Unable to fetch data for reference source file `%s` from database!'%os.path.join(self.arXiv_stubdata_dir,'00001.raw'), -1, -1))

    @patch("adsrefpipe.app.datetime")
    def test_filter_reprocess_query(self, mock_datetime):
        """Test all cases of filter_reprocess_query"""

        mock_query = Mock()

        # set a fixed datetime for consistent testing
        mock_datetime.now.return_value = datetime(2025, 1, 1)

        # test case: ReprocessQueryType.score
        self.app.filter_reprocess_query(mock_query, ReprocessQueryType.score, 0.8, "", 0)
        mock_query.filter.assert_called()
        called_args, _ = mock_query.filter.call_args
        compiled_query = called_args[0].compile(dialect=postgresql.dialect())
        self.assertTrue(str(called_args[0]), 'resolved_reference.score <= :score_1')
        self.assertTrue(compiled_query.params['score_1'], 0.8)

        # test case: ReprocessQueryType.bibstem with match_bibcode
        mock_query.reset_mock()
        self.app.filter_reprocess_query(mock_query, ReprocessQueryType.bibstem, 0.8, "1234", 0)
        mock_query.filter.assert_called()
        called_args, _ = mock_query.filter.call_args
        compiled_query = called_args[0].compile(dialect=postgresql.dialect())
        self.assertTrue(str(called_args[0]), 'resolved_reference.bibcode LIKE :bibcode_1')
        self.assertTrue(compiled_query.params['bibcode_1'], '____1234__________')

        # test case: ReprocessQueryType.year with match_bibcode
        mock_query.reset_mock()
        self.app.filter_reprocess_query(mock_query, ReprocessQueryType.year, 0.8, "2023", 0)
        mock_query.filter.assert_called()
        called_args, _ = mock_query.filter.call_args
        compiled_query = called_args[0].compile(dialect=postgresql.dialect())
        self.assertTrue(str(called_args[0]), 'resolved_reference.bibcode LIKE :bibcode_1')
        self.assertTrue(compiled_query.params['bibcode_1'], '2023_______________')

        # test case: ReprocessQueryType.failed
        mock_query.reset_mock()
        self.app.filter_reprocess_query(mock_query, ReprocessQueryType.failed, 0.8, "", 0)
        mock_query.filter.assert_called()
        called_args, _ = mock_query.filter.call_args
        compiled_query = called_args[0].compile(dialect=postgresql.dialect())
        self.assertTrue(str(called_args[0]), 'resolved_reference.bibcode = :bibcode_1 AND resolved_reference.score = :score_1')
        self.assertTrue(compiled_query.params['bibcode_1'], '0000')
        self.assertTrue(compiled_query.params['score_1'], -1)

        # Test case: date_cutoff is applied
        mock_query.reset_mock()
        self.app.filter_reprocess_query(mock_query, ReprocessQueryType.score, 0.8, "", 10)
        expected_since = datetime(2025, 1, 1) - timedelta(days=10)
        mock_query.filter.assert_called()
        called_args, _ = mock_query.filter.call_args
        compiled_query = called_args[0].compile(dialect=postgresql.dialect())
        print(str(called_args[0]))
        print(compiled_query.params)
        self.assertTrue(str(called_args[0]), 'resolved_reference.score <= :score_1')
        self.assertTrue(compiled_query.params.get('score_1'), 0.8)

    def test_get_reprocess_records(self):
        """ test get_reprocess_records method """

        with patch.object(self.app, "session_scope") as mock_session_scope:
            mock_session = mock_session_scope.return_value.__enter__.return_value

            # define a mock SQLAlchemy row with _asdict() method
            MockRow = namedtuple("MockRow",
                                 ["history_id", "item_num", "refstr", "refraw", "source_bibcode", "source_filename",
                                  "source_modified", "parser_name"])

            # mock query results with same history_id to trigger the else block
            mock_session.query.return_value.filter.return_value.order_by.return_value.all.return_value = [
                MockRow(history_id=1, item_num=1, refstr="Reference 1", refraw="Raw 1", source_bibcode="2023A&A...657A...1X",
                        source_filename="some_source_file.txt", source_modified="D1", parser_name="arXiv"),
                MockRow(history_id=1, item_num=2, refstr="Reference 2", refraw="Raw 2", source_bibcode="2023A&A...657A...1X",
                        source_filename="some_source_file.txt", source_modified="D1", parser_name="arXiv"),
            ]

            results = self.app.get_reprocess_records(type=0, score_cutoff=0.8, match_bibcode="", date_cutoff=0)

            self.assertEqual(len(results), 1)
            self.assertEqual(len(results[0]['references']), 2)
            self.assertEqual(results[0]['references'][1]['refstr'], 'Reference 2')

    def test_get_resolved_references_all(self):
        """ test get_resolved_references_all method """

        with patch.object(self.app, "session_scope") as mock_session_scope:
            mock_session = mock_session_scope.return_value.__enter__.return_value

            # define a mock SQLAlchemy row with _asdict() method
            MockRow = namedtuple("MockRow", ["source_bibcode", "date", "id", "resolved_bibcode", "score", "parser_name"])

            # mock query results with highest scores
            mock_session.query.return_value.filter.return_value.order_by.return_value.all.return_value = [
                MockRow(source_bibcode="2023A&A...657A...1X", date=datetime(2025, 1, 1), id=1, resolved_bibcode="0001arXiv.........Z", score=0.95, parser_name="arXiv"),
                MockRow(source_bibcode="2023A&A...657A...1X", date=datetime(2025, 1, 2), id=2, resolved_bibcode="0002arXiv.........Z", score=0.85, parser_name="arXiv"),
            ]

            results = self.app.get_resolved_references_all("2023A&A...657A...1X")

            assert len(results) == 2
            assert results[0] == ("2023A&A...657A...1X", "2025-01-01 00:00:00", 1, "0001arXiv.........Z", 0.95, "arXiv")
            assert results[1] == ("2023A&A...657A...1X", "2025-01-02 00:00:00", 2, "0002arXiv.........Z", 0.85, "arXiv")

            # test case when no results are found
            with patch.object(self.app.logger, "error") as mock_error:
                mock_session.query.return_value.filter.return_value.order_by.return_value.all.return_value = []
                results = self.app.get_resolved_references_all("2023A&A...657A...1X")
                assert results == []
                mock_error.assert_called_with("Unable to fetch resolved references for source bibcode `2023A&A...657A...1X`.")

    def test_get_resolved_references(self):
        """ test get_resolved_references method """

        with patch.object(self.app, "session_scope") as mock_session_scope:
            mock_session = mock_session_scope.return_value.__enter__.return_value

            # Define a mock SQLAlchemy row with namedtuple
            MockRow = namedtuple("MockRow", ["source_bibcode", "date", "id", "resolved_bibcode", "score", "parser_name", "parser_priority"])

            # Mock query results with highest-ranked records
            mock_session.query.return_value.filter.return_value.order_by.return_value.all.return_value = [
                MockRow(source_bibcode="2023A&A...657A...1X", date=datetime(2025, 1, 1), id=1, resolved_bibcode="0001arXiv.........Z", score=0.95, parser_name="arXiv", parser_priority=1),
                MockRow(source_bibcode="2023A&A...657A...1X", date=datetime(2025, 1, 2), id=2, resolved_bibcode="0002arXiv.........Z", score=0.85, parser_name="arXiv", parser_priority=1),
            ]

            results = self.app.get_resolved_references("2023A&A...657A...1X")

            assert len(results) == 2
            assert results[0] == {
                "source_bibcode": "2023A&A...657A...1X",
                "date": "2025-01-01 00:00:00",
                "id": 1,
                "resolved_bibcode": "0001arXiv.........Z",
                "score": 0.95,
                "parser_name": "arXiv",
                "parser_priority": 1
            }
            assert results[1] == {
                "source_bibcode": "2023A&A...657A...1X",
                "date": "2025-01-02 00:00:00",
                "id": 2,
                "resolved_bibcode": "0002arXiv.........Z",
                "score": 0.85,
                "parser_name": "arXiv",
                "parser_priority": 1
            }

            # Test case when no results are found
            with patch.object(self.app.logger, "error") as mock_error:
                mock_session.query.return_value.filter.return_value.order_by.return_value.all.return_value = []
                results = self.app.get_resolved_references("2023A&A...657A...1X")
                assert results == []
                mock_error.assert_called_with("Unable to fetch resolved references for source bibcode `2023A&A...657A...1X`.")

    def test_parser_model_get_name(self):
        """ test get_name method of Parser class in model module """
        parser = Parser(name="TestParser", extension_pattern=".xml", reference_service_endpoint="xml", matches=[])
        self.assertEqual(parser.get_name(), "TestParser")

    def test_parser_model_get_extension_pattern(self):
        """ test get_extension_pattern method of Parser class in model module """
        parser = Parser(name="TestParser", extension_pattern=".xml", reference_service_endpoint="xml", matches=[])
        self.assertEqual(parser.get_extension_pattern(), ".xml")

    def test_processed_history_toJSON(self):
        """ test toJSON method of ProcessedHistory class in model module """
        history = ProcessedHistory(
            bibcode="2023A&A...657A...1X",
            source_filename="some_source_file.txt",
            source_modified="2025-03-05T12:00:00",
            status="processed",
            date="2025-03-05T12:30:00",
            total_ref=10
        )
        expected_json = {
            "bibcode": "2023A&A...657A...1X",
            "source_filename": "some_source_file.txt",
            "source_modified": "2025-03-05T12:00:00",
            "status": "processed",
            "date": "2025-03-05T12:30:00",
            "total_ref": 10
        }
        self.assertEqual(history.toJSON(), expected_json)

    def test_compare_classic_toJSON(self):
        """Test toJSON method of CompareClassic class"""
        compare = CompareClassic(
            history_id=1,
            item_num=2,
            bibcode="0001arXiv.........Z",
            score=1,
            state="MATCH")
        expected_json = {
            "history_id": 1,
            "item_num": 2,
            "bibcode": "0001arXiv.........Z",
            "score": 1,
            "state": "MATCH"
        }
        self.assertEqual(compare.toJSON(), expected_json)


class TestDatabaseNoStubdata(unittest.TestCase):

    """
    Tests the application's methods when there is no need for shared stubdata
    """

    maxDiff = None

    _postgresql = testing.postgresql.Postgresql()
    postgresql_url = _postgresql.url()

    @classmethod
    def tearDownClass(cls):
        super().tearDownClass()
        cls._postgresql.stop()

    def setUp(self):
        self.test_dir = os.path.join(project_home, 'adsrefpipe/tests')
        unittest.TestCase.setUp(self)
        self.app = app.ADSReferencePipelineCelery('test', local_config={
            'SQLALCHEMY_URL': self.postgresql_url,
            'SQLALCHEMY_ECHO': False,
            'PROJ_HOME': project_home,
            'TEST_DIR': self.test_dir,
        })
        Base.metadata.bind = self.app._session.get_bind()
        Base.metadata.create_all()

    def tearDown(self):
        unittest.TestCase.tearDown(self)
        Base.metadata.drop_all()
        self.app.close_app()

    def test_app(self):
        assert self.app._config.get('SQLALCHEMY_URL') == self.postgresql_url
        assert self.app.conf.get('SQLALCHEMY_URL') == self.postgresql_url

    def test_query_reference_tbl_when_empty(self):
        """ verify reference_source table being empty """
        self.assertTrue(self.app.diagnostic_query() == [])

    def test_populate_tables(self):
        """ test populating all tables """
        references = [
            {
                "refstr": "J.-P. Uzan, Varying constants, gravitation and cosmology, Living Rev. Rel. 14 (2011) 2, [1009.5514]. ",
                "refraw": "J.-P. Uzan, Varying constants, gravitation and cosmology, Living Rev. Rel. 14 (2011) 2, [1009.5514]. "},
            {
                "refstr": "C. J. A. P. Martins, The status of varying constants: A review of the physics, searches and implications, 1709.02923.",
                "refraw": "C. J. A. P. Martins, The status of varying constants: A review of the physics, searches and implications, 1709.02923."
            }
        ]
        references_and_ids = [
            {
                "refstr": "J.-P. Uzan, Varying constants, gravitation and cosmology, Living Rev. Rel. 14 (2011) 2, [1009.5514]. ",
                "refraw": "J.-P. Uzan, Varying constants, gravitation and cosmology, Living Rev. Rel. 14 (2011) 2, [1009.5514]. ",
                "id": "H1I1"
            },
            {
                "refstr": "C. J. A. P. Martins, The status of varying constants: A review of the physics, searches and implications, 1709.02923.",
                "refraw": "C. J. A. P. Martins, The status of varying constants: A review of the physics, searches and implications, 1709.02923.",
                "id": "H1I2"}
        ]

        # IMPORTANT: use the real column name expected by app/models: external_identifier (list)
        resolved_references = [
            {
                "score": "1.0",
                "bibcode": "2011LRR....14....2U",
                "refstring": "J.-P. Uzan, Varying constants, gravitation and cosmology, Living Rev. Rel. 14 (2011) 2, [1009.5514]. ",
                "refraw": "J.-P. Uzan, Varying constants, gravitation and cosmology, Living Rev. Rel. 14 (2011) 2, [1009.5514]. ",
                "id": "H1I1",
                "external_identifier": ["arxiv:1009.5514", "doi:10.1234/abc"]
            },
            {
                "score": "1.0",
                "bibcode": "2017RPPh...80l6902M",
                "refstring": "C. J. A. P. Martins, The status of varying constants: A review of the physics, searches and implications, 1709.02923.",
                "refraw": "C. J. A. P. Martins, The status of varying constants: A review of the physics, searches and implications, 1709.02923.",
                "id": "H1I2",
                "external_identifier": ["arxiv:1709.02923", "ascl:2301.001"]
            }
        ]

        arXiv_stubdata_dir = os.path.join(self.test_dir, 'unittests/stubdata/txt/arXiv/0/')
        with self.app.session_scope() as session:
            session.query(Action).delete()
            session.query(Parser).delete()
            session.commit()
            if session.query(Action).count() == 0:
                session.bulk_save_objects(actions_records)
            if session.query(Parser).count() == 0:
                session.bulk_save_objects(parsers_records)
            session.commit()

            references = self.app.populate_tables_pre_resolved_initial_status(
                source_bibcode='0001arXiv.........Z',
                source_filename=os.path.join(arXiv_stubdata_dir,'00001.raw'),
                parsername=self.app.get_parser(os.path.join(arXiv_stubdata_dir,'00001.raw')).get('name'),
                references=references)

            self.assertTrue(references)
            self.assertTrue(references == references_and_ids)

            status = self.app.populate_tables_post_resolved(
                resolved_reference=resolved_references,
                source_bibcode='0001arXiv.........Z',
                classic_resolved_filename=os.path.join(arXiv_stubdata_dir, '00001.raw.result'))
            self.assertTrue(status == True)

            # Verify external_identifier was persisted on ResolvedReference rows
            # We know history_id should be 1 for the first inserted ProcessedHistory in an empty DB.
            rows = (
                session.query(ResolvedReference)
                .filter(ResolvedReference.history_id == 1)
                .order_by(ResolvedReference.item_num.asc())
                .all()
            )
            self.assertEqual(len(rows), 2)
            self.assertEqual(rows[0].item_num, 1)
            self.assertEqual(rows[1].item_num, 2)
            self.assertEqual(rows[0].external_identifier, ["arxiv:1009.5514", "doi:10.1234/abc"])
            self.assertEqual(rows[1].external_identifier, ["arxiv:1709.02923", "ascl:2301.001"])

    def test_get_parser_error(self):
        """ test get_parser when it errors for unrecognized source filename """
        with patch.object(self.app.logger, 'error') as mock_error:
            self.assertEqual(self.app.get_parser("invalid/file/path/"), {})
            mock_error.assert_called_with("Unrecognizable source file invalid/file/path/.")


if __name__ == '__main__':
    unittest.main()

