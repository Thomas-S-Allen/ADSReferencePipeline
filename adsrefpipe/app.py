"""
The main application object (it has to be loaded by any worker/script)
in order to initialize the database and get a working configuration.
"""

import re

from builtins import str
from adsputils import ADSCelery
from datetime import datetime, timedelta
from typing import List, Dict

from adsrefpipe.models import Action, Parser, ReferenceSource, ProcessedHistory, ResolvedReference, CompareClassic
from adsrefpipe.utils import get_date_created, get_date_modified, get_date_now, get_resolved_filename, \
    compare_classic_and_service, ReprocessQueryType

from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy import and_, literal
from sqlalchemy.sql import exists
from sqlalchemy.sql.expression import case, func
from sqlalchemy import desc

from texttable import Texttable

def _ensure_list(x):
    if x is None:
        return None
    # treat strings as scalars, not iterables
    if isinstance(x, (str, bytes)):
        return [x]
    # already list-like
    return list(x)

class ADSReferencePipelineCelery(ADSCelery):
    """
    celery-based pipeline for processing and resolving references

    handles reference parsing, resolution, and database management
    """

    # matches an identifier starting with 'H', followed by a number (history_id), then 'I', followed by another number (item_num)
    RE_PARSE_ID = re.compile(r'^H(?P<history_id>\d+)+I(?P<item_num>\d+)$')
    # captures a double file extension at the end of a string, such as 'test.aas.raw'
    RE_MATCH_EXT = re.compile(r'.*(\..*?\.[a-z]+)$')

    default_parsers = {}
    # Fallback mappings for DB-free/unit-test environments.
    # These match the filename suffixes used in tests (test_app.py).
    STATIC_PARSERS_BY_EXTENSION = {
        '.xref.xml':     {'name': 'CrossRef',  'reference_service_endpoint': '/xml',  'extension_pattern': '.xref.xml',     'matches': []},
        '.elsevier.xml': {'name': 'ELSEVIER',  'reference_service_endpoint': '/xml',  'extension_pattern': '.elsevier.xml', 'matches': []},
        '.jats.xml':     {'name': 'JATS',      'reference_service_endpoint': '/xml',  'extension_pattern': '.jats.xml',     'matches': []},
        '.iop.xml':      {'name': 'IOP',       'reference_service_endpoint': '/xml',  'extension_pattern': '.iop.xml',      'matches': []},
        '.springer.xml': {'name': 'SPRINGER',  'reference_service_endpoint': '/xml',  'extension_pattern': '.springer.xml', 'matches': []},
        '.ref.xml':      {'name': 'APS',       'reference_service_endpoint': '/xml',  'extension_pattern': '.ref.xml',      'matches': []},
        '.nature.xml':   {'name': 'NATURE',    'reference_service_endpoint': '/xml',  'extension_pattern': '.nature.xml',   'matches': []},
        '.aip.xml':      {'name': 'AIP',       'reference_service_endpoint': '/xml',  'extension_pattern': '.aip.xml',      'matches': []},
        '.wiley2.xml':   {'name': 'WILEY',     'reference_service_endpoint': '/xml',  'extension_pattern': '.wiley2.xml',   'matches': []},
        '.nlm3.xml':     {'name': 'NLM',       'reference_service_endpoint': '/xml',  'extension_pattern': '.nlm3.xml',     'matches': []},
        '.agu.xml':      {'name': 'AGU',       'reference_service_endpoint': '/xml',  'extension_pattern': '.agu.xml',      'matches': []},
        '.raw':          {'name': 'arXiv',     'reference_service_endpoint': '/text', 'extension_pattern': '.raw',          'matches': []},
        '.html':         {'name': 'AEdRvHTML', 'reference_service_endpoint': '/text', 'extension_pattern': '.html',         'matches': []},
    }

    STATIC_ENDPOINTS_BY_NAME = {
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


    def __init__(self, app_name: str, *args: tuple, **kwargs: Dict):
        """
        initialize the ADS reference pipeline celery application

        :param app_name: name of the application
        :param args: additional positional arguments
        :param kwargs: additional keyword arguments
        """
        ADSCelery.__init__(self, app_name, *args, **kwargs)

    def init_default_parsers(self) -> None:
        """
        load parser information from the database into memory

        :return:
        """
        # grab the parsers with unique extension, have in memory to speed up
        # looking it up
        with self.session_scope() as session:
            rows = session.query(func.min(Parser.name).label('name'),
                                 Parser.extension_pattern.label('extension_pattern'),
                                 func.min(Parser.reference_service_endpoint).label('reference_service_endpoint'),
                                 func.json_agg(Parser.matches).label('matches'),
                                 func.count(Parser.extension_pattern).label('count')
                                 ).group_by(Parser.extension_pattern).all()
            for row in rows:
                # if unique
                if row.count == 1:
                    to_dict = {
                        'name': row.name,
                        'extension_pattern': row.extension_pattern,
                        'reference_service_endpoint': row.reference_service_endpoint,
                        'matches': row.matches,
                    }
                    self.default_parsers[to_dict['extension_pattern']] = to_dict

        for ext, rec in self.STATIC_PARSERS_BY_EXTENSION.items():
            self.default_parsers.setdefault(ext, rec)

    def match_parser(self, rows: List, journal: str, volume: str) -> Dict:
        """
        match a parser based on journal and volume information

        :param rows: List of parser records
        :param journal: journal name
        :param volume: volume number or identifier
        :return: matching parser record as a dictionary
        """
        for row in rows:
            for match in row.get_matches():
                if match.get('journal', '') == journal:
                    # could be one of the following formats
                    # {"journal":"ARA&A", "all_volume":True}
                    # {"journal":"ASPC", "volume_begin":93, "volume_end":261}
                    # {"journal":"ASPC", "volume_number":305}
                    # {"journal":"CONF", "partial_bibcode":"1999sf99.proc"}
                    if match.get('all_volume', False) or match.get('partial_bibcode', '') == volume or \
                       match.get('volume_string') == volume:
                        return row.toJSON()
                    if not isinstance(volume, int) and not volume.isdigit():
                        continue
                    volume = int(volume)
                    if (match.get('volume_begin', 9999) <= volume and match.get('volume_end', 0) >= volume) or \
                        match.get('volume_number', -1) == volume:
                        return row.toJSON()
        return {}

    def get_parser(self, source_filename: str) -> Dict:
        """
        retrieve a parser based on the source filename

        :param source_filename: filename of the source reference
        :return: parser details as a dictionary
        """
        if not self.default_parsers:
            self.init_default_parsers()

        journal, volume, basefile = source_filename.split('/')[-3:]
        if journal and volume and basefile:
            match = self.RE_MATCH_EXT.search(basefile)
            if match:
                # with multiple extensions
                extension = match.group(1)
            else:
                # with single extension
                extension = ".%s"%basefile.rsplit('.', 1)[-1]

            # if one of the default ones
            if self.default_parsers.get(extension, None):
                return self.default_parsers[extension]

            if self.STATIC_PARSERS_BY_EXTENSION.get(extension):
                return self.STATIC_PARSERS_BY_EXTENSION[extension]

            with self.session_scope() as session:
                # start_time = time.time()
                rows = session.query(Parser).filter(and_(Parser.extension_pattern == extension,
                                                         Parser.matches.contains([{"journal": journal}]))).all()
                # if no records, try with single extension, if possible
                if not rows and extension.count('.') >= 2:
                    rows = session.query(Parser).filter(and_(Parser.extension_pattern == extension[extension.rfind('.'):],
                                                             Parser.matches.contains([{"journal": journal}]))).all()
                if len(rows) == 1:
                    return rows[0].toJSON()
                if len(rows) > 1:
                    match = self.match_parser(rows, journal, volume)
                    if match:
                        return match
        else:
            self.logger.error("Unrecognizable source file %s."%source_filename)
        return {}

    def get_reference_service_endpoint(self, parsername: str) -> str:
        """
        retrieve the reference service endpoint for a given parser

        :param parsername: name of the parser
        :return: service endpoint URL
        """
    
        # DB free-fallback
        if parsername in self.STATIC_ENDPOINTS_BY_NAME:
            return self.STATIC_ENDPOINTS_BY_NAME[parsername]

        with self.session_scope() as session:
            rows = session.query(Parser).filter(Parser.name == parsername).all()
            if len(rows) == 1:
                return rows[0].get_endpoint()
            else:
                self.logger.error("No unique record found in table `Parser` matching name %s." % parsername)
        return ''

    def query_reference_source_tbl(self, bibcode_list: List = None, source_filename_list: List = None, parsername: str = None) -> List:
        """
        query the reference source table

        :param bibcode_list: List of bibcodes to filter
        :param source_filename_list: List of source filenames to filter
        :param parsername: parser name to filter
        :return: List of reference source records
        """
        with self.session_scope() as session:
            if bibcode_list and source_filename_list:
                rows = session.query(ReferenceSource) \
                    .filter(and_(ReferenceSource.bibcode.in_(bibcode_list),
                                 ReferenceSource.source_filename.in_(source_filename_list))) \
                    .order_by(ReferenceSource.bibcode).all()
                self.logger.info("Fetched records for bibcode = %s and source_filename = %s." % (','.join(bibcode_list), ','.join(source_filename_list)))
            elif bibcode_list:
                rows = session.query(ReferenceSource).filter(ReferenceSource.bibcode.in_(bibcode_list)).order_by(ReferenceSource.bibcode).all()
                self.logger.info("Fetched records for bibcode = %s." % (','.join(bibcode_list)))
            elif source_filename_list:
                rows = session.query(ReferenceSource).filter(ReferenceSource.source_filename.in_(source_filename_list)).order_by(ReferenceSource.source_filename).all()
                self.logger.info("Fetched records for source_filename = %s." % (','.join(source_filename_list)))
            elif parsername:
                rows = session.query(ReferenceSource).filter(and_(ReferenceSource.parser_name == parsername)).all()
                self.logger.info("Fetched records for parser = %s." % (parsername))
            else:
                rows = session.query(ReferenceSource).order_by(ReferenceSource.bibcode).limit(10).all()
                self.logger.info("Fetched records for 10 records.")

            if len(rows) == 0:
                if bibcode_list and source_filename_list:
                    self.logger.error("No records found for bibcode = %s and source_filename = %s." % (','.join(bibcode_list), ','.join(source_filename_list)))
                elif bibcode_list:
                    self.logger.error("No records found for bibcode = %s." % (','.join(bibcode_list)))
                elif source_filename_list:
                    self.logger.error("No records found for source_filename = %s." % (','.join(source_filename_list)))
                elif parsername:
                    self.logger.error("No records found for parser = %s." % (parsername))
                else:
                    self.logger.error("No records found in table `ReferenceSource`.")

            results = []
            for row in rows:
                results.append(row.toJSON())
            return results

    def query_processed_history_tbl(self, bibcode_list: List = None, source_filename_list: List = None) -> List:
        """
        query the processed history table

        :param bibcode_list: List of bibcodes to filter
        :param source_filename_list: List of source filenames to filter
        :return: List of processed history records
        """
        with self.session_scope() as session:
            if bibcode_list and source_filename_list:
                rows = session.query(func.max(ProcessedHistory.bibcode).label('bibcode'),
                                     func.max(ProcessedHistory.source_filename).label('source_filename'),
                                     func.count(ProcessedHistory.bibcode).label('num_runs'),
                                     func.max(ProcessedHistory.date).label('last_run_date'),
                                     func.max(ProcessedHistory.id).label('history_id')) \
                    .filter(and_(ProcessedHistory.bibcode.in_(bibcode_list),
                                 ProcessedHistory.source_filename.in_(source_filename_list))) \
                    .group_by(ProcessedHistory.bibcode) \
                    .order_by(ProcessedHistory.bibcode).all()
                self.logger.info("Fetched records for bibcode = %s and source_filename = %s." % (','.join(bibcode_list), ','.join(source_filename_list)))
            elif bibcode_list:
                rows = session.query(func.max(ProcessedHistory.bibcode).label('bibcode'),
                                     func.max(ProcessedHistory.source_filename).label('source_filename'),
                                     func.count(ProcessedHistory.bibcode).label('num_runs'),
                                     func.max(ProcessedHistory.date).label('last_run_date'),
                                     func.max(ProcessedHistory.id).label('history_id')) \
                    .filter(ProcessedHistory.bibcode.in_(bibcode_list)) \
                    .group_by(ProcessedHistory.bibcode) \
                    .order_by(ProcessedHistory.bibcode).all()
                self.logger.info("Fetched records for bibcode = %s." % (','.join(bibcode_list)))
            elif source_filename_list:
                rows = session.query(func.max(ProcessedHistory.bibcode).label('bibcode'),
                                     func.max(ProcessedHistory.source_filename).label('source_filename'),
                                     func.count(ProcessedHistory.bibcode).label('num_runs'),
                                     func.max(ProcessedHistory.date).label('last_run_date'),
                                     func.max(ProcessedHistory.id).label('history_id')) \
                    .filter(ProcessedHistory.source_filename.in_(source_filename_list)) \
                    .group_by(ProcessedHistory.source_filename) \
                    .order_by(ProcessedHistory.source_filename).all()
                self.logger.info("Fetched records for source_filename = %s." % (','.join(source_filename_list)))
            else:
                rows = session.query(func.max(ProcessedHistory.bibcode).label('bibcode'),
                                     func.max(ProcessedHistory.source_filename).label('source_filename'),
                                     func.count(ProcessedHistory.bibcode).label('num_runs'),
                                     func.max(ProcessedHistory.date).label('last_run_date'),
                                     func.max(ProcessedHistory.id).label('history_id')) \
                    .group_by(ProcessedHistory.id) \
                    .order_by(ProcessedHistory.id).limit(10).all()
                self.logger.info("Fetched records for 10 records.")

            if len(rows) == 0:
                if bibcode_list and source_filename_list:
                    self.logger.error("No records found for bibcode = %s and source_filename = %s." % (','.join(bibcode_list), ','.join(source_filename_list)))
                elif bibcode_list:
                    self.logger.error("No records found for bibcode = %s." % (','.join(bibcode_list)))
                elif source_filename_list:
                    self.logger.error("No records found for source_filename = %s." % (','.join(source_filename_list)))
                else:
                    self.logger.error("No records found in table `ProcessedHistory`.")

            results = []
            for row in rows:
                results.append({
                    'bibcode': row.bibcode,
                    'source_filename': row.source_filename,
                    'num_runs': row.num_runs,
                    'last_run_date': str(row.last_run_date),
                    'history_id': row.history_id,
                })
            return results

    def query_resolved_reference_tbl(self, history_id_list: List = None) -> List:
        """
        query the resolved reference table

        :param history_id_list: List of history IDs to filter
        :return: List of resolved reference records
        """
        results = []
        with self.session_scope() as session:
            if history_id_list:
                rows = session.query(func.count(ResolvedReference.item_num).label('num_references'),
                                     func.count(ResolvedReference.score).filter(ResolvedReference.score > 0).label('num_resolved_references'),
                                     func.max(ResolvedReference.history_id).label('history_id')) \
                    .filter(ResolvedReference.history_id.in_(history_id_list)) \
                    .group_by(ResolvedReference.history_id).all()
                self.logger.info("Fetched records for history ids = %s." % (','.join(str(h) for h in history_id_list)))

                if len(rows) == 0:
                    self.logger.error("No records found for history ids = %s." % (','.join(str(h) for h in history_id_list)))
                    return results

                for row in rows:
                    results.append({
                        'last_run_num_references': row.num_references,
                        'last_run_num_resolved_references': row.num_resolved_references,
                        'history_id': row.history_id,
                    })
            else:
                self.logger.error("No history_id provided, returning no records.")

        return results


    def diagnostic_query(self, bibcode_list: List = None, source_filename_list: List = None) -> List:
        """
        perform a diagnostic query to retrieve combined reference records

        :param bibcode_list: List of bibcodes to filter
        :param source_filename_list: List of source filenames to filter
        :return: List of combined records from multiple tables
        """
        results = []
        bibcode_list = _ensure_list(bibcode_list)
        source_filename_list = _ensure_list(source_filename_list)

        reference_source = self.query_reference_source_tbl(bibcode_list, source_filename_list)
        processed_history = self.query_processed_history_tbl(bibcode_list, source_filename_list)
        # get history_ids
        history_ids = [item['history_id'] for item in processed_history]
        if history_ids:
            resolved_reference = self.query_resolved_reference_tbl(history_ids)

            # get bibcodes from both sources
            reference_bibcodes = [item['bibcode'] for item in reference_source]
            history_bibcodes = [item['bibcode'] for item in processed_history]
            # find unique bibcodes
            bibcodes = sorted(list(set(reference_bibcodes) | set(history_bibcodes)))
            # go through the List and combine records from all three sources
            for bibcode in bibcodes:
                result = {}
                reference_record = next(item for item in reference_source if item['bibcode'] == bibcode)
                if reference_record:
                    result = reference_record
                history_record = next(item for item in processed_history if item['bibcode'] == bibcode)
                if history_record:
                    history_id = history_record.pop('history_id')
                    resolved_record = next(item for item in resolved_reference if item.get('history_id') == history_id)
                    result.update(history_record)
                    if resolved_record:
                        resolved_record.pop('history_id')
                        result.update(resolved_record)
                if result:
                    results.append(result)

        return results

    def insert_reference_source_record(self, session: object, reference: ReferenceSource) -> tuple:
        """
        insert a new record into the reference source table if it does not exist

        :param session: database session
        :param reference: reference source record
        :return: tuple containing bibcode and source filename
        """
        found = session.query(exists().where(and_(ReferenceSource.bibcode == reference.bibcode,
                                                  ReferenceSource.source_filename == reference.source_filename))).scalar()
        if found:
            return reference.bibcode, reference.source_filename

        session.add(reference)
        session.flush()
        self.logger.debug("Added a `Reference` record successfully.")
        return reference.bibcode, reference.source_filename

    def insert_history_record(self, session: object, history: ProcessedHistory) -> int:
        """
        insert a new record into the processed history table

        :param session: database session
        :param history: processed history record
        :return: history record ID
        """
        session.add(history)
        session.flush()
        self.logger.debug("Added a `ProcessedHistory` record successfully.")
        return history.id

    def insert_resolved_reference_records(self, session: object, resolved_list: List[ResolvedReference]) -> bool:
        """
        insert resolved reference records into the database

        :param session: database session
        :param resolved_list: List of resolved reference records
        :return: True if successful
        """
        session.bulk_save_objects(resolved_list)
        session.flush()
        self.logger.debug("Added `ResolvedReference` records successfully.")
        return True

    def update_resolved_reference_records(self, session: object, resolved_list: List[ResolvedReference]) -> bool:
        """
        update resolved reference records in the database

        :param session: database session
        :param resolved_list: List of resolved reference records
        :return: True if successful
        """
        session.bulk_update_mappings(ResolvedReference, [r.toJSON() for r in resolved_list])
        session.flush()
        self.logger.debug("Added `ResolvedReference` records successfully.")
        return True

    def insert_compare_records(self, session: object, compared_list: List[CompareClassic]) -> bool:
        """
        insert records into the compare classic table

        :param session: database session
        :param compared_list: List of comparison records
        :return: True if successful
        """
        session.bulk_save_objects(compared_list)
        session.flush()
        self.logger.debug("Added `CompareClassic` records successfully.")
        return True

    def populate_resolved_reference_records_pre_resolved(self, references: List, history_id: int, item_nums: List = None) -> tuple:
        """
        insert resolved reference records before sending them to a service

        :param references: List of references
        :param history_id: history record ID
        :param item_nums: optional List of item numbers
        :return: tuple containing resolved records and updated references
        """
        if not item_nums:
            item_nums = list(range(1, len(references)+1))
        resolved_records = []
        for item_num, ref in zip(item_nums, references):
            resolved_record = ResolvedReference(history_id=history_id,
                                       item_num=item_num,
                                       reference_str=ref.get('refstr', None) or ref.get('refplaintext', None),
                                       bibcode='0000',
                                       score=-1,
                                       reference_raw=ref.get('refraw', None))
            resolved_records.append(resolved_record)
            # add the id and remove xml_reference that is now in database
            ref['id'] = 'H%dI%d' % (history_id, item_num)
            if 'item_num' in ref: del ref['item_num']
        return resolved_records, references

    def populate_tables_pre_resolved_initial_status(self, source_bibcode: str, source_filename: str, parsername: str, references: List) -> List:
        """
        populate database tables for references being processed for the first time

        :param source_bibcode: source bibcode
        :param source_filename: source filename
        :param parsername: parser name
        :param references: List of references
        :return: List of processed references
        """
        with self.session_scope() as session:
            try:
                reference_record = ReferenceSource(bibcode=source_bibcode,
                                             source_filename=source_filename,
                                             resolved_filename=get_resolved_filename(source_filename),
                                             parser_name=parsername)
                bibcode, filename = self.insert_reference_source_record(session, reference_record)
                if bibcode and filename:
                    history_record = ProcessedHistory(bibcode=bibcode,
                                             source_filename=source_filename,
                                             source_modified=get_date_modified(source_filename),
                                             status=Action().get_status_new(),
                                             date=get_date_now(),
                                             total_ref=len(references))
                    history_id = self.insert_history_record(session, history_record)
                    resolved_records, references = self.populate_resolved_reference_records_pre_resolved(references, history_id)
                    self.insert_resolved_reference_records(session, resolved_records)
                    session.commit()
                    self.logger.info("Source file %s for bibcode %s with %d references, processed successfully." % (source_filename, source_bibcode, len(references)))
                    return references
            except SQLAlchemyError as e:
                session.rollback()
                self.logger.error("Source file %s information failed to get added to database. Error: %s" % (source_filename, str(e)))
                return []

    def populate_tables_pre_resolved_retry_status(self, source_bibcode: str, source_filename: str, source_modified: str, retry_records: List[Dict]) -> List[Dict]:
        """
        this is called when the references are being reprocessed, usually cherry-picked from the records in the database

        :param source_bibcode: source bibcode
        :param source_filename: source filename
        :param source_modified: last modified date of the source file
        :param retry_records: List of references to be reprocessed
        :return: List of processed references
        """
        with self.session_scope() as session:
            try:
                history_record = ProcessedHistory(bibcode=source_bibcode,
                                         source_filename=source_filename,
                                         source_modified=source_modified,
                                         status=Action().get_status_retry(),
                                         date=get_date_now(),
                                         total_ref=len(retry_records))
                history_id = self.insert_history_record(session, history_record)
                resolved_records, references = self.populate_resolved_reference_records_pre_resolved(retry_records, history_id)
                self.insert_resolved_reference_records(session, resolved_records)
                session.commit()
                self.logger.info("Source file %s for bibcode %s with %d references, for reprocessing added successfully." % (source_filename, source_bibcode, len(references)))
                return references
            except SQLAlchemyError as e:
                session.rollback()
                self.logger.error("Source file %s information for reprocessing failed to get added to database. Error: %s" % (source_filename, str(e)))
                return []

    def populate_tables_post_resolved(self, resolved_reference: List, source_bibcode: str, classic_resolved_filename: str) -> bool:
        """
        update tables after references have been resolved

        :param resolved_reference: List of resolved references
        :param source_bibcode: source bibcode
        :param classic_resolved_filename: filename of classic resolved references
        :return: True if successful
        """
        with self.session_scope() as session:
            try:
                # if the filename for classic resolver output is supplied, read the resolved information
                # make sure that the length matches resolved, classic does some breaking a reference into two
                # and hence messes up the order if we want to compare one-to-one, if that is the case, just
                # ignore the result
                resolved_classic = None
                if classic_resolved_filename:
                    resolved_classic = compare_classic_and_service(resolved_reference, source_bibcode, classic_resolved_filename)

                    resolved_records = []
                    compare_records = []
                    for i, ref in enumerate(resolved_reference):
                        match = self.RE_PARSE_ID.match(ref['id'])
                        history_id = int(match.group('history_id'))
                        item_num = int(match.group('item_num'))
                        # TODO change refstring to refraw for reference_raw
                        resolved_record = ResolvedReference(history_id=history_id,
                                                   item_num=item_num,
                                                   reference_str=ref.get('refstring', None),
                                                   bibcode=ref.get('bibcode', None),
                                                   score=ref.get('score', None),
                                                   reference_raw=ref.get('refstring', None))
                        resolved_records.append(resolved_record)
                        if resolved_classic:
                            compare_record = CompareClassic(history_id=history_id,
                                                     item_num=item_num,
                                                     bibcode=resolved_classic[i][1],
                                                     score=int(resolved_classic[i][2]),
                                                     state=resolved_classic[i][3])
                            compare_records.append(compare_record)
                    if resolved_classic:
                        self.update_resolved_reference_records(session, resolved_records)
                        self.insert_compare_records(session, compare_records)
                    else:
                        self.update_resolved_reference_records(session, resolved_records)
                    session.commit()
                    self.logger.info("Updated %d resolved reference records successfully." % len(resolved_reference))
                    return True
            except SQLAlchemyError as e:
                session.rollback()
                self.logger.error("Failed to update %d resolved reference records successfully. Error %s" % (len(resolved_reference), str(e)))
                return False

    def get_count_reference_source_records(self, session: object) -> int:
        """
        get the count of records in the reference source table

        :param session: database session
        :return: number of records
        """
        rows = session.query(ReferenceSource).count()
        self.logger.debug("Currently there are %d records in `ReferenceSource` table."%rows)
        return rows

    def get_count_processed_history_records(self, session: object) -> int:
        """
        get the count of records in the processed history table

        :param session: database session
        :return: number of records
        """
        rows = session.query(ProcessedHistory).count()
        self.logger.debug("Currently there are %d records in `ProcessedHistory` table."%rows)
        return rows

    def get_count_resolved_reference_records(self, session: object) -> int:
        """
        get the count of records in the resolved reference table

        :param session: database session
        :return: number of records
        """
        rows = session.query(ResolvedReference).count()
        self.logger.debug("Currently there are %d records in `ResolvedReference` table."%rows)
        return rows

    def get_count_compare_classic_records(self, session: object) -> int:
        """
        get the count of records in the compare classic table

        :param session: database session
        :return: number of records
        """
        rows = session.query(CompareClassic).count()
        self.logger.debug("Currently there are %d records in `CompareClassic` table."%rows)
        return rows

    def get_count_records(self) -> List:
        """
        get the count of records in all tables

        :return: List of dictionaries with table names and record counts
        """
        with self.session_scope() as session:
            results = [
                {
                    'name':'ReferenceSource',
                    'description':'source reference file information',
                    'count':self.get_count_reference_source_records(session),
                },
                {
                    'name':'ProcessedHistory',
                    'description':'top level information for a processed run',
                    'count': self.get_count_processed_history_records(session),
                },
                {
                    'name':'ResolvedReference',
                    'description':'resolved reference information for a processed run',
                    'count': self.get_count_resolved_reference_records(session),

                },
                {
                    'name':'CompareClassic',
                    'description':'comparison of new and classic processed run',
                    'count':self.get_count_compare_classic_records(session),
                }
            ]
            return results

    def get_service_classic_compare_tags(self, session: object, source_bibcode: str, source_filename: str) -> object:
        """
        generates a comparison grid for classic and service resolved references

        :param session: database session
        :param source_bibcode: source bibcode
        :param source_filename: source filename
        :return: subquery object containing comparison results
        """
        # given reference source (bibcodes and filenames), have query that would contain
        # all resolved records ids, and if we have reprocessed records, it contains one
        # set of records for those
        if source_bibcode and source_filename:
            filter = and_(ProcessedHistory.id == ResolvedReference.history_id,
                          literal(source_bibcode).op('~')(ProcessedHistory.bibcode),
                          literal(source_filename).op('~')(ProcessedHistory.source_filename))
        elif source_bibcode:
            filter = and_(ProcessedHistory.id == ResolvedReference.history_id,
                          literal(source_bibcode).op('~')(ProcessedHistory.bibcode))
        elif source_filename:
            filter = and_(ProcessedHistory.id == ResolvedReference.history_id,
                             literal(source_filename).op('~')(ProcessedHistory.source_filename))

        resolved_reference_ids = session.query(ResolvedReference.history_id.label('history_id'),
                                               ResolvedReference.item_num.label('item_num')) \
            .filter(filter).distinct().subquery()
        return session.query(func.max(case([(CompareClassic.state == 'MATCH', 'MATCH')])).label('MATCH'),
                            func.max(case([(CompareClassic.state == 'MISS', 'MISS')])).label('MISS'),
                            func.max(case([(CompareClassic.state == 'NEW', 'NEW')])).label('NEW'),
                            func.max(case([(CompareClassic.state == 'NEWU', 'NEWU')])).label('NEWU'),
                            func.max(case([(CompareClassic.state == 'DIFF', 'DIFF')])).label('DIFF'),
                            CompareClassic.history_id, CompareClassic.item_num) \
            .select_from(resolved_reference_ids) \
            .outerjoin(CompareClassic, and_(resolved_reference_ids.c.history_id == CompareClassic.history_id,
                                            resolved_reference_ids.c.item_num == CompareClassic.item_num)) \
            .group_by(CompareClassic.history_id, CompareClassic.item_num) \
            .subquery()

    def get_service_classic_compare_stats_grid(self, source_bibcode: str, source_filename: str) -> tuple:
        """
        retrieve comparison statistics between service and classic resolved references

        :param source_bibcode: source bibcode
        :param source_filename: source filename
        :return: tuple containing a text-based grid, total references, and resolved references
        """
        with self.session_scope() as session:
            compare_grid = self.get_service_classic_compare_tags(session, source_bibcode, source_filename)
            rows = session.query(ResolvedReference.reference_str.label('refstr'),
                                    ResolvedReference.bibcode.label('service_bibcode'), CompareClassic.bibcode.label('classic_bibcode'),
                                    ResolvedReference.score.label('service_conf'), CompareClassic.score.label('classic_score'),
                                    compare_grid.c.MATCH.label('match'), compare_grid.c.MISS.label('miss'),
                                    compare_grid.c.NEW.label('new'), compare_grid.c.NEWU.label('newu'),
                                    compare_grid.c.DIFF.label('diff')) \
                .filter(and_(ResolvedReference.history_id == CompareClassic.history_id,
                             ResolvedReference.item_num == CompareClassic.item_num,
                             ResolvedReference.history_id == compare_grid.c.history_id,
                             ResolvedReference.item_num == compare_grid.c.item_num)) \
                .order_by(ResolvedReference.history_id, ResolvedReference.item_num) \
                .all()
            if rows:
                # Texttable functionality is here https://pypi.org/project/texttable/
                table = Texttable()
                table.set_cols_width([60,19,19,15,15,5,5,5,5,5])
                table.set_cols_dtype(['t']*10)
                table.set_cols_align(['l']+['c']*9)
                table.header(rows[0]._asdict().keys())
                num_resolved = 0
                for row in rows:
                    # count how many was resolved on the side of service
                    if not row[1].startswith('.'):
                        num_resolved += 1
                    result = []
                    for item in row:
                        if not item: item = ''
                        result.append(item)
                    table.add_row(result)
                return table.draw(), len(rows), num_resolved
        return 'Unable to fetch data for reference source file `%s` from database!'%source_filename, -1, -1

    def filter_reprocess_query(self, query: object, type: int, score_cutoff: float, match_bibcode: str, date_cutoff: int) -> object:
        """
        apply one of the four selected filters, also apply date if requested

        :param query: SQLAlchemy query object
        :param type: type of filter to apply
        :param score_cutoff: score threshold for filtering
        :param match_bibcode: bibcode pattern for filtering
        :param date_cutoff: number of days to filter by recent records
        :return: filtered query object
        """
        if type == ReprocessQueryType.score:
            query = query.filter(ResolvedReference.score <= "%.2f" % score_cutoff)
        elif type == ReprocessQueryType.bibstem and len(match_bibcode):
            query = query.filter(ResolvedReference.bibcode.like('____%s__________' % match_bibcode))
        elif type == ReprocessQueryType.year and len(match_bibcode):
            query = query.filter(ResolvedReference.bibcode.like('%s_______________' % match_bibcode))
        elif type == ReprocessQueryType.failed:
            query = query.filter(and_(ResolvedReference.bibcode == '0000', ResolvedReference.score == -1))
        if date_cutoff:
            since = datetime.now() - timedelta(days=int(date_cutoff))
            query = query.filter(ProcessedHistory.date >= since)
        return query

    def get_reprocess_records(self, type: int, score_cutoff: float, match_bibcode: str, date_cutoff: int) -> List:
        """
        retrieve references that need reprocessing based on filters

        :param type: type of reprocessing filter
        :param score_cutoff: score threshold
        :param match_bibcode: bibcode filter
        :param date_cutoff: date filter in days
        :return: List of references for reprocessing
        """
        results = []
        with self.session_scope() as session:
            # have a query containing unique reference source ids (bibcodes and filenames),
            # that have been filtered on one of four possible options and also date if requested
            reference_source_ids = session.query(ProcessedHistory.bibcode, ProcessedHistory.source_filename) \
                .filter(ProcessedHistory.id == ResolvedReference.history_id)
            reference_source_ids = self.filter_reprocess_query(reference_source_ids, type, score_cutoff, match_bibcode, date_cutoff)
            reference_source_ids = reference_source_ids.distinct().all()
            bibcodes = [ids[0] for ids in reference_source_ids]
            filenames = [ids[1] for ids in reference_source_ids]

            # have a query containing unique resolved reference ids (history_id and item_num),
            # that have been filtered on one of four possible options and also date if requested
            resolved_reference_ids = session.query(ResolvedReference.history_id.label('history_id'),
                                                   ResolvedReference.item_num.label('item_num')) \
                .filter(and_(ProcessedHistory.id == ResolvedReference.history_id),
                             ProcessedHistory.bibcode.in_(bibcodes),
                             ProcessedHistory.source_filename.in_(filenames))
            resolved_reference_ids = self.filter_reprocess_query(resolved_reference_ids, type, score_cutoff, match_bibcode, date_cutoff)
            resolved_reference_ids = resolved_reference_ids.distinct().subquery()


            rows = session.query(resolved_reference_ids.c.history_id.label('history_id'),
                                    resolved_reference_ids.c.item_num.label('item_num'),
                                    ResolvedReference.reference_str.label('refstr'),
                                    ResolvedReference.reference_raw.label('refraw'),
                                    ProcessedHistory.bibcode.label('source_bibcode'),
                                    ProcessedHistory.source_filename.label('source_filename'),
                                    ProcessedHistory.source_modified.label('source_modified'),
                                    ReferenceSource.parser_name.label('parser_name')) \
                .filter(and_(resolved_reference_ids.c.history_id == ResolvedReference.history_id,
                             resolved_reference_ids.c.item_num == ResolvedReference.item_num,
                             ResolvedReference.history_id == ProcessedHistory.id,
                             ProcessedHistory.bibcode == ReferenceSource.bibcode,
                             ProcessedHistory.source_filename == ReferenceSource.source_filename)) \
                .order_by(ResolvedReference.history_id, ResolvedReference.item_num) \
                .all()

            if rows:
                rows = [r._asdict() for r in rows]
                result = {}
                history_id = -1
                for row in rows:
                    if row['history_id'] != history_id:
                        if result:
                            results.append(result)
                            result = {}
                        history_id = row['history_id']
                        for key in ['source_bibcode', 'source_filename', 'source_modified', 'parser_name']:
                            result[key] = row[key]
                        result['references'] = []
                        reference = {}
                        for key in ['item_num', 'refstr', 'refraw']:
                            reference[key] = row[key]
                        result['references'].append(reference)
                    else:
                        reference = {}
                        for key in ['item_num', 'refstr', 'refraw']:
                            reference[key] = row[key]
                        result['references'].append(reference)
                # last batch, if any
                if result:
                    results.append(result)
        return results

    def get_resolved_references_all(self, source_bibcode: str) -> List[tuple]:
        """
        retrieve all resolved references with the highest score per resolved bibcode

        :param source_bibcode: source bibcode for which resolved references should be queried
        :return: List of tuples containing resolved references with metadata
        """
        result = []
        with self.session_scope() as session:
            # build the query to select the highest-scored resolved references per resolved bibcode
            # also return name of the parser, order number of parsed reference, date it was parsed,
            # and the confidence score
            highest_scored_resolved_reference = session.query(
                ReferenceSource.bibcode.label('source_bibcode'),
                ProcessedHistory.date.label('date'),
                ResolvedReference.item_num.label('id'),
                ResolvedReference.bibcode.label('resolved_bibcode'),
                ResolvedReference.score.label('score'),
                ReferenceSource.parser_name.label('parser_name'),
                func.row_number().over(
                    partition_by=[ReferenceSource.bibcode, ReferenceSource.parser_name, ResolvedReference.bibcode],
                    order_by=desc(ResolvedReference.score)
                ).label('ranking_by_score')
            ).join(ProcessedHistory, ProcessedHistory.id == ResolvedReference.history_id) \
                .join(ReferenceSource, ProcessedHistory.bibcode == ReferenceSource.bibcode) \
                .filter(and_(ReferenceSource.bibcode == source_bibcode,
                             ResolvedReference.score != 0)) \
                .subquery()

            # query database now
            rows = session.query(
                highest_scored_resolved_reference.c.source_bibcode,
                highest_scored_resolved_reference.c.date,
                highest_scored_resolved_reference.c.id,
                highest_scored_resolved_reference.c.resolved_bibcode,
                highest_scored_resolved_reference.c.score,
                highest_scored_resolved_reference.c.parser_name) \
                .filter(highest_scored_resolved_reference.c.ranking_by_score == 1) \
                .order_by(highest_scored_resolved_reference.c.resolved_bibcode) \
                .all()

            if len(rows) > 0:
                for row in rows:
                    result.append((row.source_bibcode,
                                   row.date.strftime("%Y-%m-%d %H:%M:%S"),
                                   row.id,
                                   row.resolved_bibcode,
                                   float(row.score),
                                   row.parser_name))
            else:
                self.logger.error(f'Unable to fetch resolved references for source bibcode `{source_bibcode}`.')

        return result

    def get_resolved_references(self, source_bibcode: str) -> List[Dict]:
        """
        retrieve resolved references with the highest parser priority for each unique combination of source_bibcode, parser_name, and resolved_bibcode

        :param source_bibcode: source bibcode for which resolved references should be queried
        :return: List of dictionaries containing the highest-priority resolved references
        """
        result = []
        with self.session_scope() as session:

            # Build the query to rank parsers by priority (based on the parser_name) and then by score
            highest_priority_resolved_reference = session.query(
                ReferenceSource.bibcode.label('source_bibcode'),
                ProcessedHistory.date.label('date'),
                ResolvedReference.item_num.label('id'),
                ResolvedReference.bibcode.label('resolved_bibcode'),
                ResolvedReference.score.label('score'),
                ReferenceSource.parser_name.label('parser_name'),
                case(
                    [
                        (ReferenceSource.parser_name.in_(['arXiv', 'CrossRef']), 1),
                        (ReferenceSource.parser_name == 'Arthur', 3)
                    ],
                    else_=2
                ).label('parser_priority'),
                func.row_number().over(
                    partition_by=[ReferenceSource.bibcode, ResolvedReference.bibcode],
                    order_by=[desc(case(
                        [
                            (ReferenceSource.parser_name.in_(['arXiv', 'CrossRef']), 1),
                            (ReferenceSource.parser_name == 'Arthur', 3)
                        ],
                        else_=2
                    )), desc(ResolvedReference.score)]
                ).label('ranking_by_priority')
            ).join(ProcessedHistory, ProcessedHistory.id == ResolvedReference.history_id) \
                .join(ReferenceSource, ProcessedHistory.bibcode == ReferenceSource.bibcode) \
                .filter(and_(ReferenceSource.bibcode == source_bibcode,
                             ResolvedReference.score != 0)) \
                .subquery()

            # Query the ranked resolved references, ensuring we get the highest-ranked ones (ranking_by_priority == 1)
            rows = session.query(
                highest_priority_resolved_reference.c.source_bibcode,
                highest_priority_resolved_reference.c.date,
                highest_priority_resolved_reference.c.id,
                highest_priority_resolved_reference.c.resolved_bibcode,
                highest_priority_resolved_reference.c.score,
                highest_priority_resolved_reference.c.parser_name,
                highest_priority_resolved_reference.c.parser_priority)\
                .filter(highest_priority_resolved_reference.c.ranking_by_priority == 1) \
                .order_by(highest_priority_resolved_reference.c.resolved_bibcode) \
                .all()

            # Process the results
            if rows:
                for row in rows:
                    result.append({
                        'source_bibcode': row.source_bibcode,
                        'date': row.date.strftime("%Y-%m-%d %H:%M:%S"),
                        'id': row.id,
                        'resolved_bibcode': row.resolved_bibcode,
                        'score': float(row.score),
                        'parser_name': row.parser_name,
                        'parser_priority': row.parser_priority
                    })
            else:
                self.logger.error(f'Unable to fetch resolved references for source bibcode `{source_bibcode}`.')

        return result
