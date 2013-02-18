#!/usr/bin/env python
'''
@author Luke Campbell <LCampbell@ASAScience.com>
@file ion/processes/data/ingestion/science_granule_ingestion_worker.py
@date 06/26/12 11:38
@description Ingestion Process
'''
from pyon.core.exception import CorruptionError
from pyon.public import log
from ion.processes.data.ingestion.stream_coverage import StreamCoverageReader
from ion.services.dm.utility.granule.record_dictionary import RecordDictionaryTool
from pyon.event.event import handle_stream_exception
import time

class ScienceGranuleIngestionWorker(StreamCoverageReader):

    def __init__(self, *args,**kwargs):
        super(ScienceGranuleIngestionWorker, self).__init__(*args, **kwargs)
    
    @handle_stream_exception()
    def recv_packet(self, msg, stream_route, stream_id):
        if msg == {}:
            log.error('Received empty message from stream: %s', stream_id)
            return
        coverage = self.sc.load_coverage(stream_id)    
        rdt = RecordDictionaryTool.load_from_granule(msg)
        if rdt is None:
            return 
        elements = len(rdt)
        if not elements:
            return
        try:
            coverage.insert_timesteps(elements, oob=False)
        except IOError as e:
            log.error("Couldn't insert time steps for coverage: %s" % self.sc.get_coverage_path())
            log.exception('IOError')
            try:
                coverage.close()
            finally:
                self.sc.mark_bad_coverage(coverage.name)
                raise CorruptionError(e.message)
        
        
        start_index = coverage.num_timesteps - elements
        slice_ = slice(start_index, None)
        now = time.time() + 2208988800
        coverage.set_parameter_values(param_name="ingestion_timestamp", tdoa=slice_, value=now)
        for k,v in rdt.iteritems():
            try:
                coverage.set_parameter_values(param_name=k, tdoa=slice_, value=v)
            except IOError as e:
                log.error("Couldn't insert values for coverage: %s" % self.sc.get_coverage_path())
                log.exception('IOError')
                try:
                    coverage.close()
                finally:
                    self.sc.mark_bad_coverage(coverage.name)
                    raise CorruptionError(e.message)
            coverage.flush()

