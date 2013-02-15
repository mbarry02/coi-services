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

class ScienceGranuleIngestionWorker(StreamCoverageReader):

    def __init__(self, *args,**kwargs):
        super(ScienceGranuleIngestionWorker, self).__init__(*args, **kwargs)
    
    def process_data(self, coverage, msg):
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
        for k,v in rdt.iteritems():
            slice_ = slice(start_index, None)
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

