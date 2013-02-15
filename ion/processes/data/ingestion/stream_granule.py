from pyon.util.file_sys import FileSystem, FS
from pyon.public import log
from pyon.core.exception import CorruptionError
from interface.objects import Granule
from ion.processes.data.ingestion.stream_coverage import StreamCoverageReader, StreamCoverageWriter
import time

class StreamGranuleReader(StreamCoverageReader):
    def __init__(self, *args, **kwargs):
        super(StreamGranuleReader, self).__init__(*args, **kwargs)

    def process_data(self, coverage, msg):
        timesteps = 1
        try:
            coverage.insert_timesteps(timesteps, oob=False)
        except IOError as e:
            log.error("Couldn't insert time steps for coverage: %s" % FileSystem.get_url(FS.CACHE,'datasets'))
            log.exception('IOError')
            try:
                coverage.close()
            finally:
                self.sc.mark_bad_coverage(coverage.id)
                raise CorruptionError(e.message)
        
        granule_meta,granule = msg
        if not isinstance(granule, Granule):
            log.error('received a message that is not a granule. %s' % msg)
            return

        now = time.time() + 2208988800
        start_index = coverage.num_timesteps - 1
        slice_ = slice(start_index, None)
        try:
            coverage.set_parameter_values(param_name="time", tdoa=slice_, value=now)
            coverage.set_parameter_values(param_name='granule', tdoa=slice_, value=granule.__dict__)
            coverage.set_parameter_values(param_name='granule_meta', tdoa=slice_, value=granule_meta)
        except IOError as e:
            log.error("Couldn't insert values for coverage: %s" % FileSystem.get_url(FS.CACHE,'datasets'))
            log.exception('IOError')
            try:
                coverage.close()
            finally:
                self.sc.mark_bad_coverage(coverage.id)
                raise CorruptionError(e.message)
    
class StreamGranuleWriter(StreamCoverageWriter):
    
    def publish(self, msg, to=''):
        self.publisher.publish(msg)

    def write_data(self, stream_id, slice_):
        
        coverage = self.sc.load_coverage(stream_id)
         
        granules = coverage.get_parameter_values('granule', tdoa=slice_)
        gmetas = coverage.get_parameter_values('granule_meta', tdoa=slice_)
        
        if isinstance(granules, dict):
            granules = [granules]
        
        if isinstance(gmetas, dict):
            gmetas = [gmetas]

        for gmeta,granule in zip(gmetas, granules):
            self.publish((gmeta, granule))

