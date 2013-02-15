from pyon.ion.process import SimpleProcess
from pyon.ion.stream import StreamSubscriber, StreamPublisher
from pyon.util.file_sys import FileSystem, FS
from pyon.public import log, RT, PRED, CFG
from pyon.core.exception import CorruptionError
from ion.services.dm.utility.granule_utils import SimplexCoverage
from pyon.event.event import handle_stream_exception
from interface.objects import Granule
import collections
import time
    

class StreamGranuleCache(object):
    CACHE_LIMIT=CFG.get_safe('container.ingestion_cache',5)
    
    def __init__(self, container):
        self.container = container
        self._datasets  = collections.OrderedDict()
        self._coverages = collections.OrderedDict()
        self._bad_coverages = {}

    def _new_dataset(self, stream_id):
        datasets, _  = self.container.resource_registry.find_subjects(subject_type=RT.DataSet,predicate=PRED.hasStream,object=stream_id,id_only=True)
        if datasets:
            return datasets[0]
        return None

    def get_dataset(self,stream_id):
        try:
            result = self._datasets.pop(stream_id)
        except KeyError:
            result = self._new_dataset(stream_id)
            if result is None:
                return None
            if len(self._datasets) >= self.CACHE_LIMIT:
                self._datasets.popitem(0)
        self._datasets[stream_id] = result
        return result

    def get_coverage(self, stream_id):
        try:
            result = self._coverages.pop(stream_id)
        except KeyError:
            dataset_id = self.get_dataset(stream_id)
            if dataset_id is None:
                return None
            file_root = FileSystem.get_url(FS.CACHE,'datasets')
            result = SimplexCoverage(file_root, dataset_id, mode='w')
            if result is None:
                return None
            if len(self._coverages) >= self.CACHE_LIMIT:
                k, coverage = self._coverages.popitem(0)
                coverage.close(timeout=5)
        self._coverages[stream_id] = result
        return result

class StreamGranuleReader(SimpleProcess):
    """
    reads from a stream and writes to a coverage
    """  
    
    def on_start(self):
        self.sc = StreamGranuleCache(self.container)
        self.queue_name = self.CFG.get_safe('process.queue_name',self.id)
        self.subscriber = StreamSubscriber(process=self, exchange_name=self.queue_name, callback=self.recv_granule)
        self.subscriber.start()
    
    def get_coverage(self, stream_id):
        return self.sc.get_coverage(stream_id)

    @handle_stream_exception()
    def recv_granule(self, msg, stream_route, stream_id):
        if msg == {}:
            log.error('Received empty message from stream: %s', stream_id)
            return
        if not isinstance(msg[1], Granule):
            log.error('received a message that is not a granule. %s' % msg)
            return
        self.add_granule(stream_id, msg)


    def add_granule(self, stream_id, granule_data):
        '''
        Appends the granule's data to the coverage and persists it.
        '''
        if stream_id in self.sc._bad_coverages:
            log.info('Message attempting to be inserted into bad coverage: %s' % FileSystem.get_url(FS.CACHE,'datasets'))
            
        #--------------------------------------------------------------------------------
        # Coverage determiniation and appending
        #--------------------------------------------------------------------------------
        dataset_id = self.sc.get_dataset(stream_id)
        if not dataset_id:
            log.error('No dataset could be determined on this stream: %s', stream_id)
            return
        try:
            coverage = self.sc.get_coverage(stream_id)
        except IOError as e:
            log.error("Couldn't open coverage: %s" % FileSystem.get_url(FS.CACHE,'datasets'))
            log.exception('IOError')
            raise CorruptionError(e.message)
        
        if not coverage:
            log.error('Could not persist coverage from granule, coverage is None')
            return
        #--------------------------------------------------------------------------------
        # Actual persistence
        #-------------------------------------------------------------------------------- 

        try:
            coverage.insert_timesteps(1, oob=False)
        except IOError as e:
            log.error("Couldn't insert time steps for coverage: %s" % FileSystem.get_url(FS.CACHE,'datasets'))
            log.exception('IOError')
            try:
                coverage.close()
            finally:
                self.sc._bad_coverages[stream_id] = 1
                raise CorruptionError(e.message)
        
        now = time.time() + 2208988800
        start_index = coverage.num_timesteps - 1
        slice_ = slice(start_index, None)
        granule_meta,granule = granule_data
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
                self._bad_coverages[stream_id] = 1
                raise CorruptionError(e.message)

    def on_quit(self):
        self.subscriber.stop()

class StreamGranuleWriter(SimpleProcess):
    #reads from a coverage and writes to a stream
    
    def on_start(self):
        self.sc = StreamGranuleCache(self.container)
        stream_id = self.CFG.get_safe('process.stream_id', None)
        self.publisher = StreamPublisher(process=self, stream_id=stream_id)
    
    def get_coverage(self, stream_id):
        return self.sc.get_coverage(stream_id)

    def write_granule(self, stream_id, slice_):
        
        if self.sc is None:
            log.error("cache not created")
            return
        
        if stream_id in self.sc._bad_coverages:
            log.info('Message attempting to be inserted into bad coverage: %s' % FileSystem.get_url(FS.CACHE,'datasets'))
        
        dataset_id = self.sc.get_dataset(stream_id)
        if not dataset_id:
            log.error('No dataset could be determined on this stream: %s', stream_id)
            return
        try:
            coverage = self.sc.get_coverage(stream_id)
        except IOError as e:
            log.error("Couldn't open coverage: %s" % FileSystem.get_url(FS.CACHE,'datasets'))
            log.exception('IOError')
            raise CorruptionError(e.message)
        
        if not coverage:
            log.error('Could not persist coverage from granule, coverage is None')
            return
         
        granules = coverage.get_parameter_values('granule', tdoa=slice_)
        gmetas = coverage.get_parameter_values('granule_meta', tdoa=slice_)
        
        if isinstance(granules, dict):
            granules = [granules]
        
        if isinstance(gmetas, dict):
            gmetas = [gmetas]

        for gmeta,granule in zip(gmetas, granules):
            self.publisher.publish((gmeta, granule))
