from pyon.public import log, RT, PRED, CFG
from pyon.core.exception import CorruptionError
from ion.core.process.transform import TransformStreamListener, TransformStreamPublisher
from ion.services.dm.inventory.dataset_management_service import DatasetManagementService
from pyon.container.cc import Container
import collections

class StreamCoverageCache(object):
    CACHE_LIMIT=CFG.get_safe('container.ingestion_cache',5)
    
    def __init__(self, container):
        self.container = Container.instance
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
            result = DatasetManagementService._get_coverage(dataset_id)
            if result is None:
                return None
            if len(self._coverages) >= self.CACHE_LIMIT:
                k, coverage = self._coverages.popitem(0)
                coverage.close(timeout=5)
        self._coverages[stream_id] = result
        return result
    
    def get_coverage_path(self, did):
        return DatasetManagementService._get_coverage_path(did)

    def mark_bad_coverage(self, cname):
        self._bad_coverages[cname] = 1

    def load_coverage(self, stream_id):
        
        dataset_id = self.get_dataset(stream_id)
        if not dataset_id:
            log.error('No dataset could be determined on this stream: %s', stream_id)
            return
        try:
            coverage = self.get_coverage(stream_id)
        except IOError as e:
            log.error("Couldn't open coverage: %s" % self.get_coverage_path(dataset_id))
            log.exception('IOError')
            raise CorruptionError(e.message)
        

        if not coverage:
            log.error('Could not persist coverage from granule, coverage is None')
            return
        
        if coverage.name in self._bad_coverages:
            log.info('loading bad coverage: %s' % self.get_coverage_path(dataset_id))
        return coverage

class StreamCoverageReader(TransformStreamListener):
    """
    reads from a stream and writes to a coverage
    """  
    
    def on_start(self):
        TransformStreamListener.on_start(self)
        self.sc = StreamCoverageCache(self.container)

    def get_coverage(self, stream_id):
        return self.sc.get_coverage(stream_id)
    
    def on_quit(self):
        TransformStreamListener.on_quit(self)
        self.sc.close()

class StreamCoverageWriter(TransformStreamPublisher):
    #reads from a coverage and writes to a stream
    def on_start(self):
        TransformStreamPublisher.on_start(self)
        self.sc = StreamCoverageCache(self.container)

    def get_coverage(self, stream_id):
        return self.sc.get_coverage(stream_id)
    
    def on_quit(self):
        TransformStreamPublisher.on_quit(self)
        self.sc.close()

