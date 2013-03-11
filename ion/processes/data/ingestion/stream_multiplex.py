from ion.services.dm.utility.granule.record_dictionary import RecordDictionaryTool
from ion.core.process.transform import TransformMultiStreamListener
from interface.services.dm.ipubsub_management_service import PubsubManagementServiceProcessClient
from coverage_model import ParameterDictionary
from pyon.util.memoize import memoize_lru
from interface.objects import Granule
from pyon.public import log
from pyon.event.event import handle_stream_exception
import sys
from pprint import pprint

class StreamMultiplex(TransformMultiStreamListener):
    
    def __init__(self):
        TransformMultiStreamListener.__init__(self)
        self.granules = {}
        self.received = []
    
    def on_start(self):
        TransformMultiStreamListener.on_start(self)
        self.pubsub_management = PubsubManagementServiceProcessClient(process=self)
    
    @memoize_lru(maxsize=100)
    def _read_stream_def(self, stream_id):
        return self.pubsub_management.read_stream_definition(stream_id=stream_id)
    
    def _get_time(self, stream_id):
        stream_def_in = self._read_stream_def(stream_id)
        incoming_pdict_dump = stream_def_in.parameter_dictionary
        incoming_pdict = ParameterDictionary.load(incoming_pdict_dump)
        pprint(incoming_pdict)

    @handle_stream_exception()
    def recv_packet(self, msg, stream_route, stream_id):
        if msg == {}:
            log.error('Received empty message from stream: %s', stream_id)
            return
        if not isinstance(msg, Granule):
            log.error('Received a message that is not a granule. %s' % msg)
            return
        rdt = RecordDictionaryTool.load_from_granule(msg)
        print >> sys.stderr, stream_id, stream_route
        #pprint(rdt, stream=sys.stderr)      
        self._get_time(stream_id)
        #find all stream ids that we need to send along combined data

        #find the axis parameter time
        #look up stream defs pdict and find axis parameter key for this stream id
        
        if stream_id not in self.received:
            self.received.append(stream_id)

        return rdt.to_granule()
    
    def publish(self, msg, stream_out_id):
        publisher = getattr(self, stream_out_id)
        input_streams = self.CFG.get_safe('process.input_streams', [])
        if not self.received or not input_streams:
            return
        #if we have received a granule for each incoming stream then publish the new granule
        if set(self.received) == set(input_streams):
            publisher.publish(msg)
            self.received = [] 
