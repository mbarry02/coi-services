from ion.services.dm.utility.granule.record_dictionary import RecordDictionaryTool
from ion.core.process.transform import TransformMultiStreamListener
from interface.services.dm.ipubsub_management_service import PubsubManagementServiceProcessClient
from coverage_model import ParameterDictionary,ParameterContext
from pyon.util.memoize import memoize_lru
from interface.objects import Granule
from pyon.public import log
from coverage_model.parameter_types import QuantityType
from coverage_model.utils import find_nearest_index
import numpy as np
import sys
#import time
import math

class StreamMultiplex(TransformMultiStreamListener):
    
    def __init__(self):
        TransformMultiStreamListener.__init__(self)
        self.storage = {}
        self.received = []

    def on_start(self):
        TransformMultiStreamListener.on_start(self)
        self.pubsub_management = PubsubManagementServiceProcessClient(process=self)
        
    @memoize_lru(maxsize=100)
    def _read_stream_def(self, stream_id):
        return self.pubsub_management.read_stream_definition(stream_id=stream_id)
    
    def find_nearest_index_value(self, seq, val):
        a = np.asanyarray(seq)
        idx = find_nearest_index(a, val)
        fv = a.flat[idx]
        if val < fv:
            return (idx,a.flat[idx-1])
        else:
            return (idx,fv)


    def recv_packet(self, msg, stream_route, stream_id):
        if msg == {}:
            log.error('Received empty message from stream: %s', stream_id)
            return
        
        if not isinstance(msg, Granule):
            log.error('Received a message that is not a granule. %s' % msg)
            return
        
        master_stream = self.CFG.get_safe('process.master_stream', "")
        rdt = RecordDictionaryTool.load_from_granule(msg)
        
        if master_stream != stream_id:
            try:
                self.storage[stream_id].append(rdt)
            except KeyError:
                self.storage[stream_id] = []
                self.storage[stream_id].append(rdt)
        
        if master_stream == stream_id:
            output_streams = self.CFG.get_safe('process.publish_streams', {})
            for stream_out_id,stream_out_id in output_streams.iteritems(): 
                stream_def = self._read_stream_def(stream_out_id)
                pdict_dump = stream_def.parameter_dictionary
                pdict = ParameterDictionary.load(pdict_dump)
                result = RecordDictionaryTool(pdict)
                print >> sys.stderr, "result", result 
                print >> sys.stderr, pdict.temporal_parameter_name
                #print >> sys.stderr, rdt[pdict.temporal_parameter_name]
                
                closest_values = []
                for stream_id,rdts in self.storage.iteritems():
                    for srdt in rdts:
                        stream_def = self._read_stream_def(stream_id)
                        pdict_dump = stream_def.parameter_dictionary
                        pdict_s = ParameterDictionary.load(pdict_dump)
                        for time_val in rdt[pdict.temporal_parameter_name]:
                            print >> sys.stderr, "time_val", time_val
                            print >> sys.stderr, "find_nearest_index_value", time_val, srdt[pdict_s.temporal_parameter_name]
                            (idx,val) = self.find_nearest_index_value(time_val, srdt[pdict_s.temporal_parameter_name])   
                            closest_values.append((idx,val))

                print >> sys.stderr, "closest_values", closest_values 
             

    
    def publish(self, msg, stream_out_id):
        publisher = getattr(self, stream_out_id)
        rdt_out = self._align_and_create_rdt(stream_out_id)
        publisher.publish(rdt_out.to_granule())

class OutputStreamDefError(Exception):
    pass 
