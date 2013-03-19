from ion.services.dm.utility.granule.record_dictionary import RecordDictionaryTool
from ion.core.process.transform import TransformMultiStreamListener
from interface.services.dm.ipubsub_management_service import PubsubManagementServiceProcessClient
from coverage_model import ParameterDictionary
from pyon.util.memoize import memoize_lru
from interface.objects import Granule
from pyon.public import log
from coverage_model.utils import find_nearest_index
import sys
import numpy as np

class StreamMultiplex(TransformMultiStreamListener):
    
    def __init__(self):
        TransformMultiStreamListener.__init__(self)
        self.storage = {}
        self.storage_len = 10

    def on_start(self):
        TransformMultiStreamListener.on_start(self)
        self.pubsub_management = PubsubManagementServiceProcessClient(process=self)
        
    @memoize_lru(maxsize=100)
    def _read_stream_def(self, stream_id):
        return self.pubsub_management.read_stream_definition(stream_id=stream_id)
    
    def recv_packet(self, msg, stream_route, stream_id):
        if msg == {}:
            log.error('Received empty message from stream: %s', stream_id)
            return
        
        if not isinstance(msg, Granule):
            log.error('Received a message that is not a granule. %s' % msg)
            return
        
        master_stream = self.CFG.get_safe('process.master_stream', "")
        rdt = RecordDictionaryTool.load_from_granule(msg)
        
        self.storage[stream_id] = rdt 
        
        input_streams = self.CFG.get_safe('process.input_streams', {})
        input_len = len(input_streams)  
        storage_key_len = len(self.storage.keys())
        
        #don't send until we have a series of complete data from all configured streams
        if input_len >= storage_key_len and master_stream == stream_id:
            output_streams = self.CFG.get_safe('process.publish_streams', {})
            for stream_out_id,stream_out_id in output_streams.iteritems(): 
                stream_def = self._read_stream_def(stream_out_id)
                pdict_dump = stream_def.parameter_dictionary
                pdict = ParameterDictionary.load(pdict_dump)
                result = RecordDictionaryTool(pdict)
                print >> sys.stderr, "start"
                #print >> sys.stderr, "result", result 
                #print >> sys.stderr, pdict.temporal_parameter_name
                #print >> sys.stderr, rdt[pdict.temporal_parameter_name]
                #build indices
                indices = {}
                for sid,srdt in self.storage.iteritems():
                    print >> sys.stderr, "stream", sid
                    stream_def = self._read_stream_def(sid)
                    pdict_dump = stream_def.parameter_dictionary
                    pdict_s = ParameterDictionary.load(pdict_dump)
                    for time_val in rdt[pdict.temporal_parameter_name]:
                        idx = find_nearest_index(srdt[pdict_s.temporal_parameter_name], time_val)   
                        try:
                            indices[sid].append(idx)
                        except KeyError:
                            indices[sid] = []
                            indices[sid].append(idx)
                
                print >> sys.stderr, "indices", indices
                
                #build result        
                temp = {}
                for sid,srdt in self.storage.iteritems():
                    stream_def = self._read_stream_def(sid)
                    pdict_dump = stream_def.parameter_dictionary
                    pdict_s = ParameterDictionary.load(pdict_dump)
                    for pname,vals in srdt.iteritems():
                        if pdict_s.temporal_parameter_name != pname:
                            print >> sys.stderr, sid, pname
                            temp[pname] = np.asanyarray([self.storage[sid][pname][idx] for idx in indices[sid]])
                
                result[pdict.temporal_parameter_name] = rdt[pdict.temporal_parameter_name]
                for pname,val in result.iteritems():
                    if pname != pdict.temporal_parameter_name:
                        result[pname] = temp[pname]
                    
                print >> sys.stderr, "result", result

    def publish(self, msg, stream_out_id):
        publisher = getattr(self, stream_out_id)
        rdt_out = self._align_and_create_rdt(stream_out_id)
        publisher.publish(rdt_out.to_granule())

class OutputStreamDefError(Exception):
    pass 
