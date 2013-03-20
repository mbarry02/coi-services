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

    def on_start(self):
        TransformMultiStreamListener.on_start(self)
        self.pubsub_management = PubsubManagementServiceProcessClient(process=self)
        self.storage_depth = self.CFG.get_safe('process.storage_depth', 10)
        
    @memoize_lru(maxsize=100)
    def _read_stream_def(self, stream_id):
        return self.pubsub_management.read_stream_definition(stream_id=stream_id)
    
    def _check_out_of_order(self, stream_id, rdt):
        """
        checks if the the new rdt is out of order with the previous saved rdt
        @param stream_id incoming stream_id
        @param rdt the rdt associated with the stream_id
        @return True if out of order False if not out of order
        """
        stream_def = self._read_stream_def(stream_id)
        pdict_dump = stream_def.parameter_dictionary
        pdict = ParameterDictionary.load(pdict_dump)
        #if storage has not been filled in yet then not out of order
        try:
            previous = self.storage[stream_id][-1]
        except KeyError:
            return False
        
        #print >> sys.stderr, "previous", previous['TIME']
        #print >> sys.stderr, "current", rdt['TIME']
        previous_max = np.amax(np.asanyarray(previous[pdict.temporal_parameter_name]))
        current_min = np.amin(np.asanyarray(rdt[pdict.temporal_parameter_name]))
        if current_min > previous_max:
            return False
        return True

    def _build_indices(self, pdict, rdt):
        indices = {}
        for sid,srdts in self.storage.iteritems():
            #use most recent since it will be in order
            srdt = srdts[-1]
            #print >> sys.stderr, "stream", sid
            stream_def = self._read_stream_def(sid)
            pdict_dump = stream_def.parameter_dictionary
            pdict_s = ParameterDictionary.load(pdict_dump)
            for time_val in rdt[pdict.temporal_parameter_name]:
                #print >> sys.stderr, "time val", time_val, "time", srdt[pdict_s.temporal_parameter_name]
                idx = find_nearest_index(srdt[pdict_s.temporal_parameter_name], time_val)   
                try:
                    indices[sid].append(idx)
                except KeyError:
                    indices[sid] = []
                    indices[sid].append(idx)
        return indices
    
    def _build_result(self, pdict, rdt, indices):
        #build result        
        result = RecordDictionaryTool(pdict)
        temp = {}
        for sid,srdts in self.storage.iteritems():
            stream_def = self._read_stream_def(sid)
            pdict_dump = stream_def.parameter_dictionary
            pdict_s = ParameterDictionary.load(pdict_dump)
            srdt = srdts[-1]
            for pname,vals in srdt.iteritems():
                if pdict_s.temporal_parameter_name != pname:
                    temp[pname] = np.asanyarray([srdt[pname][idx] for idx in indices[sid]])
        
        result[pdict.temporal_parameter_name] = rdt[pdict.temporal_parameter_name]
        for pname in result.fields:
            if pname != pdict.temporal_parameter_name:
                result[pname] = temp[pname]
        return result

    def recv_packet(self, msg, stream_route, stream_id):
        if msg == {}:
            log.error('Received empty message from stream: %s', stream_id)
            return
        
        if not isinstance(msg, Granule):
            log.error('Received a message that is not a granule. %s' % msg)
            return

        self.queue.put((self._do_work,msg,stream_id))
    
    def _clear_storage(self):
        max_length = 0
        for stream_id,rdts in self.storage.iteritems():
            length = len(rdts)
            if length > max_length:
                max_length = length
        if max_length >= self.storage_depth:
            self.storage = {}

    def _do_work(self, msg, stream_id):
        master_stream = self.CFG.get_safe('process.master_stream', "")
        rdt = RecordDictionaryTool.load_from_granule(msg)
        
        print >> sys.stderr, "stream_id", stream_id
        self._clear_storage()

        ooo = self._check_out_of_order(stream_id, rdt)
        print >> sys.stderr, "out of order", ooo
        
        #reset if we received out of order granule
        if ooo == True:
            self.storage = {}
        elif ooo == False:
            try:
                self.storage[stream_id].append(rdt)
            except KeyError:
                self.storage[stream_id] = []
                self.storage[stream_id].append(rdt)
        
        input_streams = self.CFG.get_safe('process.input_streams', {})
        input_len = len(input_streams)  
        storage_key_len = len(self.storage.keys())
        print >> sys.stderr, "input_len", input_len
        print >> sys.stderr, "storage_key_len", storage_key_len
        
        #don't send until we have a series of data from all configured streams
        if input_len == storage_key_len and master_stream == stream_id:
            output_streams = self.CFG.get_safe('process.publish_streams', {})
            for stream_out_id,stream_out_id in output_streams.iteritems(): 
                stream_def = self._read_stream_def(stream_out_id)
                pdict_dump = stream_def.parameter_dictionary
                pdict = ParameterDictionary.load(pdict_dump)
                print >> sys.stderr, pdict, "master_stream_id == stream_id"
                #print >> sys.stderr, "result", result 
                #print >> sys.stderr, pdict.temporal_parameter_name
                #print >> sys.stderr, rdt[pdict.temporal_parameter_name]
                #build indices
                indices = self._build_indices(pdict, rdt) 
                print >> sys.stderr, "indices", indices
                result = self._build_result(pdict, rdt, indices) 
                for k,v in result.iteritems():
                    print >> sys.stderr, "result",k,result[k]
                 
                self.publish(result.to_granule(), stream_out_id)
    
    def publish(self, msg, stream_out_id):
        publisher = getattr(self, stream_out_id)
        publisher.publish(msg)

class OutputStreamDefError(Exception):
    pass 
