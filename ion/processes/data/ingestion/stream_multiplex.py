from ion.services.dm.utility.granule.record_dictionary import RecordDictionaryTool
from ion.core.process.transform import TransformMultiStreamListener
from interface.services.dm.ipubsub_management_service import PubsubManagementServiceProcessClient
from coverage_model import ParameterDictionary,ParameterContext
from pyon.util.memoize import memoize_lru
from interface.objects import Granule
from pyon.public import log
from pyon.event.event import handle_stream_exception
from coverage_model.parameter_types import QuantityType
import numpy as np
import sys

class StreamMultiplex(TransformMultiStreamListener):
    
    def __init__(self):
        print >> sys.stderr, "init"
        TransformMultiStreamListener.__init__(self)
        self.granules = {}
        self.received = []
    
    def on_start(self):
        print >> sys.stderr, "on_start"
        TransformMultiStreamListener.on_start(self)
        self.pubsub_management = PubsubManagementServiceProcessClient(process=self)
    
    @memoize_lru(maxsize=100)
    def _read_stream_def(self, stream_id):
        return self.pubsub_management.read_stream_definition(stream_id=stream_id)
    
    def _get_temporal_key(self, stream_id):
        stream_def = self._read_stream_def(stream_id)
        pdict_dump = stream_def.parameter_dictionary
        pdict = ParameterDictionary.load(pdict_dump)
        return pdict.temporal_parameter_name
    
    def _find_temporal_avg_interval(self):
        result = []
        for stream_id,granule in self.granules.iteritems():
            tkey = self._get_temporal_key(stream_id)
            if tkey:
                rdt = RecordDictionaryTool.load_from_granule(granule)
                #interval between values [x1 - x0, x2 - x1, x3 - x2]
                intervals = [abs(rdt[tkey][i+1] - rdt[tkey][i]) for i,t in enumerate(rdt[tkey]) if i+1 < len(rdt[tkey])]
                result = result + intervals 
        avg = int(sum([interval/float(len(result)) for interval in result]))
        return avg

    def _find_temporal_avg_start(self):
        start_values = []
        for stream_id,granule in self.granules.iteritems():
            tkey = self._get_temporal_key(stream_id)
            if tkey:
                rdt = RecordDictionaryTool.load_from_granule(granule)
                start_values.append(rdt[tkey][0])
        return int(sum([v/float(len(start_values)) for v in start_values]))
    
    def _find_temporal_max_length(self):
        result = 0
        for stream_id,granule in self.granules.iteritems():
            tkey = self._get_temporal_key(stream_id)
            if tkey:
                rdt = RecordDictionaryTool.load_from_granule(granule)
                length  = len(rdt[tkey]) 
                if length >= result:
                    result = length
        return result

    def _get_temporal_values(self):
        start = self._find_temporal_avg_start()
        interval = self._find_temporal_avg_interval()
        length = self._find_temporal_max_length()
        if length > 0 and interval > 0:
            end = start + sum([interval] * length)
            return np.arange(start, end, interval)
        return np.array([])
    
    def _get_stream_input_output_diff(self, stream_out_id):
        input_pdict_lst = []
        for stream_id,granule in self.granules.iteritems():
            stream_def = self._read_stream_def(stream_id)
            pdict_dump = stream_def.parameter_dictionary
            pdict = ParameterDictionary.load(pdict_dump)
            pdict_lst = [name for name,v in pdict.iteritems() if name != pdict.temporal_parameter_name] 
            input_pdict_lst = input_pdict_lst + pdict_lst
        stream_def = self._read_stream_def(stream_out_id)
        pdict_dump = stream_def.parameter_dictionary
        pdict = ParameterDictionary.load(pdict_dump)
        output_pdict_lst = [name for name,v in pdict.iteritems() if name != pdict.temporal_parameter_name] 
        
        return set(input_pdict_lst) - set(output_pdict_lst)

    def _create_rdt(self, stream_out_id):
        print >> sys.stderr, "_create_rdt"
        diff = self._get_stream_input_output_diff(stream_out_id)
        if diff:
            raise OutputStreamDefError("output stream definition parameters are different than the combined input stream definition parameters %s" % diff)
        
        #set result
        stream_def = self._read_stream_def(stream_out_id)
        pdict_dump = stream_def.parameter_dictionary
        pdict = ParameterDictionary.load(pdict_dump)
        for stream_id,granule in self.granules.iteritems():
            stream_def = self._read_stream_def(stream_id)
            pdict_dump = stream_def.parameter_dictionary
            pdict_input = ParameterDictionary.load(pdict_dump)
            pnames = [name for name,v in pdict_input.iteritems() if name != pdict_input.temporal_parameter_name]
            pnames = pnames + ['delta_time'] 
            delta_key = '_'.join(pnames)
            pdict.add_context(ParameterContext(delta_key, param_type=QuantityType(value_encoding='l'), fill_value=-9999))
        
        result = RecordDictionaryTool(param_dictionary=pdict)
        if pdict.temporal_parameter_name is None:
            log.warning('%s output stream definition parameter dictionary does not define a temporal parameter', stream_out_id)
        #need to set temporal domain to set shape
        if pdict.temporal_parameter_name:
            #set one averaged time domain
            result[pdict.temporal_parameter_name] = self._get_temporal_values()
        print >> sys.stderr, "result rdt shape", result._shp, "dirty shape", result._dirty_shape  
        #put values from all input streams onto output stream
        for stream_id,granule in self.granules.iteritems():
            stream_def = self._read_stream_def(stream_id)
            pdict_dump = stream_def.parameter_dictionary
            pdict = ParameterDictionary.load(pdict_dump)
            rdt = RecordDictionaryTool.load_from_granule(granule)
            for field in rdt.fields:
                if field != pdict.temporal_parameter_name:
                    print >> sys.stderr, field
                    result[field] = rdt[field]

        if pdict.temporal_parameter_name:
            #add delta times to output granule
            for stream_id,granule in self.granules.iteritems():
                tkey = self._get_temporal_key(stream_id) 
                if tkey:
                    stream_def = self._read_stream_def(stream_id)
                    pdict_dump = stream_def.parameter_dictionary
                    pdict = ParameterDictionary.load(pdict_dump)
                    rdt = RecordDictionaryTool.load_from_granule(granule)
                    pnames = [name for name,v in pdict.iteritems() if name != pdict.temporal_parameter_name]
                    pnames = pnames + ['delta_time'] 
                    delta_key = '_'.join(pnames)
                    delta = [a-b for a,b in zip(rdt[tkey], result[pdict.temporal_parameter_name])]
                    for x in range(len(rdt[tkey]), len(result[pdict.temporal_parameter_name])):
                        delta.append(-9999)
                    result[delta_key]= np.array(delta)  
        return result

    @handle_stream_exception()
    def recv_packet(self, msg, stream_route, stream_id):
        if msg == {}:
            log.error('Received empty message from stream: %s', stream_id)
            return
        if not isinstance(msg, Granule):
            log.error('Received a message that is not a granule. %s' % msg)
            return
        
        if stream_id not in self.received:
            self.received.append(stream_id)
        
        #TODO:
        #keep track of frequencies for each stream_id
        #if frequency plus error offset is missed....then raise exception or send incomplete granule
        
        self.granules[stream_id] = msg
        
        output_streams = self.CFG.get_safe('process.publish_streams', {})
        for stream_out_id,stream_out_id in output_streams.iteritems():
            self.publish(msg, stream_out_id)
    
    def publish(self, msg, stream_out_id):
        publisher = getattr(self, stream_out_id)
        
        input_streams = self.CFG.get_safe('process.input_streams', [])
        if not self.received or not input_streams:
            return
        
        if set(self.received) == set(input_streams):
            rdt = self._create_rdt(stream_out_id)
            publisher.publish(rdt.to_granule())
            self.received = []

class OutputStreamDefError(Exception):
    pass 
