import time
import gevent
import numpy as np
from ion.services.dm.utility.granule.record_dictionary import RecordDictionaryTool
from pyon.util.int_test import IonIntegrationTestCase
from nose.plugins.attrib import attr
from interface.services.dm.idataset_management_service import DatasetManagementServiceClient
from interface.services.dm.ipubsub_management_service import PubsubManagementServiceClient
from pyon.ion.stream import StandaloneStreamPublisher,StandaloneStreamSubscriber
from coverage_model import ParameterContext, AxisTypeEnum, QuantityType, ConstantType, NumexprFunction, ParameterFunctionType, VariabilityEnum
from ion.services.dm.utility.granule_utils import ParameterDictionary
import sys

@attr('INT')
class TestStreamMultiplex(IonIntegrationTestCase):
    def setUp(self):
        self._start_container()
        self.container.start_rel_from_url('res/deploy/r2deploy.yml')
        self.dataset_management  = DatasetManagementServiceClient()
        self.pubsub_management  = PubsubManagementServiceClient()
        self.pdicts = {}
   
    def _create_stream(self, i, pdict_id):
        exchange_pt = 'xp%s'%i
        stream_def_id = self.pubsub_management.create_stream_definition('std_%s'%i, parameter_dictionary_id=pdict_id)
        stream_id, route = self.pubsub_management.create_stream('stream_%s'%i, exchange_pt, stream_definition_id=stream_def_id)
        stream_def = self.pubsub_management.read_stream_definition(stream_def_id)
        publisher = StandaloneStreamPublisher(stream_id=stream_id, stream_route=route)
        pdict_dump = stream_def.parameter_dictionary
        pdict = ParameterDictionary.load(pdict_dump)
        record = {'exchange_pt':exchange_pt, 'stream_def_id':stream_def_id, 'stream_id':stream_id, 'route':route, 'stream_def':stream_def, 'publisher':publisher, 'pdict':pdict}
        return record

    def _launch_multiplex_process(self, input_pdict_ids, output_pdict_id):
        input_streams = {}
        for i,pdict_id in enumerate(input_pdict_ids):
            record = self._create_stream(i+1, pdict_id)
            input_streams[pdict_id] = record

        exchange_pts = [row['exchange_pt'] for pdict_id,row in input_streams.iteritems()]
        input_stream_ids = [row['stream_id'] for pdict_id,row in input_streams.iteritems()]
        output_stream = self._create_stream(0, output_pdict_id)
        output_stream_id = output_stream['stream_id']

        config = {'queue_name':exchange_pts, 'input_streams':input_stream_ids, 'publish_streams':{str(output_stream_id):output_stream_id}, 'process_type':'stream_process'}
        pid = self.container.spawn_process('StreamMultiplex', 'ion.processes.data.ingestion.stream_multiplex', 'StreamMultiplex', {'process':config}) 
        
        for pdict_id,row in input_streams.iteritems():
            self.container.proc_manager.procs[pid].subscribers[row['exchange_pt']].xn.bind(row['route'].routing_key, row['publisher'].xp)
        
        return (pid, input_streams, output_stream)

    def test_recv_packet(self):

        pdict_id1 = self._get_pdict(['TIME', 'CONDWAT_L0', 'TEMPWAT_L0'])
        pdict_id2 = self._get_pdict(['TIME', 'LAT', 'LON'])
        pdict_id3 = self._get_pdict(['TIME', 'CONDWAT_L0', 'TEMPWAT_L0', 'LAT', 'LON'])
        
        pid,input_streams,output_stream = self._launch_multiplex_process([pdict_id1,pdict_id2], pdict_id3)

        #validate multiplexed data
        e = gevent.event.Event()
        def cb(msg, sr, sid):
            self.assertEqual(sid, output_stream['stream_id'])
            rdt_out = RecordDictionaryTool.load_from_granule(msg)
            #print >> sys.stderr, rdt3
            #print >> sys.stderr, rdt_out
            for field in rdt_out.fields:
                print >> sys.stderr,field,rdt_out[field] 
            e.set()

        sub = StandaloneStreamSubscriber('stream_subscriber', cb)
        sub.xn.bind(output_stream['route'].routing_key, getattr(self.container.proc_manager.procs[pid], output_stream['stream_id']).xp)
        self.addCleanup(sub.stop)
        sub.start()
        
        dt = 10
        rdt2 = RecordDictionaryTool(input_streams[pdict_id2]['pdict'])
        rdt = RecordDictionaryTool(input_streams[pdict_id1]['pdict'])
        for i in range(10):
            now = time.time()
            
            if i % 2 == 0:
                rdt2['TIME'] = np.arange(now, now+1)
                rdt2['LAT'] = np.array([32])
                rdt2['LON'] = np.array([41])
                input_streams[pdict_id2]['publisher'].publish(rdt2.to_granule())
            
            rdt['TIME'] = np.arange(now-dt, now)
            rdt['TEMPWAT_L0'] = np.array([45]*dt)
            rdt['CONDWAT_L0'] = np.array(np.sin(np.arange(dt) * 2 * np.pi / 60))
            input_streams[pdict_id1]['publisher'].publish(rdt.to_granule())
        
        self.container.proc_manager.terminate_process(pid)

    def _get_pdict(self, filter_values):
        t_ctxt = ParameterContext('TIME', param_type=QuantityType(value_encoding=np.dtype('int64')))
        t_ctxt.uom = 'seconds since 01-01-1900'
        t_ctxt.fill_value = -9999
        t_ctxt_id = self.dataset_management.create_parameter_context(name='TIME', parameter_context=t_ctxt.dump(), parameter_type='quantity<int64>', unit_of_measure=t_ctxt.uom)

        lat_ctxt = ParameterContext('LAT', param_type=QuantityType(value_encoding=np.dtype('float32')))
        lat_ctxt.axis = AxisTypeEnum.LAT
        lat_ctxt.uom = 'degree_north'
        lat_ctxt.fill_value = -9999
        lat_ctxt_id = self.dataset_management.create_parameter_context(name='LAT', parameter_context=lat_ctxt.dump(), parameter_type='quantity<float32>', unit_of_measure=lat_ctxt.uom)

        lon_ctxt = ParameterContext('LON', param_type=QuantityType(value_encoding=np.dtype('float32')))
        lon_ctxt.axis = AxisTypeEnum.LON
        lon_ctxt.uom = 'degree_east'
        lon_ctxt.fill_value = -9999
        lon_ctxt_id = self.dataset_management.create_parameter_context(name='LON', parameter_context=lon_ctxt.dump(), parameter_type='quantity<float32>', unit_of_measure=lon_ctxt.uom)


        temp_ctxt = ParameterContext('TEMPWAT_L0', param_type=QuantityType(value_encoding=np.dtype('float32')))
        temp_ctxt.uom = 'deg_C'
        temp_ctxt.fill_value = -9999
        temp_ctxt_id = self.dataset_management.create_parameter_context(name='TEMPWAT_L0', parameter_context=temp_ctxt.dump(), parameter_type='quantity<float32>', unit_of_measure=temp_ctxt.uom)

        # Conductivity - values expected to be the decimal results of conversion from hex
        cond_ctxt = ParameterContext('CONDWAT_L0', param_type=QuantityType(value_encoding=np.dtype('float32')))
        cond_ctxt.uom = 'S m-1'
        cond_ctxt.fill_value = -9999
        cond_ctxt_id = self.dataset_management.create_parameter_context(name='CONDWAT_L0', parameter_context=cond_ctxt.dump(), parameter_type='quantity<float32>', unit_of_measure=cond_ctxt.uom)

        # Pressure - values expected to be the decimal results of conversion from hex
        press_ctxt = ParameterContext('PRESWAT_L0', param_type=QuantityType(value_encoding=np.dtype('float32')))
        press_ctxt.uom = 'dbar'
        press_ctxt.fill_value = -9999
        press_ctxt_id = self.dataset_management.create_parameter_context(name='PRESWAT_L0', parameter_context=press_ctxt.dump(), parameter_type='quantity<float32>', unit_of_measure=press_ctxt.uom)

        # TEMPWAT_L1 = (TEMPWAT_L0 / 10000) - 10
        tl1_func = '(TEMPWAT_L0 / 10000) - 10'
        tl1_pmap = {'TEMPWAT_L0':'TEMPWAT_L0'}
        func = NumexprFunction('TEMPWAT_L1', tl1_func, tl1_pmap)
        tempL1_ctxt = ParameterContext('TEMPWAT_L1', param_type=ParameterFunctionType(function=func), variability=VariabilityEnum.TEMPORAL)
        tempL1_ctxt.uom = 'deg_C'
        tempL1_ctxt_id = self.dataset_management.create_parameter_context(name=tempL1_ctxt.name, parameter_context=tempL1_ctxt.dump(), parameter_type='pfunc', unit_of_measure=tempL1_ctxt.uom)
        #import sys
        ids = [t_ctxt_id, lat_ctxt_id, lon_ctxt_id, temp_ctxt_id, cond_ctxt_id, press_ctxt_id, tempL1_ctxt_id]
        contexts = [t_ctxt, lat_ctxt, lon_ctxt, temp_ctxt, cond_ctxt, press_ctxt, tempL1_ctxt]
        context_ids = [ids[i] for i,ctxt in enumerate(contexts) if ctxt.name in filter_values]
        pdict_name = '_'.join([ctxt.name for ctxt in contexts if ctxt.name in filter_values])
        
        #print >> sys.stderr, filter_values
        #print >> sys.stderr, ids
        #print >> sys.stderr, context_ids
        #print >> sys.stderr, pdict_name
        try:
            self.pdicts[pdict_name]
            return self.pdicts[pdict_name]
        except KeyError:
            pdict_id = self.dataset_management.create_parameter_dictionary(pdict_name, parameter_context_ids=context_ids, temporal_context='TIME')
            self.pdicts[pdict_name] = pdict_id
            return pdict_id 
