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
    
    def test_recv_packet(self):
        exchange_pt1 = 'xp1'
        exchange_pt2 = 'xp2'
        exchange_pt3 = 'xp3'

        pdict_id1 = self._get_pdict(['TIME', 'CONDWAT_L0', 'TEMPWAT_L0'])
        pdict_id2 = self._get_pdict(['TIME', 'LAT', 'LON'])
        pdict_id3 = self._get_pdict(['TIME', 'CONDWAT_L0', 'TEMPWAT_L0', 'LAT', 'LON'])
        
        stream_def_id1 = self.pubsub_management.create_stream_definition('std_1', parameter_dictionary_id=pdict_id1)
        stream_def_id2 = self.pubsub_management.create_stream_definition('std_2', parameter_dictionary_id=pdict_id2)
        stream_def_id3 = self.pubsub_management.create_stream_definition('std_3', parameter_dictionary_id=pdict_id3)
        
        stream_id1, route1 = self.pubsub_management.create_stream('instrument_stream_1', exchange_pt1, stream_definition_id=stream_def_id1)
        stream_id2, route2 = self.pubsub_management.create_stream('instrument_stream_2', exchange_pt2, stream_definition_id=stream_def_id2)
        stream_id3, route3 = self.pubsub_management.create_stream('output_stream', exchange_pt3, stream_definition_id=stream_def_id3)
        
        stream_def_1 = self.pubsub_management.read_stream_definition(stream_def_id1)
        stream_def_2 = self.pubsub_management.read_stream_definition(stream_def_id2)
        stream_def_3 = self.pubsub_management.read_stream_definition(stream_def_id3)

        pdict1_dump = stream_def_1.parameter_dictionary
        pdict2_dump = stream_def_2.parameter_dictionary
        pdict3_dump = stream_def_3.parameter_dictionary
         
        pdict1 = ParameterDictionary.load(pdict1_dump)
        pdict2 = ParameterDictionary.load(pdict2_dump)
        pdict3 = ParameterDictionary.load(pdict3_dump)

        publisher1 = StandaloneStreamPublisher(stream_id=stream_id1, stream_route=route1)
        publisher2 = StandaloneStreamPublisher(stream_id=stream_id2, stream_route=route2)
        
        config = {'queue_name':[exchange_pt1,exchange_pt2], 'input_streams':[stream_id1,stream_id2], 'publish_streams':{str(stream_id3):stream_id3}, 'process_type':'stream_process'}
        pid = self.container.spawn_process('StreamMultiplex', 'ion.processes.data.ingestion.stream_multiplex', 'StreamMultiplex', {'process':config}) 
        
        self.container.proc_manager.procs[pid].subscribers[exchange_pt1].xn.bind(route1.routing_key, publisher1.xp)
        self.container.proc_manager.procs[pid].subscribers[exchange_pt2].xn.bind(route2.routing_key, publisher2.xp)
        
        #validate multiplexed data
        e = gevent.event.Event()
        def cb(msg, sr, sid):
            self.assertEqual(sid, stream_id3)
            rdt3 = RecordDictionaryTool(pdict3)
            rdt_out = RecordDictionaryTool.load_from_granule(msg)
            self.assertEqual(set(rdt3.keys()), set(rdt_out.keys()))
            e.set()

        sub = StandaloneStreamSubscriber('stream_subscriber', cb)
        sub.xn.bind(route3.routing_key, getattr(self.container.proc_manager.procs[pid], stream_id3).xp)
        self.addCleanup(sub.stop)
        sub.start()
        
        dt = 10
        rdt = RecordDictionaryTool(pdict1)
        for i in range(5):
            now = time.time()
            rdt['TIME'] = np.arange(now-dt, now)
            rdt['TEMPWAT_L0'] = np.array([45]*dt)
            rdt['CONDWAT_L0'] = np.array(np.sin(np.arange(dt) * 2 * np.pi / 60))
            publisher1.publish(rdt.to_granule())
        
        rdt2 = RecordDictionaryTool(pdict2)
        dt = 1
        for i in range(1):
            now = time.time()
            rdt2['TIME'] = np.arange(now, now+dt, 30)
            rdt2['LAT'] = np.arange(now, now+dt)
            rdt2['LON'] = np.arange(now, now+dt)
            publisher2.publish(rdt2.to_granule())
        
        self.container.proc_manager.terminate_process(pid)

    def _get_pdict(self, filter_values):
        t_ctxt = ParameterContext('TIME', param_type=QuantityType(value_encoding=np.dtype('int64')))
        t_ctxt.uom = 'seconds since 01-01-1900'
        t_ctxt.fill_value = -9999
        t_ctxt_id = self.dataset_management.create_parameter_context(name='TIME', parameter_context=t_ctxt.dump(), parameter_type='quantity<int64>', unit_of_measure=t_ctxt.uom)

        lat_ctxt = ParameterContext('LAT', param_type=ConstantType(QuantityType(value_encoding=np.dtype('float32'))))
        lat_ctxt.axis = AxisTypeEnum.LAT
        lat_ctxt.uom = 'degree_north'
        lat_ctxt.fill_value = -9999
        lat_ctxt_id = self.dataset_management.create_parameter_context(name='LAT', parameter_context=lat_ctxt.dump(), parameter_type='quantity<float32>', unit_of_measure=lat_ctxt.uom)

        lon_ctxt = ParameterContext('LON', param_type=ConstantType(QuantityType(value_encoding=np.dtype('float32'))))
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
