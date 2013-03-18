import time
import gevent
import numpy as np
from ion.services.dm.utility.granule.record_dictionary import RecordDictionaryTool
from pyon.util.int_test import IonIntegrationTestCase
from nose.plugins.attrib import attr
from interface.services.dm.idataset_management_service import DatasetManagementServiceClient
from interface.services.dm.ipubsub_management_service import PubsubManagementServiceClient
from pyon.ion.stream import StandaloneStreamPublisher,StandaloneStreamSubscriber
from coverage_model import ParameterContext, AxisTypeEnum, QuantityType
from ion.services.dm.utility.granule_utils import ParameterDictionary
import uuid

@attr('INT')
class TestStreamMultiplex(IonIntegrationTestCase):
    def setUp(self):
        self._start_container()
        self.container.start_rel_from_url('res/deploy/r2deploy.yml')
        self.dataset_management  = DatasetManagementServiceClient()
        self.pubsub_management  = PubsubManagementServiceClient()
        self.max_context = 100 
    
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

        config = {'queue_name':exchange_pts, 'input_streams':input_stream_ids, 'master_stream':input_stream_ids[0], 'publish_streams':{str(output_stream_id):output_stream_id}, 'process_type':'stream_process'}
        pid = self.container.spawn_process('StreamMultiplex', 'ion.processes.data.ingestion.stream_multiplex', 'StreamMultiplex', {'process':config}) 
        
        for pdict_id,row in input_streams.iteritems():
            self.container.proc_manager.procs[pid].subscribers[row['exchange_pt']].xn.bind(row['route'].routing_key, row['publisher'].xp)
        
        return (pid, input_streams, output_stream)
    
    def _make_rdt(self, pdict, now, size=1, factor=1):
        rdt = RecordDictionaryTool(pdict)
        for name,(n,pc) in pdict.iteritems():
            if pdict.temporal_parameter_name == name:
                rdt[name] = np.arange(now, now+size)
            else:
                rdt[name] = np.array(np.sin(np.arange(size) * 2 * np.pi / 60 * factor))
        return rdt 
    
    def _test_two_input_streams(self, data_size):
        pdict_id1 = self._get_pdict(['TIME', 'CONDWAT_L0', 'TEMPWAT_L0'])
        pdict_id2 = self._get_pdict(['TIME', 'LAT', 'LON'])
        pdict_id3 = self._get_pdict(['TIME', 'CONDWAT_L0', 'TEMPWAT_L0', 'LAT', 'LON'])
        
        pid,input_streams,output_stream = self._launch_multiplex_process([pdict_id1,pdict_id2], pdict_id3)
        

        #validate multiplexed data
        e = gevent.event.Event()
        def cb(msg, sr, sid):
            self.assertEqual(sid, output_stream['stream_id'])
            #rdt_out = RecordDictionaryTool.load_from_granule(msg)
            #self.assertTrue(np.array_equal(rdt_out['CONDWAT_L0'], rdt['CONDWAT_L0']))
            #self.assertTrue(np.array_equal(rdt_out['TEMPWAT_L0'], rdt['TEMPWAT_L0']))
            #self.assertTrue(np.array_equal(rdt_out['LAT'], rdt2['LAT']))
            #self.assertTrue(np.array_equal(rdt_out['LON'], rdt2['LON']))
            e.set()

        sub = StandaloneStreamSubscriber('stream_subscriber', cb)
        sub.xn.bind(output_stream['route'].routing_key, getattr(self.container.proc_manager.procs[pid], output_stream['stream_id']).xp)
        self.addCleanup(sub.stop)
        sub.start()

        for i in range(10):
            if i % 2 == 0:
                now = time.time()
                rdt2 = self._make_rdt(input_streams[pdict_id2]['pdict'], now, data_size)
                input_streams[pdict_id2]['publisher'].publish(rdt2.to_granule())
                gevent.sleep(.5)
            now = time.time()
            rdt = self._make_rdt(input_streams[pdict_id1]['pdict'], now, data_size, 5000)
            input_streams[pdict_id1]['publisher'].publish(rdt.to_granule())
            gevent.sleep(1)
        
        self.assertTrue(e.wait(4))
        self.addCleanup(self.container.proc_manager.terminate_process, pid);
        
    def test_two_input_streams_size1(self):
        self._test_two_input_streams(1)
    
    #def test_two_input_streams_size10(self):
    #    self._test_two_input_streams(10)
    
    #def test_two_input_streams_size10000(self):
    #    self._test_two_input_streams(10000)
    
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

        cond_ctxt = ParameterContext('CONDWAT_L0', param_type=QuantityType(value_encoding=np.dtype('float32')))
        cond_ctxt.uom = 'S m-1'
        cond_ctxt.fill_value = -9999
        cond_ctxt_id = self.dataset_management.create_parameter_context(name='CONDWAT_L0', parameter_context=cond_ctxt.dump(), parameter_type='quantity<float32>', unit_of_measure=cond_ctxt.uom)

        press_ctxt = ParameterContext('PRESWAT_L0', param_type=QuantityType(value_encoding=np.dtype('float32')))
        press_ctxt.uom = 'dbar'
        press_ctxt.fill_value = -9999
        press_ctxt_id = self.dataset_management.create_parameter_context(name='PRESWAT_L0', parameter_context=press_ctxt.dump(), parameter_type='quantity<float32>', unit_of_measure=press_ctxt.uom)
        
        ids = [t_ctxt_id, lat_ctxt_id, lon_ctxt_id, temp_ctxt_id, cond_ctxt_id, press_ctxt_id]
        ids = ids
        contexts = [t_ctxt, lat_ctxt, lon_ctxt, temp_ctxt, cond_ctxt, press_ctxt]
        contexts = contexts
        context_ids = [ids[i] for i,ctxt in enumerate(contexts) if ctxt.name in filter_values]
        pdict_name = str(uuid.uuid4()) 
        return self.dataset_management.create_parameter_dictionary(pdict_name, parameter_context_ids=context_ids, temporal_context='TIME')
