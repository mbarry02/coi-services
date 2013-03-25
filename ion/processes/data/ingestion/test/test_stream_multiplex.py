import gevent
import numpy as np
from ion.services.dm.utility.granule.record_dictionary import RecordDictionaryTool
from pyon.util.int_test import IonIntegrationTestCase
from nose.plugins.attrib import attr
from interface.services.dm.idataset_management_service import DatasetManagementServiceClient
from interface.services.dm.ipubsub_management_service import PubsubManagementServiceClient
from interface.services.sa.idata_product_management_service import DataProductManagementServiceClient
from interface.services.sa.idata_process_management_service import DataProcessManagementServiceClient
from pyon.ion.stream import StandaloneStreamPublisher,StandaloneStreamSubscriber
from coverage_model import ParameterContext, AxisTypeEnum, QuantityType
from ion.services.dm.utility.granule_utils import ParameterDictionary
from interface.objects import DataProduct
from ion.services.dm.utility.granule_utils import time_series_domain
from interface.objects import DataProcessDefinition, DataProcessTypeEnum
from coverage_model.utils import find_nearest_index
from pyon.public import PRED
import uuid
import sys

@attr('INT')
class TestStreamMultiplex(IonIntegrationTestCase):
    def setUp(self):
        self._start_container()
        self.container.start_rel_from_url('res/deploy/r2deploy.yml')
        self.dataset_management  = DatasetManagementServiceClient()
        self.pubsub_management  = PubsubManagementServiceClient()
        self.data_product_management = DataProductManagementServiceClient()
        self.data_process_management = DataProcessManagementServiceClient()
        self.resource_registry = self.container.resource_registry
        self.max_context = 100 
        self.validators = 0


    def _launch_multiplex_process(self, input_pdict_ids, output_pdict_id, master_pdict_id):
        input_streams = {}
        master_stream_id = ''
        for i,pdict_id in enumerate(input_pdict_ids):
            record = self._create_stream(i+1, pdict_id)
            input_streams[pdict_id] = record
            if pdict_id == master_pdict_id:
                master_stream_id = input_streams[pdict_id]['stream_id']
        
        exchange_pts = [row['exchange_pt'] for pdict_id,row in input_streams.iteritems()]
        input_stream_ids = [row['stream_id'] for pdict_id,row in input_streams.iteritems()]
        output_stream = self._create_stream(0, output_pdict_id)
        output_stream_id = output_stream['stream_id']
        config = {'queue_name':exchange_pts, 'input_streams':input_stream_ids, 'master_stream':master_stream_id, 'publish_streams':{str(output_stream_id):output_stream_id}, 'storage_depth':10, 'process_type':'stream_process'}
        pid = self.container.spawn_process('StreamMultiplex', 'ion.processes.data.ingestion.stream_multiplex', 'StreamMultiplex', {'process':config}) 
        
        for pdict_id,row in input_streams.iteritems():
            self.container.proc_manager.procs[pid].subscribers[row['exchange_pt']].xn.bind(row['route'].routing_key, row['publisher'].xp)
        
        return (pid, input_streams, output_stream)
    
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
    
    def make_data_product(self, i, pdict_id, dp_name, available_fields=[]):
        exchange_pt = 'xp%s'%i
        stream_def_id = self.pubsub_management.create_stream_definition('%s stream_def' % dp_name, parameter_dictionary_id=pdict_id, available_fields=available_fields or None)
        self.addCleanup(self.pubsub_management.delete_stream_definition, stream_def_id)
        tdom, sdom = time_series_domain()
        tdom = tdom.dump()
        sdom = sdom.dump()
        dp_obj = DataProduct(name=dp_name)
        dp_obj.temporal_domain = tdom
        dp_obj.spatial_domain = sdom
        data_product_id = self.data_product_management.create_data_product(dp_obj, stream_definition_id=stream_def_id)
        self.addCleanup(self.data_product_management.delete_data_product, data_product_id)
        
        stream_ids, _ = self.resource_registry.find_objects(subject=data_product_id, predicate=PRED.hasStream, id_only=True)
        self.assertTrue(len(stream_ids))
        stream_id = stream_ids.pop()
        route = self.pubsub_management.read_stream_route(stream_id)
        stream_definition = self.pubsub_management.read_stream_definition(stream_id=stream_id)
        stream_def_id = stream_definition._id
        publisher = StandaloneStreamPublisher(stream_id=stream_id, stream_route=route)
        pdict_dump = stream_definition.parameter_dictionary
        pdict = ParameterDictionary.load(pdict_dump)
        record = {'data_product_id':data_product_id, 'exchange_pt':exchange_pt, 'stream_def_id':stream_def_id, 'stream_id':stream_id, 'route':route, 'stream_def':stream_definition, 'publisher':publisher, 'pdict':pdict}
        return record

    def setup_subscriber(self, data_product_id, callback, stream, pid):
        stream_ids, _ = self.resource_registry.find_objects(subject=data_product_id, predicate=PRED.hasStream, id_only=True)
        self.assertTrue(len(stream_ids))
        stream_id = stream_ids.pop()
        
        sub_id = self.pubsub_management.create_subscription('validator_%s'%self.validators, stream_ids=[stream_id])
        self.addCleanup(self.pubsub_management.delete_subscription, sub_id)

        self.pubsub_management.activate_subscription(sub_id)
        self.addCleanup(self.pubsub_management.deactivate_subscription, sub_id)
        
        subscriber = StandaloneStreamSubscriber('validator_%s' % self.validators, callback=callback)
        subscriber.xn.bind(stream['route'].routing_key, getattr(self.container.proc_manager.procs[pid], stream['stream_id']).xp)
        subscriber.start()
        self.addCleanup(subscriber.stop)
        self.validators+=1
        
        return subscriber

    def _test_two_input_streams(self, data_size, data_size2):
        
        fields1 = ['TIME', 'CONDWAT_L0', 'TEMPWAT_L0']
        fields2 = ['TIME', 'LAT', 'LON']
        fields3 = ['TIME', 'CONDWAT_L0', 'TEMPWAT_L0', 'LAT', 'LON']
        
        pdict_id1 = self._get_pdict(fields1)
        pdict_id2 = self._get_pdict(fields2)
        pdict_id3 = self._get_pdict(fields3)

        input_stream_1 = self.make_data_product(1, pdict_id1, 'dp1', fields1)
        input_stream_2 = self.make_data_product(2, pdict_id2, 'dp2', fields2)
        output_stream = self.make_data_product(3, pdict_id3, 'dp3', fields3)
        
        
        input_stream_ids = [input_stream_1['stream_id'],input_stream_2['stream_id']] 
        exchange_pts = [input_stream_1['exchange_pt'], input_stream_2['exchange_pt'], output_stream['exchange_pt']]
        
        dpd = DataProcessDefinition(name='stream multiplex')
        dpd.data_process_type = DataProcessTypeEnum.TRANSFORM
        dpd.module = 'ion.processes.data.ingestion.stream_multiplex' 
        dpd.class_name = 'StreamMultiplex'
        
        data_process_definition_id = self.data_process_management.create_data_process_definition(dpd)
        self.addCleanup(self.data_process_management.delete_data_process_definition, data_process_definition_id)
        
        config = {'process':{'exchange_pts':exchange_pts, 'input_streams':input_stream_ids, 'master_stream':input_stream_1['stream_id'], 'publish_streams':{str(output_stream['stream_id']):output_stream['stream_id']}, 'storage_depth':10, 'process_type':'stream_process'}}
        data_process_id = self.data_process_management.create_data_process2(data_process_definition_id=data_process_definition_id, in_data_product_ids=[input_stream_1['data_product_id'],input_stream_2['data_product_id']], out_data_product_ids=[output_stream['data_product_id']], configuration=config)
        
        pids,assocs = self.resource_registry.find_objects(subject=data_process_id, predicate='hasProcess', id_only=True) 
        pid = pids[0]

        #print >> sys.stderr, "data process id", data_process_id
        #print >> sys.stderr, "pid", pid
        #print >> sys.stderr, "assocs", assocs

        self.container.proc_manager.procs[pid].subscribers[input_stream_1['exchange_pt']].xn.bind(input_stream_1['route'].routing_key, input_stream_1['publisher'].xp)
        self.container.proc_manager.procs[pid].subscribers[input_stream_2['exchange_pt']].xn.bind(input_stream_2['route'].routing_key, input_stream_2['publisher'].xp)
        
        self.addCleanup(self.data_process_management.delete_data_process2,data_process_id)

        rdt = RecordDictionaryTool(stream_definition_id=input_stream_1['stream_def_id'])
        ts = 10
        publish_time_rdt = np.arange(ts, ts+data_size)
        rdt['CONDWAT_L0'] = np.arange(ts, ts+data_size)
        ts = 20
        rdt2 = RecordDictionaryTool(stream_definition_id=input_stream_2['stream_def_id'])
        rdt2['LAT'] = np.arange(ts, ts+data_size)
        publish_time_rdt2 = np.arange(ts, ts+data_size2)
        
        validated = gevent.event.Event()
        def validation(msg, route, stream_id):
            #print >> sys.stderr, "validation"
            rdt_out = RecordDictionaryTool.load_from_granule(msg) 
            #print >> sys.stderr, "rdt_out", rdt_out
            indices = [find_nearest_index(publish_time_rdt2, time_val) for time_val in publish_time_rdt]
            self.assertTrue(np.array_equal(rdt_out['CONDWAT_L0'], rdt['CONDWAT_L0']))
            lat = np.asanyarray([rdt2['LAT'][idx] for idx in indices])
            self.assertTrue(np.array_equal(rdt_out['LAT'], lat))
            validated.set()
        
        self.data_process_management.activate_data_process2(data_process_id)
        self.addCleanup(self.data_process_management.deactivate_data_process2, data_process_id)
        
        self.setup_subscriber(output_stream['data_product_id'], validation, output_stream, pid)

        ts = 10
        rdt2['TIME'] = publish_time_rdt2
        input_stream_2['publisher'].publish(rdt2.to_granule())
        
        ts = 10
        rdt['TIME'] = publish_time_rdt
        input_stream_1['publisher'].publish(rdt.to_granule())
        
        self.assertTrue(validated.wait(4))
    
    def _test_two_input_streams_old(self, data_size, data_size2):
        pdict_id1 = self._get_pdict(['TIME', 'CONDWAT_L0', 'TEMPWAT_L0'])
        pdict_id2 = self._get_pdict(['TIME', 'LAT', 'LON'])
        pdict_id3 = self._get_pdict(['TIME', 'CONDWAT_L0', 'TEMPWAT_L0', 'LAT', 'LON'])
        
        
        pid,input_streams,output_stream = self._launch_multiplex_process([pdict_id1,pdict_id2], pdict_id3, pdict_id1)
        
        ts = 10
        publish_time_rdt = np.arange(ts, ts+data_size)
        
        ts = 10
        publish_time_rdt2 = np.arange(ts, ts+data_size2)
        
        ts = 20
        publish_time_2_rdt = np.arange(ts, ts+data_size) 
        
        rdt = self._make_rdt(input_streams[pdict_id1]['pdict'], publish_time_rdt)
        rdt2 = self._make_rdt(input_streams[pdict_id2]['pdict'], publish_time_rdt2)
        
        #validate multiplexed data
        e = gevent.event.Event()
        def cb(msg, sr, sid):
            self.assertEqual(sid, output_stream['stream_id'])
            
            rdt_out = RecordDictionaryTool.load_from_granule(msg)
            indices = self.container.proc_manager.procs[pid]._align_temporal_values(publish_time_2_rdt, publish_time_rdt2)
            
            self.assertTrue(np.array_equal(rdt_out['CONDWAT_L0'], rdt['CONDWAT_L0']))

            lat = np.asanyarray([rdt2['LAT'][idx] for idx in indices])
            self.assertTrue(np.array_equal(rdt_out['LAT'], lat))
            e.set()
        
        sub = StandaloneStreamSubscriber('stream_subscriber', cb)
        sub.xn.bind(output_stream['route'].routing_key, getattr(self.container.proc_manager.procs[pid], output_stream['stream_id']).xp)
        self.addCleanup(sub.stop)
        sub.start()
        
        ts = 10
        rdt['TIME'] = publish_time_rdt
        input_streams[pdict_id1]['publisher'].publish(rdt.to_granule())
        
        ts = 10
        rdt2['TIME'] = publish_time_rdt2
        input_streams[pdict_id2]['publisher'].publish(rdt2.to_granule())
        
        ts = 20
        rdt['TIME'] = publish_time_2_rdt
        input_streams[pdict_id1]['publisher'].publish(rdt.to_granule())
        
        self.assertTrue(e.wait(4))
        self.addCleanup(self.container.proc_manager.terminate_process, pid)
    
    #def test_two_input_streams_size1(self):
    #    self._test_two_input_streams(1, 1)

    #def test_two_input_streams_size10(self):
    #    self._test_two_input_streams(10, 10)
    
    def test_two_input_streams_ireg1(self):
        self._test_two_input_streams(3, 10)
    
    #def test_two_input_streams_ireg2(self):
    #    self._test_two_input_streams(10, 2)
    
    def _test_three_input_streams_old(self, data_size, data_size2, data_size3):
        pdict_id1 = self._get_pdict(['TIME', 'CONDWAT_L0'])
        pdict_id2 = self._get_pdict(['TIME', 'TEMPWAT_L0'])
        pdict_id3 = self._get_pdict(['TIME', 'LAT', 'LON'])
        pdict_id4 = self._get_pdict(['TIME', 'CONDWAT_L0', 'TEMPWAT_L0', 'LAT', 'LON'])
        
        pid,input_streams,output_stream = self._launch_multiplex_process([pdict_id1,pdict_id2,pdict_id3], pdict_id4, pdict_id3)
        
        ts = 10
        publish_time_rdt = np.arange(ts, ts+data_size)
        
        ts = 10
        publish_time_rdt2 = np.arange(ts, ts+data_size2)
        
        ts = 20
        publish_time_rdt3 = np.arange(ts, ts+data_size3) 
        
        rdt = self._make_rdt(input_streams[pdict_id1]['pdict'], publish_time_rdt)
        rdt2 = self._make_rdt(input_streams[pdict_id2]['pdict'], publish_time_rdt2)
        rdt3 = self._make_rdt(input_streams[pdict_id3]['pdict'], publish_time_rdt3)

        #validate multiplexed data
        e = gevent.event.Event()
        def cb(msg, sr, sid):
            self.assertEqual(sid, output_stream['stream_id'])
            
            rdt_out = RecordDictionaryTool.load_from_granule(msg)
            self.assertTrue(np.array_equal(rdt_out['LAT'], rdt3['LAT']))

            indices = self.container.proc_manager.procs[pid]._align_temporal_values(publish_time_rdt3, publish_time_rdt)
            condwat = np.asanyarray([rdt['CONDWAT_L0'][idx] for idx in indices])
            self.assertTrue(np.array_equal(rdt_out['CONDWAT_L0'], condwat))
            
            indices = self.container.proc_manager.procs[pid]._align_temporal_values(publish_time_rdt3, publish_time_rdt2)
            tempwat = np.asanyarray([rdt2['TEMPWAT_L0'][idx] for idx in indices])
            self.assertTrue(np.array_equal(rdt_out['TEMPWAT_L0'], tempwat))

            e.set()
        
        sub = StandaloneStreamSubscriber('stream_subscriber', cb)
        sub.xn.bind(output_stream['route'].routing_key, getattr(self.container.proc_manager.procs[pid], output_stream['stream_id']).xp)
        self.addCleanup(sub.stop)
        sub.start()

        ts = 10
        rdt['TIME'] = publish_time_rdt
        input_streams[pdict_id1]['publisher'].publish(rdt.to_granule())
        
        ts = 20
        rdt2['TIME'] = publish_time_rdt2
        input_streams[pdict_id2]['publisher'].publish(rdt2.to_granule())
        
        ts = 30
        rdt3['TIME'] = publish_time_rdt3
        input_streams[pdict_id3]['publisher'].publish(rdt3.to_granule())
        
        self.assertTrue(e.wait(4))
        self.addCleanup(self.container.proc_manager.terminate_process, pid)
    
    #def test_three_input_streams_size1(self):
    #    self._test_three_input_streams(1, 1, 1)
    
    #def test_three_input_streams_size10(self):
    #    self._test_three_input_streams(10, 10, 10)
    
    #def test_three_input_streams_ireg1(self):
    #    self._test_three_input_streams(1, 2, 3)
    
    #def test_three_input_streams_ireg2(self):
    #    self._test_three_input_streams(1, 3, 2)
    
    #def test_three_input_streams_ireg3(self):
    #    self._test_three_input_streams(2, 1, 3)
    
    #def test_three_input_streams_ireg4(self):
    #    self._test_three_input_streams(2, 3, 1)
    
    #def test_three_input_streams_ireg5(self):
    #    self._test_three_input_streams(3, 2, 1)
    
    #def test_three_input_streams_ireg6(self):
    #    self._test_three_input_streams(3, 1, 2)
    
    def _get_pdict(self, filter_values):
        t_ctxt = ParameterContext('TIME', param_type=QuantityType(value_encoding=np.dtype('int64')))
        t_ctxt.uom = 'seconds since 01-01-1900'
        t_ctxt.fill_value = -9999
        t_ctxt_id = self.dataset_management.create_parameter_context(name='TIME', parameter_context=t_ctxt.dump(), parameter_type='quantity<int64>')

        lat_ctxt = ParameterContext('LAT', param_type=QuantityType(value_encoding=np.dtype('float32')))
        lat_ctxt.axis = AxisTypeEnum.LAT
        lat_ctxt.uom = 'degree_north'
        lat_ctxt.fill_value = -9999
        lat_ctxt_id = self.dataset_management.create_parameter_context(name='LAT', parameter_context=lat_ctxt.dump(), parameter_type='quantity<float32>')

        lon_ctxt = ParameterContext('LON', param_type=QuantityType(value_encoding=np.dtype('float32')))
        lon_ctxt.axis = AxisTypeEnum.LON
        lon_ctxt.uom = 'degree_east'
        lon_ctxt.fill_value = -9999
        lon_ctxt_id = self.dataset_management.create_parameter_context(name='LON', parameter_context=lon_ctxt.dump(), parameter_type='quantity<float32>')

        temp_ctxt = ParameterContext('TEMPWAT_L0', param_type=QuantityType(value_encoding=np.dtype('float32')))
        temp_ctxt.uom = 'deg_C'
        temp_ctxt.fill_value = -9999
        temp_ctxt_id = self.dataset_management.create_parameter_context(name='TEMPWAT_L0', parameter_context=temp_ctxt.dump(), parameter_type='quantity<float32>')

        cond_ctxt = ParameterContext('CONDWAT_L0', param_type=QuantityType(value_encoding=np.dtype('float32')))
        cond_ctxt.uom = 'S m-1'
        cond_ctxt.fill_value = -9999
        cond_ctxt_id = self.dataset_management.create_parameter_context(name='CONDWAT_L0', parameter_context=cond_ctxt.dump(), parameter_type='quantity<float32>')

        press_ctxt = ParameterContext('PRESWAT_L0', param_type=QuantityType(value_encoding=np.dtype('float32')))
        press_ctxt.uom = 'dbar'
        press_ctxt.fill_value = -9999
        press_ctxt_id = self.dataset_management.create_parameter_context(name='PRESWAT_L0', parameter_context=press_ctxt.dump(), parameter_type='quantity<float32>')
        
        ids = [t_ctxt_id, lat_ctxt_id, lon_ctxt_id, temp_ctxt_id, cond_ctxt_id, press_ctxt_id]
        ids = ids
        contexts = [t_ctxt, lat_ctxt, lon_ctxt, temp_ctxt, cond_ctxt, press_ctxt]
        contexts = contexts
        context_ids = [ids[i] for i,ctxt in enumerate(contexts) if ctxt.name in filter_values]
        pdict_name = str(uuid.uuid4()) 
        return self.dataset_management.create_parameter_dictionary(pdict_name, parameter_context_ids=context_ids, temporal_context='TIME')

