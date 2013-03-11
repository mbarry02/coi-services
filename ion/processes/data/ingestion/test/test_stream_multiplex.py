import time
import gevent
import numpy as np
from ion.services.dm.utility.granule.record_dictionary import RecordDictionaryTool
from pyon.util.int_test import IonIntegrationTestCase
from nose.plugins.attrib import attr
from interface.services.dm.idataset_management_service import DatasetManagementServiceClient
from interface.services.dm.ipubsub_management_service import PubsubManagementServiceClient
from pyon.ion.stream import StandaloneStreamPublisher
from coverage_model import ParameterContext, AxisTypeEnum, QuantityType, ConstantType, NumexprFunction, ParameterFunctionType, VariabilityEnum
from ion.services.dm.utility.granule_utils import ParameterDictionary
from ion.services.dm.inventory.dataset_management_service import DatasetManagementService
from pyon.public import RT, PRED

@attr('INT')
class TestStreamMultiplex(IonIntegrationTestCase):
    def setUp(self):
        self._start_container()
        self.container.start_rel_from_url('res/deploy/r2deploy.yml')
        self.dataset_management  = DatasetManagementServiceClient()
        self.pubsub_management  = PubsubManagementServiceClient()
        self.pdicts = {}
    

    def test_recv_packet(self):
        pdict_id1 = self._get_pdict(['TIME', 'LAT', 'LON', 'TEMPWAT_L0'])
        pdict_id2 = self._get_pdict(['TIME', 'LAT', 'LON', 'CONDWAT_L0'])
        stream_def_id1 = self.pubsub_management.create_stream_definition('std_1', parameter_dictionary_id=pdict_id1)
        stream_def_id2 = self.pubsub_management.create_stream_definition('std_2', parameter_dictionary_id=pdict_id2)
        
        stream_def_id1 = self.read_stream_definition(stream_def_id1)
        stream_def_id2 = self.read_stream_definition(stream_def_id2)
        
        pdict1_dump = stream_def_id1.parameter_dictionary
        pdict2_dump = stream_def_id2.parameter_dictionary

        pdict1 = ParameterDictionary.load(pdict1_dump)
        pdict2 = ParameterDictionary.load(pdict2_dump)

        stream1_id, stream1_route = self.pubsub_management.create_stream('stream1', exchange_point='xp1')
        stream2_id, stream2_route = self.pubsub_management.create_stream('stream2', exchange_point='xp2')
        
        publisher1 = StandaloneStreamPublisher(stream_id=stream1_id, stream_route=stream1_route)
        publisher2 = StandaloneStreamPublisher(stream_id=stream2_id, stream_route=stream2_route)

        pid = self.container.spawn_process('StreamMultiplex', 'ion.processes.data.ingestion.stream_multiplex', 'StreamMultiplex', {'process':{'input_streams':[], 'queue_name':['xp1', 'xp2']}}) 
        
        self.container.proc_manager.procs[pid].subscribers['xp1'].xn.bind(stream1_route.routing_key, publisher1.xp)
        self.container.proc_manager.procs[pid].subscribers['xp2'].xn.bind(stream2_route.routing_key, publisher2.xp)
        
        #import sys
        dt = 10
        rdt = RecordDictionaryTool(pdict1)
        #print >> sys.stderr, rdt
        rdt2 = RecordDictionaryTool(pdict2)
        #print >> sys.stderr, rdt2
        for i in range(5):
            now = time.time()
            rdt['TIME'] = np.arange(now-dt, now)
            rdt['TEMPWAT_L0'] = np.array([45]*dt)
            rdt2['TIME'] = np.arange(now, now+dt)
            rdt2['CONDWAT_L0'] = np.array(np.sin(np.arange(dt) * 2 * np.pi / 60))
            publisher1.publish(rdt.to_granule())
            gevent.sleep(1)
            publisher2.publish(rdt2.to_granule())
            publisher2.publish(rdt2.to_granule())
        gevent.sleep(1)
        self.container.proc_manager.terminate_process(pid)
    
    def read_stream_definition(self, stream_definition_id='', stream_id=''):
        retval = None
        if stream_id:
            sds, assocs = self.container.resource_registry.find_objects(subject=stream_id, predicate=PRED.hasStreamDefinition,id_only=False)
            retval = sds[0]
        stream_definition = retval or self.container.resource_registry.read(stream_definition_id)
        pdicts, _ = self.container.resource_registry.find_objects(subject=stream_definition._id, predicate=PRED.hasParameterDictionary, object_type=RT.ParameterDictionary, id_only=True)
        if len(pdicts):
            stream_definition.parameter_dictionary = DatasetManagementService.get_parameter_dictionary(pdicts[0]).dump()
        return stream_definition

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
            pdict_id = self.dataset_management.create_parameter_dictionary(pdict_name, parameter_context_ids=context_ids, temporal_context='time')
            self.pdicts[pdict_name] = pdict_id
            return pdict_id 
