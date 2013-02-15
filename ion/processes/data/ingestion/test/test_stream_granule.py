#from mock import Mock
#from pyon.util.unit_test import IonUnitTestCase
from pyon.util.int_test import IonIntegrationTestCase
from nose.plugins.attrib import attr
from interface.objects import Granule
from interface.services.dm.ipubsub_management_service import PubsubManagementServiceClient
from interface.services.dm.idataset_management_service import DatasetManagementServiceClient
from ion.services.dm.utility.granule_utils import time_series_domain
from pyon.ion.stream import StreamPublisher, StreamSubscriber
from pyon.ion.process import SimpleProcess
import gevent


@attr('INT')
class StreamGranuleReaderWriterIntegrationTest(IonIntegrationTestCase):
    
    def setUp(self):
        self._start_container()
        self.container.start_rel_from_url('res/deploy/r2deploy.yml')
        self.pubsub_management    = PubsubManagementServiceClient(node=self.container.node)
        self.dataset_management   = DatasetManagementServiceClient(node=self.container.node)
   
    def _setup_stream(self, stream_name, exchange_name):
        pdict_id = self.dataset_management.read_parameter_dictionary_by_name('granule_params', id_only=True)
        stream_def_id = self.pubsub_management.create_stream_definition('std', parameter_dictionary_id=pdict_id)
        stream_id, route = self.pubsub_management.create_stream(stream_name, exchange_name, stream_definition_id=stream_def_id)
        stream_route = self.pubsub_management.read_stream_route(stream_id)
        tdom, sdom = time_series_domain()
        self.dataset_management.create_dataset('instrument_dataset', stream_id=stream_id, parameter_dictionary_id=pdict_id, spatial_domain=sdom.dump(), temporal_domain=tdom.dump())
        return (stream_route, stream_id)
    
    @attr('LOCOINT')
    @unittest.skipIf(os.getenv('CEI_LAUNCH_TEST', False), 'Host requires file-system access to coverage files, CEI mode does not support.')
    def test_recv_granule(self):
        stream_name = 'instrument_stream'
        exchange_name = 'xp1'
        spid = self.container.spawn_process('stream_subscriber','ion.processes.data.ingestion.stream_granule','StreamGranuleReader',{'process':{'queue_name':exchange_name}})
        (stream_route, stream_id) = self._setup_stream(stream_name, exchange_name)
        
        pubproc = SimpleProcess()
        pubproc.id = "some_id"
        pubproc.container = self.container
        publisher = StreamPublisher(process=pubproc, stream_id=stream_id, stream_route=stream_route)
        self.container.proc_manager.procs[spid].subscriber.xn.bind(stream_route.routing_key, publisher.xp)
       
        gmeta = {'something':'ok'}
        g = Granule()
        publisher.publish((gmeta, g))
        gmeta = {'something':'okagain'}
        g = Granule()
        publisher.publish((gmeta, g))
        
        gevent.sleep(3)

        cov = self.container.proc_manager.procs[spid].sc.get_coverage(stream_id)
        gdicts = cov.get_parameter_values('granule')
        self.assertEqual(len(gdicts), 2)

    @attr('LOCOINT')
    @unittest.skipIf(os.getenv('CEI_LAUNCH_TEST', False), 'Host requires file-system access to coverage files, CEI mode does not support.')
    def test_write_granule(self):
        stream_name = 'instrument_stream'
        exchange_name = 'xp1'
        
        #create stream
        (stream_route, stream_id) = self._setup_stream(stream_name, exchange_name)
        
        #launch writer
        ppid = self.container.spawn_process('stream_publisher','ion.processes.data.ingestion.stream_granule','StreaGranuleWriter',{'process':{'stream_id':stream_id}})
        
        #populate coverage
        cov = self.container.proc_manager.procs[ppid].get_coverage(stream_id)
        timesteps = 10
        cov.insert_timesteps(timesteps)
        cov.set_parameter_values('granule', [Granule().__dict__] * timesteps)
        cov.set_parameter_values('granule_meta', [{'something':'ok'}] * timesteps)
        cov.flush() 

        e = gevent.event.Event()
        def cb(msg, sr, sid):
            self.assertEqual(sid, stream_id)
            self.assertTrue(isinstance(msg[1], Granule))
            e.set()
        
        subproc = SimpleProcess()
        subproc.id = 'some_sub_id'
        subproc.container = self.container
        subscriber = StreamSubscriber(process=subproc, exchange_name=exchange_name, callback=cb)
        subscriber.xn.bind(stream_route.routing_key, self.container.proc_manager.procs[ppid].publisher.xp)
        subscriber.start()

        #write slice
        self.container.proc_manager.procs[ppid].write_granule(stream_id, slice(0, 1))

        self.assertTrue(e.wait(4))
        
        subscriber.stop()

